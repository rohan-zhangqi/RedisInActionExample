import binascii
from collections import defaultdict
from datetime import date
from decimal import Decimal
import functools
import json
from queue import Empty, Queue
import threading
import time
import unittest
import uuid

import redis


CONFIGS = {}
CHECKED = {}


def get_config(conn, type, component, wait=1):
    key = 'config:%s:%s' % (type, component)

    t = CHECKED.get(key)
    if (not t) or t < time.time() - wait:
        CHECKED[key] = time.time()
        config = json.loads(conn.get(key) or '{}')
        config = dict((str(k), config[k]) for k in config)
        old_config = CONFIGS.get(key)

        if config != old_config:
            CONFIGS[key] = config

    return CONFIGS.get(key)


REDIS_CONNECTIONS = {}
config_connection = None


def redis_connection(component, wait=1):
    key = 'config:redis:' + component

    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            _config = get_config(
                config_connection, 'redis', component, wait)

            config = _config

            if config != old_config:
                REDIS_CONNECTIONS[key] = redis.Redis(**config)

            return function(
                REDIS_CONNECTIONS.get(key), *args, **kwargs)
        return call
    return wrapper


def index_document(conn, docid, words, scores):
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, docid)
    pipeline.hmset('kb:doc:%s' % docid, scores)
    return len(pipeline.execute())


def parse_and_search(conn, query, ttl):
    id = str(uuid.uuid4())
    conn.sinterstore(
        'idx:' + id,
        ['idx:'+key for key in query])
    conn.expire('idx:' + id, ttl)
    return id


def search_and_sort(conn, query, id=None, ttl=300, sort="-updated",
                    start=0, num=20):
    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    by = "kb:doc:*->" + sort
    alpha = sort not in ('updated', 'id', 'created')

    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

    pipeline = conn.pipeline(True)
    pipeline.scard('idx:' + id)
    pipeline.sort(
        'idx:' + id, by=by, alpha=alpha,
        desc=desc, start=start, num=num)
    results = pipeline.execute()

    return results[0], results[1], id


def zintersect(conn, keys, ttl):
    id = str(uuid.uuid4())
    conn.zinterstore('idx:' + id,
        dict(('idx:'+k, v) for k,v in keys.items()))
    conn.expire('idx:' + id, ttl)
    return id


def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0,
                    start=0, num=20, desc=True):

    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

        scored_search = {
            id: 0,
            'sort:update': update,
            'sort:votes': vote
        }
        id = zintersect(conn, scored_search, ttl)

    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:' + id)
    if desc:
        pipeline.zrevrange('idx:' + id, start, start + num - 1)
    else:
        pipeline.zrange('idx:' + id, start, start + num - 1)
    results = pipeline.execute()

    return results[0], results[1], id


def execute_later(conn, queue, name, args):
    t = threading.Thread(target=globals()[name], args=tuple(args))
    t.setDaemon(1)
    t.start()


HOME_TIMELINE_SIZE = 1000
POSTS_PER_PASS = 1000


def shard_key(base, key, total_elements, shard_size):
    if isinstance(key, int) or key.isdigit():
        shard_id = int(str(key), 10) // shard_size
    else:
        if isinstance(key, str):
            key = key.encode('latin-1')
        shards = 2 * total_elements // shard_size
        shard_id = binascii.crc32(key) % shards
    return "%s:%s" % (base, shard_id)


def shard_sadd(conn, base, member, total_elements, shard_size):
    shard = shard_key(base,
        'x'+str(member), total_elements, shard_size)
    return conn.sadd(shard, member)


SHARD_SIZE = 512
EXPECTED = defaultdict(lambda: 1000000)


# 代码清单10-1 根据指定名称的配置获取Redis连接的函数
def get_redis_connection(component, wait=1):
    key = 'config:redis:' + component
    old_config = CONFIGS.get(key, object())
    config = get_config(
        config_connection, 'redis', component, wait)

    if config != old_config:
        REDIS_CONNECTIONS[key] = redis.Redis(**config)

    return REDIS_CONNECTIONS.get(key)


# 代码清单10-2 基于分片信息获取一个连接
def get_sharded_connection(component, key, shard_count, wait=1):
    shard = shard_key(component, 'x'+str(key), shard_count, 2)
    return get_redis_connection(shard, wait)


# 代码清单10-3 一个支持分片功能的连接装饰器
def sharded_connection(component, shard_count, wait=1):
    # shard_count：分片数量
    def wrapper(function):
        @functools.wraps(function)
        def call(key, *args, **kwargs):
            conn = get_sharded_connection(
                component, key, shard_count, wait)
            return function(conn, key, *args, **kwargs)
        return call
    return wrapper


# 代码清单10-4 对机器以及数据库键进行分片的count_visit()函数
@sharded_connection('unique', 16)
def count_visit(conn, session_id):
    today = date.today()
    key = 'unique:%s' % today.isoformat()
    # conn2：非分片连接
    conn2, expected = get_expected(key, today)

    id = int(session_id.replace('-', '')[:15], 16)
    if shard_sadd(conn, key, id, expected, SHARD_SIZE):
        conn2.incr(key)


@redis_connection('unique')
def get_expected(conn, key, today):
    'all of the same function body as before, except the last line'
    return conn, EXPECTED[key]
