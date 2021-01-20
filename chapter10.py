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


# 代码清单10-5 基于SORT命令实现的搜索程序，它能够获取已排序的搜索结果
def search_get_values(conn, query, id=None, ttl=300, sort="-updated",
                      start=0, num=20):
    count, docids, id = search_and_sort(
        conn, query, id, ttl, sort, 0, start+num)

    key = "kb:doc:%s"
    sort = sort.lstrip('-')

    pipe = conn.pipeline(False)
    for docid in docids:
        if isinstance(docid, bytes):
            docid = docid.decode('latin-1')
        pipe.hget(key % docid, sort)
    sort_column = pipe.execute()

    data_pairs = list(zip(docids, sort_column))
    return count, data_pairs, id


# 代码清单10-6 这个函数负责在所有分片上面执行搜索函数
def get_shard_results(
        component, shards, query, ids=None, ttl=300,
        sort="-updated", start=0, num=20, wait=1):

    count = 0
    data = []
    ids = ids or shards * [None]
    for shard in range(shards):
        conn = get_redis_connection('%s:%s' % (component, shard), wait)
        c, d, i = search_get_values(
            conn, query, ids[shard], ttl, sort, start, num)

        count += c
        data.extend(d)
        ids[shard] = i

    return count, data, ids


# 代码清单10-7 负责对分片搜索结果进行合并的函数
def to_numeric_key(data):
    try:
        return Decimal(data[1] or '0')
    except:
        return Decimal('0')


def to_string_key(data):
    return data[1] or ''


def search_shards(component, shards, query, ids=None, ttl=300,
                  sort="-updated", start=0, num=20, wait=1):

    count, data, ids = get_shard_results(
        component, shards, query, ids, ttl, sort, start, num, wait)

    reversed = sort.startswith('-')
    sort = sort.strip('-')
    key = to_numeric_key
    if sort not in ('updated', 'id', 'created'):
        key = to_string_key

    data.sort(key=key, reverse=reversed)

    results = []
    for docid, score in data[start:start+num]:
        results.append(docid)

    return count, results, ids


# 代码清单10-8 基于有序集合实现的搜索操作，它会返回搜索结果以及搜索结果的分值
def search_get_zset_values(
                    conn, query, id=None, ttl=300, update=1,
                    vote=0, start=0, num=20, desc=True):

    count, r, id = search_and_zsort(
        conn, query, id, ttl, update, vote, 0, 1, desc)

    if desc:
        data = conn.zrevrange(id, 0, start + num - 1, withscores=True)
    else:
        data = conn.zrange(id, 0, start + num - 1, withscores=True)

    return count, data, id


# 代码清单10-9 一个对有序集合进行分片搜索查询的函数，它返回的是分页之后的搜索结果
def search_shards_zset(
                component, shards, query, ids=None, ttl=300,
                update=1, vote=0, start=0, num=20, desc=True, wait=1):

    count = 0
    data = []
    ids = ids or shards * [None]
    for shard in range(shards):
        conn = get_redis_connection('%s:%s' % (component, shard), wait)
        c, d, i = search_get_zset_values(
            conn, query, ids[shard],
            ttl, update, vote, start, num, desc)

        count += c
        data.extend(d)
        ids[shard] = i

    def key(result):
        return result[1]

    data.sort(key=key, reversed=desc)
    results = []
    for docid, score in data[start:start+num]:
        results.append(docid)

    return count, results, ids


# 代码清单10-11 一个根据给定键查找分片连接的类
class KeyShardedConnection(object):
    def __init__(self, component, shards):
        self.component = component
        self.shards = shards

    def __getitem__(self, key):
        return get_sharded_connection(
            self.component, key, self.shards)


# 代码清单10-10 这个函数展示了分片API是如何运作起来的
sharded_timelines = KeyShardedConnection('timelines', 8)


def follow_user(conn, uid, other_uid):
    fkey1 = 'following:%s' % uid
    fkey2 = 'followers:%s' % other_uid

    if conn.zscore(fkey1, other_uid):
        print("already followed", uid, other_uid)
        return None

    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zadd(fkey1, {other_uid: now})
    pipeline.zadd(fkey2, {uid: now})
    pipeline.zcard(fkey1)
    pipeline.zcard(fkey2)
    following, followers = pipeline.execute()[-2:]
    pipeline.hset('user:%s' % uid, 'following', following)
    pipeline.hset('user:%s' % other_uid, 'followers', followers)
    pipeline.execute()

    pkey = 'profile:%s' % other_uid
    status_and_score = sharded_timelines[pkey].zrevrange(
        pkey, 0, HOME_TIMELINE_SIZE-1, withscores=True)

    if status_and_score:
        hkey = 'home:%s' % uid
        pipe = sharded_timelines[hkey].pipeline(True)
        pipe.zadd(hkey, dict(status_and_score))
        pipe.zremrangebyrank(hkey, 0, -HOME_TIMELINE_SIZE-1)
        pipe.execute()

    return True


# 代码清单10-13 根据ID对查找相应的分片连接
class KeyDataShardedConnection(object):
    def __init__(self, component, shards):
        self.component = component
        self.shards = shards

    def __getitem__(self, ids):
        id1, id2 = list(map(int, ids))
        if id2 < id1:
            id1, id2 = id2, id1
        key = "%s:%s" % (id1, id2)
        return get_sharded_connection(
            self.component, key, self.shards)


# 代码清单10-12 访问存储着关注者有序集合以及正在关注有序集合的分片
sharded_timelines = KeyShardedConnection('timelines', 8)
sharded_followers = KeyDataShardedConnection('followers', 16)


def follow_user(conn, uid, other_uid):
    fkey1 = 'following:%s' % uid
    fkey2 = 'followers:%s' % other_uid

    sconn = sharded_followers[uid, other_uid]
    if sconn.zscore(fkey1, other_uid):
        return None

    now = time.time()
    spipe = sconn.pipeline(True)
    spipe.zadd(fkey1, {other_uid: now})
    spipe.zadd(fkey2, {uid: now})
    following, followers = spipe.execute()

    pipeline = conn.pipeline(True)
    pipeline.hincrby('user:%s' % uid, 'following', int(following))
    pipeline.hincrby('user:%s' % other_uid, 'followers', int(followers))
    pipeline.execute()

    pkey = 'profile:%s' % other_uid
    status_and_score = sharded_timelines[pkey].zrevrange(
        pkey, 0, HOME_TIMELINE_SIZE-1, withscores=True)

    if status_and_score:
        hkey = 'home:%s' % uid
        pipe = sharded_timelines[hkey].pipeline(True)
        pipe.zadd(hkey, dict(status_and_score))
        pipe.zremrangebyrank(hkey, 0, -HOME_TIMELINE_SIZE-1)
        pipe.execute()

    return True


# 代码清单10-14 分片版的ZRANGEBYSCORE命令的实现函数
def sharded_zrangebyscore(component, shards, key, min, max, num):
    data = []
    for shard in range(shards):
        conn = get_redis_connection("%s:%s" % (component, shard))
        data.extend(conn.zrangebyscore(
            key, min, max, start=0, num=num, withscores=True))

    def key(pair):
        return pair[1], pair[0]
    data.sort(key=key)

    return data[:num]


# 代码清单10-15 更新后的状态广播函数
def syndicate_status(uid, post, start=0, on_lists=False):
    root = 'followers'
    key = 'followers:%s' % uid
    base = 'home:%s'
    if on_lists:
        root = 'list:out'
        key = 'list:out:%s' % uid
        base = 'list:statuses:%s'

    followers = sharded_zrangebyscore(
        root, sharded_followers.shards, key, start, 'inf', POSTS_PER_PASS)

    to_send = defaultdict(list)
    for follower, start in followers:
        timeline = base % follower
        shard = shard_key(
            'timelines', timeline, sharded_timelines.shards, 2)
        to_send[shard].append(timeline)

    for timelines in to_send.values():
        pipe = sharded_timelines[timelines[0]].pipeline(False)
        for timeline in timelines:
            pipe.zadd(timeline, post)
            pipe.zremrangebyrank(
                timeline, 0, -HOME_TIMELINE_SIZE-1)
        pipe.execute()

    conn = redis.Redis()
    if len(followers) >= POSTS_PER_PASS:
        execute_later(
            conn, 'default', 'syndicate_status',
            [uid, post, start, on_lists])

    elif not on_lists:
        execute_later(
            conn, 'default', 'syndicate_status',
            [uid, post, 0, True])
