import http.server
import cgi
import functools
import json
import math
import random
import socket
import socketserver
import time
import threading
import unittest
import uuid
import urllib.parse

import redis


def to_bytes(x):
    return x.encode() if isinstance(x, str) else x


def to_str(x):
    return x.decode() if isinstance(x, bytes) else x


def acquire_lock_with_timeout(
        conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif conn.ttl(lockname) < 0:
            conn.expire(lockname, lock_timeout)

        time.sleep(.001)

    return False


def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    identifier = to_bytes(identifier)

    while True:
        try:
            pipe.watch(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True

            pipe.unwatch()
            break

        except redis.exceptions.WatchError:
            pass

    return False


CONFIGS = {}
CHECKED = {}


def get_config(conn, type, component, wait=1):
    key = 'config:%s:%s' % (type, component)

    if CHECKED.get(key) < time.time() - wait:
        CHECKED[key] = time.time()
        config = json.loads(conn.get(key) or '{}')
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

            config = {}
            for k, v in _config.items():
                config[k.encode('utf-8')] = v

            if config != old_config:
                REDIS_CONNECTIONS[key] = redis.Redis(**config)

            return function(
                REDIS_CONNECTIONS.get(key), *args, **kwargs)
        return call
    return wrapper


def execute_later(conn, queue, name, args):
    # this is just for testing purposes
    assert conn is args[0]
    t = threading.Thread(target=globals()[name], args=tuple(args))
    t.setDaemon(1)
    t.start()


# 代码清单8-1 创建新的用户信息散列的方法
def create_user(conn, login, name):
    llogin = login.lower()
    lock = acquire_lock_with_timeout(conn, 'user:' + llogin, 1)
    if not lock:
        return None

    if conn.hget('users:', llogin):
        release_lock(conn, 'user:' + llogin, lock)
        return None

    id = conn.incr('user:id:')
    pipeline = conn.pipeline(True)
    pipeline.hset('users:', llogin, id)
    pipeline.hmset('user:%s' % id, {
        'login': login,
        'id': id,
        'name': name,
        'followers': 0,
        'following': 0,
        'posts': 0,
        'signup': time.time(),
    })
    pipeline.execute()
    release_lock(conn, 'user:' + llogin, lock)
    return id


# 代码清单8-2 创建状态消息散列的方法
def create_status(conn, uid, message, **data):
    pipeline = conn.pipeline(True)
    pipeline.hget('user:%s' % uid, 'login')
    pipeline.incr('status:id:')
    login, id = pipeline.execute()

    if not login:
        return None

    data.update({
        'message': message,
        'posted': time.time(),
        'id': id,
        'uid': uid,
        'login': login,
    })
    pipeline.hmset('status:%s' % id, data)
    pipeline.hincrby('user:%s' % uid, 'posts')
    pipeline.execute()
    return id


# 代码清单8-3 这个函数负责从时间线里面获取给定页数的最新状态消息
def get_status_messages(conn, uid, timeline='home:', page=1, count=30):
    statuses = conn.zrevrange(
        '%s%s'%(timeline, uid), (page-1)*count, page*count-1)

    pipeline = conn.pipeline(True)
    for id in statuses:
        pipeline.hgetall('status:%s'%(to_str(id),))

    return [_f for _f in pipeline.execute() if _f]


# 代码清单8-4 对执行关注操作的用户的主页时间线进行更新
HOME_TIMELINE_SIZE = 1000


def follow_user(conn, uid, other_uid):
    fkey1 = 'following:%s' % uid
    fkey2 = 'followers:%s' % other_uid

    if conn.zscore(fkey1, other_uid):
        return None

    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zadd(fkey1, {other_uid: now})
    pipeline.zadd(fkey2, {uid: now})
    pipeline.zrevrange(
            'profile:%s' % other_uid,
            0,
            HOME_TIMELINE_SIZE - 1,
            withscores=True)
    following, followers, status_and_score = pipeline.execute()[-3:]

    pipeline.hincrby('user:%s' % uid, 'following', int(following))
    pipeline.hincrby('user:%s' % other_uid, 'followers', int(followers))
    if status_and_score:
        pipeline.zadd('home:%s' % uid, dict(status_and_score))
    pipeline.zremrangebyrank('home:%s' % uid, 0, -HOME_TIMELINE_SIZE-1)

    pipeline.execute()
    return True
