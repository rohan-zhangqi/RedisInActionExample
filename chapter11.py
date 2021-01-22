from _typeshed import NoneType
import bisect
import math
import threading
import time
import unittest
import uuid

import redis


# 代码清单11-1 将脚本载入Redis里面，等待将来使用
def script_load(script):
    sha = [None]

    def call(conn, keys=[], args=[], force_eval=False):
        if not force_eval:
            if not sha[0]:
                sha[0] = conn.execute_command(
                    "SCRIPT", "LOAD", script, parse="LOAD")    
            try:
                return conn.execute_command(
                    "EVALSHA", sha[0], len(keys), *(keys+args))
            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):
                    raise
        return conn.execute_command(
            "EVAL", script, len(keys), *(keys+args))
    return call


# 代码清单11-2 之前在代码清单8-2展示过的创建状态消息散列的函数
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


_create_status = create_status


# 代码清单11-3 使用Lua脚本创建一条状态消息
def create_status(conn, uid, message, **data):
    args = [
        'message', message,
        'posted', time.time(),
        'uid', uid,
    ]
    for key, value in data.items():
        args.append(key)
        args.append(value)

    return create_status_lua(
        conn, ['user:%s' % uid, 'status:id:'], args)


create_status_lua = script_load('''
local login = redis.call('hget', KEYS[1], 'login')
if not login then
    return false
end
local id = redis.call('incr', KEYS[2])
local key = string.format('status:%s', id)
redis.call('hmset', key,
    'login', login,
    'id', id,
    unpack(ARGV))
redis.call('hincrby', KEYS[1], 'posts', 1)
return id
''')


# 代码清单11-4 曾经在6.2.5节中展示过的最终版acquire_lock_with_timeout()函数
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


_acquire_lock_with_timeout = acquire_lock_with_timeout


# 代码清单11-5 使用Lua重写的acquire_lock_with_timeout()函数
def acquire_lock_with_timeout(
        conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())                    
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout)) 

    acquired = False
    end = time.time() + acquire_timeout
    while time.time() < end and not acquired:
        acquired = acquire_lock_with_timeout_lua(
            conn, [lockname], [lock_timeout, identifier]) == b'OK'

        time.sleep(.001 * (not acquired))

    return acquired and identifier


acquire_lock_with_timeout_lua = script_load('''
if redis.call('exists', KEYS[1]) == 0 then
    return redis.call('setex', KEYS[1], unpack(ARGV))
end
''')


def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname

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


_release_lock = release_lock


# 代码清单11-6 使用Lua重写的release_lock()函数
def release_lock(conn, lockname, identifier):
    lockname = 'lock:' + lockname
    return release_lock_lua(conn, [lockname], [identifier])


release_lock_lua = script_load('''
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1]) or true
end
''')


# 代码清单11-7 来自6.3.1节的acquire_semaphore()函数
def acquire_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zadd(semname, {identifier: now})
    pipeline.zrank(semname, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier

    conn.zrem(semname, identifier)
    return None


_acquire_semaphore = acquire_semaphore


# 代码清单11-8 使用Lua重写的acquire_semaphore()函数
def acquire_semaphore(conn, semname, limit, timeout=10):
    now = time.time()
    return acquire_semaphore_lua(
        conn, [semname],
        [now-timeout, limit, now, str(uuid.uuid4())])


acquire_semaphore_lua = script_load('''
redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])
if redis.call('zcard', KEYS[1]) < tonumber(ARGV[2]) then
    redis.call('zadd', KEYS[1], ARGV[3], ARGV[4])
    return ARGV[4]
end
''')


# 代码清单11-9 使用Lua实现的refresh_semaphore()函数
def refresh_semaphore(conn, semname, identifier):
    return refresh_semaphore_lua(
                conn, [semname], [identifier, time.time()]
            ) != None


refresh_semaphore_lua = script_load('''
if redis.call('zscore', KEYS[1], ARGV[1]) then
    return redis.call('zadd', KEYS[1], ARGV[2], ARGV[1]) or tru
end
''')

valid_characters = '`abcdefghijklmnopqrstuvwxyz{'


def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '{', prefix + '{'


# 代码清单11-10 来自6.1.2节的自动补全代码
def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    start += identifier
    end += identifier
    zset_name = 'members:' + guild

    conn.zadd(zset_name, start, 0, end, 0)
    pipeline = conn.pipeline(True)
    while 1:
        try:
            pipeline.watch(zset_name)
            sindex = pipeline.zrank(zset_name, start)
            eindex = pipeline.zrank(zset_name, end)
            erange = min(sindex + 9, eindex - 2)
            pipeline.multi()
            pipeline.zrem(zset_name, start, end)
            pipeline.zrange(zset_name, sindex, erange)
            items = pipeline.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue

        return [item for item in items if b'{' not in item]


_autocomplete_on_prefix = autocomplete_on_prefix


# 代码清单11-11 使用Lua脚本对用户名前缀进行自动补全
def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())

    items = autocomplete_on_prefix_lua(conn,
        ['members:' + guild],
        [start+identifier, end+identifier])

    return [item for item in items if b'{' not in item]

autocomplete_on_prefix_lua = script_load('''
redis.call('zadd', KEYS[1], 0, ARGV[1], 0, ARGV[2])
local sindex = redis.call('zrank', KEYS[1], ARGV[1])
local eindex = redis.call('zrank', KEYS[1], ARGV[2])
eindex = math.min(sindex + 9, eindex - 2)
redis.call('zrem', KEYS[1], unpack(ARGV))
return redis.call('zrange', KEYS[1], sindex, eindex)
''')


# 代码清单11-12 来自6.2节的带有锁的商品购买函数
def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    # 确实未出现
    locked = acquire_lock(conn, 'market:')
    if not locked:
        return False

    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.execute()
        if price is None or price > funds:
            return NoneType

        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyer, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem("market:", item)
        pipe.execute()
        return True
    finally:
        release_lock(conn, 'market:', locked)


# 代码清单11-13 使用Lua重写的商品购买函数
def purchase_item(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    return purchase_item_lua(
        conn,
        ['market:', buyer, seller, inventory], [item, itemid])


purchase_item_lua = script_load('''
local price = tonumber(redis.call('zscore', KEYS[1], ARGV[1]))
local funds = tonumber(redis.call('hget', KEYS[2], 'funds'))
if price and funds and funds >= price then
    redis.call('hincrby', KEYS[3], 'funds', price)
    redis.call('hincrby', KEYS[2], 'funds', -price)
    redis.call('sadd', KEYS[4], ARGV[2])
    redis.call('zrem', KEYS[1], ARGV[1])
    return true
end
''')


# 代码清单11-14 将元素推入分片列表里面的函数
LIST_CHUNK_SIZE = 512


def sharded_push_helper(conn, key, *items, **kwargs):
    items = list(items)
    total = 0
    while items:
        pushed = sharded_push_lua(
            conn,
            [key+':', key+':first', key+':last'],
            [LIST_CHUNK_SIZE, kwargs['cmd']] + items[:64])
        total += pushed
        del items[:pushed]
    return total


def sharded_lpush(conn, key, *items):
    return sharded_push_helper(conn, key, *items, cmd='lpush')


def sharded_rpush(conn, key, *items):
    return sharded_push_helper(conn, key, *items, cmd='rpush')


sharded_push_lua = script_load('''
local max = tonumber(ARGV[1])
if #ARGV < 3 or max < 2 then return 0 end
local skey = ARGV[2] == 'lpush' and KEYS[2] or KEYS[3]
local shard = redis.call('get', skey) or '0'
while 1 do
    local current = tonumber(redis.call('llen', KEYS[1]..shard))
    local topush = math.min(#ARGV - 2, max - current - 1)
    if topush > 0 then
        redis.call(ARGV[2], KEYS[1]..shard, unpack(ARGV, 3, topush+2))
        return topush
    end
    shard = redis.call(ARGV[2] == 'lpush' and 'decr' or 'incr', skey)
end
''')


# 代码清单11-15 负责从分片列表里面弹出元素的Lua脚本
def sharded_lpop(conn, key):
    return sharded_list_pop_lua(
        conn, [key+':', key+':first', key+':last'], ['lpop'])


def sharded_rpop(conn, key):
    return sharded_list_pop_lua(
        conn, [key+':', key+':first', key+':last'], ['rpop'])


sharded_list_pop_lua = script_load('''
local skey = ARGV[1] == 'lpop' and KEYS[2] or KEYS[3]
local okey = ARGV[1] ~= 'lpop' and KEYS[2] or KEYS[3]
local shard = redis.call('get', skey) or '0'
local ret = redis.call(ARGV[1], KEYS[1]..shard)
if not ret or redis.call('llen', KEYS[1]..shard) == '0' then
    local oshard = redis.call('get', okey) or '0'
    if shard == oshard then
        return ret
    end
    local cmd = ARGV[1] == 'lpop' and 'incr' or 'decr'
    shard = redis.call(cmd, skey)
    if not ret then
        ret = redis.call(ARGV[1], KEYS[1]..shard)
    end
end
return ret
''')


# 代码清单11-16 对分片列表执行阻塞弹出操作的函数
DUMMY = str(uuid.uuid4())


def sharded_bpop_helper(conn, key, timeout, pop, bpop, endp, push):
    pipe = conn.pipeline(False)
    timeout = max(timeout, 0) or 2**64
    end = time.time() + timeout

    while time.time() < end:
        result = pop(conn, key)
        if result not in (None, DUMMY):
            return result

        shard = conn.get(key + endp) or '0'
        sharded_bpop_helper_lua(pipe, [key + ':', key + endp],
            [shard, push, DUMMY], force_eval=True)
        getattr(pipe, bpop)(key + ':' + shard, 1)

        result = (pipe.execute()[-1] or [None])[-1]
        if result not in (None, DUMMY):
            return result


def sharded_blpop(conn, key, timeout=0):
    return sharded_bpop_helper(
        conn, key, timeout, sharded_lpop, 'blpop', ':first', 'lpush')

def sharded_brpop(conn, key, timeout=0):
    return sharded_bpop_helper(
        conn, key, timeout, sharded_rpop, 'brpop', ':last', 'rpush')


sharded_bpop_helper_lua = script_load('''
local shard = redis.call('get', KEYS[2]) or '0'
if shard ~= ARGV[1] then
    redis.call(ARGV[2], KEYS[1]..ARGV[1], ARGV[3])
end
''')
