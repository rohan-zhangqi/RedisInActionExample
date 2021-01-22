import bisect
from collections import defaultdict, deque
import json
import math
import os
import time
import unittest
import uuid
import zlib

import redis


QUIT = False
pipe = inv = item = buyer = seller = inventory = None


# 代码清单6-1 add_update_contact()函数
def add_update_contact(conn, user, contact):
    ac_list = 'recent:' + user
    pipeline = conn.pipeline(True)
    pipeline.lrem(ac_list, contact)
    pipeline.lpush(ac_list, contact)
    pipeline.ltrim(ac_list, 0, 99)
    pipeline.execute()


# 代码清单6-2 fetch_autocomplete_list()函数
def fetch_autocomplete_list(conn, user, prefix):
    candidates = conn.lrange('recent:' + user, 0, -1)
    matches = []
    for candidate in candidates:
        if candidate.lower().startswith(prefix):
            matches.append(candidate)
    return matches


# 代码清单6-3 find_prefix_range()函数
valid_characters = '`abcdefghijklmnopqrstuvwxyz{'


def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])
    suffix = valid_characters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '{', prefix + '{'


# 代码清单6-4 autocomplete_on_prefix()函数
def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    start += identifier
    end += identifier
    zset_name = 'members:' + guild

    conn.zadd(zset_name, {start: 0, end: 0})
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


# 代码清单6-5 join_guild()函数和leave_guild()函数
def join_guild(conn, guild, user):
    conn.zadd('members:' + guild, {user: 0})


def leave_guild(conn, guild, user):
    conn.zrem('members:' + guild, user)


# 代码清单6-6 来自4.4.2节中的list_item()函数
def list_item(conn, itemid, sellerid, price):
    #...
        pipe.watch(inv)
        if not pipe.sismember(inv, itemid):
            pipe.unwatch()
            return None

        pipe.multi()
        pipe.zadd("market:", {item: price})
        pipe.srem(inv, itemid)
        pipe.execute()
        return True
    #...


# 代码清单6-7 来自4.4.3节中的purchase_item()函数
def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    #...
        pipe.watch("market:", buyer)

        price = pipe.zscore("market:", item)
        funds = int(pipe.hget(buyer, 'funds'))
        if price != lprice or price > funds:
            pipe.unwatch()
            return None

        pipe.multi()
        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyerid, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem("market:", item)
        pipe.execute()
        return True

    #...


# 代码清单6-8 acquire_lock()函数
def acquire_lock(conn, lockname, acquire_timeout=10):
    identifier = str(uuid.uuid4())

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):
            return identifier

        time.sleep(.001)

    return False


# 代码清单6-9 purchase_item_with_lock()函数
def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    locked = acquire_lock(conn, 'market:')
    if not locked:
        return False

    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.execute()
        if price is None or price > funds:
            return None

        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyer, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem("market:", item)
        pipe.execute()
        return True
    finally:
        release_lock(conn, 'market:', locked)


# 代码清单6-10 release_lock()函数
def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    if isinstance(identifier, str):
        identifier = identifier.encode()

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


# 代码清单6-11 acquire_lock_with_timeout()函数
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


# 代码清单6-12 acquire_semaphore()函数
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


# 代码清单6-12 release_semaphore()函数
def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)


# 代码清单6-14 acquire_fair_semaphore()函数
def acquire_fair_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    czset = semname + ':owner'
    ctr = semname + ':counter'

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zinterstore(czset, {czset: 1, semname: 0})

    pipeline.incr(ctr)
    counter = pipeline.execute()[-1]

    pipeline.zadd(semname, {identifier: now})
    pipeline.zadd(czset, {identifier: counter})

    pipeline.zrank(czset, identifier)
    if pipeline.execute()[-1] < limit:
        return identifier

    pipeline.zrem(semname, identifier)
    pipeline.zrem(czset, identifier)
    pipeline.execute()
    return None


# 代码清单6-15 release_fair_semaphore()函数
def release_fair_semaphore(conn, semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname + ':owner', identifier)
    return pipeline.execute()[0]


# 代码清单6-16 refresh_fair_semaphore()函数
def refresh_fair_semaphore(conn, semname, identifier):
    if conn.zadd(semname, {identifier: time.time()}):
        release_fair_semaphore(conn, semname, identifier)
        return False
    return True


# 代码清单6-17 acquire_semaphore_with_lock()函数
def acquire_semaphore_with_lock(conn, semname, limit, timeout=10):
    identifier = acquire_lock(conn, semname, acquire_timeout=.01)
    if identifier:
        try:
            return acquire_fair_semaphore(conn, semname, limit, timeout)
        finally:
            release_lock(conn, semname, identifier)


# 代码清单6-18 send_sold_email_via_queue()函数
def send_sold_email_via_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }
    conn.rpush('queue:email', json.dumps(data))


# 代码清单6-19 process_sold_email_queue()函数
def process_sold_email_queue(conn):
    while not QUIT:
        packed = conn.blpop(['queue:email'], 30)
        if not packed:
            continue

        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except EmailSendError as err:
            log_error("Failed to send sold email", err, to_send)
        else:
            log_success("Sent sold email", to_send)


# 代码清单6-20 worker_watch_queue()函数
def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_error("Unknown callback %s"%name)
            continue
        callbacks[name](*args)


# 代码清单6-21 worker_watch_queues函数
def worker_watch_queues(conn, queues, callbacks):
    while not QUIT:
        packed = conn.blpop(queues, 30)
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_error("Unknown callback %s" % name)
            continue
        callbacks[name](*args)


# 代码清单6-22 execute_later()函数
def execute_later(conn, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())
    item = json.dumps([identifier, queue, name, args])
    if delay > 0:
        conn.zadd('delayed:', {item: time.time() + delay})
    else:
        conn.rpush('queue:' + queue, item)
    return identifier


# 代码清单6-23 poll_queue()函数
def poll_queue(conn):
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0, withscores=True)
        if not item or item[0][1] > time.time():
            time.sleep(.01)
            continue

        item = item[0][0]
        identifier, queue, function, args = json.loads(item)

        locked = acquire_lock(conn, identifier)
        if not locked:
            continue

        if conn.zrem('delayed:', item):
            conn.rpush('queue:' + queue, item)

        release_lock(conn, identifier, locked)


# 代码清单6-24 create_chat()函数
def create_chat(conn, sender, recipients, message, chat_id=None):
    chat_id = chat_id or str(conn.incr('ids:chat:'))

    recipients.append(sender)
    recipientsd = dict((r, 0) for r in recipients)

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, recipientsd)
    for rec in recipients:
        pipeline.zadd('seen:' + rec, {chat_id: 0})
    pipeline.execute()

    return send_message(conn, chat_id, sender, message)


# 代码清单6-25 send_message()函数
def send_message(conn, chat_id, sender, message):
    identifier = acquire_lock(conn, 'chat:' + chat_id)
    if not identifier:
        raise Exception("Couldn't get the lock")
    try:
        mid = conn.incr('ids:' + chat_id)
        ts = time.time()
        packed = json.dumps({
            'id': mid,
            'ts': ts,
            'sender': sender,
            'message': message,
        })

        conn.zadd('msgs:' + chat_id, {packed: mid})
    finally:
        release_lock(conn, 'chat:' + chat_id, identifier)
    return chat_id


# 代码清单6-26 fetch_pending_messages()函数
def fetch_pending_messages(conn, recipient):
    seen = conn.zrange('seen:' + recipient, 0, -1, withscores=True)

    pipeline = conn.pipeline(True)

    for chat_id, seen_id in seen:
        pipeline.zrangebyscore(
            b'msgs:' + chat_id, seen_id+1, 'inf')
    chat_info = list(zip(seen, pipeline.execute()))

    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue
        messages[:] = list(map(json.loads, messages))
        seen_id = messages[-1]['id']
        conn.zadd(b'chat:' + chat_id, {recipient: seen_id})

        min_id = conn.zrange(
            b'chat:' + chat_id, 0, 0, withscores=True)

        pipeline.zadd('seen:' + recipient, {chat_id: seen_id})
        if min_id:
            pipeline.zremrangebyscore(
                b'msgs:' + chat_id, 0, min_id[0][1])
        chat_info[i] = (chat_id, messages)
    pipeline.execute()

    return chat_info


# 代码清单6-27 join_chat()函数
def join_chat(conn, chat_id, user):
    message_id = int(conn.get('ids:' + chat_id))

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, {user: message_id})
    pipeline.zadd('seen:' + user, {chat_id: message_id})
    pipeline.execute()


# 代码清单6-28 leave_chat()函数
def leave_chat(conn, chat_id, user):
    pipeline = conn.pipeline(True)
    pipeline.zrem('chat:' + chat_id, user)
    pipeline.zrem('seen:' + user, chat_id)
    pipeline.zcard('chat:' + chat_id)

    if not pipeline.execute()[-1]:
        pipeline.delete('msgs:' + chat_id)
        pipeline.delete('ids:' + chat_id)
        pipeline.execute()
    else:
        oldest = conn.zrange(
            'chat:' + chat_id, 0, 0, withscores=True)
        conn.zremrangebyscore('msgs:' + chat_id, 0, oldest[0][1])


# 代码清单6-29 一个本地聚合计算回调函数，用于每天以国家维度对日志进行聚合
aggregates = defaultdict(lambda: defaultdict(int))


def daily_country_aggregate(conn, line):
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = find_city_by_ip_local(ip)[2]
        aggregates[day][country] += 1
        return

    for day, aggregate in list(aggregates.items()):
        conn.zadd('daily:country:' + day, aggregate)
        del aggregates[day]


# 代码清单6-30 copy_logs_to_redis()函数
def copy_logs_to_redis(conn, path, channel, count=10,
                       limit=2**30, quit_when_done=True):
    bytes_in_redis = 0
    waiting = deque()
    create_chat(conn, 'source', list(map(str, list(range(count)))), '', channel)
    count = str(count).encode()
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(path, logfile)

        fsize = os.stat(full_path).st_size
        while bytes_in_redis + fsize > limit:
            cleaned = _clean(conn, channel, waiting, count)
            if cleaned:
                bytes_in_redis -= cleaned
            else:
                time.sleep(.25)

        with open(full_path, 'rb') as inp:
            block = ' '
            while block:
                block = inp.read(2**17)
                conn.append(channel+logfile, block)

        send_message(conn, channel, 'source', logfile)

        bytes_in_redis += fsize
        waiting.append((logfile, fsize))

    if quit_when_done:
        send_message(conn, channel, 'source', ':done')

    while waiting:
        cleaned = _clean(conn, channel, waiting, count)
        if cleaned:
            bytes_in_redis -= cleaned
        else:
            time.sleep(.25)


def _clean(conn, channel, waiting, count):
    if not waiting:
        return 0
    w0 = waiting[0][0]
    if (conn.get(channel + w0 + ':done') or b'0') >= count:
        conn.delete(channel + w0, channel + w0 + ':done')
        return waiting.popleft()[1]
    return 0


# 代码清单6-31 process_logs_from_redis()函数
def process_logs_from_redis(conn, id, callback):
    while 1:
        fdata = fetch_pending_messages(conn, id)

        for ch, mdata in fdata:
            if isinstance(ch, bytes):
                ch = ch.decode()
            for message in mdata:
                logfile = message['message']

                if logfile == ':done':
                    return
                elif not logfile:
                    continue

                block_reader = readblocks
                if logfile.endswith('.gz'):
                    block_reader = readblocks_gz

                for line in readlines(conn, ch+logfile, block_reader):
                    callback(conn, line)
                callback(conn, None)

                conn.incr(ch + logfile + ':done')

        if not fdata:
            time.sleep(.1)


# 代码清单6-32 readlines()函数
def readlines(conn, key, rblocks):
    out = b''
    for block in rblocks(conn, key):
        if isinstance(block, str):
            block = block.encode()
        out += block
        posn = out.rfind(b'\n')
        if posn >= 0:
            for line in out[:posn].split(b'\n'):
                yield line + b'\n'
            out = out[posn+1:]
        if not block:
            yield out
            break


# 代码清单6-33 readblocks()生成器
def readblocks(conn, key, blocksize=2**17):
    lb = blocksize
    pos = 0
    while lb == blocksize:
        block = conn.substr(key, pos, pos + blocksize - 1)
        yield block
        lb = len(block)
        pos += lb
    yield ''


# 代码清单6-34 readblocks_gz()生成器
def readblocks_gz(conn, key):
    inp = b''
    decoder = None
    for block in readblocks(conn, key, 2**17):
        if not decoder:
            inp += block
            try:
                if inp[:3] != b"\x1f\x8b\x08":
                    raise IOError("invalid gzip data")
                i = 10
                flag = inp[3]
                if flag & 4:
                    i += 2 + inp[i] + 256*inp[i+1]
                if flag & 8:
                    i = inp.index(b'\0', i) + 1
                if flag & 16:
                    i = inp.index(b'\0', i) + 1
                if flag & 2:
                    i += 2

                if i > len(inp):
                    raise IndexError("not enough data")
            except (IndexError, ValueError):
                continue

            else:
                block = inp[i:]
                inp = None
                decoder = zlib.decompressobj(-zlib.MAX_WBITS)
                if not block:
                    continue

        if not block:
            yield decoder.flush()
            break

        yield decoder.decompress(block)
