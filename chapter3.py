import threading
import time

import redis

# 代码清单3-1 这个交互示例展示了Redis的INCR操作和DECR操作
conn = redis.Redis()

conn.get('key')
conn.incr('key')
# 1
conn.incr('key', 15)
# 16
conn.decr('key', 5)
# 11
conn.get('key')
# '11'
conn.set('key', '13')
# True
conn.incr('key')
# 14

# 代码清单3-2 这个交互示例展示了Redis的子串操作和二进制位操作
conn.append('new-string-key', 'hello ')
# 6L
conn.append('new-string-key', 'world!')
# 12L
conn.substr('new-string-key', 3, 7)
# 'lo wo'
conn.setrange('new-string-key', 0, 'H')
# 12
conn.setrange('new-string-key', 6, 'W')
# 12
conn.get('new-string-key')
# 'Hello World!'
conn.setrange('new-string-key', 11, ', how are you?')
# 25
conn.get('new-string-key')
# 'Hello World, how are you?'
conn.setbit('another-key', 2, 1)
# 0
conn.setbit('another-key', 7, 1)
# 0
conn.get('another-key')
# '!'

# 代码清单3-3 这个交互示例展示了Redis列表的推入操作和弹出操作
conn.rpush('list-key', 'last')
# 1L
conn.lpush('list-key', 'first')
# 2L
conn.rpush('list-key', 'new last')
# 3L
conn.lrange('list-key', 0, -1)
# ['first', 'last', 'new last']
conn.lpop('list-key')
# 'first'
conn.lpop('list-key')
# 'last'
conn.lrange('list-key', 0, -1)
# ['new last']
conn.rpush('list-key', 'a', 'b', 'c')
# 4L
conn.lrange('list-key', 0, -1)
# ['new last', 'a', 'b', 'c']
conn.ltrim('list-key', 2, -1)
# True
conn.lrange('list-key', 0, -1)
# ['b', 'c']

# 代码清单3-4 这个交互示例展示了Redis列表的阻塞弹出命令以及元素移动命令
conn.rpush('list', 'item1')
# 1
conn.rpush('list', 'item2')
# 2
conn.rpush('list2', 'item3')
# 1

# 从list2弹出最右端的元素，推入list的最左端，1秒内阻塞并等待可弹出的元素出现
conn.brpoplpush('list2', 'list', 1)
# 'item3'
conn.brpoplpush('list2', 'list', 1)
conn.lrange('list', 0, -1)
# ['item3', 'item1', 'item2']
conn.brpoplpush('list', 'list2', 1)
# 'item2'

# 从第一个非空列表中弹出位于最左端的元素，或在1秒内阻塞并等待弹出的元素出现
conn.blpop(['list', 'list2'], 1)
# ('list', 'item3')
conn.blpop(['list', 'list2'], 1)
# ('list', 'item1')
conn.blpop(['list', 'list2'], 1)
# ('list2', 'item2')
conn.blpop(['list', 'list2'], 1)

# 代码清单3-5 这个交互示例展示了Redis中的一些常用的集合命令
conn.sadd('set-key', 'a', 'b', 'c')
# 3
conn.srem('set-key', 'c', 'd')
# True
conn.srem('set-key', 'c', 'd')
# False
conn.scard('set-key')
# 2
conn.smembers('set-key')
# set(['a', 'b'])
conn.smove('set-key', 'set-key2', 'a')
# True
conn.smove('set-key', 'set-key2', 'c')
# False
conn.smembers('set-key2')
# set(['a'])

# 代码清单3-6 这个交互示例展示了Redis中的差集运算、交集运算以及并集运算
conn.sadd('skey1', 'a', 'b', 'c', 'd')
# 4
conn.sadd('skey2', 'c', 'd', 'e', 'f')
# 4
conn.sdiff('skey1', 'skey2')
# set(['a', 'b'])
conn.sinter('skey1', 'skey2')
# set(['c', 'd'])
conn.sunion('skey1', 'skey2')
# set(['a', 'b', 'c', 'd', 'e', 'f'])

# 代码清单3-7 这个交互示例展示了Redis中的一些常用的散列命令
conn.hmset('hash-key', {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
# True
conn.hmget('hash-key', ['k2', 'k3'])
# ['v2', 'v3']
conn.hlen('hash-key')
# 3
conn.hdel('hash-key', 'k1', 'k3')
# True

# 代码清单3-8 这个交互示例展示了Redis散列的一些更高级的特性
conn.hmset('hash-key2', {'short': 'hello', 'long': 1000*'1'})
# True
conn.hkeys('hash-key2')
# ['long', 'short']
conn.hexists('hash-key2', 'num')
# False
conn.hincrby('hash-key2', 'num')
# 1L
conn.hexists('hash-key2', 'num')
# True

# 代码清单3-9 这个交互示例展示了Redis中的一些常用的有序集合命令
conn.zadd('zset-key', 'a', 3, 'b', 2, 'c', 1)
# 3
conn.zcard('zset-key')
# 3
conn.zincrby('zset-key', 'c', 3)
# 4.0
conn.zscore('zset-key', 'b')
# 2.0

# 定义：返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
# 语法：ZRANK key member
# 返回值：如果成员是有序集key的成员，返回member的排名。 如果成员不是有序集key的成员，返回nil。
conn.zrank('zset-key', 'c')
# 2
conn.zcount('zset-key', 0, 3)
# 2L
conn.zrem('zset-key', 'b')
# True
conn.zrange('zset-key', 0, -1, withscores=True)
# [('a', 3.0), ('c', 4.0)]

# 代码清单3-10 这个交互示例展示了ZINTERSTORE命令和ZUNIONSTORE命令的用法


# 代码清单3-11 这个交互示例展示了如何使用Redis中的PUBLISH命令以及SUBSCRIBE命令
# python3
def publisher(n):
    time.sleep(1)
    # 描述：xrange() 函数用法与 range 完全相同，所不同的是生成的不是一个数组，而是一个生成器。
    # 语法：
    # xrange(stop)
    # xrange(start, stop[, step])
    # 参数说明：
    #
    # start: 计数从 start 开始。默认是从 0 开始。例如 xrange(5) 等价于 xrange(0， 5)
    # stop: 计数到 stop 结束，但不包括 stop。例如：xrange(0， 5) 是 [0, 1, 2, 3, 4] 没有 5
    # step：步长，默认为1。例如：xrange(0， 5) 等价于 xrange(0, 5, 1)
    # 返回值：返回生成器。
    # 备注：python3中xrange()不存在，可用range()代替
    for i in range(n):
        conn.publish('channel', i)
        time.sleep(1)


def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print(item)
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break


'''
# python2
>>> def publisher(n):
    time.sleep(1)
    for i in xrange(n):
        conn.publish('channel', i)
        time.sleep(1)


>>> def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print item
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break

>>> run_pubsub()
'''
















