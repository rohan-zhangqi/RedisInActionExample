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

# 代码清单3-3 这个交互示例展示了Redis列表的推入操作和弹出操作

# 代码清单3-4 这个交互示例展示了Redis列表的阻塞弹出命令以及元素移动命令

# 代码清单3-5 这个交互示例展示了Redis中的一些常用的集合命令

# 代码清单3-6 这个交互示例展示了Redis中的差集运算、交集运算以及并集运算

# 代码清单3-7 这个交互示例展示了Redis中的一些常用的散列命令

# 代码清单3-8 这个交互示例展示了Redis散列的一些更高级的特性

# 代码清单3-9 这个交互示例展示了Redis中的一些常用的有序集合命令

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
















