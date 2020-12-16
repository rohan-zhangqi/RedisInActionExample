import os
import time
import unittest
import uuid

import redis


# 代码清单4-1 Redis提供的持久化配置选项
# 快照持久化选项
# save 60 1000
# stop-writes-on-bgsave-error no
# rdbcompression yes
# dbfilename dump.rdb

# AOF持久化选项
# appendonly no
# appendfsync everysec
# no-appendfsync-on-rewrite no
# auto-aof-rewrite-percentage 100
# auto-aof-rewrite-min-size 64mb
# dir ./


# 代码清单4-2 process_logs()函数会将被处理日志的信息存储到Redis里面
def process_logs(conn, path, callback):
    # Redis MGET命令返回所有(一个或多个)给定key的值，值的类型是字符串。如果给定的key里面，有某个key不存在，那么这个key返回特殊值nil。
    # 语法：MGET KEY1 KEY2 .. KEYN
    # 返回值：一个包含所有给定 key 的值的列表。
    current_file, offset = conn.mget('progress:file', 'progress:position')

    pipe = conn.pipeline()

    # python允许创建嵌套函数
    def update_progress():
        # Redis MSET命令用于同时设置一个或多个key-value对
        # 语法：MSET key1 value1 key2 value2 .. keyN valueN
        # 返回值：总是返回OK。
        pipe.mset({
            'progress:file': fname,
            'progress:position': offset
        })
        pipe.execute()

    # sorted()函数对所有可迭代的对象进行排序操作。
    # 语法：sorted(iterable, key=None, reverse=False)
    # 参数说明：
    # iterable -- 可迭代对象。
    # key -- 主要是用来进行比较的元素，只有一个参数，具体的函数的参数就是取自于可迭代对象中，指定可迭代对象中的一个元素来进行排序。
    # reverse -- 排序规则，reverse = True降序， reverse = False升序（默认）。
    #
    # os.listdir() 方法用于返回指定的文件夹包含的文件或文件夹的名字的列表。
    # 它不包括 . 和 .. 即使它在文件夹中。
    # 语法：os.listdir(path)
    # 参数说明：
    # path -- 需要列出的目录路径
    # 返回值：返回指定路径下的文件和文件夹列表。
    for fname in sorted(os.listdir(path)):
        if fname < current_file:
            continue

        # 用于打开一个文件，创建一个file对象，相关的方法才可以调用它进行读写。
        # 语法：open(name[, mode[, buffering]])
        # 参数说明：
        # name : 一个包含了你要访问的文件名称的字符串值。
        # mode : mode 决定了打开文件的模式：只读，写入，追加等。所有可取值见如下的完全列表。这个参数是非强制的，默认文件访问模式为只读(r)。
        # buffering : 如果 buffering 的值被设为 0，就不会有寄存。如果 buffering 的值取 1，访问文件时会寄存行。如果将 buffering 的值设
        # 为大于 1 的整数，表明了这就是的寄存区的缓冲大小。如果取负值，寄存区的缓冲大小则为系统默认。
        # 模式rb：以二进制格式打开一个文件用于只读。文件指针将会放在文件的开头。这是默认模式。一般用于非文本文件如图片等。
        inp = open(os.path.join(path, fname), 'rb')
        if fname == current_file:
            # seek(偏移量,[起始位置])：用来移动文件指针。
            # 参数说明：
            # 偏移量: 单位为字节，可正可负
            # 起始位置: 0 - 文件头, 默认值; 1 - 当前位置; 2 - 文件尾
            # int() 函数用于将一个字符串或数字转换为整型
            # 语法：class int(x, base=10)
            # 参数说明：
            # x -- 字符串或数字。
            # base -- 进制数，默认十进制
            # 返回值：返回整型数据
            inp.seek(int(offset, 10))
        else:
            offset = 0

        current_file = None
        # 用于将一个可遍历的数据对象(如列表、元组或字符串)组合为一个索引序列，同时列出数据和数据下标，一般用在for循环当中。
        # Python 2.3. 以上版本可用，2.6 添加 start 参数
        # 语法：enumerate(sequence, [start=0])
        # 参数说明：
        # sequence -- 一个序列、迭代器或其他支持迭代对象
        # start -- 下标起始位置
        # 返回值：返回 enumerate(枚举) 对象
        for lno, line in enumerate(inp):
            callback(pipe, line)
            offset += int(offset) + len(line)
            if not (lno + 1) % 1000:
                update_progress()

        update_progress()
        inp.close()


# 代码清单4-3 wait_for_sync()函数
def wait_for_sync(mconn, sconn):
    identifier = str(uuid.uuid4())
    mconn.zadd('sync:wait', identifier, time.time())

    # 等待从服务器完成同步
    while not sconn.info()['master_link_status'] != 'up':
        time.sleep(.001)

    # 等待从服务器接收数据更新
    while not sconn.zscore('sync:wait', identifier):
        time.sleep(.001)

    deadline = time.time() + 1.01
    while time.time() < deadline:
        # 检查数据更新是否已经被同步到了硬盘
        if sconn.info()['aof_pending_bio_fsync'] == 0:
            break
        time.sleep(.001)

    # 清理刚刚创建的新令牌以及之前可能留下的旧令牌
    mconn.zrem('sync:wait', identifier)
    mconn.zremrangebyscore('sync:wait', 0, time.time() - 900)


# 代码清单4-4 用于替换故障主节点的一连串命令
'''
user@vpn-master ~:$ ssh root@machine-b.vpn                          #A
Last login: Wed Mar 28 15:21:06 2012 from ...                       #A
root@machine-b ~:$ redis-cli                                        #B
redis 127.0.0.1:6379> SAVE                                          #C
OK                                                                  #C
redis 127.0.0.1:6379> QUIT                                          #C
root@machine-b ~:$ scp \\                                           #D
> /var/local/redis/dump.rdb machine-c.vpn:/var/local/redis/         #D
dump.rdb                      100%   525MB  8.1MB/s   01:05         #D
root@machine-b ~:$ ssh machine-c.vpn                                #E
Last login: Tue Mar 27 12:42:31 2012 from ...                       #E
root@machine-c ~:$ sudo /etc/init.d/redis-server start              #E
Starting Redis server...                                            #E
root@machine-c ~:$ exit
root@machine-b ~:$ redis-cli                                        #F
redis 127.0.0.1:6379> SLAVEOF machine-c.vpn 6379                    #F
OK                                                                  #F
redis 127.0.0.1:6379> QUIT
root@machine-b ~:$ exit
user@vpn-master ~:$
'''


# 代码清单4-5 list_item()函数
def list_item(conn, itemid, sellerid, price):
    inventory = "inventory:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()
    while time.time() < end:
        try:
            pipe.watch(inventory)
            if not pipe.sismember(inventory, itemid):
                pipe.unwatch()
                return None
            pipe.multi()
            pipe.zadd("market:", item, price)
            pipe.srem(inventory, itemid)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
        return False
















