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

        # TODO
        inp = open(os.path.join(path, fname), 'rb')
        if fname == current_file:
            inp.seek(int(offset, 10))
        else:
            offset = 0

        current_file = None

        for lno, line in enumerate(inp):
            callback(pipe, line)
            offset += int(offset) + len(line)
            if not (lno + 1) % 1000:
                update_progress()

        update_progress()
        inp.close()
