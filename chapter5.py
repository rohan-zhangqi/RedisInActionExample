import bisect
import contextlib
import csv
from datetime import datetime
import functools
import json
import logging
import random
import threading
import time
import unittest
import uuid

import redis

QUIT = False
SAMPLE_COUNT = 100

# 代码清单5-1 log_recent()函数
# Python字典是一种可变容器模型，且可存储任意类型对象。
# 字典的每个键值key=>value对用冒号:分割，每个键值对之间用逗号,分割，整个字典包括在花括号{}中，格式如下所示：
SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical',
}
# Python字典(Dictionary)update()函数把字典dict2的键/值对更新到dict里
# 语法：dict.update(dict2)
# 参数：dict2 -- 添加到指定字典dict里的字典
# 返回值：该方法没有任何返回值
# Python字典(Dictionary)values()函数以列表返回字典中的所有值
# 语法：dict.values()
# 返回值：返回字典中的所有值
# 遍历语法：
# for iterating_var in sequence:
#    statements(s)
SEVERITY.update((name, name) for name in SEVERITY.values())


def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    # Python字典(Dictionary)get()函数返回指定键的值
    # dict.get(key, default=None)
    # 参数:
    # key -- 字典中要查找的键。
    # default -- 如果指定键的值不存在时，返回该默认值。
    # 返回值：返回指定键的值，如果键不在字典中返回默认值None或者设置的默认值。
    # Python 字典(Dictionary) str() 函数将值转化为适于人阅读的形式，以可打印的字符串表示。
    # 语法：str(dict)
    # 参数：dict -- 字典。
    # 返回值：返回字符串。
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'recent:%s:%s' % (name, severity)

    # Python time asctime() 函数接受时间元组并返回一个可读的形式为"Tue Dec 11 18:07:14 2008"（2008年12月11日 周二18时07分14秒）的24个字符的字符串。
    message = time.asctime() + ' ' + message
    # 当pipe为空时，将conn.pipeline()赋值给pipe
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


# 代码清单5-2 log_common()函数
def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'common:%s:%s' % (name, severity)
    start_key = destination + ':start'
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            # utcnow()：Return the current UTC date and time, with tzinfo None.
            # timetuple()：Return a time.struct_time such as returned by time.localtime().
            now = datetime.utcnow().timetuple()
            # datetime.utcnow()执行结果：datetime.datetime(2020, 12, 18, 0, 23, 42, 486013)
            # datetime.utcnow().timetuple()执行结果：time.struct_time(tm_year=2020, tm_mon=12, tm_mday=18, tm_hour=0, tm_min=23, tm_sec=53, tm_wday=4, tm_yday=353, tm_isdst=-1)

            # now[:4]：从now中取出第1~4个元素
            # datetime()：创建datetime类的实例
            # isoformat()：Return a string representing the date and time in ISO 8601 format
            hour_start = datetime(*now[:4]).isoformat()
            # now[:4]执行结果：(2020, 12, 18, 4)
            # datetime(*now[:4])执行结果：datetime.datetime(2020, 12, 18, 4, 0)
            # datetime(*now[:4]).isoformat()执行结果：'2020-12-18T04:00:00'

            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                # os.rename()方法用于命名文件或目录，从 src 到 dst,如果dst是一个存在的目录, 将抛出OSError。
                # 语法：os.rename(src, dst)
                # 参数：
                # src -- 要修改的目录名
                # dst -- 修改后的目录名
                # 返回值：该方法没有返回值
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)

            pipe.zincrby(destination, message)
            log_recent(pipe, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue


# 代码清单5-3 update_counter()函数
PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(conn, name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now / prec) * prec
        hash = '%s:%s' % (prec, name)
        pipe.zadd('known:', hash, 0)
        pipe.hincrby('count:' + hash, pnow, count)
    pipe.execute()


# 代码清单5-4 get_count()函数
def get_count(conn, name, precision):
    hash = '%s:%s' % (precision, name)
    data = conn.hgetall('count:' + hash)
    to_return = []
    for key, value in data.iteritems():
        to_return.append(int(key), int(value))
    to_return.sort()
    return to_return


# 代码清单5-5 clean_counters()函数
def clean_counters(conn):
    pipe = conn.pipeline(True)
    passes = 0
    while not QUIT:
        start = time.time()
        index = 0
        while index < conn.zcard('known:'):
            hash = conn.zrange('known:', index, index)
            index += 1
            if not hash:
                break
            hash = hash[0]
            prec = int(hash.partition(':')[0])
            # " / "就表示 浮点数除法，返回浮点结果;" // "表示整数除法
            bprec = int(prec // 60) or 1
            if passes % bprec:
                continue
            hkey = 'count:' + hash
            cutoff = time.time() - SAMPLE_COUNT * prec
            # map() 会根据提供的函数对指定序列做映射。
            # 第一个参数 function 以参数序列中的每一个元素调用 function 函数，返回包含每次 function 函数返回值的新列表。
            # 语法：map(function, iterable, ...)
            # 参数：
            # function -- 函数
            # iterable -- 一个或多个序列
            # 返回值：
            # Python 2.x 返回列表。
            # Python 3.x 返回迭代器。
            samples = map(int, conn.hkeys(hkey))
            samples.sort()
            remove = bisect.bisect_right(samples, cutoff)
            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', hash)
                            pipe.execute
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass

        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        time.sleep(max(60 - duration, 1))


# 代码清单5-6 update_stats()函数
def update_stats(conn, context, type, value, timeout=5):
    destination = 'stats:%s:%s' % (context, type)
    start_key = destination + ':start'
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()
            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)

            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            pipe.zadd(tkey1, 'min', value)
            pipe.zadd(tkey2, 'max', value)
            pipe.zunionstore(destination, [destination, tkey1], aggregate='min')
            pipe.zunionstore(destination, [destination, tkey2], aggregate='max')
            pipe.delete(tkey1, tkey2)
            pipe.zincrby(destination, 'count')
            pipe.zincrby(destination, 'sum', value)
            pipe.zincrby(destination, 'sumsq', value * value)

            return pipe.execute()[-3:]
        except redis.exceptions.WatchError:
            continue


# 代码清单5-7 get_status()函数
def get_status(conn, context, type):
    key = 'stats:%s:%s' % (context, type)
    # 用于创建一个字典
    # 语法：
    # class dict(**kwarg)
    # class dict(mapping, **kwarg)
    # class dict(iterable, **kwarg)
    # 参数：
    # **kwargs -- 关键字
    # mapping -- 元素的容器。
    # iterable -- 可迭代对象。
    # 返回值：返回一个字典。
    data = dict(conn.zrange(key, 0, -1, withscores=True))
    data['average'] = data['sum'] / data['count']
    # 幂 - 返回x的y次幂
    numerator = data['sumsq'] - data['sum'] ** 2 / data['count']
    data['stddev'] = (numerator / (data['count'] - 1 or 1)) ** .5
    return data


# 代码清单5-8 access_time()上下文管理器
# 创建上下文管理实际就是创建一个类，添加__enter__和__exit__方法。
# 上下文管理器工具模块contextlib，它是通过生成器实现的，我们不需要再创建类以及__enter__和__exit__这两个方法
# yield之前就是__init__中的代码块；yield之后是__exit__中的代码块
@contextlib.contextmanager
def access_time(conn, context):
    start = time.time()
    yield

    delta = time.time() - start
    stats = update_stats(conn, context, 'AccessTime', delta)
    average = stats[1] / stats[0]

    pipe = conn.pipeline(True)
    pipe.zadd('slowest:AccessTime', context, average)
    pipe.zremragebyrank('slowest:AccessTime', 0, -101)
    pipe.execute()


# 代码清单5-9 ip_to_score()函数
def ip_to_score(ip_address):
    score = 0
    for v in ip_address.split('.'):
        # 将v转换为十进制整型
        score = score * 256 + int(v, 10)
    return score


# 代码清单5-10 import_ips_to_redis()函数
def import_ips_to_redis(conn, filename):
    csv_file = csv.reader(open(filename, 'rb'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ''
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else:
            continue

        city_id = row[2] + '_' + str(count)
        conn.zadd('ip2cityid:', city_id, start_ip)































