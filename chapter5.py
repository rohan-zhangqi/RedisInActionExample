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




