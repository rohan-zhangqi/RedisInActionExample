import math
import re
import unittest
import uuid

import redis


# 代码清单7-1 对文档进行标记化处理并创建索引的函数
# python三引号允许一个字符串跨多行，字符串中可以包含换行符、制表符以及其他特殊字符。
STOP_WORDS = set('''able about across after all almost also am among
an and any are as at be because been but by can cannot could dear did
do does either else ever every for from get got had has have he her
hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us
wants was we were what when where which while who whom why will with
would yet you your'''.split())

# 认定单词只能由英文字母和单引号（‘）组成，并且每个单词至少要两个字符长
WORDS_RE = re.compile("[a-z']{2,}")


def tokenize(content):
    words = set()
    # 在字符串中找到正则表达式所匹配的所有子串，并把它们作为一个迭代器返回。
    # 语法：
    # re.finditer(pattern, string, flags=0)
    # 参数：
    # pattern：匹配的正则表达式
    # string：要匹配的字符串。
    # flags	标志位，用于控制正则表达式的匹配方式，如：是否区分大小写，多行匹配等等。参见：正则表达式修饰符 - 可选标志
    # 例子：
    for match in WORDS_RE.finditer(content.lower()):
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    return words - STOP_WORDS


def index_document(conn, docid, content):
    words = tokenize(content)
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, docid)
    return len(pipeline.execute())