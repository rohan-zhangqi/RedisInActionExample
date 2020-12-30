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
    # flags	标志位，用于控制正则表达式的匹配方式，如：是否区分大小写，多行匹配等等。
    for match in WORDS_RE.finditer(content.lower()):
        # group(num=0)：匹配的整个表达式的字符串，group()可以一次输入多个组号，在这种情况下它将返回一个包含那些组所对应值的元组。
        # 用于移除字符串头尾指定的字符（默认为空格或换行符）或字符序列。
        # 注意：该方法只能删除开头或是结尾的字符，不能删除中间部分的字符。
        # 语法：str.strip([chars])
        # 参数：chars -- 移除字符串头尾指定的字符序列。
        # 返回值：返回移除字符串头尾指定的字符生成的新字符串。
        word = match.group().strip("'")
        if len(word) >= 2:
            words.add(word)
    # 差集
    return words - STOP_WORDS


def index_document(conn, docid, content):
    words = tokenize(content)
    pipeline = conn.pipeline(True)
    for word in words:
        pipeline.sadd('idx:' + word, docid)
    return len(pipeline.execute())


# 代码清单7-2 对集合进行交集计算、并集计算和差集计算的辅助函数
def _set_common(conn, method, names, ttl=30, execute=True):
    id = str(uuid.uuid4())
    pipeline = conn.pipeline(True) if execute else conn
    # 增加前缀
    names = ['idx:' + name for name in names]
    '''
        描述：用于返回一个对象属性值
        语法：getattr(object, name[, default])
        参数：
            object -- 对象。
            name -- 字符串，对象属性。
            default -- 默认返回值，如果不提供该参数，在没有对应属性时，将触发 AttributeError。
        返回值：返回对象属性值。
        可用于实现工厂模式
        代码解释：
        getattr(pipeline, method)拿到func，接着执行func()
        对*names取交集/并集/差集，并存于idx:id中
    '''
    getattr(pipeline, method)('idx:' + id, *names)
    pipeline.expire('idx:' + id, ttl)
    if execute:
        pipeline.execute()
    return id


def intersect(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sinterstore', items, ttl, _execute)


def union(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sunionstore', items, ttl, _execute) 


def difference(conn, items, ttl=30, _execute=True):
    return _set_common(conn, 'sdiffstore', items, ttl, _execute)


# 代码清单7-3 搜索查询语句的语法分析函数
QUERY_RE = re.compile("[+-]?[a-z']{2,}")


def parse(query):
    # 用于存储不需要的单词，用“-”号表示
    unwanted = set()
    # 用于存储需要执行交集计算的单词
    all = []
    # 用于存储目前已发现的同义词，用“+”号表示，同义词要统一放在给定词后面
    current = set()
    for match in QUERY_RE.finditer(query.lower()):
        word = match.group()
        prefix = word[:1]
        if prefix in '+-':
            word = word[1:]
        else:
            prefix = None

        word = word.strip("'")
        if len(word) < 2 or word in STOP_WORDS:
            continue

        if prefix == '-':
            unwanted.add(word)
            continue

        if current and not prefix:
            all.append(list(current))
            current = set()
        current.add(word)

    if current:
        all.append(list(current))

    return all, list(unwanted)


# 代码清单7-4 用于分析查询语句并搜索文档的函数
def parse_and_search(conn, query, ttl=30):
    all, unwanted = parse(query)
    if not all:
        return None

    to_intersect = []
    for syn in all:
        if len(syn) > 1:
            to_intersect.append(union(conn, syn, ttl=ttl))
        else:
            to_intersect.append(syn[0])

        if len(to_intersect) > 1:
            intersect_result = intersect(conn, to_intersect, ttl=ttl)
        else:
            intersect_result = to_intersect[0]

        if unwanted:
            '''
                描述：用于将指定对象插入列表的指定位置。
                语法：list.insert(index, obj)
                参数：
                    index -- 对象 obj 需要插入的索引位置。
                    obj -- 要插入列表中的对象。
                返回值：该方法没有返回值，但会在列表指定位置插入对象。
            '''
            unwanted.insert(0, intersect_result)
            return difference(conn, unwanted, ttl=ttl)

        return intersect_result


# 代码清单7-5 分析查询语句然后进行搜索，并对搜索结果进行排序的函数
def search_and_sort(conn, query, id=None, ttl=300, sort="-updated",
                    start=0, num=20):
    desc = sort.startswith('-')
    sort = sort.lstrip('-')
    # 根据“kb:doc:*”指定一个文档的集合，sort为文档中的属性
    by = "kb:doc:*->" + sort
    alpha = sort not in ('updated', 'id', 'created')

    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

    pipeline = conn.pipeline(True)
    pipeline.scard('idx:' + id)

    """
        未找到代码出处，大致可参考Redis的sort函数

        def sort(self, name, start=None, num=None, by=None, get=None,
                desc=False, alpha=False, store=None, groups=False):

        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where in the key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.
    """
    pipeline.sort('idx:' + id, by=by, alpha=alpha, desc=desc,
                  start=start, num=num)
    results = pipeline.execute()

    return results[0], results[1], id
