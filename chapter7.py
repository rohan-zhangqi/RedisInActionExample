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


# 代码清单7-6 更新之后的函数可以进行搜索并同时基于投票数量和更新时间进行排序
def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0, start=0, num=20, desc=True):
    if id and not conn.expire(id, ttl):
        id = None

    if not id:
        id = parse_and_search(conn, query, ttl=ttl)

        scored_search = {
            id: 0,
            'sort:update': update,
            'sort:votes': vote
        }

        id = zintersect(conn, scored_search, ttl)

    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:' + id)
    if desc:
        pipeline.zrevrange('idx:' + id, start, start + num - 1)
    else:
        pipeline.zrange('idx:' + id, start, start + num - 1)

    results = pipeline.execute()

    return results[0], results[1], id


# 代码清单7-7 负责对有序集合执行交集计算和并集计算的辅助函数
def _zset_common(conn, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())
    execute = kw.pop('_execute', True)
    pipeline = conn.pipeline(True) if execute else conn
    for key in scores.keys():
        scores['idx:' + key] = scores.pop(key)
    getattr(pipeline, method)('idx:' + id, scores, **kw)
    pipeline.execute('idx:' + id, ttl)
    if execute:
        pipeline.execute()
    return id


def zintersect(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)


def zunion(conn, items, ttl=30, **kw):
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)


# 代码清单7-8 将字符串转换为数字分值的函数
def string_to_score(string, ignore_case=False):
    if ignore_case:
        string = string.lower()
    # 1
    # ord() 函数是 chr() 函数（对于8位的ASCII字符串）或 unichr() 函数（对于Unicode对象）的配对函数，
    # 它以一个字符（长度为1的字符串）作为参数，返回对应的 ASCII 数值，或者 Unicode 数值，如果所给的 Unicode
    # 字符超出了你的 Python 定义范围，则会引发一个 TypeError 的异常。
    # 语法：ord(c)
    # 参数：c -- 字符
    # 返回值：返回值是对应的十进制整数。
    # 2
    # map()会根据提供的函数对指定序列做映射。
    # 第一个参数 function 以参数序列中的每一个元素调用 function 函数，返回包含每次 function 函数返回值的新列表。
    # 参数：
    # function -- 函数
    # iterable -- 一个或多个序列
    # 返回值：
    # Python 2.x 返回列表。
    # Python 3.x 返回迭代器
    # 3
    # 描述：list() 方法用于将元组转换为列表。
    # 注：元组与列表是非常类似的，区别在于元组的元素值不能修改，元组是放在括号中，列表是放于方括号中。
    # 语法：list(tup)
    # 参数：tup -- 要转换为列表的元组。
    # 返回值:返回列表。
    pieces = list(map(ord, string[:6]))
    while len(pieces) < 6:
        pieces.append(-1)

    score = 0
    for piece in pieces:
        score = score * 257 + piece + 1

    return score * 2 + (len(string) > 6)


# 代码清单7-9 根据广告的CPC信息和CPA信息计算广告eCPM的函数
def cpc_to_ecpm(views, clicks, cpc):
    # views：展示次数
    # clicks：点击次数
    # cpc：每次点击价格
    # clicks / views = 点击通过率
    return 1000. * cpc * clicks / views


def cpa_to_ecpm(views, actions, cpa):
    # views：展示次数
    # actions：动作执行次数
    # cpa：每次执行动作价格
    # actions / views = 动作执行概率
    return 1000. * cpa * actions / views


# 代码清单7-10 一个广告索引函数，被索引的广告会基于位置和广告内容进行定向
AVERAGE_PER_1K = {}

TO_ECPM = {
    'cpc': cpc_to_ecpm,
    'cpa': cpa_to_ecpm,
    # 描述：python 使用 lambda 来创建匿名函数。
    # lambda只是一个表达式，函数体比def简单很多。
    # lambda的主体是一个表达式，而不是一个代码块。仅仅能在lambda表达式中封装有限的逻辑进去。
    # lambda函数拥有自己的命名空间，且不能访问自有参数列表之外或全局命名空间里的参数。
    # 虽然lambda函数看起来只能写一行，却不等同于C或C++的内联函数，后者的目的是调用小函数时不占用栈内存从而增加运行效率。
    # 语法：lambda [arg1 [,arg2,.....argn]]:expression
    'cpm': lambda *args : args[-1],
}


def index_ad(conn, id, locations, content, type, value):
    # type：广告类型
    # value：基础价格
    pipeline = conn.pipeline(True)
    # 描述：isinstance() 函数来判断一个对象是否是一个已知的类型，类似 type()。
    # 语法：isinstance(object, classinfo)
    # 参数：
    # object -- 实例对象。
    # classinfo -- 可以是直接或间接类名、基本类型或者由它们组成的元组。
    # 返回值：如果对象的类型与参数二的类型（classinfo）相同则返回True，否则返回False。
    if not isinstance(type, bytes):
        type = type.encode('latin-1')

    for location in locations:
        # 将广告ID添加到所有相关的位置集合里面
        pipeline.sadd('idx:req:' + location, id)

    # return a set()
    words = tokenize(content)
    for word in words:
        pipeline.zadd('idx:' + word, {id: 0})

    rvalue = TO_ECPM[type](
        1000, AVERAGE_PER_1K.get(type, 1), value)
    # 记录广告类型
    pipeline.hset('type:', id, type)
    # 将广告的eCPM添加到一个记录了所有广告的eCPM的有序集合里
    pipeline.zadd('idx:ad:value:', {id: rvalue})
    # 将广告的基础价格（base value）添加到一个记录了所有广告的基础价格的有序集合里面
    pipeline.zadd('ad:base_value:', {id: value})
    # 把能够对广告进行定向的单词全部记录起来
    pipeline.sadd('terms:' + id, *list(words))
    pipeline.execute()


# 代码清单7-11 通过位置和页面内容附加值实现广告定向操作
def target_ads(conn, locations, content):
    pipeline = conn.pipeline(True)
    matched_ads, base_ecpm = match_location(pipeline, locations)
    words, targeted_ads = finish_scoring(
        pipeline, matched_ads, base_ecpm, content)

    pipeline.incr('ads:served:')
    pipeline.zrevrange('idx:' + targeted_ads, 0, 0)
    target_id, targeted_ad = pipeline.execute()[-2:]

    if not targeted_ad:
        return None, None

    ad_id = targeted_ad[0]
    record_targeting_result(conn, target_id, ad_id, words)

    return target_id, ad_id


# 代码清单7-12 基于位置执行广告定向操作的辅助函数
def match_location(pipe, locations):
    required = ['req:' + loc for loc in locations]
    matched_ads = union(pipe, required, ttl=300, _execute=False)
    return matched_ads, zintersect(pipe, {matched_ads: 0, 'ad:value:': 1}, _execute=False)


# 代码清单7-13 计算包含了内容匹配附加值的广告eCPM
def finish_scoring(pipe, matched, base, content):
    bonus_ecpm = {}
    words = tokenize(content)
    for word in words:
        word_bonus = zintersect(
            pipe, {matched: 0, word: 1}, _execute=False)
        bonus_ecpm[word_bonus] = 1

    if bonus_ecpm:
        minimum = zunion(
            pipe, bonus_ecpm, aggregate='MIN', _execute=False)
        maximum = zunion(
            pipe, bonus_ecpm, aggregate='MAX', _execute=False)

        return words, zunion(
            pipe, {base: 1, minimum: .5, maximum: .5}, _execute=False)
    return words, base


# 代码清单7-14 负责在广告定向操作执行完毕之后记录执行结果的函数
def record_targeting_result(conn, target_id, ad_id, words):
    pipeline = conn.pipeline(True)

    terms = conn.smembers(b'terms:' + ad_id)
    matched = list(words & terms)
    if matched:
        matched_key = 'terms:matched:%s' % target_id
        pipeline.sadd(matched_key, *matched)
        pipeline.expire(matched_key, 900)

    type = conn.hget('type:', ad_id)
    pipeline.incr('type:%s:views:' % type)
    for word in matched:
        pipeline.zincrby('views:%s' % ad_id, 1, word)
    pipeline.zincrby('views:%s' % ad_id, 1, '')

    if not pipeline.execute()[-1] % 100:
        update_cpms(conn, ad_id)


# 代码清单7-15 记录广告呗点击信息的函数
def record_click(conn, target_id, ad_id, action=False):
    pipeline = conn.pipeline(True)
    click_key = 'clicks:%s' % ad_id

    match_key = 'terms:matched:%s' % target_id

    type = conn.hget('type:', ad_id)
    if type == 'cpa':
        pipeline.expire(match_key, 900)
        if action:
            click_key = 'actions:%s' % ad_id

    if action and type == 'cpa':
        pipeline.incr('type:%s:actions:' % type)
    else:
        pipeline.incr('type:%s:clicks:' % type)

    matched = list(conn.smembers(match_key))
    matched.append('')
    for word in matched:
        pipeline.zincrby(click_key, 1, word)
    pipeline.execute()

    update_cpms(conn, ad_id)


# 代码清单7-16 负责对广告eCPM以及每个单词的eCPM附加值进行更新的函数
def update_cpms(conn, ad_id):
    pipeline = conn.pipeline(True)
    pipeline.hget('type:', ad_id)
    pipeline.zscore('ad:base_value:', ad_id)
    pipeline.smembers(b'terms:' + ad_id)
    type, base_value, words = pipeline.execute()

    which = 'clicks'
    if type == 'cpa':
        which = 'actions'

    pipeline.get('type:%s:views:' % type)
    pipeline.get('type:%s:%s' % (type, which))
    type_views, type_clicks = pipeline.execute()
    AVERAGE_PER_1K[type] = (
        1000. * int(type_clicks or '1') / int(type_views or '1'))

    if type == 'cpm':
        return

    view_key = 'views:%s' % ad_id
    click_key = '%s:%s' % (which, ad_id)

    to_ecpm = TO_ECPM[type]

    pipeline.zscore(view_key, '')
    pipeline.zscore(click_key, '')
    ad_views, ad_clicks = pipeline.execute()
    if (ad_clicks or 0) < 1:
        ad_ecpm = conn.zscore('idx:ad:value:', ad_id)
    else:
        ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)
        pipeline.zadd('idx:ad:value:', {ad_id: ad_ecpm})

    for word in words:
        pipeline.zscore(view_key, word)
        pipeline.zscore(click_key, word)
        views, clicks = pipeline.execute()[-2:]

        if (clicks or 0) < 1:
            continue

        word_ecpm = to_ecpm(views or 1, clicks or 0, base_value)
        bonus = word_ecpm - ad_ecpm
        pipeline.zadd('idx:' + word, {ad_id: bonus})
    pipeline.execute()
