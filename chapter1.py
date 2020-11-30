import time
import unittest

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432


# 代码清单1-6 article_vote函数 PS：pycharm的编程格式规定，定义函数前必须有两行空行。
def article_vote(conn, user, article):
    # conn：Redis连接
    # user：用户标识
    # article：格式为article:id
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:  # 一周前的文章不允许投票 PS：行尾注释需要空两个空格
        return
    '''
    partition() 方法
    描述：partition() 方法用来根据指定的分隔符将字符串进行分割。
    如果字符串包含指定的分隔符，则返回一个三元的元组，第一个为分隔符左边的子串，第二个为分隔符本身，第三个为分隔符右边的子串。
    语法：str.partition(separator)
    注意：匹配多个分隔符时，按第一个分隔符切割

    元组-负索引：表示从末尾开始，-1 表示最后一个项目，-2 表示倒数第二个项目，依此类推。
    '''
    # 从article:id标识符（identifier）里面取出文章的ID
    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):  # 对该文章未投过票的用户投票，才能增加得分和投票数
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)


# 代码清单1-7 post_article()函数
def post_article(conn, user, title, link):
    # str()函数将对象转化为适于人阅读的形式。
    # 生产新的文章ID
    article_id = str(conn.incr('article:'))

    voted = 'voted:' + article_id
    conn.sadd(voted, user)
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    now = time.time()
    article = 'article:' + article_id
    # HMSET：同时将多个filed-value对设置到HASH的key中
    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })
    conn.zadd('score:', article, now + VOTE_SCORE)
    conn.zadd('time:', article, now)
    return article_id


# 代码清单1-8 get_articles()函数
ARTICLES_PER_PAGE = 25


def get_articles(conn, page, order='score:'):
    # order='score'：默认值参数
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    # ZREVRANGE：返回有序集合中指定区间内的成员，通过索引，分数从高到低
    ids = conn.zrevrange(order, start, end)
    articles = []
    for id in ids:
        # HGETALL：获取在哈希表中指定key的所有字段和值
        # id中存储的格式是'article:id'
        article_data = conn.hgetall(id)
        article_data['id'] = id
        articles.append(article_data)
    return articles


# 代码清单1-9 add_remove_groups()函数
def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)


# 代码清单1-10 get_group_articles()函数
def get_group_articles(conn, group, page, order='score:'):
    key = order + group
    if not conn.exists(key):
        # python不需要指定key的数量
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)
