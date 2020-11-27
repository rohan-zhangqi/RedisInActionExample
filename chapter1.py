import time
import unittest

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432


# pycharm的编程格式规定，定义函数前必须有两行空行。
def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time:', article) < cutoff:  # 一周前的文章不允许投票 PS：行尾注释需要空两个空格
        return
    '''
    partition() 方法
    描述：partition() 方法用来根据指定的分隔符将字符串进行分割。
    如果字符串包含指定的分隔符，则返回一个三元的元组，第一个为分隔符左边的子串，第二个为分隔符本身，第三个为分隔符右边的子串。
    语法：str.partition(separator)

    元组-负索引：表示从末尾开始，-1 表示最后一个项目，-2 表示倒数第二个项目，依此类推。
    '''
    # 从article:id标识符（identifier）里面取出文章的ID
    article_id = article.partition(':')[-1]
    if conn.sadd('voted:' + article_id, user):
        conn.zincrby('score:', article, VOTE_SCORE)
        conn.hincrby(article, 'votes', 1)
