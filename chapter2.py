import time
import json
import urllib.parse


# 代码清单2-1 check_token()函数
def check_token(conn, token):
    return conn.hget('login:', token)


# 代码清单2-2 update_token()函数
def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        # 只保留最新的25个商品
        conn.zremrangebyrank('viewed:' + token, 0, -26)


# 代码清单2-3 clean_sessions()函数
QUIT = False
LIMIT = 10000000


def clean_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size < LIMIT:
            time.sleep(1)
            continue
        end_index = min(size - LIMIT, 100)
        tokens = conn.zrange('recent:', 0, end_index - 1)

        # []为python的列表，使用append()方法添加列表项
        session_keys = []
        for token in tokens:
            session_keys.append('viewed:' + token)

        # *号语法可以直接将一连串的多个参数传入函数里面，而不必先对这些参数进行解包（unpack）
        conn.delete(*session_keys)
        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)


# 代码清单2-4 add_to_cart()函数
# 添加到购物车
def add_to_cart(conn, session, item, count):
    if count <= 0:
        conn.hrem('cart:' + session, item)
    else:
        conn.hset('cart:' + session, item, count)


# 代码清单2-5 clean_full_sessions()函数
# 清理旧会话的同时把旧会话对应用户的购物车也一并删除
def clean_full_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size - LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index - 1)

        session_keys = []
        for sess in sessions:
            session_keys.append('viewed' + sess)
            session_keys.append('cart:' + sess)
        conn.delete(*session_keys)
        conn.hdel('login:', *sessions)
        conn.zrem('recent:', *sessions)


# 代码清单2-6 cache_request()函数
def cache_request(conn, request, callback):
    if not can_cache(conn, request):
        return callback(request)
    page_key = 'cache:' + hash_request(request)
    content = conn.get(page_key)

    if not content:
        content = callback(request)
        # 定义：Setex命令为指定的key设置值及其过期时间。如果key已经存在，SETEX命令将会替换旧的值。
        # 语法：SETEX KEY_NAME TIMEOUT VALUE
        conn.setex(page_key, content, 300)
    return content


def hash_request(request):
    return str(hash(request))


# 代码清单2-7 schedule_row_cache()函数
def schedule_row_cache(conn, row_id, delay):
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:', row_id, time.time())


# 代码清单2-8 守护进程函数cache_rows()
def cache_rows(conn):
    while not QUIT:
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(.05)
            continue
        row_id = next[0][1]

        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:' + row_id)
            continue
        row = Inventory.get(row_id)
        conn.zadd('schedule:', row_id, now + delay)
        conn.set('inv:' + row_id, json.dumps(row.to_dict()))


class Inventory(object):
    def __init__(self, id):
        self.id = id

    @classmethod
    def get(cls, id):
        return Inventory(id)

    def to_dict(self):
        return {'id': self.id, 'data': 'data to cache...', 'cached': time.time()}


# 代码清单2-9 修改后的update_token()函数
def update_token_modified(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        # 只保留最新的25个商品
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        conn.zincrby('viewed:', item, -1)


# 代码清单2-10 守护进程函数rescale_viewed()
def rescale_viewed(conn):
    while not QUIT:
        conn.zremrangebyrank('viewed:', 0, -20001)
        conn.zinterstore('viewed:', {'viewed:': .5})
        time.sleep(300)


# 代码清单2-11 can_cache()函数
def can_cache(conn, request):
    item_id = extract_item_id(request)
    if not item_id or is_dynamic(request):
        return False
    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000


def extract_item_id(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]


def is_dynamic(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.urlparse(parsed.query)
    return '_' in query

