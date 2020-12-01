import time

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


# 代码清单2-4 clean_full_sessions()函数
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
