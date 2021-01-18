import binascii
import bisect
from datetime import date, timedelta
from collections import defaultdict
import math
import time
import unittest
import uuid
import json
import csv

import redis


def to_bytes(x):
    return x.encode('latin-1') if isinstance(x, str) else x


def to_str(x):
    return x.decode('latin-1') if isinstance(x, bytes) else x


def readblocks(conn, key, blocksize=2**17):
    lb = blocksize
    pos = 0
    while lb == blocksize:
        block = conn.substr(key, pos, pos + blocksize - 1)
        yield block
        lb = len(block)
        pos += lb
    yield b''


# 代码清单9-5 对不同大小的压缩列表进行性能测试的函数
def long_ziplist_performance(conn, key, length, passes, psize):
    conn.delete(key)
    conn.rpush(key, *list(range(length)))
    pipeline = conn.pipeline(False)

    t = time.time()
    for p in range(passes):
        for pi in range(psize):
            pipeline.rpoplpush(key, key)
        pipeline.execute()

    return (passes * psize) / (time.time() - t or .001)


def long_ziplist_index(conn, key, length, passes, psize):
    conn.delete(key)
    conn.rpush(key, *list(range(length)))
    length >>= 1
    pipeline = conn.pipeline(False)
    t = time.time()
    for p in range(passes):
        for pi in range(psize):
            pipeline.lindex(key, length)
        pipeline.execute()
    return (passes * psize) / (time.time() - t or .001)


def long_intset_performance(conn, key, length, passes, psize):
    conn.delete(key)
    conn.sadd(key, *list(range(1000000, 1000000+length)))
    cur = 1000000-1
    pipeline = conn.pipeline(False)
    t = time.time()
    for p in range(passes):
        for pi in range(psize):
            pipeline.spop(key)
            pipeline.sadd(key, cur)
            cur -= 1
        pipeline.execute()
    return (passes * psize) / (time.time() - t or .001)


# 代码清单9-7 根据基础键以及散列包含的键计算出分片键的函数
def shard_key(base, key, total_elements, shard_size):
    if isinstance(key, int) or key.isdigit():
        shard_id = int(str(key), 10) // shard_size
    else:
        key = to_bytes(key)
        shards = 2 * total_elements // shard_size
        shard_id = binascii.crc32(key) % shards
    return "%s:%s" % (base, shard_id)


def shard_hset(conn, base, key, value, total_elements, shard_size):
    shard = shard_key(base, key, total_elements, shard_size)
    return conn.hset(shard, key, value)


def shard_hget(conn, base, key, total_elements, shard_size):
    shard = shard_key(base, key, total_elements, shard_size)
    return conn.hget(shard, key)


TOTAL_SIZE = 320000
SHARD_SIZE = 1024


def import_cities_to_redis(conn, filename):
    for row in csv.reader(open(filename)):
        ...
        shard_hset(
            conn, 'cityid2city:', city_id,
            json.dumps([city, region, country]),
            TOTAL_SIZE, SHARD_SIZE)


def find_city_by_ip(conn, ip_address):
    ...
    data = shard_hget(
        conn, 'cityid2city:', city_id,
        TOTAL_SIZE, SHARD_SIZE)
    return json.loads(data)


def shard_sadd(conn, base, member, total_elements, shard_size):
    shard = shard_key(
        base,
        'x'+str(member), total_elements, shard_size)
    return conn.sadd(shard, member)


SHARD_SIZE = 512


def count_visit(conn, session_id):
    today = date.today()
    key = 'unique:%s' % today.isoformat()
    expected = get_expected(conn, key, today)

    id = int(session_id.replace('-', '')[:15], 16)
    if shard_sadd(conn, key, id, expected, SHARD_SIZE):
        conn.incr(key)


DAILY_EXPECTED = 1000000
EXPECTED = {}


def get_expected(conn, key, today):
    if key in EXPECTED:
        return EXPECTED[key]

    exkey = key + ':expected'
    expected = conn.get(exkey)

    if not expected:
        yesterday = (today - timedelta(days=1)).isoformat()
        expected = conn.get('unique:%s' % yesterday)
        expected = int(expected or DAILY_EXPECTED)

        expected = 2**int(math.ceil(math.log(expected*1.5, 2)))
        if not conn.setnx(exkey, expected):
            expected = conn.get(exkey)

    EXPECTED[key] = int(expected)
    return EXPECTED[key]


COUNTRIES = '''
ABW AFG AGO AIA ALA ALB AND ARE ARG ARM ASM ATA ATF ATG AUS AUT AZE BDI
BEL BEN BES BFA BGD BGR BHR BHS BIH BLM BLR BLZ BMU BOL BRA BRB BRN BTN
BVT BWA CAF CAN CCK CHE CHL CHN CIV CMR COD COG COK COL COM CPV CRI CUB
CUW CXR CYM CYP CZE DEU DJI DMA DNK DOM DZA ECU EGY ERI ESH ESP EST ETH
FIN FJI FLK FRA FRO FSM GAB GBR GEO GGY GHA GIB GIN GLP GMB GNB GNQ GRC
GRD GRL GTM GUF GUM GUY HKG HMD HND HRV HTI HUN IDN IMN IND IOT IRL IRN
IRQ ISL ISR ITA JAM JEY JOR JPN KAZ KEN KGZ KHM KIR KNA KOR KWT LAO LBN
LBR LBY LCA LIE LKA LSO LTU LUX LVA MAC MAF MAR MCO MDA MDG MDV MEX MHL
MKD MLI MLT MMR MNE MNG MNP MOZ MRT MSR MTQ MUS MWI MYS MYT NAM NCL NER
NFK NGA NIC NIU NLD NOR NPL NRU NZL OMN PAK PAN PCN PER PHL PLW PNG POL
PRI PRK PRT PRY PSE PYF QAT REU ROU RUS RWA SAU SDN SEN SGP SGS SHN SJM
SLB SLE SLV SMR SOM SPM SRB SSD STP SUR SVK SVN SWE SWZ SXM SYC SYR TCA
TCD TGO THA TJK TKL TKM TLS TON TTO TUN TUR TUV TWN TZA UGA UKR UMI URY
USA UZB VAT VCT VEN VGB VIR VNM VUT WLF WSM YEM ZAF ZMB ZWE'''.split()

STATES = {
    'CAN':'''AB BC MB NB NL NS NT NU ON PE QC SK YT'''.split(),
    'USA':'''AA AE AK AL AP AR AS AZ CA CO CT DC DE FL FM GA GU HI IA ID
IL IN KS KY LA MA MD ME MH MI MN MO MP MS MT NC ND NE NH NJ NM NV NY OH
OK OR PA PR PW RI SC SD TN TX UT VA VI VT WA WI WV WY'''.split(),
}


def get_code(country, state):
    cindex = bisect.bisect_left(COUNTRIES, country)
    if cindex > len(COUNTRIES) or COUNTRIES[cindex] != country:
        cindex = -1
    cindex += 1

    sindex = -1
    if state and country in STATES:
        states = STATES[country]
        sindex = bisect.bisect_left(states, state)
        if sindex > len(states) or states[sindex] != state:
            sindex = -1
    sindex += 1

    return chr(cindex) + chr(sindex)


USERS_PER_SHARD = 2**20


def set_location(conn, user_id, country, state):
    code = get_code(country, state)

    shard_id, position = divmod(user_id, USERS_PER_SHARD)
    offset = position * 2

    pipe = conn.pipeline(False)
    pipe.setrange('location:%s' % shard_id, offset, code)

    tkey = str(uuid.uuid4())
    pipe.zadd(tkey, {'max': user_id})
    pipe.zunionstore(
        'location:max',
        [tkey, 'location:max'], aggregate='max')
    pipe.delete(tkey)

    pipe.execute()


# 代码清单9-16 对所有用户的位置信息进行聚合计算的函数
def aggregate_location(conn):
    countries = defaultdict(int)
    states = defaultdict(lambda: defaultdict(int))

    max_id = int(conn.zscore('location:max', 'max'))
    max_block = max_id // USERS_PER_SHARD

    for shard_id in range(max_block + 1):
        for block in readblocks(conn, 'location:%s' % shard_id):
            for offset in range(0, len(block)-1, 2):
                code = block[offset: offset + 2]
                update_aggregates(countries, states, [code])

    return countries, states


# 代码清单9-17 将位置编码转换成国家信息或地区信息
def update_aggregates(countries, states, codes):
    for code in codes:
        if len(code) != 2:
            continue

        code = to_str(code)

        country = ord(code[0]) - 1
        state = ord(code[1]) - 1

        if country < 0 or country >= len(COUNTRIES):
            continue

        country = COUNTRIES[country]
        countries[country] += 1

        if country not in STATES:
            continue
        if state < 0 or state >= len(STATES[country]):
            continue

        state = STATES[country][state]
        states[country][state] += 1


def aggregate_location_list(conn, user_ids):
    pipe = conn.pipeline(False)
    countries = defaultdict(int)
    states = defaultdict(lambda: defaultdict(int))

    for i, user_id in enumerate(user_ids):
        shard_id, position = divmod(user_id, USERS_PER_SHARD)
        offset = position * 2

        pipe.substr('location:%s' % shard_id, offset, offset+1)

        if (i+1) % 1000 == 0:
            update_aggregates(countries, states, pipe.execute())

    update_aggregates(countries, states, pipe.execute())

    return countries, states
