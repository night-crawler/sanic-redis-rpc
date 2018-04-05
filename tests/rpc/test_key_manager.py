import random
from itertools import permutations, chain

import aioredis
import pytest

from sanic_redis_rpc.key_manager.exceptions import WrongNumberError, WrongPageSizeError, PageNotFoundError, SearchIdNotFoundError
from sanic_redis_rpc.key_manager import KeyManager

COMB_PARTS = sorted({
    'anonymous',
    'bool',
    'bower',
    'chai',
    'cheat',
    'chicken'
})

KEYS_LOOKUP_PATTERN = 'sanic-redis-rpc*'
mk_key = lambda combination: 'sanic-redis-rpc-test:%s' % ':'.join(combination)
ALL_COMBINATIONS = list(permutations(COMB_PARTS, 4))
ALL_KEYS = sorted(mk_key(c) for c in ALL_COMBINATIONS)


async def write_combinations(redis: aioredis.Redis):
    keys = await redis.mget(*ALL_KEYS)

    # do nothing, 60sec is enough to pass all tests
    if set(keys) != {None}:
        return

    pipe = redis.pipeline()

    for i, comb in enumerate(ALL_COMBINATIONS):
        key_ = mk_key(comb)

        if i % 3 == 0:
            bundle = {random.choice(COMB_PARTS): random.choice(COMB_PARTS) for k in range(len(COMB_PARTS))}
            pipe.hmset_dict(key_, **bundle)
        elif i % 5 == 0:
            bundle = {random.choice(COMB_PARTS): random.randint(1, 10) for k in range(len(COMB_PARTS))}
            pipe.zadd(key_, *chain(
                *[reversed(pair) for pair in bundle.items()]
            ))
        elif i % 7 == 0:
            pipe.sadd(key_, *COMB_PARTS)
        elif i % 11 == 0:
            pipe.lpush(key_, *COMB_PARTS)
        else:
            pipe.set(key_, key_)
        pipe.expire(key_, 60)

    return await pipe.execute()


@pytest.fixture
async def key_manager(get_redis):
    redis0: aioredis.Redis = await get_redis('redis_0')
    redis1: aioredis.Redis = await get_redis('redis_1')
    await write_combinations(redis0)
    return KeyManager(redis0, redis1)


# noinspection PyMethodMayBeStatic,PyShadowingNames
class KeyManagerTest:
    pytestmark = [pytest.mark.key_manager]

    async def test__init(self, key_manager):
        assert await key_manager

    async def test___get_sorted_keys(self, key_manager):
        km = await key_manager
        sorted_keys = await km._get_sorted_keys(KEYS_LOOKUP_PATTERN)
        assert len(sorted_keys) >= len(ALL_COMBINATIONS), \
            'Ensure count of keys retrieved from redis cannot be less than count of all combinations'

        for natively_sorted_item, sorted_set_item in zip(sorted(list(sorted_keys)), list(sorted_keys)):
            assert natively_sorted_item == sorted_set_item, \
                'Each item from redis must be equal to the locally sorted one'

    async def test__get_page__sorted(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=10)

        page = await km.get_page(search['id'], 1, 10)
        assert len(page) == 10, 'The count of retrieved items must match the request'
        assert page == ALL_KEYS[:10], 'Remote redis slicing must be equal to the local'

        page = await km.get_page(search['id'], 2, 10)
        assert len(page) == 10
        assert page == ALL_KEYS[10:20]

        page = await km.get_page(search['id'], 1, len(ALL_KEYS) + 1000000)
        assert len(page) >= len(ALL_KEYS), 'Must load all keys since page size is bigger than total key count'

    async def test__get_page__unsorted(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=False, ttl_seconds=10, redis_name='redis_0')

        page = await km.get_page(search['id'], 1, 10)
        assert len(page) == 10

        page = await km.get_page(search['id'], 2, 10)
        assert len(page) == 10

        page = await km.get_page(search['id'], 1, len(ALL_KEYS) + 1000000)
        assert len(page) >= len(ALL_KEYS)

    async def test__get_page__exceptions(self, key_manager):
        km: KeyManager = await key_manager

        # must not be tolerant to pointless options
        with pytest.raises(WrongNumberError, message='Must raise if page number below zero'):
            await km.get_page('qwe', 0, 100)

        with pytest.raises(WrongPageSizeError, message='Must raise if page size is not positive'):
            await km.get_page('qwe', 1, 0)

        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=10)
        
        with pytest.raises(PageNotFoundError, message='Must raise if requested page is too far from reality'):
            await km.get_page(search['id'], 1000000000, 10)

    async def test___load_more(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=False, ttl_seconds=10, redis_name='redis_0')

        await km._load_more(search['id'], KEYS_LOOKUP_PATTERN, 0, 10)

        results_key = km._mk_results_key(search['id'])
        loaded_keys = await km.service_redis.lrange(results_key, 0, -1, encoding='utf8')
        assert len(loaded_keys) >= 10, 'Loaded key count cannot be less than requested'

        # retrieve a new info object with a new cursor
        info = await km.get_search_info(search['id'])
        await km._load_more(search['id'], KEYS_LOOKUP_PATTERN, info['cursor'], 25)
        loaded_keys = await km.service_redis.lrange(results_key, 0, -1, encoding='utf8')
        assert len(loaded_keys) >= 25, 'Loaded key count cannot be less than requested'

    async def test___load_more__no_results(self, key_manager):
        search_pattern = 'kjh5kjlh34kl5h6klj34h5kl6jh3456*'
        km: KeyManager = await key_manager
        search = await km.search(search_pattern, sort_keys=False, ttl_seconds=10, redis_name='redis_0')
        
        # just check if it doesn't hang or fail
        await km._load_more(search['id'], search_pattern, 0, 10)

    async def test__paginate(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=2)
        search_key = km._mk_search_key(search['id'])
        assert search['id']
        assert (await km.service_redis.ttl(search_key)) >= 1, \
            'Ensure hash with search results will be destroyed'

        bundle = await km.service_redis.hgetall(search_key, encoding='utf8')
        assert bundle
        assert 'results_key' in bundle

    async def test__paginate__sorted(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=2)

        info = await km.get_search_info(search['id'])
        assert info['sorted'] == 1
        assert (await km.service_redis.ttl(info['results_key'])) >= 1, \
            'Ensure list with search results will be destroyed'

        keys = await km.service_redis.lrange(info['results_key'], 0, -1)
        assert len(keys), 'sorted results must not be empty'

    async def test__paginate__unsorted(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=False, ttl_seconds=2, redis_name='redis_0')

        info = await km.get_search_info(search['id'])
        assert info['sorted'] == 0
        assert info['count']
        assert (await km.service_redis.lrange(info['results_key'], 0, -1)) == [], \
            'Ensure there is no keys in unsorted search set'

    async def test__get_search_info(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=1)
        info = await km.get_search_info(search['id'])

        # check type casting
        assert type(info['cursor']) == int
        assert type(info['sorted']) == int
        assert type(info['ttl_seconds']) == int
        assert type(info['count']) == int

        with pytest.raises(SearchIdNotFoundError):
            await km.get_search_info('does_not_exists')

    async def test__get_match_count(self, key_manager):
        km = await key_manager
        assert await km._get_match_count(KEYS_LOOKUP_PATTERN) >= len(ALL_COMBINATIONS)

    async def test__refresh_ttl(self, key_manager):
        km: KeyManager = await key_manager
        search = await km.search(KEYS_LOOKUP_PATTERN, sort_keys=True, ttl_seconds=1)
        
        refresh_result = await km.refresh_ttl(search['id'], 20)
        assert refresh_result == [True, True]
        assert await km.service_redis.ttl(km._mk_search_key(search['id'])) >= 19, \
            'Ensure search key TTL updated'
        assert await km.service_redis.ttl(km._mk_results_key(search['id'])) >= 19, \
            'Ensure results key TTL updated'

    @pytest.mark.skip
    async def test__get_all_keys_set(self, key_manager):
        km = await key_manager
        sorted_keys = await km._get_sorted_keys('*')
        assert len(sorted_keys)

    @pytest.mark.skip
    async def test__cleanup(self, get_redis):
        redis0: aioredis.Redis = await get_redis('redis_0')
        redis1: aioredis.Redis = await get_redis('redis_1')
        async for k in redis0.iscan(match=KEYS_LOOKUP_PATTERN, count=5000):
            print(
                'REDIS-0 DELETE KEY', k,
                await redis0.type(k), await redis0.ttl(k),
                await redis0.delete(k)
            )

        async for k in redis1.iscan(match=KEYS_LOOKUP_PATTERN, count=5000):
            print(
                'REDIS-1 DELETE KEY', k,
                await redis1.type(k), await redis1.ttl(k),
                await redis1.delete(k)
            )
