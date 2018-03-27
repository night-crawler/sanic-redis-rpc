import random
from itertools import permutations, chain

import aioredis
import pytest

from sanic_redis_rpc.exceptions import WrongNumberError, WrongPageSizeError, PageNotFoundError
from sanic_redis_rpc.key_manager import KeyManager

comb_parts = sorted({
    'anonymous',
    'bool',
    'bower',
    'chai',
    'cheat',
    'chicken'
})

keys_lookup_pattern = 'sanic-redis-rpc*'
mk_key = lambda combination: 'sanic-redis-rpc-test:%s' % ':'.join(combination)
all_combinations = list(permutations(comb_parts, 4))
all_keys = sorted(mk_key(c) for c in all_combinations)


async def write_combinations(redis: aioredis.Redis):
    keys = await redis.mget(*all_keys)

    # do nothing, 60sec is enough to pass all tests
    if set(keys) != {None}:
        return

    pipe = redis.pipeline()

    for i, comb in enumerate(all_combinations):
        key_ = mk_key(comb)

        if i % 3 == 0:
            bundle = {random.choice(comb_parts): random.choice(comb_parts) for k in range(len(comb_parts))}
            pipe.hmset_dict(key_, **bundle)
        elif i % 5 == 0:
            bundle = {random.choice(comb_parts): random.randint(1, 10) for k in range(len(comb_parts))}
            pipe.zadd(key_, *chain(
                *[reversed(pair) for pair in bundle.items()]
            ))
        elif i % 7 == 0:
            pipe.sadd(key_, *comb_parts)
        elif i % 11 == 0:
            pipe.lpush(key_, *comb_parts)
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

    async def test__get_sorted_keys(self, key_manager):
        km = await key_manager
        sorted_keys = await km._get_sorted_keys(keys_lookup_pattern)
        assert len(sorted_keys) >= len(all_combinations)

        for natively_sorted_item, sorted_set_item in zip(sorted(list(sorted_keys)), list(sorted_keys)):
            assert natively_sorted_item == sorted_set_item

    async def test__get_page__sorted(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=10)

        page = await km.get_page(search_id, 1, 10)
        assert len(page) == 10
        assert page == all_keys[:10]

        page = await km.get_page(search_id, 2, 10)
        assert len(page) == 10
        assert page == all_keys[10:20]

        page = await km.get_page(search_id, 1, len(all_keys) + 1000000)
        assert len(page) == len(all_keys)
        assert page == all_keys

    async def test__get_page__unsorted(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=False, ttl_seconds=10)
        page = await km.get_page(search_id, 1, 10)
        assert len(page) == 10

        page = await km.get_page(search_id, 2, 10)
        assert len(page) == 10

        page = await km.get_page(search_id, 1, len(all_keys) + 1000000)
        assert len(page) == len(all_keys)

    async def test__get_page__exceptions(self, key_manager):
        km: KeyManager = await key_manager
        with pytest.raises(WrongNumberError):
            await km.get_page('qwe', 0, 100)

        with pytest.raises(WrongPageSizeError):
            await km.get_page('qwe', 1, 0)

        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=10)
        with pytest.raises(PageNotFoundError):
            await km.get_page(search_id, 1000000000, 10)

    async def test___load_more(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=False, ttl_seconds=10)

        await km._load_more(search_id, keys_lookup_pattern, 0, 10)

        results_key = km._mk_results_key(search_id)
        loaded_keys = await km.service_redis.lrange(results_key, 0, -1, encoding='utf8')
        assert len(loaded_keys) >= 10

        # retrieve a new info object with new cursor
        info = await km.get_search_info(search_id)
        await km._load_more(search_id, keys_lookup_pattern, info['cursor'], 25)
        loaded_keys = await km.service_redis.lrange(results_key, 0, -1, encoding='utf8')
        assert len(loaded_keys) >= 25

    async def test___load_more__no_results(self, key_manager):
        search_pattern = 'kjh5kjlh34kl5h6klj34h5kl6jh3456*'
        km: KeyManager = await key_manager
        search_id = await km.paginate(search_pattern, sort_keys=False, ttl_seconds=10)

        await km._load_more(search_id, search_pattern, 0, 10)

    async def test__paginate(self, key_manager):
        km: KeyManager = await key_manager

        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=2)
        search_key = km._mk_search_key(search_id)
        assert search_id
        assert (await km.service_redis.ttl(search_key)) >= 1, \
            'Ensure hash with search results will be destroyed'

        bundle = await km.service_redis.hgetall(search_key, encoding='utf8')
        assert bundle
        assert 'results_key' in bundle

    async def test__paginate__sorted(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=2)

        info = await km.get_search_info(search_id)
        assert info['sorted'] == 1
        assert (await km.service_redis.ttl(info['results_key'])) >= 1, \
            'Ensure list with search results will be destroyed'

        keys = await km.service_redis.lrange(info['results_key'], 0, -1)
        assert len(keys), 'sorted results must not be empty'

    async def test__paginate__unsorted(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=False, ttl_seconds=2)

        info = await km.get_search_info(search_id)
        assert info['sorted'] == 0
        assert info['count']
        assert (await km.service_redis.lrange(info['results_key'], 0, -1)) == [], \
            'Ensure there is no keys in unsorted search set'

    async def test__get_search_info(self, key_manager):
        km: KeyManager = await key_manager
        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=1)
        info = await km.get_search_info(search_id)
        assert type(info['cursor']) == int
        assert type(info['sorted']) == int
        assert type(info['ttl_seconds']) == int
        assert type(info['count']) == int

        with pytest.raises(Exception):
            await km.get_search_info('does_not_exists')

    async def test__get_match_count(self, key_manager):
        km = await key_manager
        assert await km._get_match_count(keys_lookup_pattern) >= len(all_combinations)

    async def test__refresh_ttl(self, key_manager):
        km: KeyManager = await key_manager

        search_id = await km.paginate(keys_lookup_pattern, sort_keys=True, ttl_seconds=1)
        refresh_result = await km.refresh_ttl(search_id, 20)
        assert refresh_result == [True, True]
        assert await km.service_redis.ttl(km._mk_search_key(search_id)) >= 19, \
            'Ensure search key TTL updated'
        assert await km.service_redis.ttl(km._mk_results_key(search_id)) >= 19, \
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
        async for k in redis0.iscan(match=keys_lookup_pattern, count=5000):
            print(
                'REDIS-0 DELETE KEY', k,
                await redis0.type(k), await redis0.ttl(k),
                await redis0.delete(k)
            )

        async for k in redis1.iscan(match=keys_lookup_pattern, count=5000):
            print(
                'REDIS-1 DELETE KEY', k,
                await redis1.type(k), await redis1.ttl(k),
                await redis1.delete(k)
            )
