import pytest
from sanic import Sanic

from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper

pytestmark = pytest.mark.utils


@pytest.fixture
def pools_wrapper(app: Sanic):
    wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options)
    return wrapper


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisPoolsShareWrapperTest:
    pytestmark = [pytest.mark.redis, pytest.mark.wrapper]

    async def test___create_pool(self, app: Sanic, loop):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options, loop=loop)
        res = await wrapper._create_pool('redis_0')
        assert res

    async def test__get_pool(self, pools_wrapper: RedisPoolsShareWrapper):
        pool = await pools_wrapper._get_pool('redis_0')
        assert pool
        assert len(pools_wrapper._pool_map) == 1

    async def test__initialize_pools(self, pools_wrapper: RedisPoolsShareWrapper):
        await pools_wrapper._initialize_pools()
        assert len(pools_wrapper._pool_map) == 2

    async def test__close(self, pools_wrapper: RedisPoolsShareWrapper):
        await pools_wrapper._initialize_pools()
        await pools_wrapper.close()

        for pool in pools_wrapper._pool_map.values():
            assert pool.closed

    async def test__get_status(self, pools_wrapper: RedisPoolsShareWrapper):
        await pools_wrapper._initialize_pools()
        res = await pools_wrapper.get_status()
        assert res[0]['name'] == 'redis_0'
        assert res[1]['name'] == 'redis_1'

    async def test__get_service_pool_name(self, pools_wrapper: RedisPoolsShareWrapper):
        assert pools_wrapper._get_service_pool_name() == 'redis_0', \
            'Ensure default service redis is the first one'
        pools_wrapper._redis_connections_options['redis_1']['service'] = True
        assert pools_wrapper._get_service_pool_name() == 'redis_1'

    async def test__get_service_redis(self, pools_wrapper: RedisPoolsShareWrapper):
        service_redis = await pools_wrapper.get_service_redis()
        redis_0 = await pools_wrapper.get_redis('redis_0')

        assert service_redis is redis_0, 'Ensure default service redis is the first one'
