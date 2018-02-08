import pytest
from sanic import Sanic

from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper

pytestmark = pytest.mark.utils


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisPoolsShareWrapperTest:
    pytestmark = [pytest.mark.redis]

    async def test___create_pool(self, app: Sanic, loop):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options, loop=loop)
        res = await wrapper._create_pool('redis_0')
        assert res

    async def test__get_pool(self, app: Sanic):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options)
        pool = await wrapper.get_pool('redis_0')
        assert pool
        assert len(wrapper.pool_map) == 1

    async def test__initialize_pools(self, app: Sanic):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options)
        await wrapper.initialize_pools()
        assert len(wrapper.pool_map) == 2

    async def test__close(self, app: Sanic):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options)
        await wrapper.initialize_pools()
        await wrapper.close()

        for pool in wrapper.pool_map.values():
            assert pool.closed

    async def test__get_status(self, app: Sanic):
        wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options)
        await wrapper.initialize_pools()
        res = await wrapper.get_status()
        assert 'redis_0' in res
        assert 'redis_1' in res
