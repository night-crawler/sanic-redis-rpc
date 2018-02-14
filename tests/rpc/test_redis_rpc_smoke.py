import aioredis
import pytest
from sanic import Sanic


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisSmokeTest:
    pytestmark = [pytest.mark.redis, pytest.mark.smoke]

    async def test_nothing(self, app: Sanic):
        redis: aioredis.Redis = await app._pools_wrapper.get_redis('redis_0')

    async def test_smoke_redis_rpc(self, rpc):
        assert (await rpc('/', 'redis_0.set', 'qwe', 1))['result']
        assert (await rpc('/', 'redis_0.config_get'))['result']
        assert (await rpc('/', 'redis_0.info', section='all'))['result']
        assert (await rpc('/', 'redis_0.dbsize'))['result']
        assert (await rpc('/', 'redis_0.client_list'))['result']
        assert (await rpc('/', 'redis_0.client_list'))['result']
        assert 'result' in (await rpc('/', 'redis_0.client_getname'))
