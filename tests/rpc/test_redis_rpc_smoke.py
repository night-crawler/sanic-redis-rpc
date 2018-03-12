import aioredis
import pytest
from sanic import Sanic
from sanic_redis_rpc.signature_serializer import SignatureSerializer


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisSmokeTest:
    pytestmark = [pytest.mark.redis, pytest.mark.smoke]

    async def test_nothing(self, app: Sanic):
        redis: aioredis.Redis = await app._pools_wrapper.get_redis('redis_0')
        ser = SignatureSerializer(redis.pipeline())

        # execute() from pipeline != redis.execute()
        assert ser.inspect_entity('execute')

    async def test_smoke_redis_rpc(self, rpc):
        assert (await rpc('/', 'redis_0.set', 'qwe', 1))['result']
        assert (await rpc('/', 'redis_0.config_get'))['result']
        assert (await rpc('/', 'redis_0.info', section='all'))['result']
        assert (await rpc('/', 'redis_0.dbsize'))['result']
        assert (await rpc('/', 'redis_0.client_list'))['result']
        assert (await rpc('/', 'redis_0.client_list'))['result']
        assert 'result' in (await rpc('/', 'redis_0.client_getname'))
        assert (await rpc('/', 'redis_0.execute', command='get', args=['qwe']))['result']

    async def test_smoke_batch__cmd_execute(self, batch_rpc):
        # execute() from pipeline != redis.execute()
        res = await batch_rpc(
            '/',
            ['redis_0.execute', {'command': 'get', 'args': ['qwe']}]
        )
        assert 'error' in res
