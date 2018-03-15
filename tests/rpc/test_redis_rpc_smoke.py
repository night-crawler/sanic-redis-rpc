from base64 import standard_b64encode

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

    async def test_set_test_values(self, app: Sanic):
        redis: aioredis.Redis = await app._pools_wrapper.get_redis('redis_0')
        assert (await redis.set('test::blob:ascii:string', b'I AM THE BLOB!11'))

        assert (await redis.set('test::string:utf8:ascii', 'I AM THE BLOB!11'))
        assert (await redis.set('test::string:utf8:cyr', 'Я БЛОБ'))

        b_jpg = open('./tests/data/sample.jpg', 'rb').read()
        b64_jpg = standard_b64encode(b_jpg)

        b_png = open('./tests/data/sample.png', 'rb').read()
        b64_png = standard_b64encode(b_png)

        b_gif = open('./tests/data/sample.gif', 'rb').read()
        b64_gif = standard_b64encode(b_gif)

        assert (await redis.set('test::blob:bytes:jpg', b_jpg))
        assert (await redis.set('test::blob:b64:jpg', b64_jpg))

        assert (await redis.set('test::blob:bytes:png', b_png))
        assert (await redis.set('test::blob:b64:png', b64_png))

        assert (await redis.set('test::blob:bytes:gif', b_gif))
        assert (await redis.set('test::blob:b64:gif', b64_gif))
