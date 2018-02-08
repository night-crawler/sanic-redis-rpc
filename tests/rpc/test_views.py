import pytest

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc import handler
from tests.utils import mk_rpc_bundle, AttrObject

pytestmark = pytest.mark.views


# noinspection PyMethodMayBeStatic,PyShadowingNames
class BlueprintTest:
    pytestmark = [pytest.mark.blueprint, pytest.mark.rpc]

    async def test__status(self, test_cli):
        resp = await test_cli.get('/status')
        assert resp.status == 200
        resp_json = await resp.json()
        assert 'redis_0' in resp_json
        assert 'redis_1' in resp_json

    async def test__parser_error(self, test_cli):
        resp = await test_cli.post('/', data="""{"trash": [1,}""")
        resp_json = await resp.json()
        assert resp_json['error']['code'] == -32700


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisRpcHandlerTest:
    pytestmark = [pytest.mark.blueprint, pytest.mark.redis, pytest.mark.handler]

    async def test_parse_error(self, app):
        with pytest.raises(exceptions.RpcParseError):
            handler.RedisRpcHandler(AttrObject(body='{qwe', app=app))

    async def test__handle_single(self, app):
        body = mk_rpc_bundle('redis_0.set', {'key': 'qwe', 'value': 1}, dump=True)
        h = handler.RedisRpcHandler(AttrObject(body=body, app=app))
        res = await h.handle_single()
        assert res['result'] is True
        assert res['id'] in body
