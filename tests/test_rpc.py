import typing as t
import json
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic.request import Request
from sanic.websocket import WebSocketProtocol

from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper
from sanic_redis_rpc.conf import configure
from sanic_redis_rpc.rpc.views import sanic_redis_rpc_bp
from sanic_redis_rpc.rpc import handler
from sanic_redis_rpc.rpc import exceptions

pytestmark = pytest.mark.rpc


@pytest.yield_fixture
def app(loop):
    _app = Sanic('test_sanic_app')
    _app.blueprint(sanic_redis_rpc_bp)
    _app = configure(
        _app,
        {
            'REDIS_0': 'redis://localhost:6379?db=0',
            'REDIS_1': 'redis://localhost:6379?db=1'
        }
    )
    _app._pools_wrapper = RedisPoolsShareWrapper(_app.config.redis_connections_options, loop)
    yield _app


# noinspection PyShadowingNames
@pytest.fixture
def test_cli(loop, app, test_client):
    return loop.run_until_complete(test_client(app, protocol=WebSocketProtocol))


def _mk_rpc_bundle(method: str, params, dump=False):
    bundle = {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': uuid4().hex
    }
    if not dump:
        return bundle
    return json.dumps(bundle)


# noinspection PyShadowingNames
@pytest.fixture
def rpc(test_cli):
    async def rpc_call(base_url, method: str, *args):
        return await test_cli.post(
            base_url,
            json=_mk_rpc_bundle(method, args),
        )

    return rpc_call


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisPoolsShareWrapperTest:
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


# noinspection PyMethodMayBeStatic,PyShadowingNames
@pytest.mark.blueprint
class BlueprintTest:
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


class AttrObject:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestTest:
    def test___validate(self):
        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({"jsonrpc": "qwe"})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({'id': 1, 'jsonrpc': '2.0', 'method': ''})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest([1])


class NestedSample:
    def __init__(self):
        self.base = 100

    def find_truth(self, *args, **kwargs):
        return all(args) and all(kwargs.values())

    def add_many(self, a, b, *args, **kwargs):
        return self.base + a + b + sum(args) + sum(kwargs.values())


class SampleRpcObject:
    def __init__(self, base: int):
        self.base = base
        self.nested = NestedSample()

    def add(self, a: int, b: t.Union[int, float], *, make_negative: bool=True) -> int:
        """
        Adds a to b and makes in negative.
        :param a: an integer
        :param b: an integer or float
        :param make_negative: a flag
        :return: result
        """
        return self.base + int(a + b) * (-1 * int(make_negative))

    def add_many(self, a, b, *args, **kwargs):
        return self.base + a + b + sum(args) + sum(kwargs.values())

    def kwonly(self, a, b, *, trash: bool=True):
        pass


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestProcessorTest:
    def test__get_method(self):
        rpc_request = handler.RpcRequest(_mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        sample = SampleRpcObject(10)
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        assert processor.get_method() == sample.add

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(_mk_rpc_bundle('base', {'a': 1, 'b': 1.1, 'make_negative': True}))
            processor = handler.RpcRequestProcessor(rpc_request, sample)
            processor.get_method()

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(_mk_rpc_bundle('nothing', {'qwe': 1}))
            processor = handler.RpcRequestProcessor(rpc_request, sample)
            processor.get_method()

    def test__prepare_call_args(self):
        sample = SampleRpcObject(10)

        rpc_request = handler.RpcRequest(_mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        ca = processor.prepare_call_args()
        assert ca == ((1, 1.1), {'make_negative': True})

        rpc_request = handler.RpcRequest(_mk_rpc_bundle('nested.add_many', {'a': 1, 'b': 1.1, 'c': 2}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        ca = processor.prepare_call_args()
        assert ca == ((1, 1.1), {'c': 2})

        rpc_request = handler.RpcRequest(_mk_rpc_bundle('kwonly', [1, 2, 3, 4]))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        with pytest.raises(exceptions.RpcInvalidParamsError):
            processor.prepare_call_args()

    def test__apply(self):
        sample = SampleRpcObject(10)
        rpc_request = handler.RpcRequest(_mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        assert processor.apply() == 8


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisRpcHandlerTest:
    async def test_parse_error(self, app):
        with pytest.raises(exceptions.RpcParseError):
            handler.RedisRpcHandler(AttrObject(body='{qwe', app=app))

    async def test__handle_single(self, app):
        body = _mk_rpc_bundle('redis_0.set', {'key': 'qwe', 'value': 1}, dump=True)
        h = handler.RedisRpcHandler(AttrObject(body=body, app=app))
        res = h.handle_single()
        print('!!!', type(res))
