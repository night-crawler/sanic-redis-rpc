from pprint import pprint

import pytest
from sanic import Sanic

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc import redis_rpc
from sanic_redis_rpc.rpc.generic import RpcBatchRequest
from sanic_redis_rpc.rpc.redis_rpc import RedisRpcBatchProcessor, RedisRpcRequest
from tests.utils import mk_rpc_bundle, AttrObject


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RedisRpcTest:
    pytestmark = [pytest.mark.blueprint, pytest.mark.redis, pytest.mark.handler]

    async def test_parse_error(self, app: Sanic):
        with pytest.raises(exceptions.RpcParseError):
            await redis_rpc.RedisRpc(app._pools_wrapper).handle(AttrObject(body='{qwe', app=app))

    async def test__handle_single(self, app: Sanic):
        rpc = redis_rpc.RedisRpc(app._pools_wrapper)
        rpc_request_bundle = mk_rpc_bundle('redis_0.set', {'key': 'qwe', 'value': 1})
        res = await rpc.handle_single(rpc_request_bundle)
        assert res['result'] is True
        assert res['id'] == rpc_request_bundle['id']

    async def test__handle_batch(self, app: Sanic):
        rpc = redis_rpc.RedisRpc(app._pools_wrapper)
        rpc_batch_request_bundle = [
            mk_rpc_bundle('redis_0.set', {'key': 'qwe1', 'value': 1}),
            mk_rpc_bundle('redis_0.set', {'key': 'qwe2', 'value': 2})
        ]
        res = await rpc.handle_batch(rpc_batch_request_bundle)
        assert len(res) == 2


# noinspection PyMethodMayBeStatic
class RedisRpcBatchProcessorTest:
    def test__reorder_requests_by_pool_name(self):
        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.set', {'key': 'qwe1', 'value': 1}),
            mk_rpc_bundle('redis_1.set', {'key': 'qwe1', 'value': 100}),
            mk_rpc_bundle('redis_0.set', {'key': 'qwe2', 'value': 1}),
            mk_rpc_bundle('redis_1.set', {'key': 'qwe2', 'value': 100}),
        ], request_cls=RedisRpcRequest)

        mapping = RedisRpcBatchProcessor._reorder_requests_by_pool_name(br)
        assert len(mapping['redis_0']) == 2
        assert len(mapping['redis_1']) == 2

    def test__validate_pool_tasks(self):
        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.multi_exec', []),
            mk_rpc_bundle('redis_0.multi_exec', []),
        ], request_cls=RedisRpcRequest)

        declined = RedisRpcBatchProcessor._validate_pool_tasks(br.requests)
        assert len(declined) == 2, 'Ensure no double transactions'

        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.pipeline', []),
            mk_rpc_bundle('redis_0.pipeline', []),
        ], request_cls=RedisRpcRequest)

        declined = RedisRpcBatchProcessor._validate_pool_tasks(br.requests)
        assert len(declined) == 2, 'Ensure no double pipelines'

        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.set', []),
            mk_rpc_bundle('redis_0.pipeline', []),
        ], request_cls=RedisRpcRequest)

        declined = RedisRpcBatchProcessor._validate_pool_tasks(br.requests)
        assert len(declined) == 2, 'Ensure pipeline should be first'

        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.set', []),
            mk_rpc_bundle('redis_0.multi_exec', []),
        ], request_cls=RedisRpcRequest)

        declined = RedisRpcBatchProcessor._validate_pool_tasks(br.requests)
        assert len(declined) == 2, 'Ensure transaction should be first'

        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.pipeline', []),
            {'id': 1, 'method': 2, 'jsonrpc': '2.0'},
        ], request_cls=RedisRpcRequest)

        declined = RedisRpcBatchProcessor._validate_pool_tasks(br.requests)
        assert not declined, 'Ensure failed requests are accepted in pipeline'

    async def test__process_pool_tasks(self, app: Sanic):
        processor = RedisRpcBatchProcessor(app._pools_wrapper)
        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.pipeline', []),
            {'id': 1, 'method': 2, 'jsonrpc': '2.0'},  # add some fails
            mk_rpc_bundle('redis_0.set', {'key': 'qwe1', 'value': 1}),
            mk_rpc_bundle('redis_0.set', {'key': 'qwe2', 'value': 2}),
            mk_rpc_bundle('redis_0.set', {'key': 'qwe3', 'value': 3}),
            mk_rpc_bundle('redis_0.get', {'key': 'qwe1'}),
            mk_rpc_bundle('redis_0.get', {'key': 'qwe1', 'encoding': 'lol'}),
        ], request_cls=RedisRpcRequest)

        res = await processor.process_pool_tasks('redis_0', br.requests)
        errors = sum(1 for r in res if 'error' in r)
        assert errors == 2
        assert len(res) == (len(br.requests) - 1), 'Ensure response count matches to request count'

    async def test__process(self, app: Sanic):
        processor = RedisRpcBatchProcessor(app._pools_wrapper)
        br = RpcBatchRequest([
            mk_rpc_bundle('redis_0.pipeline', []),
            {'id': 1, 'method': 2, 'jsonrpc': '2.0'},  # add some fails
            mk_rpc_bundle('redis_0.set', {'key': 'qwe1', 'value': 1}),
            mk_rpc_bundle('redis_0.set', {'key': 'qwe2', 'value': 2}),
            mk_rpc_bundle('redis_1.set', {'key': 'qwe3', 'value': 3}),
            mk_rpc_bundle('redis_1.get', {'key': 'qwe1'}),
            mk_rpc_bundle('redis_1.get', {'key': 'qwe1', 'encoding': 'lol'}),
        ], request_cls=RedisRpcRequest)

        res = await processor.process(br)
        assert len(res) == len(br.requests) - 1

