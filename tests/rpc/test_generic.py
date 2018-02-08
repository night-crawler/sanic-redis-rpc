from inspect import Signature

import pytest

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc import handler
from tests.utils import SampleRpcObject, mk_rpc_bundle

pytestmark = pytest.mark.generic


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.request, pytest.mark.single]

    def test___validate__raises(self):
        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({"jsonrpc": "qwe"})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest({'id': 1, 'jsonrpc': '2.0', 'method': ''})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcRequest([1])


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcBatchRequestTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.request, pytest.mark.batch]

    def test___validate__raises(self):
        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcBatchRequest({})
        with pytest.raises(exceptions.RpcInvalidRequestError):
            handler.RpcBatchRequest([])

    def test___validate(self):
        br = handler.RpcBatchRequest([
            mk_rpc_bundle('add', [1, 2]),
            mk_rpc_bundle('nested.add_many', [1, 2, 3, 4, 5])
        ])
        assert br.count == 2


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestProcessorTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.request, pytest.mark.processor]

    def test__get_method(self):
        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        sample = SampleRpcObject(10)
        processor = handler.RpcRequestProcessor(sample)
        assert processor._get_method(rpc_request) == sample.add

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(mk_rpc_bundle('base', {'a': 1, 'b': 1.1, 'make_negative': True}))
            processor._get_method(rpc_request)

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(mk_rpc_bundle('nothing', {'qwe': 1}))
            processor._get_method(rpc_request)

    def test__prepare_call_args(self):
        sample = SampleRpcObject(10)

        # test dict args
        processor = handler.RpcRequestProcessor(SampleRpcObject(10))
        ca = processor._prepare_call_args(
            Signature.from_callable(sample.add),
            {'a': 1, 'b': 1.1, 'make_negative': True}
        )
        assert ca == ((1, 1.1), {'make_negative': True})

        ca = processor._prepare_call_args(
            Signature.from_callable(sample.nested.add_many),
            {'a': 1, 'b': 1.1, 'c': 2}
        )
        assert ca == ((1, 1.1), {'c': 2})

    def test__process(self):
        rpc_request = handler.RpcRequest(mk_rpc_bundle('kwonly', [1, 2, 3, 4]))
        processor = handler.RpcRequestProcessor(SampleRpcObject(10))
        with pytest.raises(exceptions.RpcInvalidParamsError):
            processor.process(rpc_request)

        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        assert processor.process(rpc_request) == 8

    def test__response(self):
        processor = handler.RpcRequestProcessor(SampleRpcObject(10))
        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        assert processor.response(rpc_request)['id'] == rpc_request.id