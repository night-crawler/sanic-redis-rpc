from inspect import Signature

import pytest

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.generic import RpcRequest, RpcBatchRequest, RpcRequestProcessor, RpcBatchRequestProcessor
from tests.utils import SampleRpcObject, mk_rpc_bundle

pytestmark = pytest.mark.generic


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.request, pytest.mark.single]

    def test___validate__raises(self):
        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcRequest({})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcRequest({"jsonrpc": "qwe"})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcRequest({'id': 1, 'jsonrpc': '2.0', 'method': ''})

        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcRequest([1])


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcBatchRequestTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.request, pytest.mark.batch]

    def test___validate__raises(self):
        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcBatchRequest({})
        with pytest.raises(exceptions.RpcInvalidRequestError):
            RpcBatchRequest([])

    def test___validate(self):
        br = RpcBatchRequest([
            mk_rpc_bundle('add', [1, 2]),
            mk_rpc_bundle('nested.add_many', [1, 2, 3, 4, 5])
        ])
        assert br.count == 2


# noinspection PyMethodMayBeStatic,PyShadowingNames
class RpcRequestProcessorTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.single, pytest.mark.request, pytest.mark.processor]

    def test__get_method(self):
        rpc_request = RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        sample = SampleRpcObject(10)
        processor = RpcRequestProcessor(sample)
        assert processor._get_method(rpc_request) == sample.add

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = RpcRequest(mk_rpc_bundle('base', {'a': 1, 'b': 1.1, 'make_negative': True}))
            processor._get_method(rpc_request)

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = RpcRequest(mk_rpc_bundle('nothing', {'qwe': 1}))
            processor._get_method(rpc_request)

    def test__prepare_call_args(self):
        sample = SampleRpcObject(10)

        # test dict args
        processor = RpcRequestProcessor(SampleRpcObject(10))
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

    def test__apply(self):
        rpc_request = RpcRequest(mk_rpc_bundle('kwonly', [1, 2, 3, 4]))
        processor = RpcRequestProcessor(SampleRpcObject(10))
        with pytest.raises(exceptions.RpcInvalidParamsError):
            processor.apply(rpc_request)

        rpc_request = RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        assert processor.apply(rpc_request) == 8

    def test__process(self):
        processor = RpcRequestProcessor(SampleRpcObject(10))
        rpc_request = RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        assert processor.process(rpc_request)['id'] == rpc_request.id


class RpcBatchRequestProcessorTest:
    pytestmark = [pytest.mark.rpc, pytest.mark.batch, pytest.mark.request, pytest.mark.processor]

    def test__process(self):
        sample = SampleRpcObject(10)
        br = RpcBatchRequest([
            mk_rpc_bundle('add', [1, 2]),
            mk_rpc_bundle('nested.add_many', [1, 2, 3, 4, 5])
        ])
        processor = RpcBatchRequestProcessor(sample)
        res = processor.process(br)
        assert res[0]['result'] == 7
        assert res[1]['result'] == 115
