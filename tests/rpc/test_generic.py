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
    def test__get_method(self):
        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        sample = SampleRpcObject(10)
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        assert processor.get_method() == sample.add

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(mk_rpc_bundle('base', {'a': 1, 'b': 1.1, 'make_negative': True}))
            processor = handler.RpcRequestProcessor(rpc_request, sample)
            processor.get_method()

        with pytest.raises(exceptions.RpcMethodNotFoundError):
            rpc_request = handler.RpcRequest(mk_rpc_bundle('nothing', {'qwe': 1}))
            processor = handler.RpcRequestProcessor(rpc_request, sample)
            processor.get_method()

    def test__prepare_call_args(self):
        sample = SampleRpcObject(10)

        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        ca = processor.prepare_call_args()
        assert ca == ((1, 1.1), {'make_negative': True})

        rpc_request = handler.RpcRequest(mk_rpc_bundle('nested.add_many', {'a': 1, 'b': 1.1, 'c': 2}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        ca = processor.prepare_call_args()
        assert ca == ((1, 1.1), {'c': 2})

        rpc_request = handler.RpcRequest(mk_rpc_bundle('kwonly', [1, 2, 3, 4]))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        with pytest.raises(exceptions.RpcInvalidParamsError):
            processor.prepare_call_args()

    def test__apply(self):
        sample = SampleRpcObject(10)
        rpc_request = handler.RpcRequest(mk_rpc_bundle('add', {'a': 1, 'b': 1.1, 'make_negative': True}))
        processor = handler.RpcRequestProcessor(rpc_request, sample)
        assert processor.apply() == 8
