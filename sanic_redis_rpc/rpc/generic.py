import typing as t
from inspect import Signature, BoundArguments

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.utils import JSON_RPC_VERSION


class RpcRequest:
    def __init__(self, data: t.Dict[str, t.Any], silent: bool=False):
        self._data: t.Dict[str, t.Any] = data
        self.silent = silent
        self._error = None
        try:
            self._validate()
        except exceptions.RpcError as e:
            self._error = e

        if self._error and not silent:
            raise self._error

    def _validate(self):
        if not isinstance(self._data, dict):
            raise exceptions.RpcInvalidRequestError(id=None, message='Single RPC call should be a mapping')

        if not self._data:
            raise exceptions.RpcInvalidRequestError(id=self.id, message='Request is empty')

        if self.jsonrpc != JSON_RPC_VERSION:
            raise exceptions.RpcInvalidRequestError(id=self.id, message='Wrong jsonrpc version')

        if not isinstance(self.method, str):
            raise exceptions.RpcInvalidRequestError(id=self.id, message='Method should be a string')

        if not self.method:
            raise exceptions.RpcInvalidRequestError(id=self.id, message='No method was specified')

    @property
    def error(self):
        return self._error

    @property
    def id(self):
        return self._data.get('id', None)

    @property
    def jsonrpc(self) -> str:
        return self._data.get('jsonrpc')

    @property
    def is_notify(self) -> bool:
        return 'id' in self._data

    @property
    def params(self):
        return self._data.get('params', [])

    @property
    def method(self) -> str:
        return self._data.get('method')

    @property
    def method_path(self) -> t.List[str]:
        return self.method.split('.')

    @property
    def method_name(self) -> str:
        return self.method_path[-1]


class RpcBatchRequest:
    def __init__(self, data, request_cls: t.Type[RpcRequest] = RpcRequest):
        self._data: t.List[t.Dict[str, t.Any]] = data
        self._request_cls = request_cls
        self._rpc_requests = self._validate()

    def _validate(self):
        if not isinstance(self._data, list):
            raise exceptions.RpcInvalidRequestError(message='Batch RPC call should be a list')

        if not self._data:
            raise exceptions.RpcInvalidRequestError(message='Request is empty')

        return [
            self._request_cls(single_rpc_call_bundle, silent=True)
            for single_rpc_call_bundle in self._data
        ]

    @property
    def count(self) -> int:
        return len(self._rpc_requests)

    @property
    def requests(self) -> t.List[RpcRequest]:
        return self._rpc_requests


class RpcRequestProcessor:
    def __init__(self, instance):
        self._instance = instance

    @staticmethod
    def _get_signature(method: t.Callable) -> Signature:
        return Signature.from_callable(method)

    @staticmethod
    def _prepare_call_args(signature: Signature, params):
        args, kwargs = [], {}
        if isinstance(params, list):
            args = params
        else:
            kwargs = params

        ba: BoundArguments = signature.bind(*args, **kwargs)
        ba.apply_defaults()
        return ba.args, ba.kwargs

    def process(self, rpc_request: RpcRequest):
        method = self._get_method(rpc_request)
        signature = self._get_signature(method)

        try:
            args, kwargs = self._prepare_call_args(signature, rpc_request.params)
        except TypeError as e:
            raise exceptions.RpcInvalidParamsError(id=rpc_request.id, data=str(e))

        result = method(*args, **kwargs)
        return result

    def _get_method_path(self, rpc_request: RpcRequest):
        return rpc_request.method_path

    def _get_method(self, rpc_request: RpcRequest) -> t.Callable:
        val = self._instance
        for path_item in self._get_method_path(rpc_request):
            val = getattr(val, path_item, None)
            if val is None:
                raise exceptions.RpcMethodNotFoundError(
                    id=rpc_request.id, data=rpc_request.method,
                    message=f'Method path`{path_item}` is empty in {rpc_request.method_path}'
                )

        if not callable(val):
            raise exceptions.RpcMethodNotFoundError(
                id=rpc_request.id, data=rpc_request.method,
                message=f'{val} in {rpc_request.method} is not callable'
            )
        return val

    def response(self, rpc_request: RpcRequest) -> t.Dict[str, t.Any]:
        return {
            'id': rpc_request.id,
            'jsonrpc': rpc_request.jsonrpc,
            'result': self.process(rpc_request),
        }
