import collections
import typing as t
from inspect import Signature, BoundArguments

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.utils import JSON_RPC_VERSION


class RpcRequest:
    def __init__(self, data: t.Dict[str, t.Any]):
        self._data: t.Dict[str, t.Any] = data
        self._validate()

    def _validate(self):
        if not isinstance(self._data, collections.Mapping):
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


class RpcRequestProcessor:
    def __init__(self, rpc_request: RpcRequest, instance):
        self._rpc_request = rpc_request
        self._instance = instance
        self._method = self.get_method()
        self._signature = Signature.from_callable(self._method)

    @property
    def method_path(self) -> t.List[str]:
        return self._rpc_request.method_path

    @property
    def root_instance(self):
        return self._instance

    def get_method(self) -> t.Callable:
        val = self.root_instance
        for path_item in self.method_path:
            val = getattr(val, path_item, None)
            if val is None:
                raise exceptions.RpcMethodNotFoundError(
                    id=self._rpc_request.id, data=self._rpc_request.method
                )

        if not callable(val):
            raise exceptions.RpcMethodNotFoundError(
                id=self._rpc_request.id, data=self._rpc_request.method,
                message=f'{val} in {self._rpc_request.method} is not callable'
            )
        return val

    def prepare_call_args(self):
        args, kwargs = [], {}
        if isinstance(self._rpc_request.params, list):
            args = self._rpc_request.params
        else:
            kwargs = self._rpc_request.params

        try:
            ba: BoundArguments = self._signature.bind(*args, **kwargs)
        except TypeError as e:
            raise exceptions.RpcInvalidParamsError(id=self._rpc_request.id, data=str(e))

        ba.apply_defaults()
        return ba.args, ba.kwargs

    def apply(self):
        args, kwargs = self.prepare_call_args()
        result = self._method(*args, **kwargs)
        return result

    def process(self):
        return {
            'id': self._rpc_request.id,
            'jsonrpc': self._rpc_request.jsonrpc,
            'result': self.apply(),
        }
