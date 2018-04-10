import typing as t
from copy import deepcopy
from inspect import Signature, BoundArguments, Parameter

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.utils import JSON_RPC_VERSION


class RpcRequest:
    def __init__(self, data: t.Dict[str, t.Any], silent: bool = False):
        self._data: t.Dict[str, t.Any] = data
        self.silent = silent
        self._error = None
        try:
            self._validate()
        except exceptions.RpcError as e:
            self._error = e

        if self._error and not silent:
            raise self._error

    def __repr__(self):
        failed = bool(self._error)
        try:
            method = self.method
        except:
            method = 'Unparsable'

        try:
            id = self.id
        except:
            id = 'Unparsable'

        return f'{self.__class__.__name__}(id={id} method="{method}" failed={failed})'

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
        return str(self._data.get('method', ''))

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
    def _prepare_call_args(signature: Signature, params: t.Union[list, t.Dict[str, t.Any]]):
        if isinstance(params, list):
            return RpcRequestProcessor._prepare_call_args_from_list(signature, params)
        return RpcRequestProcessor._prepare_call_args_from_dict(signature, params)

    @staticmethod
    def _prepare_call_args_from_list(signature: Signature, params: list) -> t.Tuple[list, t.Dict[str, t.Any]]:
        ba: BoundArguments = signature.bind(*params)
        ba.apply_defaults()
        return ba.args, ba.kwargs

    @staticmethod
    def _prepare_call_args_from_dict(
            signature: Signature, params: t.Dict[str, t.Any]
    ) -> t.Tuple[list, t.Dict[str, t.Any]]:
        """
        Creates populated ``args`` and ``kwargs`` from params bound to signature.
        Processes every parameter in ``Signature.parameters`` and takes corresponding values from ``params``.

        NOTE: Just cannot simply call ``signature.bind(**params)`` because it cannot take VAR_POSITIONAL from dict by
        a key name. But we need this features since there's no way to pass ``*args`` to rpc call with dict params.
        Also it cannot do the same unpack thing with VAR_KEYWORD passed as a key in ``params``.

        Links:
            - https://www.python.org/dev/peps/pep-0457/#id14

        :param signature: a signature of a callable
        :param params: a dict with callable arguments
        :return: a tuple with args and kwargs
        """
        sentinel = object()
        params, args, kwargs = deepcopy(params), [], {}

        for name, parameter in signature.parameters.items():
            parameter: Parameter = parameter
            value = params.pop(name, sentinel)
            if value is sentinel:
                value = parameter.default

            # Positional-only parameters don't accept default values according to PEP
            if parameter.kind is Parameter.POSITIONAL_ONLY:
                if value is Parameter.empty:
                    raise TypeError(f'You must specify `{name}` argument')
                args.append(value)

            elif parameter.kind is Parameter.POSITIONAL_OR_KEYWORD:
                if value is Parameter.empty:
                    # should not raise here
                    # raise TypeError(f'You must specify `{name}` argument')
                    # Example:
                    #     def srem(self, key, member, *members):
                    # User may want to specify only ``members`` arg and it's ok for this signature
                    # If something is incorrect Signature.bind should perform a final check
                    continue
                args.append(value)

            elif parameter.kind is Parameter.VAR_POSITIONAL:
                if value is Parameter.empty:  # user may not pass *args
                    continue
                if not isinstance(value, list):
                    raise TypeError(f'`{name}` must be a list')
                args += value

            elif parameter.kind is Parameter.KEYWORD_ONLY:
                if value is Parameter.empty:
                    continue
                kwargs[name] = value

            elif parameter.kind is Parameter.VAR_KEYWORD:
                if value is Parameter.empty:
                    continue
                if not isinstance(value, dict):
                    raise TypeError(f'Keyword arguments passed in the variable `{name}` must be a dict')
                kwargs.update(value)

            else:
                raise TypeError(f'Unknown type `{parameter.kind.name}` for parameter {name}')

        # let Signature.bind do the rest
        ba: BoundArguments = signature.bind(*args, **kwargs, **params)
        ba.apply_defaults()
        return ba.args, ba.kwargs

    def apply(self, rpc_request: RpcRequest):
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

    def process(self, rpc_request: RpcRequest) -> t.Dict[str, t.Any]:
        return {
            'id': rpc_request.id,
            'jsonrpc': rpc_request.jsonrpc,
            'result': self.apply(rpc_request),
        }


class RpcBatchRequestProcessor:
    def __init__(self, instance):
        self._instance = instance
        self._processor = RpcRequestProcessor(instance)

    def process(self, rpc_batch_request: RpcBatchRequest):
        results = []
        for rpc_request in rpc_batch_request.requests:
            if not rpc_request.error:
                results.append(self._processor.process(rpc_request))
            else:
                results.append(rpc_request.error.as_dict())
        return results
