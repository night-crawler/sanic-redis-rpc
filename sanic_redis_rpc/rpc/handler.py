import typing as t
from asyncio import Future, coroutine

from sanic.request import Request

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.generic import RpcRequest, RpcRequestProcessor
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper, load_json


class RedisRpcRequest(RpcRequest):
    def _validate(self):
        super(RedisRpcRequest, self)._validate()
        if len(self.method_path) < 2:
            raise exceptions.RpcInvalidParamsError(
                id=self.id, data=self.params,
                message='Pool name should be specified in `method`, e.g. `redis_0.get`'
            )


class RedisRpcRequestProcessor(RpcRequestProcessor):
    @property
    def method_path(self) -> t.List[str]:
        # skip pool
        return super(RedisRpcRequestProcessor, self).method_path[1:]


class RpcBatchRequest:
    def __init__(self, data):
        self._data: t.List[t.Dict[str, t.Any]] = data
        self._validate()

    def _validate(self):
        pass


class RedisRpcHandler:
    def __init__(self, request: Request):
        self._pools_wrapper: RedisPoolsShareWrapper = request.app._pools_wrapper
        self._data = load_json(request.body)

        if isinstance(self._data, list):
            self._rpc_request = RpcBatchRequest(self._data)
        else:
            self._rpc_request = RedisRpcRequest(self._data)

    async def handle_single(self):
        try:
            redis_instance = await self._pools_wrapper.get_redis(self._rpc_request.method_path[0])
        except KeyError:
            raise exceptions.RpcInvalidParamsError(
                id=self._rpc_request.id, data=self._rpc_request.params,
                message=f'Pool with name `{self._rpc_request.method_path[0]}` does not exist'
            )

        processor = RedisRpcRequestProcessor(self._rpc_request, redis_instance)
        result = processor.apply()
        # print('POZIDAALSKDLKAJS KLDFASD', type(result), result)
        # if isinstance(result, Future):
        #     print('FUTURE!!!@#!@')
        return result

    async def handle(self):
        pass
