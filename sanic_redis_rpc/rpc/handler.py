import typing as t
import asyncio

from sanic.request import Request

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.generic import RpcRequest, RpcRequestProcessor, RpcBatchRequest
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper, load_json


class RedisRpcRequest(RpcRequest):
    def _validate(self):
        super(RedisRpcRequest, self)._validate()
        if len(self.method_path) < 2:
            raise exceptions.RpcInvalidParamsError(
                id=self.id, data=self.params,
                message='Pool name should be specified in `method`, e.g. `redis_0.get`'
            )

    @property
    def pool_name(self):
        return self.method_path[0]


class RedisRpcRequestProcessor(RpcRequestProcessor):
    @property
    def method_path(self) -> t.List[str]:
        # skip pool
        return super(RedisRpcRequestProcessor, self).method_path[1:]

    async def response(self):
        result = self.apply()
        if asyncio.iscoroutine(result):
            result = await result

        return {
            'id': self._rpc_request.id,
            'jsonrpc': self._rpc_request.jsonrpc,
            'result': result,
        }


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
            redis_instance = await self._pools_wrapper.get_redis(self._rpc_request.pool_name)
        except KeyError:
            raise exceptions.RpcInvalidParamsError(
                id=self._rpc_request.id, data=self._rpc_request.params,
                message=f'Pool with name `{self._rpc_request.pool_name}` does not exist'
            )
        processor = RedisRpcRequestProcessor(self._rpc_request, redis_instance)
        return await processor.response()

    async def handle(self):
        pass
