import asyncio
import base64
import typing as t
from collections import OrderedDict
from itertools import chain

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
    def _get_method_path(self, rpc_request: RpcRequest):
        # skip pool name
        return super()._get_method_path(rpc_request)[1:]

    async def process(self, rpc_request: RpcRequest):
        result = self.apply(rpc_request)
        if asyncio.iscoroutine(result):
            result = await result

        if isinstance(result, bytes):
            result = base64.encodebytes(result).decode()

        return {
            'id': rpc_request.id,
            'jsonrpc': rpc_request.jsonrpc,
            'result': result,
        }


class RedisRpcBatchProcessor:
    def __init__(self, pools_wrapper: RedisPoolsShareWrapper):
        self._pools_wrapper = pools_wrapper

    @staticmethod
    def _reorder_requests_by_pool_name(rpc_batch_request: RpcBatchRequest) -> t.Dict[str, t.List[RedisRpcRequest]]:
        mapping = OrderedDict()
        for rpc_request in rpc_batch_request.requests:
            mapping.setdefault(rpc_request.pool_name, [])
            mapping[rpc_request.pool_name].append(rpc_request)
        return mapping

    @staticmethod
    def _decline_requests(
            rpc_requests: t.List[RedisRpcRequest],
            exception: t.Type[exceptions.RpcError] = exceptions.RpcInvalidParamsError,
            message='Fail'):
        return [
            exception(id=rpc_request.id, message=message).as_dict()
            for rpc_request in rpc_requests
        ]

    @staticmethod
    def _validate_pool_tasks(rpc_requests: t.List[RedisRpcRequest]):
        multis, pipelines, failed = [], [], []
        for i, rpc_request in enumerate(rpc_requests):
            if rpc_request.error:
                failed.append(i)
                continue
            rpc_request.method_name.lower() == 'multi_exec' and multis.append(i)
            rpc_request.method_name.lower() == 'pipeline' and pipelines.append(i)

        # fail all requests
        if len(multis) >= 2 or len(pipelines) >= 2:
            return RedisRpcBatchProcessor._decline_requests(
                rpc_requests, message='pipeline/multi should be presented once')
        if (multis and multis[0] != 0) or (pipelines and pipelines[0] != 0):
            return RedisRpcBatchProcessor._decline_requests(
                rpc_requests, message='pipeline/multi method should be first')
        if failed and multis:
            return RedisRpcBatchProcessor._decline_requests(
                rpc_requests, message='Failed requests should not exist in transaction mode')

        return None

    async def process_pool_tasks(self, pool_name: str, rpc_requests: t.List[RedisRpcRequest]):
        declined = self._validate_pool_tasks(rpc_requests)
        if declined:
            return declined

        try:
            redis = await self._pools_wrapper.get_redis(pool_name)
        except KeyError:
            return self._decline_requests(
                rpc_requests, exceptions.RpcMethodNotFoundError,
                message=f'Pool with name `{pool_name}` does not exist'
            )

        if rpc_requests[0].method_name == 'multi_exec':
            instance = redis.multi_exec()
            rpc_requests = rpc_requests[1:]
        elif rpc_requests[0].method_name == 'pipeline':
            instance = redis.pipeline()
            rpc_requests = rpc_requests[1:]
        else:
            instance = redis.pipeline()

        processor = RedisRpcRequestProcessor(instance)
        pre_failed_requests = []
        pending_requests = []
        for rpc_request in rpc_requests:
            if rpc_request.error:
                pre_failed_requests.append(rpc_request.error.as_dict())
                continue
            pending_requests.append(rpc_request)
            processor.apply(rpc_request)

        results = pre_failed_requests

        for request, response in zip(pending_requests, await instance.execute(return_exceptions=True)):
            if isinstance(response, Exception):
                results.append(
                    exceptions.RpcError(id=request.id, message=repr(response)).as_dict()
                )
                continue

            if isinstance(response, bytes):
                response = base64.encodebytes(response).decode()

            results.append({
                'id': request.id,
                'jsonrpc': request.jsonrpc,
                'result': response
            })

        return results

    async def process(self, rpc_batch_request: RpcBatchRequest):
        reordered = self._reorder_requests_by_pool_name(rpc_batch_request)
        tasks = [
            self.process_pool_tasks(pool_name, rpc_requests)
            for pool_name, rpc_requests in reordered.items()
        ]
        return list(chain.from_iterable(await asyncio.gather(*tasks)))


class RedisRpc:
    def __init__(self, pools_wrapper: RedisPoolsShareWrapper):
        self._pools_wrapper = pools_wrapper

    async def handle_single(self, request_data: t.Dict[str, t.Any]):
        rpc_request = RedisRpcRequest(request_data)

        try:
            redis = await self._pools_wrapper.get_redis(rpc_request.pool_name)
        except KeyError:
            raise exceptions.RpcMethodNotFoundError(
                id=rpc_request.id, data=rpc_request.params,
                message=f'Pool with name `{rpc_request.pool_name}` does not exist'
            )
        processor = RedisRpcRequestProcessor(redis)
        return await processor.process(rpc_request)

    async def handle_batch(self, request_data: t.List[t.Dict[str, t.Any]]):
        batch_rpc_request = RpcBatchRequest(request_data, request_cls=RedisRpcRequest)
        processor = RedisRpcBatchProcessor(self._pools_wrapper)
        return await processor.process(batch_rpc_request)

    async def handle(self, request: Request):
        data = load_json(request.body)

        if isinstance(data, list):
            return await self.handle_batch(data)
        else:
            return await self.handle_single(data)
