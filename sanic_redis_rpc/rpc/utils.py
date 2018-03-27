import typing as t
from collections import OrderedDict
from ujson import loads as json_loads

import aioredis

from sanic_redis_rpc.rpc import exceptions


def load_json(body):
    if not body:
        return None

    try:
        return json_loads(body)
    except Exception as e:
        raise exceptions.RpcParseError(data={
            'exception': str(type(e)),
            'data': body,
            'message': str(e)
        })


JSON_RPC_VERSION = '2.0'


class RedisPoolsShareWrapper:
    ALLOWED_POOL_ARGS = [
        'minsize', 'maxsize', 'ssl', 'parser', 'create_connection_timeout', 'db', 'password'
    ]

    SAFE_STATUS_KEYS = ['id', 'db', 'env_variable', 'name', 'display_name', 'poolsize', 'address']

    def __init__(self, redis_connections_options: t.Dict[str, t.Dict[str, t.Any]], loop=None):
        self._redis_connections_options = redis_connections_options
        self._pool_map: t.Dict[str, aioredis.ConnectionsPool] = {}
        self._redis_map: t.Dict[str, aioredis.Redis] = {}
        self._loop = loop

    async def get_redis(self, pool_name: str) -> aioredis.Redis:
        redis: aioredis.Redis = self._redis_map.get(pool_name, None)
        if redis is not None:
            return redis
        pool = await self._get_pool(pool_name)
        self._redis_map[pool_name] = aioredis.Redis(pool)
        return self._redis_map[pool_name]

    async def get_service_redis(self) -> aioredis.Redis:
        pool_name = self._get_service_pool_name()
        return await self.get_redis(pool_name)

    async def get_status(self) -> t.List[t.Dict[str, t.Any]]:
        res = []
        for pool_name, opts in self._redis_connections_options.items():
            pool = await self._get_pool(pool_name)
            bundle = {k: v for k, v in opts.items() if k in self.SAFE_STATUS_KEYS}
            bundle.update({
                attr: getattr(pool, attr)
                for attr in ['encoding', 'freesize', 'maxsize', 'minsize', 'closed', 'size']
            })
            res.append(bundle)
        return res

    async def close(self):
        for pool in self._pool_map.values():
            pool.close()
            await pool.wait_closed()

    async def _initialize_pools(self):
        for pool_name in self._redis_connections_options.keys():
            await self._get_pool(pool_name)

    async def _get_pool(self, name: str) -> aioredis.ConnectionsPool:
        pool = self._pool_map.get(name, None)
        if pool is None:
            self._pool_map[name] = await self._create_pool(name)
        return self._pool_map[name]

    async def _create_pool(self, pool_name: str) -> aioredis.ConnectionsPool:
        pool_options = self._redis_connections_options[pool_name].copy()
        address = pool_options.pop('address')
        opts = {k: v for k, v in pool_options.items() if k in self.ALLOWED_POOL_ARGS}
        return await aioredis.create_pool(
            address,
            **opts,
            loop=self._loop
        )

    def _get_service_pool_name(self):
        for pool_name, opts in self._redis_connections_options.items():
            if opts['service']:
                return pool_name

        return list(self._redis_connections_options.keys())[0]
