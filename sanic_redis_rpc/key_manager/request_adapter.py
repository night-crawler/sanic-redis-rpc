import typing as t

import aioredis
from sanic.request import Request
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper
from .manager import KeyManager


class KeyManagerRequestAdapter:
    # noinspection PyProtectedMember
    def __init__(self, request: Request, redis_name: t.Optional[str] = None):
        self.request: Request = request
        self.pools_wrapper: RedisPoolsShareWrapper = request.app._pools_wrapper
        self.redis_name = redis_name
        self.options = self.parse_request()

        self.redis: aioredis.Redis = None
        self.service_redis: aioredis.Redis = None
        self.key_manager: KeyManager = None

    async def _init(self):
        if self.key_manager:
            return

        # don't need redis for TTL refresh
        if self.redis_name:
            self.redis: aioredis.Redis = await self.pools_wrapper.get_redis(self.redis_name)

        self.service_redis: aioredis.Redis = await self.pools_wrapper.get_service_redis()
        self.key_manager = KeyManager(
            self.redis, self.service_redis,
            scan_count=self.options['scan_count']
        )

    def parse_request(self) -> t.Dict[str, t.Any]:
        data = self.request.json or {}
        return {
            'scan_count': int(data.get('scan_count', 5000)),
            'pattern': data.get('pattern', '*'),
            'sort_keys': bool(data.get('sort_keys', True)),
            'ttl_seconds': int(data.get('ttl_seconds', 5 * 60)),
            'page_size': int(self.request.args.get('page_size', 1000)),
        }

    async def paginate(self):
        await self._init()

        return await self.key_manager.paginate(
            self.options['pattern'],
            sort_keys=self.options['sort_keys'],
            ttl_seconds=self.options['ttl_seconds']
        )

    async def refresh_ttl(self, search_id: str):
        await self._init()

        return await self.key_manager.refresh_ttl(
            search_id,
            ttl_seconds=self.options['ttl_seconds']
        )

    async def get_page(self, search_id: str, page_num: int):
        await self._init()

        return await self.key_manager.get_page(
            search_id,
            page_num=page_num,
            page_size=self.options['page_size'],
        )

    async def get_search_info(self, search_id: str):
        await self._init()

        return await self.key_manager.get_search_info(search_id)
