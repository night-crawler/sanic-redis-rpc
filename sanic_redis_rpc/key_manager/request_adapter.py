import typing as t
from asyncio import gather
from math import ceil

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

    def _get_urls(self, search_id: str) -> t.Dict[str, str]:
        return {
            'get_page': self.request.app.url_for('sanic-redis-rpc.get_page', page_number=1, search_id=search_id),
            'refresh_ttl': self.request.app.url_for('sanic-redis-rpc.refresh_ttl', search_id=search_id),
            'get_search_info': self.request.app.url_for('sanic-redis-rpc.get_search_info', search_id=search_id),
        }

    def parse_request(self) -> t.Dict[str, t.Any]:
        data = self.request.json or {}
        return {
            'scan_count': int(data.get('scan_count', 5000)),
            'pattern': data.get('pattern', '*'),
            'sort_keys': bool(data.get('sort_keys', True)),
            'ttl_seconds': int(data.get('ttl_seconds', 5 * 60)),
            'per_page': int(self.request.args.get('per_page', 1000)),
        }

    async def search(self):
        await self._init()

        info = await self.key_manager.search(
            self.options['pattern'],
            sort_keys=self.options['sort_keys'],
            ttl_seconds=self.options['ttl_seconds']
        )
        info['urls'] = self._get_urls(info['id'])

        return info

    async def refresh_ttl(self, search_id: str):
        await self._init()

        return await self.key_manager.refresh_ttl(
            search_id,
            ttl_seconds=self.options['ttl_seconds']
        )

    async def get_page(self, search_id: str, page_number: int):
        await self._init()

        per_page = self.options['per_page']
        page_number = int(page_number)

        info, results = await gather(
            self.key_manager.get_search_info(search_id),
            self.key_manager.get_page(
                search_id,
                page_number=page_number,
                per_page=per_page,
            )
        )

        count = info['count']
        num_pages = int(ceil(float(count) / per_page))
        next_page = page_number + 1 if page_number < num_pages else None
        prev_page = page_number - 1 if page_number > 1 else None

        return {
            'next': next_page,
            'previous': prev_page,
            'num_pages': num_pages,
            'results': results,
        }

    async def get_search_info(self, search_id: str):
        await self._init()

        info = await self.key_manager.get_search_info(search_id)
        info['urls'] = self._get_urls(search_id)
        return info
