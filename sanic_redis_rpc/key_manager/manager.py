import typing as t
from asyncio import gather
from datetime import datetime
from uuid import uuid4

import aioredis
from sortedcontainers import SortedSet

from sanic_redis_rpc.key_manager.exceptions import SearchIdNotFoundError, WrongPageSizeError, WrongNumberError, \
    PageNotFoundError


def chunks(l, n):
    n = max(1, n)
    return (l[i:i + n] for i in range(0, len(l), n))


class KeyManager:
    LUA_COUNT_MATCHES_SCRIPT = '''
        local cursor = "0"
        local count = 0
        
        repeat
            local r = redis.call(
                "SCAN", cursor, 
                "MATCH", "{pattern}",
                "COUNT", {scan_count}
            )
            cursor = r[1]
            count = count + #r[2]
        until cursor == "0"
        
        return count
    '''

    def __init__(
            self, redis: aioredis.Redis, service_redis: aioredis.Redis,
            scan_count: int = 5000,
            service_key_prefix: str = 'sanic-redis-rpc'):
        self.redis = redis
        self.service_redis = service_redis
        self.cursor = 0
        self.scan_count = scan_count
        self.service_key_prefix = service_key_prefix

    async def search(
            self,
            pattern: str = '*',
            sort_keys: bool = True,
            ttl_seconds: int = 5 * 60) -> t.Dict[str, t.Union[str, t.Any]]:
        search_id = uuid4().hex
        search_key = self._mk_search_key(search_id)
        results_key = self._mk_results_key(search_id)
        search_bundle = {
            'id': search_id,
            'cursor': 0,
            'sorted': int(sort_keys),
            'pattern': pattern,
            'ttl_seconds': ttl_seconds,
            'results_key': results_key,
            'timestamp': datetime.now().isoformat(),
            'count': -1,
        }

        if sort_keys:
            results = await self._get_sorted_keys(pattern)
            search_bundle.update({'cursor': -1, 'count': len(results)})
        else:
            results = []
            search_bundle.update({'count': await self._get_match_count(pattern)})

        transaction = self.service_redis.multi_exec()
        transaction.hmset_dict(search_key, search_bundle)
        transaction.expire(search_key, ttl_seconds)

        for chunk in chunks(results, self.scan_count):
            transaction.rpush(results_key, *chunk)

        transaction.expire(results_key, ttl_seconds)
        await transaction.execute()

        return search_bundle

    async def get_page(self, search_id: str, page_number: int, per_page: int = 1000) -> t.List[str]:
        page_number, per_page = int(page_number), int(per_page)
        if not (per_page > 0):
            raise WrongPageSizeError(per_page)
        if not (page_number >= 1):
            raise WrongNumberError(page_number)

        info, __skip = await gather(
            self.get_search_info(search_id),
            self.refresh_ttl(search_id)
        )

        results_key, count, cursor, pattern = info['results_key'], info['count'], info['cursor'], info['pattern']

        if count <= 0:
            return []

        start = (page_number - 1) * per_page
        finish = start + per_page - 1  # the rightmost item is included

        if start > count:
            raise PageNotFoundError(
                f'Search identifier {search_id} has {count} items, but you requested a slice from {start}')

        if finish > count:
            finish = count - 1

        if info['sorted'] == 0:
            await self._load_more(search_id, pattern, cursor, finish + 1)

        keys = await self.service_redis.lrange(results_key, start, finish, encoding='utf8')
        return keys

    async def refresh_ttl(self, search_id: str, ttl_seconds: int = 5 * 60):
        search_key = self._mk_search_key(search_id)
        results_key = self._mk_results_key(search_id)
        pipe = self.service_redis.pipeline()
        pipe.expire(search_key, ttl_seconds)
        pipe.expire(results_key, ttl_seconds)
        return await pipe.execute()

    async def get_search_info(self, search_id: str) -> t.Dict[str, t.Any]:
        search_key = self._mk_search_key(search_id)
        info_bundle = await self.service_redis.hgetall(search_key, encoding='utf8')
        if not info_bundle:
            raise SearchIdNotFoundError(search_id)

        for k in ['sorted', 'ttl_seconds', 'count', 'cursor']:
            info_bundle[k] = int(info_bundle[k])

        return info_bundle

    async def _load_more(self, search_id: str, pattern: str, cursor: int, finish: int):
        """
        :param search_id: search identifier
        :param pattern: original search pattern
        :param cursor: current cursor
        :param finish: corrected finish (it must not be gte total)
        :return:
        """
        results_key = self._mk_results_key(search_id)
        search_key = self._mk_search_key(search_id)
        llen = await self.service_redis.llen(results_key)
        if llen and cursor == 0:
            return None  # cannot load more since we've reached the end before

        keys_to_load = finish - llen
        if keys_to_load <= 0:
            return None  # no need to load

        keys_left = keys_to_load

        cur = f'{cursor}'
        transaction = self.service_redis.multi_exec()
        while cur:
            cur, keys = await self.redis.scan(cur, match=pattern, count=self.scan_count)
            if not keys:
                continue

            transaction.rpush(results_key, *keys)
            keys_left -= len(keys)

            if keys_left <= 0:
                break

        transaction.hset(search_key, 'cursor', cur)
        return await transaction.execute()

    async def _get_match_count(self, pattern: str = '*') -> int:
        pattern = pattern.replace('"', r'\"')
        script = self.LUA_COUNT_MATCHES_SCRIPT.format(pattern=pattern, scan_count=self.scan_count)
        return int(await self.redis.eval(script))

    async def _get_sorted_keys(self, match: str = '*'):
        container = SortedSet()
        cur = b'0'
        while cur:
            cur, keys = await self.redis.scan(cur, match=match, count=self.scan_count)
            for key in keys:
                container.add(key)
        return container

    def _mk_search_key(self, search_id: str) -> str:
        return ':'.join([self.service_key_prefix, search_id])

    def _mk_results_key(self, search_id: str) -> str:
        return ':'.join([self.service_key_prefix, search_id, 'results'])
