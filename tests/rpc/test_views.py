import pytest
from sanic import Sanic

pytestmark = pytest.mark.views


@pytest.fixture
async def search_id(app: Sanic, test_cli, rpc):
    await rpc('/', 'redis_0.set', 'something_long:1', 1)
    await rpc('/', 'redis_0.set', 'something_long:2', 2)

    resp = await test_cli.post(
        app.url_for('sanic-redis-rpc.search', redis_name='redis_0'),
        json={'pattern': 'something_long*'}
    )
    assert resp.status == 200
    resp_json = await resp.json()
    return resp_json['id']


# noinspection PyMethodMayBeStatic,PyShadowingNames
class BlueprintTest:
    pytestmark = [pytest.mark.blueprint, pytest.mark.rpc]

    async def test__status(self, test_cli):
        resp = await test_cli.get('/status')
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json[0]['name'] == 'redis_0'
        assert resp_json[1]['name'] == 'redis_1'

    async def test_redis_rpc_single(self, rpc):
        res = await rpc('/', 'redis_0.set', 'qwe', 1)
        assert res['result'] is True

        res = await rpc('/', 'redis_0.get', key='qwe')
        assert res['result'] == '1'

        res = await rpc('/', 'redis_0.get', key='qwe', encoding='utf8')
        assert res['result'] == '1'

    async def test_redis_rpc_batch(self, batch_rpc):
        res = await batch_rpc(
            '/',
            ['redis_0.multi_exec', None],
            ['redis_0.set', {'key': 'qwe', 'value': 1}],
            ['redis_0.set', {'key': 'qwe1', 'value': 2}],
            ['redis_0.get', {'key': 'qwe1', 'encoding': 'utf8'}],

            ['redis_1.multi_exec', None],
            ['redis_1.hmset', {'key': 'my_qwe_key', 'field': 'lol', 'value': 'field_value'}],
            ['redis_1.hmset_dict', {'key': 'my_qwe_key', 'f1': 'lol', 'f2': 'qwe'}],
        )
        assert len(res) == 5

    async def test__paginate(self, app: Sanic, test_cli, rpc):
        await rpc('/', 'redis_0.set', 'something_long:1', 1)

        resp = await test_cli.post(
            app.url_for('sanic-redis-rpc.search', redis_name='redis_0'),
            json={'pattern': 'something_long*'}
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json
        assert resp_json['id']
        assert resp_json['cursor'] == -1
        assert resp_json['pattern'] == 'something_long*'
        assert resp_json['ttl_seconds'] == 300
        assert type(resp_json['count']) is int
        assert 'results_key' in resp_json

        assert 'endpoints' in resp_json
        assert 'get_page' in resp_json['endpoints']
        assert 'refresh_ttl' in resp_json['endpoints']
        assert 'get_search_info' in resp_json['endpoints']

    async def test__refresh_ttl(self, search_id, app: Sanic, test_cli):
        search_id = await search_id

        resp = await test_cli.post(
            app.url_for('sanic-redis-rpc.refresh_ttl', search_id=search_id),
            json={'ttl_seconds': 667}
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json == [True, True]

    async def test__get_page(self, app: Sanic, test_cli, search_id):
        sid = await search_id

        resp = await test_cli.get(
            app.url_for('sanic-redis-rpc.get_page', search_id=sid, page_number=1, per_page=1)
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert len(resp_json['results']) == 1

    async def test__get_search_info(self, app: Sanic, test_cli, search_id):
        sid = await search_id
        resp = await test_cli.get(
            app.url_for('sanic-redis-rpc.get_search_info', search_id=sid)
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json
        assert resp_json['id']
        assert resp_json['cursor'] == -1
        assert resp_json['pattern'] == 'something_long*'
        assert resp_json['ttl_seconds'] == 300

    async def test__endpoints(self, app: Sanic, test_cli):
        resp = await test_cli.get(
            app.url_for('sanic-endpoints.endpoints')
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json


# noinspection PyMethodMayBeStatic
class BugsTest:
    pytestmark = [pytest.mark.get_page_unknown_redis_with_sort_keys_false, pytest.mark.bugs]

    async def test_case_001(self, rpc, test_cli, app: Sanic):
        """
        get_page must no fail when retrieving pages from searches with sort_keys == False.
        """
        await rpc('/', 'redis_0.set', 'something_long:1', 1)
        await rpc('/', 'redis_0.set', 'something_long:2', 2)

        resp = await test_cli.post(
            app.url_for('sanic-redis-rpc.search', redis_name='redis_0'),
            json={'pattern': 'something_long*', 'sort_keys': False}
        )
        resp_json = await resp.json()
        sid = resp_json['id']

        resp = await test_cli.get(
            app.url_for('sanic-redis-rpc.get_page', search_id=sid, page_number=1, per_page=1)
        )
        assert resp.status == 200
        resp_json = await resp.json()
        assert resp_json
