import pytest

pytestmark = pytest.mark.views


# noinspection PyMethodMayBeStatic,PyShadowingNames
class BlueprintTest:
    pytestmark = [pytest.mark.blueprint, pytest.mark.rpc]

    async def test__status(self, test_cli):
        resp = await test_cli.get('/status')
        assert resp.status == 200
        resp_json = await resp.json()
        assert 'redis_0' in resp_json
        assert 'redis_1' in resp_json

    async def test_redis_rpc_single(self, rpc):
        res = await rpc('/', 'redis_0.set', 'qwe', 1)
        assert res['result'] is True

        res = await rpc('/', 'redis_0.get', key='qwe')
        assert res['result'] == 'MQ=='

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
