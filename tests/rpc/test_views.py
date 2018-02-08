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
