import pytest
from sanic import Sanic
from sanic.websocket import WebSocketProtocol

from sanic_redis_rpc.conf import configure
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper
from sanic_redis_rpc.views import sanic_redis_rpc_bp
from tests.utils import mk_rpc_bundle


@pytest.fixture
def app(loop):
    _app = Sanic('test_sanic_app')
    _app.blueprint(sanic_redis_rpc_bp)
    _app = configure(
        _app,
        {
            'REDIS_0': 'redis://localhost:6379?db=0',
            'REDIS_1': 'redis://localhost:6379?db=1'
        }
    )
    _app._pools_wrapper = RedisPoolsShareWrapper(_app.config.redis_connections_options, loop)
    yield _app


# noinspection PyShadowingNames
@pytest.fixture
def test_cli(loop, app, test_client):
    return loop.run_until_complete(test_client(app, protocol=WebSocketProtocol))


# noinspection PyShadowingNames
@pytest.fixture
def rpc(test_cli):
    async def rpc_call(base_url, method: str, *args):
        return await test_cli.post(
            base_url,
            json=mk_rpc_bundle(method, args),
        )

    return rpc_call
