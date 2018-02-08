from sanic import Blueprint
from sanic import Sanic
from sanic.request import Request
from sanic.response import json

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.rpc.redis_rpc import RedisRpc
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper

sanic_redis_rpc_bp = bp = Blueprint('sanic-redis-rpc')


@bp.exception(Exception)
async def process_rpc_exceptions(request: Request, exception: Exception):
    if isinstance(exception, exceptions.RpcError):
        return json(exception.as_dict())


@bp.listener('before_server_start')
async def before_server_start(app: Sanic, loop):
    app._pools_wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options, loop)
    await app._pools_wrapper.initialize_pools()


@bp.listener('after_server_stop')
async def after_server_stop(app: Sanic, loop):
    await app._pools_wrapper.close()


@bp.route('/status', methods=['POST', 'GET'])
async def status(request: Request):
    return json(await request.app._pools_wrapper.get_status())


@bp.route('/spec', methods=['POST', 'GET'])
async def spec(request: Request):
    # print(request.json)
    print(dir(request))
    return json({'my': 'blueprint1'})


@bp.route('/', methods=['POST', 'GET'])
async def handle_rpc(request: Request):
    handler = RedisRpc(request)
    return json({'my': 'blueprint'})
