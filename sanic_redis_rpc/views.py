import aioredis
from sanic import Blueprint
from sanic import Sanic
from sanic.request import Request
from sanic.response import json

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.redis_rpc import RedisRpc
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper
from sanic_redis_rpc.signature_serializer import SignatureSerializer

sanic_redis_rpc_bp = bp = Blueprint('sanic-redis-rpc')


@bp.exception(Exception)
async def process_rpc_exceptions(request: Request, exception: Exception):
    if isinstance(exception, exceptions.RpcError):
        return json(exception.as_dict())

    import traceback
    traceback.print_exc()


@bp.listener('before_server_start')
async def before_server_start(app: Sanic, loop):
    app._pools_wrapper = RedisPoolsShareWrapper(app.config.redis_connections_options, loop)
    await app._pools_wrapper._initialize_pools()
    app._redis_rpc_handler = RedisRpc(app._pools_wrapper)


@bp.listener('after_server_stop')
async def after_server_stop(app: Sanic, loop):
    await app._pools_wrapper.close()


@bp.route('/status', methods=['GET'])
async def status(request: Request):
    if request.method == 'OPTIONS':
        return json({})
    return json(await request.app._pools_wrapper.get_status())


@bp.route('/inspect', methods=['GET'])
async def inspect(request: Request):
    return json(
        SignatureSerializer(aioredis.Redis('fake')).to_dict()
    )


@bp.route('/', methods=['POST', 'OPTIONS'])
async def handle_rpc(request: Request):
    if request.method == 'OPTIONS':
        return json({})
    handler: RedisRpc = request.app._redis_rpc_handler
    return json(await handler.handle(request))
