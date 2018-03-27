import aioredis
from sanic import Blueprint
from sanic import Sanic
from sanic.request import Request
from sanic.response import json

from sanic_redis_rpc.rpc import exceptions
from sanic_redis_rpc.redis_rpc import RedisRpc
from sanic_redis_rpc.rpc.utils import RedisPoolsShareWrapper
from sanic_redis_rpc.signature_serializer import SignatureSerializer
from sanic_redis_rpc.key_manager import KeyManagerRequestProxy

sanic_redis_rpc_bp = bp = Blueprint('sanic-redis-rpc')


# @bp.exception(Exception)
# async def process_rpc_exceptions(request: Request, exception: Exception):
#     if isinstance(exception, exceptions.RpcError):
#         return json(exception.as_dict())
#
#     import traceback
#     traceback.print_exc()


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


@bp.route('/keys/paginate/<redis_name>', methods=['POST', 'OPTIONS'])
async def paginate(request: Request, redis_name: str):
    if request.method == 'OPTIONS':
        return json({})

    return json(
        await KeyManagerRequestProxy(request, redis_name).paginate()
    )


@bp.route('/keys/paginate/refresh-ttl/<search_id>', methods=['POST', 'OPTIONS'])
async def refresh_ttl(request: Request, search_id: str):
    if request.method == 'OPTIONS':
        return json({})

    return json(
        await KeyManagerRequestProxy(request, None).refresh_ttl(search_id)
    )


@bp.route('/keys/paginate/<search_id>/page/<page_num>', methods=['GET', 'OPTIONS'])
async def get_page(request: Request, search_id: str, page_num: int):
    if request.method == 'OPTIONS':
        return json({})

    return json(
        await KeyManagerRequestProxy(request, None).get_page(search_id, page_num)
    )


@bp.route('/keys/paginate/info/<search_id>', methods=['GET', 'OPTIONS'])
async def get_search_info(request: Request, search_id: str):
    if request.method == 'OPTIONS':
        return json({})

    return json(
        await KeyManagerRequestProxy(request, None).get_search_info(search_id)
    )


@bp.route('/', methods=['POST', 'OPTIONS'])
async def handle_rpc(request: Request):
    if request.method == 'OPTIONS':
        return json({})
    handler: RedisRpc = request.app._redis_rpc_handler

    # handle exceptions manually since sanic-cors does not apply cors headers to responses
    # handled with @bp.exception(Exception)
    try:
        return json(await handler.handle(request))
    except exceptions.RpcError as e:
        return json(e.as_dict())
    except Exception as e:
        return json(exceptions.RpcError(message=str(e)).as_dict())
