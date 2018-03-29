from sanic import Blueprint
from sanic.request import Request
from sanic.response import json

sanic_urls_bp = bp = Blueprint('sanic-urls')


@bp.route('/urls')
def urls(request: Request):
    return json({
        'rpc': request.app.url_for('sanic-redis-rpc.handle_rpc'),
        'status': request.app.url_for('sanic-redis-rpc.status'),
        'inspect': request.app.url_for('sanic-redis-rpc.inspect'),
    })
