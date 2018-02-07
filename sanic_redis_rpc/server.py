from sanic import Sanic
from sanic_redis_rpc.rpc.views import sanic_redis_rpc_bp
from sanic_redis_rpc.conf import configure

app = configure(Sanic('sanic-redis'))
app.blueprint(sanic_redis_rpc_bp, url_prefix='/rpc')
