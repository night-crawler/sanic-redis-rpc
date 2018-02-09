from sanic import Sanic

from sanic_redis_rpc.conf import configure
from sanic_redis_rpc.views import sanic_redis_rpc_bp

app = configure(Sanic('sanic-redis'))
app.blueprint(sanic_redis_rpc_bp, url_prefix='/rpc')
