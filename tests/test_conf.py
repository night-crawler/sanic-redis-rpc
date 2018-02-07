import pytest
from sanic import Sanic
from sanic_redis_rpc.conf import ENV_REDIS_PREFIX, read_redis_config_from_env, configure

pytestmark = pytest.mark.conf


# noinspection PyMethodMayBeStatic
class ConfTest:
    def test__read_redis_config_from_env(self):
        env = {
            '%s21' % ENV_REDIS_PREFIX: 'redis://localhost:6379?db=21',
            '%s0' % ENV_REDIS_PREFIX: 'redis://localhost:6379?db=0',
            '%s11' % ENV_REDIS_PREFIX: 'redis://localhost:6379?db=11',
            'useless': 'qwe'
        }

        parsed = read_redis_config_from_env(env)
        assert len(parsed.keys()) == 3, 'Ensure nothing extraneous has been read'
        assert list(parsed.keys())[0] == 'redis_0', 'Ensure keys are natsorded'

    def test__read_redis_config_from_env__raises_on_duplicate_names(self):
        env = {
            '%s21' % ENV_REDIS_PREFIX: 'redis://localhost:6379?name=qwe',
            '%s0' % ENV_REDIS_PREFIX: 'redis://localhost:6379?name=qwe',
        }
        with pytest.raises(ValueError):
            read_redis_config_from_env(env)

    def test__configure(self):
        env = {
            '%s21' % ENV_REDIS_PREFIX: 'redis://localhost:6379?db=21',
        }

        app = configure(Sanic('test'), env)
        assert hasattr(app.config, 'redis_connections_options'), 'Ensure redis config has been read'
        assert app.config.redis_connections_options['redis_0'], 'Ensure default has been set'
