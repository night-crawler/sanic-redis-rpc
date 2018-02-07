import pytest

from sanic_redis_rpc import utils

pytestmark = pytest.mark.utils


# noinspection PyMethodMayBeStatic
class UtilsTest:
    def test__parse_redis_connection_string(self):
        assert utils.parse_redis_connection_string()
        assert utils.parse_redis_connection_string(
            'redis://clientid:password@127.0.0.1:6380?db=66&poolsize=12&auto_reconnect=off&name=vasya'
        ) == {
            'db': 66,
            'password': 'password',
            'address': 'redis://127.0.0.1:6380',
            'create_connection_timeout': None,
            'minsize': 1,
            'maxsize': 10,
            'name': 'vasya',
            'display_name': 'Redis root_instance'
        }
