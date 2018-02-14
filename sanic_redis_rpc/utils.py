import typing as t

from aioredis.util import parse_url
from furl import furl

REDIS_INSTANCE_KEY_PATTERN = '{0[host]}:{0[port]}:{0[db]}'

ENV_STR_BOOL_COERCE_MAP = {
    '': True,  # Flag is set

    0: False,
    '0': False,
    'false': False,
    'off': False,

    1: True,
    '1': True,
    'true': True,
    'on': True,
}


def coerce_str_to_bool(val: t.Union[str, int, bool, None], strict: bool = False) -> bool:
    """
    Converts a given string ``val`` into a boolean.

    :param val: any string representation of boolean
    :param strict: raise ``ValueError`` if ``val`` does not look like a boolean-like object
    :return: ``True`` if ``val`` is thruthy, ``False`` otherwise.

    :raises ValueError: if ``strict`` specified and ``val`` got anything except
     ``['', 0, 1, true, false, on, off, True, False]``
    """
    if isinstance(val, str):
        val = val.lower()

    flag = ENV_STR_BOOL_COERCE_MAP.get(val, None)

    if flag is not None:
        return flag

    if strict:
        raise ValueError('Unsupported value for boolean flag: `%s`' % val)

    return bool(val)


def parse_redis_dsn(raw_str: str = 'redis://localhost:6379') -> t.Dict[str, t.Any]:
    _, opts = parse_url(raw_str)
    opts.setdefault('db', 0)
    parsed = furl(raw_str)

    address = furl()
    for attr in ['scheme', 'host', 'port']:
        setattr(address, attr, getattr(parsed, attr))

    opts.update({
        'address': str(address),
        'create_connection_timeout': parsed.args.get('create_connection_timeout', None),
        'minsize': int(parsed.args.get('minsize', 1)),
        'maxsize': int(parsed.args.get('maxsize', 10)),
        'name': parsed.args.get('name', ''),
        'display_name': parsed.args.get('display_name', ''),
    })

    return opts
