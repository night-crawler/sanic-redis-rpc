import os
import typing as t
from collections import OrderedDict
from operator import itemgetter
from pprint import pformat

import click
from natsort import natsorted
from sanic import Sanic
from sanic.config import Config

from sanic_redis_rpc.utils import parse_redis_dsn

DEFAULT_REDIS_CONNECTION_STRING = 'redis://localhost:6379'
ENV_REDIS_PREFIX = 'REDIS_'


def read_redis_config_from_env(env: t.Dict[str, str]) -> t.Dict[str, t.Dict[str, t.Any]]:
    res = OrderedDict()
    redis_env_vars_mapping = {k: v for k, v in env.items() if k.startswith(ENV_REDIS_PREFIX)}
    _sorted_iter = enumerate(natsorted(redis_env_vars_mapping.items(), key=itemgetter(0)))

    for i, (rkey, conn_str) in _sorted_iter:
        parsed = parse_redis_dsn(conn_str)
        parsed['id'] = i
        parsed['env_variable'] = rkey
        if not parsed['name']:
            parsed['name'] = 'redis_%s' % i

        if parsed['name'] in res:
            raise ValueError(f'Duplicate name `{parsed["name"]}` in `{redis_env_vars_mapping}`')

        res[parsed['name']] = parsed

    return res


def display_config(config: Config):
    for k, v in config.redis_connections_options.items():
        click.echo(click.style(
            'Parsed env variable `%s`:' % v['env_variable'],
            fg='yellow'
        ))
        click.echo(click.style(
            pformat(v, compact=True, width=click.get_terminal_size()[0]),
            fg='blue',
        ))


def configure(app: Sanic, env: t.Optional[t.Dict[str, str]] = None, verbose: bool = True) -> Sanic:
    env = env or os.environ
    env.setdefault(ENV_REDIS_PREFIX + '0', DEFAULT_REDIS_CONNECTION_STRING)

    app.config.redis_connections_options = read_redis_config_from_env(env)

    verbose and display_config(app.config)
    return app
