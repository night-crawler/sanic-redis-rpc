#!/usr/bin/env python
import click


@click.group()
def cli():
    pass


@cli.command()
@click.option('--port', default=8000, help='Port')
@click.option('--host', default='0.0.0.0', help='Listen address')
@click.option('--debug', is_flag=True, help='Set debug')
@click.option('--workers', default=1, help='Workers count')
@click.option('--cors', is_flag=True, help='Add CORS middleware')
def runserver(host, port, workers, debug, cors):
    from server import app

    if cors:
        from sanic_cors import CORS
        CORS(app, supports_credentials=True)

    app.run(host=host, port=port, debug=debug, workers=workers)


if __name__ == '__main__':
    cli()
