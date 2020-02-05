from aiohttp import web
from aiohttp.web import Application
from aiohttp_rest_api.loader import \
    load_and_connect_all_endpoints_from_folder
import logging
import pathlib
import pytoml as toml
import os

BASE_DIR = pathlib.Path(__file__).parent.parent
PACKAGE_NAME = 'app'
log = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """
    Загрузка конфигурации приложения

    :param
        * *config_path* (``str``) -- путь к конфигу

    :rtype: (``dict``)
    :return: конфиг приложения
    """
    with open(f'{os.getcwd()}{config_path}') as f:
        conf = toml.load(f)
    return conf


def init_app(config: dict) -> Application:
    """
    Инициализация web приложения

    :param
        * *config* (``dict``) -- конфиг приложения

    :rtype: (``Application``)
    :return: web приложение
    """
    app = web.Application()
    app['config'] = config

    load_and_connect_all_endpoints_from_folder(
        path='{0}/{1}'.format(os.path.dirname(os.path.realpath(__file__)),
                              'endpoints'),
        app=app,
        version_prefix='v1'
    )

    log.debug(app['config'])
    return app


def main(config_path: str):
    """
    Запуск REST API

    :param
        * *config_path* (``str``) -- путь к конфигу

    :rtype (``None``)
    :return:
    """
    log.debug(f'config_path - {config_path}')

    config = load_config(config_path)

    logging.basicConfig(level=logging.DEBUG)
    app = init_app(config)
    app_config = config.get('app', None)

    web.run_app(app, port=app_config.get('port', 9999))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Provide path to config file")
    args = parser.parse_args()

    if args.config:
        main(args.config)
    else:
        parser.print_help()
