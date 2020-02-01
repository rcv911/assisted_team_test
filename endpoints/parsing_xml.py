from typing import List
from aiohttp.web import Request
from aiohttp.web_response import Response
from aiohttp_rest_api import AioHTTPRestEndpoint
from aiohttp_rest_api.responses import respond_with_json
from app_methods import get_flight_tickets
import logging

log = logging.getLogger(__name__)


class ParseEndpoint(AioHTTPRestEndpoint):

    def connected_routes(self) -> List[str]:
        """"""
        return [
            '/parse'
        ]

    async def get(self, request: Request) -> Response:
        """
        GET метод /v1/parse получение выборки данных о полётах

        :param
            * *request* (``Request``) -- запрос

        :rtype: (``Response``)
        :return: ответ на запрос в формате JSON
        """
        need_return = request.query.get('need_return', 'true')
        action = request.query.get('action', 'cheap')

        data = await get_flight_tickets(need_return=need_return, action=action)

        return respond_with_json(data)
