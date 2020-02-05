import itertools
from typing import List
from aiohttp.web import Request
from aiohttp.web_response import Response
from aiohttp_rest_api import AioHTTPRestEndpoint
from aiohttp_rest_api.responses import respond_with_json
from xml_parser import diff_parse_xml, compare_xml
import logging

log = logging.getLogger(__name__)


class DifferenceEndpoint(AioHTTPRestEndpoint):

    def connected_routes(self) -> List[str]:
        """"""
        return [
            '/onward_diff'
        ]

    async def get(self, request: Request) -> Response:
        """
        GET метод /v1/onward_diff получение различия между xml файлами
        по тегу OnwardPricedItinerary

        :param
            * *request* (``Request``) -- http запрос

        :rtype: (``Response``)
        :return: ответ на запрос в формате JSON
        """
        option = request.query.get('option', '1')

        if option == '1':
            data_f1 = await diff_parse_xml('1')
            data_f2 = await diff_parse_xml('2')
        elif option == '2':
            data_f1 = await diff_parse_xml('2')
            data_f2 = await diff_parse_xml('1')
        else:
            return respond_with_json({'error': 'option= can be "1" or "2"'})

        compare = await compare_xml(data_f1, data_f2)

        return respond_with_json(compare)
