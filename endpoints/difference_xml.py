import itertools
from typing import List
from aiohttp.web import Request
from aiohttp.web_response import Response
from aiohttp_rest_api import AioHTTPRestEndpoint
from aiohttp_rest_api.responses import respond_with_json
from xml_parser import parse_xml
import logging

log = logging.getLogger(__name__)


class DifferenceEndpoint(AioHTTPRestEndpoint):

    def connected_routes(self) -> List[str]:
        """"""
        return [
            '/diff'
        ]

    async def get(self, request: Request) -> Response:
        """
        GET метод /v1/diff получение различия тегов и атрибутов между двумя
        xml файлами

        :param
            * *request* (``Request``) -- http запрос

        :rtype: (``Response``)
        :return: ответ на запрос в формате JSON
        """
        action = request.query.get('action', 'cheap')

        data_file1 = await parse_xml(need_return='true', action=action)
        data_file2 = await parse_xml(need_return='false', action=action)

        diff = dict()
        diff['tags'] = data_file1['tags'] - data_file2['tags']

        intersect = [item for item in data_file1['attributes']
                     if item in data_file2['attributes']]
        sym_diff = [item for item in
                    itertools.chain(data_file1['attributes'],
                                    data_file2['attributes'])
                    if item not in intersect]

        # визуальная эстетика
        attrib = dict()
        for el in sym_diff:
            tag = el.pop('tag')
            if tag not in attrib.keys():
                attrib.update({f'{tag}': []})
            attrib[tag].append(el)

        diff['attributes'] = attrib
        return respond_with_json(diff)
