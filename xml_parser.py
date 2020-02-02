# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import os
import time
from collections import defaultdict
import logging

log = logging.getLogger(__name__)


def etree_to_dict(elem) -> dict:
    """
    Конвертация элемента xml.etree иерархии с его детьми в словарь

    :param
        * *elem* (``<class 'xml.etree.ElementTree.Element'>``) - элемент
        иерархии

    :rtype: (``dict``)
    :return: все данные текущего узла иерархии завернутые в словарь
    """
    elem_dict = {elem.tag: {} if elem.attrib else None}
    children = list(elem)
    if children:
        children_dict = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                children_dict[k].append(v)

        elem_dict = {elem.tag: {k: v[0] if len(v) == 1 else v
                                for k, v in children_dict.items()}}
    if elem.attrib:
        elem_dict[elem.tag].update((k, v)
                                   for k, v in elem.attrib.items())
    if elem.text:
        text = elem.text.strip()
        if children or elem.attrib:
            if text:
                elem_dict[elem.tag]['text'] = text
        else:
            elem_dict[elem.tag] = text
    return elem_dict


async def parse_xml_to_dict(**kwargs) -> dict:
    """
    Парсинг данных по полётам xml файла в словарь

    :param kwargs:
         * *need_return* (``str``) -- наличие обратного маршрута

    :rtype: (``dict``)
    :return: данные о полётах
    """
    filenames = {
        '1': 'RS_Via-3.xml',
        '2': 'RS_ViaOW.xml'
    }
    need_return = kwargs.get('need_return')
    columns = ['OnwardPricedItinerary', 'Pricing']
    if need_return in ['true', 'True', 'TRUE']:
        filename = filenames.get('1')
    elif need_return in ['false', 'False', 'FALSE']:
        filename = filenames.get('2')
        columns.append('ReturnPricedItinerary',)
    else:
        return {'error': 'Required need_return'}

    dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'
    file_path = open(f'{dir_path}/{filename}', "rb")

    log.info(f'FILE {filename}')

    tree = ET.parse(file_path)
    root = tree.getroot()

    data = dict()
    info = dict()
    count = 0
    check_keys = set(columns)

    start = time.time()

    for elem in root.iter('Flights'):

        # обработка оснвного блока Flights верхнего уровня
        if elem.tag == 'Flights':
            # прикручена логика проверок, т.к есть дубликат имен Flights в xml
            checker = [x.tag for x in list(elem)]
            is_check = set(checker) & check_keys
            if is_check:
                current_count = count
                info[count] = {elem.tag: {}}
                count += 1

                elem_dict = etree_to_dict(elem)
                data[current_count] = elem_dict.get('Flights', {})

        elem.clear()
    root.clear()
    log.info(f'PARSING DATA  {time.time() - start} sec')
    return data


async def parse_xml(**kwargs) -> dict:
    """
    Парсинг тегов и атрибутов xml файла в словарь

    :param kwargs:
        * *need_return* (``str``) -- наличие обратного маршрута

    :rtype: (``dict``)
    :return: теги и атрибуты xml файла
    """
    filenames = {
        '1': 'RS_Via-3.xml',
        '2': 'RS_ViaOW.xml'
    }
    need_return = kwargs.get('need_return')
    if need_return in ['true', 'True', 'TRUE']:
        filename = filenames.get('1')
    elif need_return in ['false', 'False', 'FALSE']:
        filename = filenames.get('2')
    else:
        return {'error': 'Required need_return'}

    dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'
    file_path = open(f'{dir_path}/{filename}', "rb")

    log.info(f'FILE {filename}')

    context = ET.iterparse(file_path, events=("start", ))
    context = iter(context)
    _, root = next(context)
    _, request_info = next(context)
    _, flight_info = next(context)

    start = time.time()

    data = dict(
        tags=set(),
        attributes=list()
    )
    for event, elem in context:
        data['tags'].add(elem.tag)
        if elem.attrib:
            attributes = elem.attrib
            attributes['tag'] = elem.tag
            if attributes not in data['attributes']:
                data['attributes'].append(attributes)
        elem.clear()

    log.info(f'PARSING DATA  {time.time() - start} sec')
    return data
