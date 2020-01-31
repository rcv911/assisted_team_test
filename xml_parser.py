# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
import os
import time
from collections import defaultdict
import dateutil.parser
import logging

log = logging.getLogger(__name__)


async def get_travel_info(input_data) -> (str, int, bool):
    """
    Получение информаии о полёте

    :param
        * *input_data* (``list or dict``) -- входные данные о полёте

    :rtype: (``str, int, bool``)
    :return:
    """
    is_direct_flight = True
    if isinstance(input_data, list):
        is_direct_flight = False
        departure_timestamp = [x['DepartureTimeStamp'] for x in input_data]
        arrival_timestamp = [x['ArrivalTimeStamp'] for x in input_data]

        time_info, total_time = await get_travel_time(departure_timestamp[0],
                                        arrival_timestamp[1])

    else:
        departure_timestamp = input_data.get('DepartureTimeStamp')
        arrival_timestamp = input_data.get('ArrivalTimeStamp')

        time_info, total_time = await get_travel_time(departure_timestamp,
                                                 arrival_timestamp)

    return time_info, total_time, is_direct_flight


async def get_travel_time(departure_timestamp, arrival_timestamp) -> (str,
                                                                      int):
    """
    Получение общего времени затраченного на полёт в текстовом и числовом
    представлении. Числовое необходимо для сортировки

    :param
        * *departure_timestamp* (``str``) -- время вылета
    :param
        * *arrival_timestamp* (``str``) -- время прилёта

    :rtype: (``str, int``)
    :return: time_info - текстовое представление общего времени полёта,
    total_time - общее время для анализа
    """
    departure_time = dateutil.parser.isoparse(departure_timestamp)
    arrival_time = dateutil.parser.isoparse(arrival_timestamp)
    delta = arrival_time - departure_time

    total_time = (delta.days * 86400) + delta.seconds
    time_info = f'{delta.seconds//3600}ч {(delta.seconds//60)%60}м'
    if delta.days:
        time_info = f'{delta.days}д {time_info}'

    return time_info, total_time


async def sort_data(data, action, filters) -> dict:
    """
    Отсортировать данные по признакам - цена, время.
    По умолчанию сортировка показывает по возрастанию цены

    Доступные методы action:
    expensive - по убыванию цены
    fast - самый быстрый
    slow - самый медленный
    optimal - оптимальны вариант быстры и дешёвый

    :param
        * *data* (``dict``) -- входные данные
    :param
        * *action* (``str``) -- выбор метода сортировки
    :param
        * *filters* (``dict``) -- статический фильтр метода

    :rtype: (``dict``)
    :return: отсортированные данные
    """
    # фильтр по умолчанию на самый дешевый билет
    filter_1 = filters.get('price')
    filter_2 = filters.get('onward')
    filter_3 = filters.get('return')
    reverse = False

    if action == 'expensive':
        reverse = True
    elif action == 'fast':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')
    elif action == 'slow':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')
        reverse = True
    elif action == 'optimal':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')

    sort_filter = lambda x: (x[1][filter_1], x[1][filter_2], x[1][filter_3])

    sorted_data = {k: v for k, v in sorted(data.items(),
                                           key=sort_filter,
                                           reverse=reverse)}
    return sorted_data


def etree_to_dict(elem) -> dict:
    """
    Конвертация элемента xml.etree иерархии с его детьми в словарь

    :param
        * *elem* (``<class 'xml.etree.ElementTree.Element'>``) - элемент иерархии

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


async def parse_xml(*args, **kwargs) -> dict:
    """"""
    filenames = {
        '1': 'RS_Via-3.xml',
        '2': 'RS_ViaOW.xml'
    }
    need_return = kwargs.get('need_return')
    action = kwargs.get('action')
    filters = {
        'price': 'total_amout',
        'onward': 'onward_total_time',
        'return': 'return_total_time'
    }

    if need_return in ['true', 'True', 'TRUE']:
        filename = filenames.get('1')
        columns = {
            'OnwardPricedItinerary': 'Flight',
            'ReturnPricedItinerary': 'Flight',
            'Pricing': '',
        }
    elif need_return in ['false', 'False', 'FALSE']:
        filename = filenames.get('2')
        columns = {
            'OnwardPricedItinerary': 'Flight',
            'Pricing': '',
        }
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
    keys = columns.keys()
    check_keys = set(keys)

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

    start_sort = time.time()

    for flights in data:

        onward_flight = data[flights]['OnwardPricedItinerary']['Flights']['Flight']
        onward_time_info, onward_total_time, onward_is_direct_flight = \
            await get_travel_info(onward_flight)

        return_time_info = None
        return_is_direct_flight = None
        return_total_time = None
        if data[flights].get('ReturnPricedItinerary'):
            return_flight = data[flights]['ReturnPricedItinerary']['Flights']['Flight']
            return_time_info, return_total_time, return_is_direct_flight = \
                await get_travel_info(return_flight)

        total_amount = next((x['text'] for x in data[flights]['Pricing'][
            'ServiceCharges'] if x['type'] == 'SingleAdult' and
                             x['ChargeType'] == 'TotalAmount'), None)

        data[flights].update({
            'total_amout': float(total_amount),
            'onward_total_time': onward_total_time,
            'onward_time_info': onward_time_info,
            'onward_is_direct_flight': onward_is_direct_flight,
            'return_total_time': return_total_time,
            'return_time_info': return_time_info,
            'return_is_direct_flight': return_is_direct_flight,
        })

    data = await sort_data(data, action, filters)
    log.info(f'ENDED SORTING {time.time() - start_sort} sec')

    return data
