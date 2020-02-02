# -*- coding: utf-8 -*-
import time
import dateutil.parser
import logging
from xml_parser import parse_xml_to_dict

log = logging.getLogger(__name__)


async def get_travel_info(input_data) -> (str, int, bool):
    """
    Получение информаии о полёте

    :param
        * *input_data* (``list or dict``) -- входные данные о полёте(ах)

    :rtype: (``str, int, bool``)
    :return:
    """
    is_direct_flight = True
    if isinstance(input_data, list):
        is_direct_flight = False
        departure_timestamps = [x['DepartureTimeStamp'] for x in input_data]
        arrival_timestamps = [x['ArrivalTimeStamp'] for x in input_data]
        time_info, total_time = await get_travel_time(departure_timestamps[0],
                                                      arrival_timestamps[1])

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


async def sort_data(data, action, filters, need_return) -> dict:
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
    :param
        * *need_return* (``bool``) -- наличие обратного перелета

    :rtype: (``dict``)
    :return: отсортированные данные
    """
    check_return = ['true', 'True', 'TRUE']
    sort_filter = None
    # фильтр по умолчанию на самый дешевый билет
    filter_1 = filters.get('price')
    filter_2 = filters.get('onward')
    filter_3 = filters.get('return')
    reverse = False

    if action == 'cheap':
        sort_filter = lambda x: (x[1][filter_1], x[1][filter_2],
                                 x[1][filter_3])
    if action == 'expensive':
        reverse = True
        if need_return in check_return:
            sort_filter = lambda x: (x[1][filter_1], -x[1][filter_2],
                                     -x[1][filter_3])
        else:
            sort_filter = lambda x: (x[1][filter_1], -x[1][filter_2],
                                     x[1][filter_3])
    elif action == 'fast':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')
        sort_filter = lambda x: (x[1][filter_1], x[1][filter_2],
                                 x[1][filter_3])
    elif action == 'slow':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')
        reverse = True
        sort_filter = lambda x: (x[1][filter_1], x[1][filter_2],
                                 -x[1][filter_3])
    elif action == 'optimal':
        filter_1 = filters.get('onward')
        filter_2 = filters.get('return')
        filter_3 = filters.get('price')
        sort_filter = lambda x: (x[1][filter_1], x[1][filter_2],
                                 x[1][filter_3])

    sorted_data = {k: v for k, v in sorted(data.items(),
                                           key=sort_filter,
                                           reverse=reverse)}
    return sorted_data


async def get_flight_tickets(**kwargs) -> dict:
    """"""
    action = kwargs.get('action')
    need_return = kwargs.get('need_return')
    filters = {
        'price': 'total_amout',
        'onward': 'onward_total_time',
        'return': 'return_total_time'
    }

    data = await parse_xml_to_dict(**kwargs)

    start_sort = time.time()

    for flights in data:

        onward_flight = data[flights]['OnwardPricedItinerary']['Flights'][
            'Flight']
        onward_time_info, onward_total_time, onward_is_direct_flight = \
            await get_travel_info(onward_flight)

        return_time_info = None
        return_is_direct_flight = None
        return_total_time = None
        if data[flights].get('ReturnPricedItinerary'):
            return_flight = data[flights]['ReturnPricedItinerary']['Flights'][
                'Flight']
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
            'return_is_direct_flight': return_is_direct_flight
        })

    data = await sort_data(data, action, filters, need_return)
    log.info(f'ENDED SORTING {time.time() - start_sort} sec')

    return data
