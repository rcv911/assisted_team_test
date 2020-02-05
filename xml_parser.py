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


async def diff_parse_xml(file_key: str) -> dict:
    """
    Парсинг данных тега OnwardPricedItinerary по полётам xml файла в словарь.
    За уникальность сделана привязка к отправной точке маршрута по ключу =
    '{ID перевозчика}-{номер рейса}' отправного билета

    :param:
         * *file_key* (``str``) -- ключ файла

    :rtype: (``dict``)
    :return: подготовленные данные о полётах тега OnwardPricedItinerary
    """
    filenames = {
        '1': 'RS_Via-3.xml',
        '2': 'RS_ViaOW.xml'
    }
    filename = filenames.get(file_key)
    source = 'DXB'

    dir_path = os.path.dirname(os.path.realpath(__file__)) + '/data'
    file_path = open(f'{dir_path}/{filename}', "rb")

    log.info(f'FILE {filename}')

    tree = ET.parse(file_path)
    root = tree.getroot()

    data = dict()
    start = time.time()

    count = 0
    for elem in list(root.iter('OnwardPricedItinerary')):
        elem_dict = etree_to_dict(elem)
        tickets = elem_dict['OnwardPricedItinerary']['Flights']['Flight']
        onward_ticket = None
        # is_direct_flight = True
        if isinstance(tickets, list):

            onward_ticket = next((x for x in tickets if x.get('Source') ==
                                  source), None)

            # is_direct_flight = False

            for el in tickets:
                carrier_info = el.pop('Carrier')
                el.update(carrier_info)

        else:
            if tickets.get('Source') == source:
                onward_ticket = tickets

                carrier_info = tickets.pop('Carrier')
                tickets.update(carrier_info)

        if onward_ticket:
            carrier_id = onward_ticket.get('id')
            flight_num = onward_ticket.get('FlightNumber')
            key = f'{carrier_id}-{flight_num}'

        if key not in data.keys():
            data[key] = []

        data[key].append(tickets)

        # if is_direct_flight:
        #     data[key].append(tickets)
        #     count += 1
        # else:
        #     data[key].extend(tickets)
        #     count += len(tickets)

        elem.clear()
    root.clear()

    log.info(f'QTY of tickets {count} in TAG OnwardPricedItinerary | '
             f'{filename}')
    log.info(f'PARSING DATA  {time.time() - start} sec')
    return data


async def compare_xml(data_f1: dict, data_f2: dict) -> dict:
    """
    Получение различия между двумя файлами. Сравниваются данные, полученные
    из метода diff_parse_xml

    :param
        * *data_f1* (``dict``) -- данные по полетам сравнивающего файла
    :param
        * *data_f2* (``dict``) -- данные по полетам сравниваемого файла

    :rtype: (``dict``)
    :return: результат сравнения, в виде
    {
        'differences': [[],{},...], - инфо о различии в параметрах билета
        'new_tickets': [[],{},...], - новые билеты (свежие + не попавшие в
        анализ)
        'wrong_tickets': [[],{},...] - неправильные билеты (с другой отправной
        точкой маршрута)
    }
    """
    compare = dict()
    new = list()
    wrong = list()
    difference = list()
    allowed_source = 'DXB'
    for flight_key, val_list in data_f1.items():
        diff_tickets = list()
        new_val = list()
        wrong_val = list()
        val_to_compare = data_f2.get(flight_key)
        if val_to_compare:

            while val_list:
                val = val_list.pop(0)
                if len(val_to_compare) > 0:
                    to_compare = val_to_compare.pop(0)
                    ticket_compare = dict()
                    if isinstance(val, list):

                        if val[0].get('Source') == allowed_source:
                            val_diff = list()
                            for num, el in enumerate(val):
                                diff = set(to_compare[num].items()) - set(
                                    el.items())
                                val_diff.append(dict(diff))

                            ticket_compare['ticket'] = val
                            ticket_compare['new_ticket'] = to_compare
                            ticket_compare['difference'] = val_diff
                            diff_tickets.append(ticket_compare)
                        else:
                            wrong_val.append(val)

                            if to_compare[0].get('Source') == allowed_source:
                                new_val.append(to_compare)
                            else:
                                wrong_val.append(to_compare)

                    else:
                        diff = set(to_compare.items()) - set(val.items())
                        ticket_compare['ticket'] = val
                        ticket_compare['new_ticket'] = to_compare
                        ticket_compare['difference'] = dict(diff)
                        diff_tickets.append(ticket_compare)
                else:

                    to_new = False
                    if isinstance(val, list):
                        if val[0].get('Source') == allowed_source:
                            to_new = True
                    else:
                        if val.get('Source') == allowed_source:
                            to_new = True

                    if to_new:
                        new_val.append(val)
                    else:
                        wrong_val.append(val)

            # добавляем отсепарированные данные
            if diff_tickets:
                difference.append({flight_key: diff_tickets})

            if new_val:
                new.append({flight_key: new_val})

            if wrong_val:
                wrong.append({flight_key: wrong_val})

            if val_to_compare:
                new.append({flight_key: val_to_compare})

        else:
            new.append({flight_key: val_list})

    compare['differences'] = difference
    compare['new_tickets'] = new
    compare['wrong_tickets'] = wrong

    log.info(f'{compare}')

    return compare
