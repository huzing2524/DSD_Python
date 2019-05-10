# -*- coding: utf-8 -*-
import time
import datetime

from apps_utils import AliOss


def get_week_timestamp():
    # return the timestamp of monday
    num_in_week = datetime.datetime.now().weekday()
    dtime = datetime.datetime.now().strftime('%Y-%m-%d')
    today = datetime.datetime.strptime(dtime, '%Y-%m-%d')
    monday = today + datetime.timedelta(days=-num_in_week)
    start_timestamp = int(time.mktime(monday.timetuple()))
    end_timestamp = start_timestamp + 7*86400
    return start_timestamp, end_timestamp


def get_month_timestamp(num):
    # return two timestamps about month, start_timestamp and end_timestamp
    # 0: this month
    # 1: last month
    # ...
    now = datetime.datetime.now()
    correct_time_1 = correct_time(now.year, now.month - num)
    start = datetime.datetime(correct_time_1[0], correct_time_1[1], 1)
    correct_time_2 = correct_time(now.year, now.month - (num - 1))
    end = datetime.datetime(correct_time_2[0], correct_time_2[1], 1) - datetime.timedelta(seconds=1)
    start_timestamp = int(time.mktime(start.timetuple()))
    end_timestamp = int(time.mktime(end.timetuple()))
    return start_timestamp, end_timestamp


def get_salesman(cursor, start_timestamp, end_timestamp, factory_id):
    cursor.execute(
        "select creator, sum(b.sell_price) as price from orders as a, order_products as b where a.factory = '%s' "
        "and a.time >%d and a.time < %d and a.id = b.order_id and b.product_count is not null group by creator order"
        " by price desc;" % (factory_id, start_timestamp, end_timestamp))
    tmp = cursor.fetchall()
    phone, sales = tmp[0] if tmp else (None, 0)
    if sales is None:
        sales = 0
    cursor.execute("select name, image from user_info where phone = '%s';" % phone)
    tmp = cursor.fetchall()
    if tmp:
        if isinstance(tmp[0][0], (list, tuple)):
            name = ''.join(tmp[0][0])
        elif isinstance(tmp[0][0], memoryview):
            name = tmp[0][0].tobytes()
        else:
            name = tmp[0][0] or ''
        if isinstance(tmp[0][1], (list, tuple)):
            image = ''.join(tmp[0][1])
        elif isinstance(tmp[0][1], memoryview):
            image = tmp[0][1].tobytes()
        else:
            image = tmp[0][1]
    else:
        name = ''
        image = ''

    return name, image, sales


def get_sales(cursor, start_timestamp, end_timestamp, factory_id):
    cursor.execute("select sum(sell_price) as price from order_products where order_id in (select id from orders where "
                "factory = '%s' and time between %d and %d);" % (factory_id, start_timestamp, end_timestamp))
    # print("select sum(sell_price) as price from order_products where order_id in (select id from orders where "
    #             "factory = '%s' and time between %d and %d);" % (factory_id, start_timestamp, end_timestamp))
    tmp = cursor.fetchall()
    sales = tmp[0][0] if tmp and tmp[0] else 0
    if not sales:
        sales = 0
    return sales


def correct_time(year, month):
    if month <= 0:
        return year - 1, month + 12
    elif month > 12:
        return year + 1, month - 12
    else:
        return year, month


def finance_format(finance):
    # recieve a float or int
    if isinstance(finance, int):
        return format(finance, ',') + '.00'
    elif isinstance(finance, float):
        finance = round(finance, 2)
        tmp = str(finance)
        if tmp[-2] == '.':
            return format(finance, ',') + '0'
        else:
            return format(finance, ',')


def image_to_string(image):
    if isinstance(image, memoryview):
        return image.tobytes().decode()
    elif isinstance(image, bytes):
        return image.decode()
    else:
        return image


def purchase(cursor, start_timestamp, end_timestamp, factory_id):
    cursor.execute(
        "select material_type_id, sum(total_price) as price from purchase where factory = '%s' and buy_time between"
        " %d and %d group by material_type_id order by price desc;" % (
            factory_id, start_timestamp, end_timestamp))
    tmp = cursor.fetchall()
    length = len(tmp)

    result = []
    top_three = tmp[:3]
    other = sum(i[1] for i in tmp[3:])

    for i in top_three:
        cursor.execute(
            "select a.name, b.name from material_types as a, material_categories as b where a.id = '%s' and b.id ="
            " a.category_id;" %
            i[0])
        data = cursor.fetchall()
        if data:
            tmp = dict()
            tmp['name'] = data[0][0]
            tmp['category_name'] = data[0][1]
            tmp['cost'] = finance_format(i[1])
            result.append(tmp)
        else:
            tmp = dict()
            tmp['name'] = None
            tmp['category_name'] = None
            tmp['cost'] = finance_format(i[1])
            result.append(tmp)
    if length > 3:
        tmp = dict()
        tmp['category_name'] = '其他'
        tmp['cost'] = finance_format(other)
        result.append(tmp)
    return result


def month_part():
    # month
    month = int(datetime.datetime.now().strftime('%m'))
    return month


def order_part(factory_id, cursor):
    # order

    start_timestamp, end_timestamp = get_week_timestamp()
    w_sales = get_sales(cursor, start_timestamp, end_timestamp, factory_id)

    w_champ_name, w_champ_id, w_champ_sales = get_salesman(cursor, start_timestamp, end_timestamp,  factory_id)

    start_timestamp, end_timestamp = get_month_timestamp(0)
    m_sales = get_sales(cursor, start_timestamp, end_timestamp, factory_id)
    m_champ_name, m_champ_id, m_champ_sales = get_salesman(cursor, start_timestamp, end_timestamp, factory_id)

    # 今年上月
    # start_timestamp, end_timestamp = get_month_timestamp(1)
    # lm_sales = get_sales(cursor, start_timestamp, end_timestamp, factory_id)
    # m_rose = round((m_sales - lm_sales) / lm_sales * 100, 2) if lm_sales else 0

    # 去年同月
    start_timestamp, end_timestamp = get_month_timestamp(12)
    ly_sales = get_sales(cursor, start_timestamp, end_timestamp, factory_id)
    m_rose = round((m_sales - ly_sales) / ly_sales * 100, 2) if ly_sales else 0

    alioss = AliOss()
    order = dict()
    order['w_sales'] = finance_format(w_sales)
    order['w_champ_sales'] = finance_format(w_champ_sales)
    order['w_champ_id'] = alioss.joint_image(image_to_string(w_champ_id)) if w_champ_id else ''
    order['w_champ_name'] = w_champ_name
    order['m_sales'] = finance_format(m_sales)
    order['m_rose'] = m_rose
    order['m_champ_sales'] = finance_format(m_champ_sales)
    order['m_champ_id'] = alioss.joint_image(image_to_string(m_champ_id)) if m_champ_id else ''
    order['m_champ_name'] = m_champ_name

    return order


def finance_part(factory_id, cursor):
    # finance

    start_timestamp, end_timestamp = get_month_timestamp(0)
    month_0 = get_sales(cursor, start_timestamp, end_timestamp, factory_id)
    start_timestamp, end_timestamp = get_month_timestamp(1)
    month_1 = get_sales(cursor, start_timestamp, end_timestamp, factory_id)

    rose = month_0 - month_1
    four_months = []

    start_timestamp, end_timestamp = get_month_timestamp(2)
    month_2 = get_sales(cursor, start_timestamp, end_timestamp, factory_id)
    start_timestamp, end_timestamp = get_month_timestamp(3)
    month_3 = get_sales(cursor, start_timestamp, end_timestamp, factory_id)

    month = month_part()
    four_months.append({'m': str(month), 'sales': finance_format(month_0)})
    four_months.append({'m': str(correct_time(0, month - 1)[1]), 'sales': finance_format(month_1)})
    four_months.append({'m': str(correct_time(0, month - 2)[1]), 'sales': finance_format(month_2)})
    four_months.append({'m': str(correct_time(0, month - 3)[1]), 'sales': finance_format(month_3)})

    finance = dict()
    finance['rose'] = finance_format(rose)
    finance['list'] = four_months

    return finance


def purchase_part(factory_id, cursor):
    # purchase

    start_timestamp, end_timestamp = get_week_timestamp()
    w = purchase(cursor, start_timestamp, end_timestamp, factory_id)
    start_timestamp, end_timestamp = get_month_timestamp(0)
    m = purchase(cursor, start_timestamp, end_timestamp, factory_id)
    material = {'w': w, 'm': m}

    return material


def run(factory_id, cursor):
    # result
    result = dict()
    result['month'] = str(month_part())
    result['order'] = order_part(factory_id, cursor)
    result['finance'] = finance_part(factory_id, cursor)
    result['material'] = purchase_part(factory_id, cursor)
    return result
