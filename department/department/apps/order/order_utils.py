# -*- coding: utf-8 -*-
import datetime
import time
import json
import apps_utils
from constants import PrimaryKeyType, OrderStatsState


def correct_time(year, month):
    if month <= 0:
        return year - 1, month + 12
    elif month > 12:
        return year + 1, month - 12
    else:
        return year, month


def make_time(year, month):
    """
    :param year: 某年
    :param month: 某月
    :return: 指定月份的第一天起始时间戳，最后一天结束时间戳
    """
    ct1 = correct_time(year, month)
    start = datetime.datetime(ct1[0], ct1[1], 1)
    ct2 = correct_time(year, month + 1)
    end = datetime.datetime(ct2[0], ct2[1], 1) - datetime.timedelta(seconds=1)
    start_time = int(time.mktime(start.timetuple()))
    end_time = int(time.mktime(end.timetuple()))
    return start_time, end_time


def create_order(cursor, factory_id, seq_id, purchase_id):
    """
    TODO(Bob):  其实可以写一个function来实现该功能
    创建订单，关键参数，客户、产品、预计到货时间
    :param cursor: 数据库游标
    :param factory_id: 工厂id
    :param seq_id: 工厂序号
    :param purchase_id: 对应采购单
    :return:
    """

    sql = '''
        select
            t1.factory,
            t1.supplier_id,
            t1.plan_arrival_time,
            t2.product_id,
            t2.product_count,
            t2.unit_price,
            t1.remark,
            t1.supplier_id
        from
            base_purchases t1
        left join base_purchase_materials t2 on
            t1.id = t2.purchase_id
        where
            t1.id = '{}';'''.format(purchase_id)

    order_id = apps_utils.generate_module_uuid(PrimaryKeyType.order.value, factory_id, seq_id)

    order_sql = "insert into base_orders (id, factory, client_id, plan_arrival_time, order_type, create_time, purchase_id) " \
                "values ('{}', '{}', '{}', {}, '2', extract(epoch from now())::integer, '{}')"
    cursor.execute(sql)
    order_products = cursor.fetchall()

    plan_arrival_time = order_products[0][2]
    remark = order_products[0][6]
    supplier_id = order_products[0][7]
    # 订单状态记录
    order_stats_sql = "insert into base_orders_stats (order_id, state, remark, optime) values " \
                      "('{}', '{}', '{}', extract(epoch from now())::integer)".format(order_id,
                                                                                      OrderStatsState.create.value,
                                                                                      remark)
    cursor.execute(order_sql.format(order_id, supplier_id, factory_id, plan_arrival_time, purchase_id))
    cursor.execute(order_stats_sql)

    for product in order_products:
        product_sql = '''insert into base_order_products (order_id, product_id, product_count, unit_price) 
                        values ('{}', '{}', '{}', '{}');'''.format(order_id, product[3], product[4],
                                                                   product[5])
        cursor.execute(product_sql)

    message = {'resource': 'PyOrderState', 'type': 'PUT',
               'params': {'fac': factory_id, 'id': order_id, 'state': '6'}}
    rabbitmq = apps_utils.UtilsRabbitmq()
    rabbitmq.send_message(json.dumps(message))


def update_order_state(cursor, factory, order_id, state):
    """
    更新订单状态
    :param factory: 工厂ID
    :param cursor: 数据库游标
    :param order_id: 订单id
    :param state:
        "3" -> 订单设置成"运输中"
        "4" -> 订单设置成"已送达"
    """
    if not (state == '3' or state == '4'):
        return
    time_dict = {
        "3": ", deliver_time = extract(epoch from now())::integer",
        "4": ", actual_arrival_time = extract(epoch from now())::integer"
    }
    cursor.execute("update base_orders set state = '{}' {} where id = '{}';".format(state, time_dict[state], order_id))

    # 订单推送状态见README
    notice_type_dict = {
        '3': '3',  # 订单已发货
        '4': '7'  # 订单已送达
    }
    message = {'resource': 'PyOrderState', 'type': 'PUT',
               'params': {'fac': factory, 'id': order_id, 'state': notice_type_dict[state]}}
    rabbitmq = apps_utils.UtilsRabbitmq()
    rabbitmq.send_message(json.dumps(message))
