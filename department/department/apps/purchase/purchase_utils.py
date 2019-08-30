# -*- coding: utf-8 -*-
import logging
import json
import traceback
import time

import apps_utils

from constants import PrimaryKeyType

logger = logging.getLogger('django')


def get_item_price(item, material_ids, material_prices):
    for index, val in enumerate(material_ids):
        if val == item:
            return material_prices[index]
    return 0


def update_purchase_state(cursor, factory_id, order_id, state):
    """更新采购单状态
    采购单状态 1: 待审批， 2: 已审核待确认 3：采购中, 4: 运输中, 5：已入库, 6:已取消
    :param cursor: 数据库游标
    :param factory_id: 工厂ID
    :param order_id: 采购单id
    :param state:
        '3' -> 相关订单审核后，采购单设置"采购中"
        '4' -> 相关发货单确认后，采购单设置"运输中"
        '5' -> 相关入库单确认后，采购单设置 "已入库"
        '6' -> 相关订单取消   采购单设置"已取消"
    """

    purchase_sql = "select purchase_id from base_orders where id = '{}' ".format(order_id)
    cursor.execute(purchase_sql)
    purchase_id = cursor.fetchall()[0][0]
    if purchase_id:
        arrival_time = ''
        if state == '5':
            arrival_time = ", arrival_time = {}".format(int(time.time()))

        update_sql = "update base_purchases set state = '{}' {} where id = '{}'".format(state, arrival_time,
                                                                                        purchase_id)
        cursor.execute(update_sql)
        notice_state_dict = {
            '3': '2',
            '4': '3',
            '5': '4',
            '6': '5'
        }
        message = {'resource': 'PyPurchaseState', 'type': 'PUT',
                   'params': {'fac': factory_id, 'id': purchase_id, 'state': notice_state_dict[state]}}
        rabbitmq = apps_utils.UtilsRabbitmq()
        rabbitmq.send_message(json.dumps(message))


def create_purchase(cursor, factory_id, seq_id, order_id, materials, product_task_id=''):
    """
    采购物料的价格确定？
    :param cursor: 数据库游标
    :param factory_id: 工厂ID
    :param seq_id 工厂序号
    :param order_id: 关联订单id, 可以为空
    :param materials: => [{"id": material_id, "count": material_count}]
    :param product_task_id: 生产单id
    :return: None
    """

    if not materials:
        return
    purchase_id = apps_utils.generate_module_uuid(PrimaryKeyType.purchase.value, factory_id, seq_id)

    try:
        if order_id:
            order_sql = "select plan_arrival_time from base_orders where id = '{}'".format(order_id)
            cursor.execute(order_sql)
            order_res = cursor.fetchone()
            plan_arrival_time = order_res[0] or 0
        else:
            plan_arrival_time = 0

        # 查询符合条件的供应商
        materials_str = '{'
        for m in materials:
            materials_str += m['id'] + ','
        materials_str = materials_str[:-1] + '}'

        supplier = '''
            select
                *
            from
                (
                select
                    supplier_id,
                    array_agg( material_id ) as material_id,
                    array_agg( unit_price ) as price,
                    array_agg(lowest_package) as lowest_package,
                    array_agg(lowest_count) as lowest_count
                from
                    base_supplier_materials
                where
                    factory_id = '{}'
                group by
                    factory_id,
                    supplier_id ) t
            where
                material_id @> '{}' :: varchar[];'''.format(factory_id, materials_str)

        cursor.execute(supplier)
        result = cursor.fetchone()

        if not result:
            if not product_task_id:
                purchase_sql = "insert into base_purchases (id, factory, plan_arrival_time) values ('{}', '{}', {}" \
                               ")".format(purchase_id, factory_id, plan_arrival_time)
            else:
                purchase_sql = "insert into base_purchases (id, product_task_id, factory, plan_arrival_time) values" \
                               " ('{}', '{}', '{}', {})".format(purchase_id, product_task_id, factory_id,
                                                                plan_arrival_time)
            cursor.execute(purchase_sql)
            for material in materials:
                materials_sql = '''insert into base_purchase_materials (purchase_id, product_id, product_count, unit_price) 
                        values ('{}', '{}', '{}', '{}');'''.format(purchase_id, material['id'], material['count'], 0)
                cursor.execute(materials_sql)
        else:
            supplier_id = result[0]
            material_ids = result[1]
            material_prices = result[2]
            purchase_sql = "insert into base_purchases (id, factory, supplier_id, plan_arrival_time) values ('{}', '{}', '{}', {})".format(
                purchase_id, factory_id, supplier_id, plan_arrival_time)
            cursor.execute(purchase_sql)
            for material in materials:
                price = get_item_price(material['id'], material_ids, material_prices)
                materials_sql = '''insert into base_purchase_materials (purchase_id, product_id, product_count, unit_price) 
                        values ('{}', '{}', '{}', '{}');'''.format(purchase_id, material['id'], material['count'],
                                                                   price)
                cursor.execute(materials_sql)
        message = {'resource': 'PyPurchaseState', 'type': 'PUT',
                   'params': {'fac': factory_id, 'id': purchase_id, 'state': '1'}}
        rabbitmq = apps_utils.UtilsRabbitmq()
        rabbitmq.send_message(json.dumps(message))
    except Exception as e:
        traceback.print_exc()
        logger.error(e)


# 自动匹配一个供应商
def choose_one_supplier(suppliers, materials):
    if len(suppliers) == 1:
        return suppliers[0]
    for supplier in suppliers:
        for index, val in enumerate(supplier[1]):
            purchase_count = 0
            price = supplier[2][index]
            package = supplier[3][index]
            count = supplier[4][index]

        pass
