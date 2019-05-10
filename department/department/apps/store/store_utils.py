# -*- coding: utf-8 -*-
# @Time   : 19-4-19 下午3:47
# @Author : huziying
# @File   : store_utils.py
import json
import time

from apps_utils import UtilsPostgresql, UtilsRabbitmq, generate_module_uuid
from constants import PrimaryKeyType


def create_picking_list(cursor, order_id, product_task_id, supplement_id, factory, style, seq_id):
    """
    生成领料单
    :param cursor:
    :param order_id: 订单id
    :param product_task_id: 由生产任务单创建
    :param supplement_id: 由补料单创建
    :param factory:
    :param style: 领料单类型，0: 生产单直接创建，1:补料单创建
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return:
    """
    serial_number = generate_module_uuid(PrimaryKeyType.picking_list.value, factory, seq_id)

    sql = """
    insert into 
      base_store_picking_list (id, order_id, product_task_id, supplement_id, factory, state, style, time) 
    values 
      ('%s', '%s', '%s', '%s', '%s', '0', '%s', %d)
    """ % (serial_number, order_id, product_task_id, supplement_id, factory, style, int(time.time()))
    cursor.execute(sql)

    # 发送RabbitMQ消息
    rabbitmq = UtilsRabbitmq()
    message = {'resource': 'PyPickingList', 'type': 'POST',
               'params': {'fac': factory, 'id': serial_number, 'state': '1'}}
    rabbitmq.send_message(json.dumps(message))


def create_completed_storage(cursor, order_id, product_task_id, factory, seq_id):
    """
    生成完工入库单
    :param cursor:
    :param order_id: 订单id
    :param product_task_id: 生产任务单id
    :param factory:
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return:
    """
    serial_number = generate_module_uuid(PrimaryKeyType.completed_storage.value, factory, seq_id)

    sql = """
    insert into
      base_store_completed_storage (id, order_id, product_task_id, factory, state, time)
    values 
      ('%s', '%s', '%s', '%s', '0', %d);
    """ % (serial_number, order_id, product_task_id, factory, int(time.time()))
    cursor.execute(sql)

    # 发送RabbitMQ消息
    rabbitmq = UtilsRabbitmq()
    message = {'resource': 'PyCompletedStorage', 'type': 'POST',
               'params': {'fac': factory, 'id': serial_number, 'state': '1'}}
    rabbitmq.send_message(json.dumps(message))


def create_invoice(cursor, order_id, completed_storage_id, factory, seq_id):
    """
    生成发货单
    :param cursor:
    :param order_id: 订单id
    :param completed_storage_id: 完工入库单id，其它地方也会用到。没有传""
    :param factory:
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return:
    """
    serial_number = generate_module_uuid(PrimaryKeyType.invoice.value, factory, seq_id)

    sql = """
    insert into 
      base_store_invoice (id, order_id, completed_storage_id, factory, state, time) 
    values 
      ('%s', '%s', '%s', '%s', '0', %d);
    """ % (serial_number, order_id, completed_storage_id, factory, int(time.time()))
    cursor.execute(sql)

    # 发送RabbitMQ消息
    rabbitmq = UtilsRabbitmq()
    message = {'resource': 'PyInvoice', 'type': 'POST',
               'params': {'fac': factory, 'id': serial_number, 'state': '1'}}
    rabbitmq.send_message(json.dumps(message))


def create_purchase_warehousing(cursor, order_id, store_invoice_id, factory, seq_id):
    """
    生成采购入库单
    :param cursor:
    :param order_id: 订单id
    :param store_invoice_id: 发货单id
    :param factory:
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return:
    """
    serial_number = generate_module_uuid(PrimaryKeyType.purchase_warehousing.value, factory, seq_id)

    sql = """
    insert into
      base_store_purchase_warehousing (id, order_id, invoice_id, factory, state, time) 
    values 
      ('%s', '%s', '%s', '%s', '0', %d);
    """ % (serial_number, order_id, store_invoice_id, factory, int(time.time()))
    cursor.execute(sql)

    # 发送RabbitMQ消息
    rabbitmq = UtilsRabbitmq()
    message = {'resource': 'PyPurchaseWarehousing', 'type': 'POST',
               'params': {'fac': factory, 'id': serial_number, 'state': '1'}}
    rabbitmq.send_message(json.dumps(message))


def update_invoice(invoice_id, state, user_id, phone, factory_id, seq_id):
    """
    更新发货单状态
    :param invoice_id: 发货单id
    :param state: 发货单状态 0: 待发货, 1: 已发货, 2: 已送达, 3: 已取消
    :param user_id: user_info.user_id
    :param phone: request.redis_cache["phone"]
    :param factory_id: request.redis_cache["factory_id"]
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return: None
    """
    pgsql = UtilsPostgresql()
    connection, cursor = pgsql.connect_postgresql()

    if state == "1":
        products_list, counts_list = [], []

        cursor.execute("select completed_storage_id from base_store_invoice where factory = '%s' and id = '%s';" %
                       (factory_id, invoice_id))
        type_check = cursor.fetchone()[0]

        # 库存不足, 生产: 发货单等待所有生产单生产完成之后才能发货
        check_sql = """
        select
          t2.state, t2.product_id, t2.complete_count
        from
          (
            select 
              *
            from
              base_store_invoice
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join base_product_task t2 on
          t1.order_id = t2.order_id;            
        """ % (factory_id, invoice_id)

        # 库存充足，直接发货
        direct_sql = """
        select
          t3.product_id, t3.product_count
        from
          (
            select 
              *
            from
              base_store_invoice
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join base_orders t2 on
          t1.order_id = t2.id
        left join base_order_products t3 on
          t2.id = t3.order_id;
        """ % (factory_id, invoice_id)

        if type_check:  # 库存不足, 生产
            cursor.execute(check_sql)
            result = cursor.fetchall()
            for res in result:
                if res[0]:
                    products_list.append(res[1])
                    counts_list.append(res[2])
                    if res[0] != "3":
                        return "还有生产单未完成，无法发货！"
        else:  # 库存充足，直接发货
            cursor.execute(direct_sql)
            result = cursor.fetchall()
            for res in result:
                products_list.append(res[0])
                counts_list.append(res[1])

        # 改变发货单状态
        update_sql = """
        update
          base_store_invoice
        set
          state = '1', deliver_person = '%s', deliver_time = %d
        where
          id = '%s'
        """ % (user_id, int(time.time()), invoice_id)
        cursor.execute(update_sql)

        # 产品预分配库存、实际库存减少
        log_sql_1 = """
        insert into base_products_log
          (product_id, type, count, source, source_id, factory, time)
        values
          ('%s', 'prepared', %f, '1','%s','%s', %d);
        """
        log_sql_2 = """
        insert into base_products_log
          (product_id, type, count, source, source_id, factory, time)
        values
          ('%s', 'actual', %f, '1','%s', '%s', %d);
        """
        # 总库存计算方式?
        storage_sql = """
        update
          base_products_storage
        set
          prepared = prepared - %d, actual = actual - %d
        where
          factory = '%s' and product_id = '%s';
        """
        combine_dict = dict(zip(products_list, counts_list))
        # print(combine_dict)
        timestamp = int(time.time())

        for product in combine_dict:
            cursor.execute(log_sql_1 % (product, -combine_dict[product], invoice_id, factory_id, timestamp))
            cursor.execute(log_sql_2 % (product, -combine_dict[product], invoice_id, factory_id, timestamp))
            cursor.execute(storage_sql % (combine_dict[product], combine_dict[product], factory_id, product))

        # 其它公司推送来的订单才会生成发货单-创建采购入库单, 自建订单不会创建采购入库单
        create_sql = """
        select
          coalesce(t1.order_id, '') as order_id, coalesce(t3.factory, '') as factory
        from
          base_store_invoice t1
        left join base_orders t2 on
          t1.order_id = t2.id
        left join base_purchases t3 on
          t2.purchase_id = t3.id
        where 
          t1.factory = '%s' and t1.id = '%s';
        """ % (factory_id, invoice_id)
        cursor.execute(create_sql)
        order_id, factory = cursor.fetchone()
        # print(order_id, factory)
        if factory:
            create_purchase_warehousing(cursor, order_id, invoice_id, factory, seq_id)

        # 改变订单状态-运输中
        from order.order_utils import update_order_state
        update_order_state(cursor, factory_id, order_id, '3')

    elif state == "2":
        cursor.execute("update base_store_invoice set state = '2' where id = '%s';" % invoice_id)

        # 发送RabbitMQ消息
        rabbitmq = UtilsRabbitmq()
        message = {'resource': 'PyStoreState', 'type': 'POST',
                   'params': {'fac': factory_id, 'id': invoice_id, 'state': '2'}}
        rabbitmq.send_message(json.dumps(message))
    elif state == "3":
        cursor.execute("update base_store_invoice set state = '3' where id = '%s';" % invoice_id)
    else:
        return "发货单状态错误！"

    connection.commit()


def update_picking_list(picking_id, state, user_id, factory_id, action):
    """
    更新领料单状态
    :param picking_id: 领料单id
    :param state: 领料单状态, 0: 未备料, 1: 待领料, 2: 已领料
    :param user_id:
    :param factory_id:
    :param action: 操作类型 1: 返回二维码内容, 记录接收人信息. 2: 入库操作, 更新状态
    :return:
    """
    rabbitmq = UtilsRabbitmq()
    pgsql = UtilsPostgresql()
    connection, cursor = pgsql.connect_postgresql()

    if state not in ["1", "2"]:
        return "状态代号错误！"

    sql = """
    select
      t1.product_task_id, t1.supplement_id, t1.style,
      t2.material_ids, t2.material_counts,t2.target_count
    from
      base_store_picking_list t1
    left join base_product_task t2 on
      t1.product_task_id = t2.id
    where 
      t1.id = '%s';
    """ % picking_id

    sql_1 = """
    select
      t2.id as supplement_id, t2.material_ids, t2.material_counts
    from
      base_store_picking_list t1
    left join base_material_supplement t2 on
      t1.supplement_id = t2.id
    where 
      t1.id = '%s';
    """ % picking_id

    sql_2 = """
    insert into
      base_materials_log (material_id, type, count, source, source_id, factory, time) 
    values 
      ('%s', 'actual', %f, '1', '%s','%s', %d);
    """

    sql_3 = """
    insert into
      base_materials_log (material_id, type, count, source, source_id, factory, time) 
    values 
      ('%s', 'prepared', %f, '1', '%s', '%s', %d);
    """

    sql_4 = """
    update
    base_materials_storage
    set
      actual = actual - %f, prepared = prepared - %f
    where 
      material_id = '%s' and factory = '%s';
    """

    timestamp = int(time.time())
    try:
        cursor.execute(sql)
        product_task_id, supplement_id, style, material_ids, material_counts, target_count = cursor.fetchone()
        # print(product_task_id, supplement_id, style, material_ids, material_counts, target_count)
        # 领料单状态，0: 未备料，1: 待领料，2: 已领料
        if state == "1":  # 未备料——>待领料
            # 更新领料单状态
            cursor.execute("update base_store_picking_list set state = '1', waited_time = %d where id = '%s';" %
                           (timestamp, picking_id))

            # 改变生产任务单状态/改变补料单对应的生产单状态
            if not supplement_id and style == "0":  # 生产单直接创建
                from products.products_utils import update_product_task
                update_product_task(product_task_id, "1")
            else:  # 补料单创建
                cursor.execute(sql_1)
                supplement_id = cursor.fetchone()[0]
                from products.products_utils import update_material_supplement
                update_material_supplement(supplement_id, "1")

            # 发送RabbitMQ消息给生产部
            message = {'resource': 'PyPickingList', 'type': 'POST',
                       'params': {'fac': factory_id, 'id': picking_id, 'state': '2'}}
            rabbitmq.send_message(json.dumps(message))

            connection.commit()
            return {"res": 0}
        else:  # 待领料——>已领料
            if action == "1":  # 返回二维码内容，记录发料人
                cursor.execute(
                    "update base_store_picking_list set send_person = '%s' where id = '%s';" % (user_id, picking_id))
                connection.commit()
                return {"res": 0}
            elif action == "2":  # 入库操作, 更新领料单状态
                cursor.execute("update base_store_picking_list set state = '2', receive_person = '%s', "
                               "picking_time = %d where id = '%s';" % (user_id, timestamp, picking_id))

                # 物料实际库存减少/物料预分配库存减少
                if not supplement_id and style == "0":  # 生产单直接创建
                    # 生产单中的物料数量（单个产品）,要乘以产生的数量
                    material_counts = [i * target_count for i in material_counts]

                    from products.products_utils import update_product_task
                    update_product_task(product_task_id, "2")  # 改变生产任务单状态-生产中
                else:  # 补料单创建
                    cursor.execute(sql_1)
                    # 补料单创建, 物料数量是用户填写的，不要相乘
                    supplement_id, material_ids, material_counts = cursor.fetchone()
                    # print(supplement_id, material_ids, material_counts)
                    from products.products_utils import update_material_supplement
                    update_material_supplement(supplement_id, "2")  # 改变补料单状态-已补料

                combine_dict = dict(zip(material_ids, material_counts))
                # print(combine_dict)
                for material in combine_dict:
                    cursor.execute(sql_2 % (material, -combine_dict[material], picking_id, factory_id, timestamp))
                    cursor.execute(sql_3 % (material, -combine_dict[material], picking_id, factory_id, timestamp))
                    cursor.execute(sql_4 % (combine_dict[material], combine_dict[material], material, factory_id))

                connection.commit()
                return {"res": 0}
            else:
                return "操作类型代号错误！"
    except Exception:
        raise Exception
    finally:
        pgsql.disconnect_postgresql(connection)


def update_completed_storage(completed_id, state, user_id, factory_id, seq_id):
    """
    更新完工入库单状态
    :param completed_id: 完工入库单id
    :param state: 0: 未入库，1: 已入库
    :param user_id: request.redis_cache["user_id"]
    :param factory_id: request.redis_cache["factory_id"]
    :param seq_id: 工厂序号 seq_id = request.redis_cache["seq_id"]
    :return: None
    """
    pgsql = UtilsPostgresql()
    connection, cursor = pgsql.connect_postgresql()

    if state != "1":
        return "完工入库单状态错误！"

    cursor.execute("select state, order_id from base_store_completed_storage where id = '%s' and factory = '%s';"
                   % (completed_id, factory_id))
    state_check, order_id = cursor.fetchone()
    if state_check != "0":
        return "状态错误，无法操作！"

    timestamp = int(time.time())

    # 发送RabbitMQ消息
    rabbitmq = UtilsRabbitmq()
    message = {'resource': 'PyCompletedStorage', 'type': 'POST',
               'params': {'fac': factory_id, 'id': completed_id, 'state': '2'}}
    rabbitmq.send_message(json.dumps(message))

    # 改变状态, 补充交接人
    update_sql = """
    update
      base_store_completed_storage
    set
      state = '1', completed_time = %d, send_person = '%s'
    where
      id = '%s';
    """ % (timestamp, user_id, completed_id)
    cursor.execute(update_sql)

    # 找多个生产任务单的产品id，数量(只找已完工的生产单, 拆单的生产单完工数量为0, 会多出来log记录)
    task_sql = """
    select
      t2.product_id, coalesce(t2.complete_count, 0) as count
    from
      (
        select 
          *
        from 
          base_store_completed_storage
        where 
          factory = '%s' and id = '%s'
      ) t1
    left join (select * from base_product_task where state = '3') t2 on
      t1.order_id = t2.order_id;
    """ % (factory_id, completed_id)
    cursor.execute(task_sql)
    result = cursor.fetchall()

    # 产品实际库存-增加
    log_sql_1 = """
    insert into base_products_log
      (product_id, type, count, source, source_id, factory, time)
    values
      ('%s', 'actual', %f, '0', '%s', '%s', %d);
    """
    # 产品预生产库存-减少
    log_sql_2 = """
    insert into base_products_log
      (product_id, type, count, source, source_id, factory, time)
    values
      ('%s', 'pre_product', %f, '0', '%s', '%s', %d);
    """
    # 更新库存
    storage_sql = """
    update
      base_products_storage
    set
      actual = actual + %f, pre_product = pre_product - %f
    where
      factory = '%s' and product_id = '%s';
    """

    for res in result:
        cursor.execute(log_sql_1 % (res[0], res[1], completed_id, factory_id, timestamp))
        cursor.execute(log_sql_2 % (res[0], -res[1], completed_id, factory_id, timestamp))
        cursor.execute(storage_sql % (res[1], res[1], factory_id, res[0]))

    # 生成发货单
    create_invoice(cursor, order_id, completed_id, factory_id, seq_id)

    connection.commit()


def update_purchase_warehousing(warehousing_id, state, phone, factory_id):
    """
    更新采购入库单状态
    :param warehousing_id: 采购入库单id
    :param state: 0: 未入库，1: 已入库
    :param phone: request.redis_cache["phone"]
    :param factory_id: request.redis_cache["factory_id"]
    :return: None
    """
    pgsql = UtilsPostgresql()
    connection, cursor = pgsql.connect_postgresql()

    if state != "1":
        return "采购入库单状态错误！"

    cursor.execute("select state, order_id from base_store_purchase_warehousing where id = '%s' and factory = '%s';"
                   % (warehousing_id, factory_id))
    state_check, order_id = cursor.fetchone()
    if state_check != "0":
        return "状态错误，无法操作！"

    timestamp = int(time.time())

    update_1 = """
    update 
      base_store_purchase_warehousing
    set
      state = '1', income_person = '%s', income_time = %d
    where 
      factory = '%s' and id = '%s';
    """ % (phone, timestamp, factory_id, warehousing_id)

    update_2 = """
    select
      t4.materials_list, t4.counts_list
    from
      (
        select
          *
        from
          base_store_purchase_warehousing
        where 
          factory = '%s' and id = '%s'
      ) t1
    left join base_orders t2 on
      t1.order_id = t2.id
    left join base_purchases t3 on
      t2.purchase_id = t3.id
    left join 
      (
        select 
          purchase_id, array_agg(product_id) as materials_list, array_agg(product_count) as counts_list
        from
          base_purchase_materials 
        group by 
          purchase_id          
      ) t4 on
      t3.id = t4.purchase_id;
    """ % (factory_id, warehousing_id)

    update_3 = """
    insert into
      base_materials_log (material_id, type, count, source, source_id, factory, time) 
    values 
      ('%s', 'actual', %f, '0', '%s', '%s', %d);
    """

    update_4 = """
    insert into
      base_materials_log (material_id, type, count, source, source_id, factory, time) 
    values 
      ('%s', 'on_road', %f, '0', '%s', '%s', %d);
    """

    update_5 = """
    update
      base_materials_storage
    set
      actual = actual + %f, on_road = on_road - %f
    where 
      factory = '%s' and material_id = '%s'
    """

    search_sql = """
    select
      t1.order_id, t1.invoice_id,
      t2.factory as order_factory,
      t3.id as purchase_id,
      t4.seq_id
    from
      base_store_purchase_warehousing t1
    left join 
      base_orders t2 on t1.order_id = t2.id
    left join 
      base_purchases t3 on t2.purchase_id = t3.id
    left join 
      factorys t4 on t2.factory = t4.id
    where 
      t1.factory = '%s' and t1.id = '%s';
    """ % (factory_id, warehousing_id)

    # 改变采购入库单状态-入库
    cursor.execute(update_1)

    # 物料库存log记录/更新物料总库存: 物料实际库存增加, 物料在途库存减少
    cursor.execute(update_2)
    result2 = cursor.fetchone()
    combine_dict = dict(zip(result2[0] or [], result2[1] or []))
    # print(combine_dict)
    for material in combine_dict:
        cursor.execute(update_3 % (material, combine_dict[material], warehousing_id, factory_id, timestamp))
        cursor.execute(update_4 % (material, -combine_dict[material], warehousing_id, factory_id, timestamp))
        cursor.execute(update_5 % (combine_dict[material], combine_dict[material], factory_id, material))

    cursor.execute(search_sql)
    search = cursor.fetchone()
    order_id, store_invoice_id, order_factory, purchase_id, seq_id = search[0], search[1], search[2], search[3], search[
        4]
    # print(order_id, store_invoice_id, order_factory, purchase_id, seq_id)

    # 改变采购单状态-已入库
    from purchase.purchase_utils import update_purchase_state
    update_purchase_state(cursor, factory_id, order_id, "5")

    # 改变订单状态-已送达
    from order.order_utils import update_order_state
    update_order_state(cursor, order_factory, order_id, "4")
    # cursor.execute(update_7 % (timestamp, order_id))

    # 改变发货单状态-已送达
    update_invoice(store_invoice_id, "2", None, phone, order_factory, seq_id)

    connection.commit()
