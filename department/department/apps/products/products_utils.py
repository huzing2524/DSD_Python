import time
import json
import random
import logging

from apps_utils import UtilsPostgresql, UtilsRabbitmq, AliOss, generate_module_uuid

logger = logging.getLogger('django')


def create_product_task(**kwargs):
    """ 创建生产单
    kwargs:
        order_id： 订单id
        factory_id： 工厂id
        product_id： 产品id
        target_count： 目标生产数量
        remark： 备注
        plan_complete_time： 计划完成生成的时间
        seq_id: 工厂序号id
    return:
        0： 创建成功
        1： 创建失败
    """
    postgresql = UtilsPostgresql()
    conn, cur = postgresql.connect_postgresql()

    Time = int(time.time())

    sql_1 = """
        insert into 
            base_product_task(
                id, 
                factory, 
                product_id, 
                target_count, 
                remark, 
                time, 
                plan_complete_time, 
                order_id) 
        values('{}', '{}', '{}', {}, '{}', {}, {}, '{}');"""
    sql_2 = """
        select 
            material_ids, 
            material_counts
        from
            base_product_processes
        where 
            factory = '{}'
            and product_id = '{}';"""
    sql_3 = """
        update 
            base_product_task
        set
            material_ids = '{}', 
            material_counts = '{}'
        where 
            id = '{}';"""
    try:
        Id = generate_module_uuid('02', kwargs['factory_id'], kwargs['seq_id'])
        cur.execute(sql_1.format(Id, kwargs['factory_id'], kwargs['product_id'], kwargs['target_count'],
                                 kwargs['remark'], Time, kwargs['plan_complete_time'], kwargs['order_id']))
        cur.execute(sql_2.format(kwargs['factory_id'], kwargs['product_id']))
        tmp = cur.fetchall()
        # 合并
        material_ids = list()
        material_counts = list()
        for i in tmp:
            for x, y in zip(i[0], i[1]):
                if x in material_ids:
                    index = material_ids.index(x)
                    material_counts[index] += y
                else:
                    material_ids.append(x)
                    material_counts.append(y)
        material_ids = '{' + ','.join(material_ids) + '}'
        material_counts = '{' + ','.join(str(i) for i in material_counts) + '}'
        cur.execute(sql_3.format(material_ids, material_counts, Id))
        conn.commit()

    except Exception as e:
        logger.error(e)
        return {'res': 1, 'errmsg': '服务器异常'}

    # MRP计算
    material_mrp_calculation(Id, kwargs['seq_id'])
    # 推送：新增生产任务
    # message = get_message_data_product(cur, Id, '', '新增生产任务')
    message = {'resource': 'PyProductTaskCreate',
               'type': '',
               'params': {"fac": kwargs['factory_id'], "id": Id, "state": "1"}}
    rabbitmq = UtilsRabbitmq()
    rabbitmq.send_message(json.dumps(message))

    postgresql.disconnect_postgresql(conn)
    return {'res': 0}


def update_product_task(Id, state, user_id=''):
    """ 更新生产单
    args：
        id： 生产单id
        state： 1：待领料，2：生产中
        user_id: 修改者id
    """
    postgresql = UtilsPostgresql()
    conn, cur = postgresql.connect_postgresql()

    sql_0 = "select factory from base_product_task where id = '{}';"

    if state == '1':
        sql = """
            update 
                base_product_task
            set 
                state = '{}',
                prepare_time = {}
            where 
                id = '{}';"""
        # sql_1 = "select t1.order_id, t2.seq_id from base_product_task t1 left join factorys t2 on " \
        #         "t1.factory = t2.id where t1.id = '{}';"
    else:
        sql = """
            update 
                base_product_task
            set 
                state = '{}', 
                start_time = {}
            where 
                id = '{}';"""

    try:
        cur.execute(sql_0.format(Id))
        factory_id = cur.fetchone()[0]
        cur.execute(sql.format(state, int(time.time()), Id))
        # if state == '1':
        #     cur.execute(sql_1.format(Id))
        #     tmp = cur.fetchone()
        #     order_id, seq_id = tmp
        #     from store.store_utils import create_picking_list
        #     create_picking_list(cur, order_id, Id, '', factory_id, 0, seq_id)
        conn.commit()
    except Exception as e:
        logger.error(e)
        return {'res': 1, 'errmsg': '服务器异常'}

    # 推送：待领料/生产中
    if state == '1':
        # message = get_message_data_product(cur, Id, user_id, '待领料')
        message = {'resource': 'PyProductTaskCreate',
                   'type': '',
                   'params': {"fac": factory_id, "id": Id, "state": "2"}}
    else:
        # message = get_message_data_product(cur, Id, user_id, '生产中')
        message = {'resource': 'PyProductTaskCreate',
                   'type': '',
                   'params': {"fac": factory_id, "id": Id, "state": "3"}}

    rabbitmq = UtilsRabbitmq()
    rabbitmq.send_message(json.dumps(message))

    postgresql.disconnect_postgresql(conn)
    return {'res': 0}


def update_material_supplement(Id, state, user_id=''):
    """ 更新补料单
    args：
        id： 补料单id
        state： 1：待补料，2：已补料
        user_id： 修改者id
    """
    postgresql = UtilsPostgresql()
    conn, cur = postgresql.connect_postgresql()

    sql_0 = "select factory from base_material_supplement where id = '{}';"
    sql_1 = "update base_material_supplement set state = '{}' where id = '{}';"""

    try:
        cur.execute(sql_0.format(Id))
        # product_task_id = cur.fetchone()[0]
        factory_id = cur.fetchone()[0]
        cur.execute(sql_1.format(state, Id))
        conn.commit()
    except Exception as e:
        logger.error(e)
        return {'res': 1, 'errmsg': '服务器异常'}

    # 推送: 补料-待补料/补料-已补料
    if state == '1':
        # message = get_message_data_material(cur, product_task_id, user_id, '补料-待补料')
        message = {'resource': 'PyProductMaterialSupplementUpdate',
                   'type': '',
                   'params': {"fac": factory_id, "id": Id, "state": "1"}}
    else:
        # message = get_message_data_material(cur, product_task_id, user_id, '补料-已补料')
        message = {'resource': 'PyProductMaterialSupplementUpdate',
                   'type': '',
                   'params': {"fac": factory_id, "id": Id, "state": "2"}}
    rabbitmq = UtilsRabbitmq()
    rabbitmq.send_message(json.dumps(message))

    postgresql.disconnect_postgresql(conn)
    return {'res': 0}


def correct_time(year, month):
    """校对年和月"""
    if month <= 0:
        return year - 1, month + 12
    elif month > 12:
        return year + 1, month - 12
    else:
        return year, month


def create_id(sql):
    """生成对应sql的唯一随机11位数字组合id"""
    postgresql = UtilsPostgresql()
    conn, cur = postgresql.connect_postgresql()

    tmp = 1
    while tmp != 0:
        Id = "".join(random.choice("0123456789") for i in range(11))
        cur.execute(sql.format(Id))
        tmp = cur.fetchone()[0]

    postgresql.disconnect_postgresql(conn)
    return Id


def material_mrp_calculation(product_task_id, seq_id):
    """ 物料库存MRP计算，执行计算之后会视结果自动生动采购单或领料单
    逻辑：
        物料可用库存 = 物料实际库存 ＋ 在途库存 - 预分配库存 - 安全库存
        物料采购需求 = （本期需求量 - 物料可用库存）/ （1 - 损耗系数）
    """
    postgresql = UtilsPostgresql()
    conn, cur = postgresql.connect_postgresql()

    Time = int(time.time())

    sql_0 = """
        select 
            factory, 
            material_ids, 
            material_counts,
            target_count, 
            order_id 
        from 
            base_product_task
        where 
            id = '{}';"""
    sql_1 = """
        select
            coalesce(actual, 0) + coalesce(on_road, 0) - coalesce(prepared, 0) - coalesce(safety, 0) as available_count
        from 
            base_materials_storage
        where
            factory = '{}'
            and material_id = '{}';"""
    sql_2 = """
        insert into
            base_materials_log(
                material_id, 
                count, 
                type, 
                factory, 
                product_count, 
                source, 
                source_id, 
                time)
        values ('{}', {}, 'prepared', '{}', {}, '3', '{}', {});"""
    sql_3 = """
        update
            base_materials_storage
        set 
            prepared = prepared + {}
        where
            material_id = '{}' 
            and factory = '{}';"""
    sql_4 = "select loss_coefficient from base_materials where id = '{}' and factory = '{}';"
    sql_5 = "update base_product_task set state = '{0}', prepare_time = {1}, start_time = {1} where id = '{2}';"
    sql_6 = "update base_product_task set purchase_state = '1' where id = '{}';"
    try:
        cur.execute(sql_0.format(product_task_id))
        tmp = cur.fetchone()
        # 计算当前库存可生产产品个数
        counts = []
        for x, y in zip(tmp[1], tmp[2]):
            cur.execute(sql_1.format(tmp[0], x))
            available_count = cur.fetchone()[0]
            available_count = 0 if available_count < 0 else available_count
            # 暂时设为整数吧
            counts.append(available_count // y)
        # 判断产品不需要物料还是物料为空
        if counts:
            number = min(counts)
            flag = True
        else:
            number = 0
            flag = False

        if not flag:
            # 因为产品不需要物料，直接进入生产中状态, 还有更新状态
            cur.execute(sql_5.format('2', int(time.time()), product_task_id))
        elif number >= tmp[3]:
            # 预分配库存
            for x, y in zip(tmp[1], tmp[2]):
                cur.execute(sql_2.format(x, tmp[3] * y, tmp[0], tmp[3], product_task_id, Time))
                cur.execute(sql_3.format(tmp[3] * y, x, tmp[0]))
            # 创建领料单
            from store.store_utils import create_picking_list
            create_picking_list(cur, tmp[4], product_task_id, '', tmp[0], 0, seq_id)
            # 更新生产单状态为待领料
            # cur.execute(sql_5.format('1', product_task_id))
        elif number == 0:
            purchase_counts = list()
            # 不需要预分配库存
            for x, y in zip(tmp[1], tmp[2]):
                cur.execute(sql_4.format(x, tmp[0]))
                loss = cur.fetchone()
                loss_coefficient = loss[0] if loss else 0
                purchase_count = (tmp[3] - number) / (1 - loss_coefficient)
                purchase_counts.append({'id': x, 'count': purchase_count})
            # 生成采购单
            from purchase.purchase_utils import create_purchase
            create_purchase(cur, tmp[0], seq_id, tmp[4], purchase_counts, product_task_id)
            # 更新采购状态
            cur.execute(sql_6.format(product_task_id))
        conn.commit()
    except Exception as e:
        logger.error(e)
        return {'res': 1, 'errmsg': '服务器异常'}

    postgresql.disconnect_postgresql(conn)
    return {'res': 0}


def get_message_data_product(cur, product_task_id, user_id, state):
    """ 推送时与产品有关的数据 """

    sql_0 = """
        select 
            t2.name,
            t2.unit,
            t1.target_count
        from 
            base_product_task t1
        left join 
            base_materials_pool t2
            on t1.id = t2.id
        where 
            t1.id = '{}';"""
    sql_1 = "select coalesce(name, ''), image from user_info where user_id = '{}';"

    cur.execute(sql_0.format(product_task_id))
    product_name, unit, target_count = cur.fetchone()
    # 操作者姓名和头像
    alioss = AliOss()
    if user_id:
        cur.execute(sql_1.format(user_id))
        operator, image = cur.fetchone() or ('', '')
        if isinstance(image, memoryview):
            image = image.tobytes().decode()
        image = alioss.joint_image(image)
    else:
        operator = '小D'
        image = alioss.joint_image('')

    message = {'id': product_task_id, 'product_name': product_name, 'product_target': '{}{}'.format(target_count, unit),
               'operator': operator, 'image': image, 'state': state}
    return message


def get_message_data_material(cur, product_task_id, user_id, state):
    """ 推送时与产品物料有关的数据 """
    sql_1 = """
        select 
            t2.name, 
            material_ids, 
            material_counts
        from 
            base_product_task t1 
        left join 
            base_materials_pool t2 
            on t1.id = t2.id
        where 
            id = '{}';"""
    sql_2 = "select coalesce(name, ''), image from user_info where user_id = '{}';"
    sql_3 = """
            select 
                t1.name, 
                unit, 
                t2.name 
            from 
                base_materials_pool t1 
            left join 
                base_material_category_pool t2 on 
                t1.category_id = t2.id
            where t1.id = '{}';"""
    # 推送-产品名称
    cur.execute(sql_1.format(product_task_id))
    product_name, material_ids, material_counts = cur.fetchone()[0]
    # 操作者姓名和头像
    alioss = AliOss()
    if user_id:
        cur.execute(sql_2.format(user_id))
        operator, image = cur.fetchone() or ('', '')
        if isinstance(image, memoryview):
            image = image.tobytes().decode()
        image = alioss.joint_image(image)
    else:
        operator = '小D'
        image = alioss.joint_image('')
    # 推送-物料清单
    material_names = list()
    material_units = list()
    material_categories = list()
    for j in material_ids:
        cur.execute(sql_3.format(j))
        tmp = cur.fetchone()
        material_names.append(tmp[0])
        material_units.append(tmp[1])
        material_categories.append(tmp[2])
    materials = ';'.join('{}:{} {}{}'.format(x, y, z, t) for x, y, z, t in zip(material_categories,
                                                                               material_names,
                                                                               material_counts,
                                                                               material_units))
    message = {'id': product_task_id, 'product_name': product_name, 'materials': materials,
               'operator': operator, 'image': image, 'state': state}
    return message
