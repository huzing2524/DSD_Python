# -*- coding: utf-8 -*-
import time
import json
import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from constants import ProductStateFour, DelState
from permissions import ProductPermission, StoreProductPermission, StorePermission
from products.products_utils import create_id
from store.store_utils import create_completed_storage, create_picking_list
from purchase.purchase_utils import create_purchase
from apps_utils import UtilsPostgresql, UtilsRabbitmq, generate_module_uuid

logger = logging.getLogger('django')

# 生产部-----------------------------------------------------------------------------------------------------------------


class ProductHomeHeader(APIView):
    """生产部主页头部 product/home/header"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = "select count(1) from base_product_task where factory = '{}' and state = '{}';"

        try:
            cur.execute(sql.format(factory_id, ProductStateFour.wait.value))
            tasks_count = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = {'tasks_count': tasks_count}

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductHomeStats(APIView):
    """生产部主页分析 product/home/stats"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        # todo 设置默认时间，不同类型：日周月季年
        start = request.query_params.get('start', 0)
        end = request.query_params.get('end', 11111111111)

        # 生产完工率
        sql_1 = "select count(1) from base_product_task where factory = '{}' and time between {} and {} and " \
                "state != '{}';"
        sql_2 = """
            select 
                coalesce(sum(case when t1.state = '0' then 1 else 0 end), 0) as out_store,  
                coalesce(sum(case when t1.state = '1' then 1 else 0 end), 0) as in_store
            from 
                base_store_completed_storage t1 
            left join 
                base_product_task t2 on 
                t1.product_task_id = t2.id 
            where 
                t2.factory = '{}' and 
                t2.time between {} and {};"""
        try:
            cur.execute(sql_1.format(factory_id, start, end, ProductStateFour.done.value))
            tmp = cur.fetchone() or (0,)
            not_done = tmp[0]
            cur.execute(sql_2.format(factory_id, start, end))
            tmp = cur.fetchone()
            out_store, in_store = tmp
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        total_tasks = not_done + out_store + in_store
        finish_rate = {'not_done': {'count': not_done,
                                    'percent': '{:.2f}%'.format(not_done / total_tasks * 100 if total_tasks else 0)},
                       'out_store': {'count': out_store,
                                     'percent': '{:.2f}%'.format(out_store / total_tasks * 100 if total_tasks else 0)},
                       'in_store': {'count': in_store,
                                    'percent': '{:.2f}%'.format(in_store / total_tasks * 100 if total_tasks else 0)}}

        sql_3 = """
            select 
                sum(target_count * coalesce(t2.price, 0)) 
            from 
                base_product_task t1 
            left join 
                base_products t2 
                on t1.product_id = t2.id 
                and t1.factory = t2.factory
            where 
                t1.factory = '{}' and 
                t1.time between {} and {};"""
        sql_4 = """
            select 
                sum(case when t2.create_time - t3.time = 0 then 1 else 0 end) as new, 
                sum(case when t2.create_time - t3.time = 0 then 0 else 1 end) as old 
            from 
                (select 
                    distinct order_id 
                from 
                    base_product_task 
                where 
                    factory = '{}' and 
                    time between {} and {}
                )t1 
            left join 
                base_orders t2 
                on t1.order_id = t2.id 
            left join 
                (select 
                    client_id, 
                    min(create_time) as time 
                from 
                    base_orders 
                group by 
                    client_id
                )t3 
                on t2.client_id = t3.client_id;"""
        sql_5 = """
            select 
                sum(case when t1.purchase_state = '1' then 1 else 0 end ) 
            from 
                base_product_task t1 
            where 
                t1.factory = '{}' and 
                t1.time between {} and {};"""
        sql_6 = """
            select 
                sum(case when state = '0' then 1 else 0 end) as not_prepared, 
                sum(case when state = '1' then 1 else 0 end) as not_picked 
            from 
                base_product_task 
            where    
                factory = '{}' and 
                time between {} and {};"""
        sql_7 = """
            select 
                t2.name, 
                sum(target_count * coalesce(price, 0)) as amount 
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            left join 
                base_products t3
                on t3.id = t1.product_id
                and t3.factory = t1.factory  
            where 
                t1.factory = '{}' and 
                t1.time between {} and {} 
            group by 
                t2.name
            order by 
                amount desc 
            limit 10;"""
        target = ['name', 'amount']
        try:
            # 生产金额分析
            cur.execute(sql_3.format(factory_id, start, end))
            total_amount = cur.fetchone()[0] or 0
            # 新老客户
            cur.execute(sql_4.format(factory_id, start, end))
            tmp = cur.fetchone()
            if any(tmp):
                new_client, old_client = tmp
            else:
                new_client, old_client = (0, 0)
            # 备料情况
            cur.execute(sql_5.format(factory_id, start, end))
            purchasing = cur.fetchone()[0] or 0
            cur.execute(sql_6.format(factory_id, start, end))
            tmp = cur.fetchone()
            if any(tmp):
                not_prepared, not_picked = tmp
            else:
                not_prepared, not_picked = (0, 0)
            # 生产需求排名
            cur.execute(sql_7.format(factory_id, start, end))
            product_demands = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        amount_stats = {'total_tasks': total_tasks, 'total_amount': total_amount,
                        'new_client': new_client, 'old_client': old_client}
        material_prepare = {'purchasing': purchasing, 'not_prepared': not_prepared, 'not_picked': not_picked}

        result['total_tasks'] = total_tasks
        result['finish_rate'] = finish_rate
        result['amount_stats'] = amount_stats
        result['material_prepare'] = material_prepare
        result['product_demands'] = product_demands

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskDoneStats(APIView):
    """生产完工率分析 product/task/done/stats"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_1 = """
            select 
                t1.id, 
                CAST 
                    (t1.target_count AS VARCHAR(15)) || coalesce(t2.unit, '') 
                as target, 
                case t1.state
                    when '{}' then t1.time
                    when '{}' then t1.prepare_time
                    when '{}' then t1.start_time
                    when '{}' then t1.complete_time 
                    else null
                end as time, 
                t2.name 
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            where 
                t1.factory = '{}' 
                and state != '{}' 
            order by 
                time desc;"""
        sql_2 = """
            select 
                t2.id, 
                CAST 
                    (t2.target_count AS VARCHAR(15)) || t3.unit 
                as target, 
                case 
                    when t1.state  = '0' then t1.time else t1.completed_time
                end as time, 
                t3.name
            from 
                base_store_completed_storage t1 
            left join 
                base_product_task t2 
                on t1.product_task_id = t2.id 
            left join 
                base_materials_pool t3 
                on t2.product_id = t3.id 
            where 
                t2.factory = '{}' 
                and t1.state = '{}' 
            order by 
                time desc;"""
        target = ['id',  'target', 'time', 'name']
        try:
            cur.execute(sql_1.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                     ProductStateFour.working.value, ProductStateFour.done.value,
                                     factory_id, ProductStateFour.done.value))
            not_done = [dict(zip(target, i)) for i in cur.fetchall()]
            cur.execute(sql_2.format(factory_id, '0'))
            out_store = [dict(zip(target, i)) for i in cur.fetchall()]
            cur.execute(sql_2.format(factory_id, '1'))
            in_store = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['not_done'] = not_done
        result['out_store'] = out_store
        result['in_store'] = in_store

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskAccountStats(APIView):
    """生产金额分析 product/task/account/stats"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_0 = """
            select 
                distinct order_id, 
                t3.time 
            from 
                base_product_task t1 
            left join 
                base_orders t2 
                on t1.order_id = t2.id 
            left join 
                (select 
                    client_id, 
                    min(create_time) as time 
                from 
                    base_orders 
                group by 
                    client_id
                )t3 
                on t2.client_id = t3.client_id 
            where 
                t1.factory = '{}' 
                and t2.create_time - t3.time {}
            order by 
                t3.time desc;"""
        sql_1 = """
            select 
                t1.id, 
                CAST 
                    (t1.target_count AS VARCHAR(15)) || t2.unit 
                as target, 
                case t1.state
                    when '{}' then t1.time
                    when '{}' then t1.prepare_time
                    when '{}' then t1.start_time
                    when '{}' then t1.complete_time 
                    else 0 
                end as time, 
                    t2.name 
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            where 
                t1.order_id = '{}'
            order by 
                time desc;"""
        target = ['id',  'target', 'time', 'name']
        try:
            # 新客户
            cur.execute(sql_0.format(factory_id, '= 0'))
            new_order = [i[0] for i in cur.fetchall()]
            new_client = list()
            for i in new_order:
                cur.execute(sql_1.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                         ProductStateFour.working.value, ProductStateFour.done.value, i))
                tmp = [dict(zip(target, j)) for j in cur.fetchall()]
                new_client += tmp
            # 旧客户
            cur.execute(sql_0.format(factory_id, '!= 0'))
            old_order = [i[0] for i in cur.fetchall()]
            old_client = list()
            for i in old_order:
                cur.execute(sql_1.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                         ProductStateFour.working.value, ProductStateFour.done.value, i))
                tmp = [dict(zip(target, j)) for j in cur.fetchall()]
                old_client += tmp
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['new_client'] = new_client
        result['old_client'] = old_client

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskPrepareStats(APIView):
    """生产备料分析 product/task/prepare/stats"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_1 = """
            select 
                t1.id, 
                CAST 
                    (t1.target_count AS VARCHAR(15)) || t2.unit 
                as target,
                case t1.state
                    when '{}' then t1.time
                    when '{}' then t1.prepare_time
                    when '{}' then t1.start_time
                    when '{}' then t1.complete_time 
                    else null
                end as time, 
                    t2.name 
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            where 
                t1.factory = '{}' 
                and t1.purchase_state = '1'
            order by 
                time desc;"""
        sql_2 = """
            select 
                t1.id, 
                CAST 
                    (t1.target_count AS VARCHAR(15)) || t2.unit 
                as target, 
                case t1.state
                    when '{}' then t1.time
                    when '{}' then t1.prepare_time
                    when '{}' then t1.start_time
                    when '{}' then t1.complete_time 
                    else null
                end as time, 
                t2.name 
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            where 
                t1.factory = '{}' 
                and t1.state = '{}' 
            order by 
                time desc;"""
        target = ['id', 'target', 'time', 'name']
        try:
            cur.execute(sql_1.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                     ProductStateFour.working.value, ProductStateFour.done.value,
                                     factory_id))
            purchasing = [dict(zip(target, i)) for i in cur.fetchall()]
            cur.execute(sql_2.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                     ProductStateFour.working.value, ProductStateFour.done.value,
                                     factory_id, '0'))
            not_prepared = [dict(zip(target, i)) for i in cur.fetchall()]
            cur.execute(sql_2.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                     ProductStateFour.working.value, ProductStateFour.done.value,
                                     factory_id, '1'))
            not_picked = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        result['purchasing'] = purchasing
        result['not_prepared'] = not_prepared
        result['not_picked'] = not_picked

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskDemandStats(APIView):
    """生产需求分析 product/task/demand/stats"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = """
            select 
                t2.name,
                t1.id, 
                t1.target_count,
                t1.time,
                t2.unit  
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 on 
                t1.product_id = t2.id
            where 
                factory = '{}' 
            order by
                t1.time desc;"""
        target = ['id', 'target', 'time', 'unit']
        try:
            cur.execute(sql.format(factory_id))
            tmp = cur.fetchall()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        pre_result = dict()
        for i in tmp:
            if i[0] in pre_result:
                pre_result[i[0]].append(dict(zip(target, i[1:])))
            else:
                pre_result[i[0]] = [dict(zip(target, i[1:]))]

        result = [{'name': i, 'list': pre_result[i]} for i in pre_result]
        sorted(result, key=lambda x: sum(y['target'] for y in x['list']))

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskList(APIView):
    """生产任务单列表 product/task/list"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_1 = """
                select 
                    t1.id, 
                    t2.name, 
                    target_count, 
                    case state
                        when '{}' then t1.time 
                        when '{}' then prepare_time 
                        when '{}' then start_time 
                        when '{}' then complete_time 
                        else null 
                    end as time, 
                    state, 
                    material_ids, 
                    material_counts
                from 
                    base_product_task t1 
                left join 
                    base_materials_pool t2 on 
                    t1.product_id = t2.id
                where 
                    factory = '{}' 
                order by 
                    time desc;"""
        sql_2 = """
            select
                coalesce(actual, 0) + coalesce(on_road, 0) - coalesce(prepared, 0) - coalesce(safety, 0) as available_count
            from 
                base_materials_storage
            where
                factory = '{}'
                and material_id = '{}';"""
        sql_3 = "select count(1) from base_store_picking_list where style = '0' and product_task_id = '{}';"
        sql_4 = "select t1.order_id, t2.seq_id from base_product_task t1 left join factorys t2 on " \
                "t1.factory = t2.id where t1.id = '{}';"
        sql_5 = """
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
        sql_6 = """
            update
                base_materials_storage
            set 
                prepared = prepared + {}
            where
                material_id = '{}' 
                and factory = '{}';"""
        sql_7 = "select loss_coefficient from base_materials where id = '{}' and factory = '{}';"
        sql_8 = "update base_product_task set purchase_state = '1' where id = '{}';"
        target_1 = ["id", "product", "target", "time", "state", 'material_ids', 'material_counts']

        try:
            cur.execute(sql_1.format(ProductStateFour.wait.value, ProductStateFour.ready.value,
                                     ProductStateFour.working.value, ProductStateFour.done.value, factory_id))
            tasks_list = [dict(zip(target_1, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = {'wait': list(), 'ready': list(), 'working': list(), 'done': list()}

        # 生产单状态 1:待备料，2:待拆单，3:物料不足，4:待领料, 5:生产中, 6:未入库，7:已入库
        # 上面的状态已不适用
        for i in tasks_list:
            if i['state'] == ProductStateFour.wait.value:
                # 判断该生产单是否已经有领料单，如果没有则创建
                cur.execute(sql_3.format(i['id']))
                check_count = cur.fetchone()[0]
                cur.execute(sql_4.format(i['id']))
                tmp = cur.fetchone()
                order_id, seq_id = tmp
                if check_count != 0:
                    i['state'] = '1'
                else:
                    # 计算当前库存可生产产品个数
                    counts = []
                    for x, y in zip(i['material_ids'], i['material_counts']):
                        cur.execute(sql_2.format(factory_id, x))
                        available_count = cur.fetchone()[0]
                        available_count = 0 if available_count < 0 else available_count
                        # 暂时设为整数吧
                        counts.append(available_count // y)
                    count = min(counts) if counts else 0
                    if count >= i['target']:
                        i['state'] = '1'
                        # 预分配库存
                        Time = int(time.time())
                        for x, y in zip(i['material_ids'], i['material_counts']):
                            cur.execute(sql_5.format(x, i['target'] * y, factory_id, i['target'], i['id'], Time))
                            cur.execute(sql_6.format(i['target'] * y, x, factory_id))
                        # 创建领料单
                        create_picking_list(cur, order_id, i['id'], '', factory_id, 0, seq_id)
                    elif count > 0:
                        i['state'] = '2'
                    else:
                        i['state'] = '3'
                        # 判断是否已经采购了
                        cur.execute("select purchase_state from base_product_task where id = '{}';".format(i['id']))
                        purchase_state = cur.fetchone()[0]
                        if purchase_state == '0':
                            # 创建采购单
                            cur.execute(sql_4.format(i['id']))
                            tmp = cur.fetchone()
                            order_id, seq_id = tmp
                            purchase_counts = list()
                            for x, y in zip(i['material_ids'], i['material_counts']):
                                cur.execute(sql_7.format(x, tmp[0]))
                                loss = cur.fetchone()
                                loss_coefficient = loss[0] if loss else 0
                                purchase_count = i['target'] * y / (1 - loss_coefficient)
                                purchase_counts.append({'id': x, 'count': purchase_count})
                            create_purchase(cur, factory_id, seq_id, order_id, purchase_counts, i['id'])
                            # 更新采购状态
                            cur.execute(sql_8.format(i['id']))
                conn.commit()
                result['wait'].append(i)
            elif i['state'] == ProductStateFour.ready.value:
                i['state'] = '0'
                result['ready'].append(i)
            elif i['state'] == ProductStateFour.working.value:
                i['state'] = '0'
                result['working'].append(i)
            elif i['state'] == ProductStateFour.done.value:
                cur.execute("select state from base_store_completed_storage where product_task_id = '%s';" % i['id'])
                tmp = cur.fetchone() or '0'
                if tmp[0] == '0':
                    i['state'] = '0'
                elif tmp[0] == '1':
                    i['state'] = '1'
                result['done'].append(i)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskDetailId(APIView):
    """生产任务单详情 product/task/detail/{id}"""
    permission_classes = [ProductPermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        # 判断
        sql_0 = "select count(1) from base_product_task where id = '{}';"
        try:
            cur.execute(sql_0.format(Id))
            if cur.fetchone()[0] == 0:
                return Response({"res": 1, "errmsg": '生产任务不存在！'}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 产品部分信息
        sql_1 = """
            select 
                t2.name, 
                t1.product_id, 
                target_count, 
                state, 
                t2.unit,
                t1.material_ids, 
                t1.material_counts,
                t3.name
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            left join 
                base_material_category_pool t3 
                on t2.category_id = t3.id
            where 
                t1.id = '{}';"""
        sql_2 = "select count(1) from base_store_picking_list where style = '0' and product_task_id = '{}';"
        sql_3 = """
            select
                coalesce(actual, 0) + coalesce(on_road, 0) - coalesce(prepared, 0) - coalesce(safety, 0) as available_count
            from 
                base_materials_storage
            where
                factory = '{}'
                and material_id = '{}';"""

        target_1 = ['product', 'product_id', 'target', 'state', 'unit', 'material_ids', 'material_counts',
                    'category_name']

        try:
            cur.execute(sql_1.format(Id))
            tmp = cur.fetchone() or []
            product_info = dict(zip(target_1, tmp))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # todo 如果前端可以调用之前的页面，就不用重复计算了
        # 状态 1:待备料，2:待拆单，3:物料不足，4:待领料, 5:生产中, 6:未入库，7:已入库
        if product_info['state'] == ProductStateFour.wait.value:
            # 判断该生产单是否已经有领料单，如果没有则，则创建
            cur.execute(sql_2.format(Id))
            check_count = cur.fetchone()[0]
            if check_count != 0:
                product_info['state'] = '1'
            else:
                # 计算当前库存可生产产品个数
                counts = []
                for x, y in zip(product_info['material_ids'], product_info['material_counts']):
                    cur.execute(sql_3.format(factory_id, x))
                    available_count = cur.fetchone()[0]
                    available_count = 0 if available_count < 0 else available_count
                    # 暂时设为整数吧
                    counts.append(available_count // y)
                count = min(counts) if counts else 0
                if count >= product_info['target']:
                    product_info['state'] = '1'
                elif count > 0:
                    product_info['state'] = '2'
                else:
                    product_info['state'] = '3'
        elif product_info['state'] == ProductStateFour.ready.value:
            product_info['state'] = '4'
        elif product_info['state'] == ProductStateFour.working.value:
            product_info['state'] = '5'
        elif product_info['state'] == ProductStateFour.done.value:
            cur.execute("select state from base_store_completed_storage where product_task_id = '%s';" % Id)
            complete_state = cur.fetchone()[0]
            if complete_state == '0':
                product_info['state'] = '6'
            elif complete_state == '1':
                product_info['state'] = '7'

        # 物料清单
        target_2 = ['name', 'count', 'unit']
        try:
            material_ids = product_info['material_ids']
            material_counts = product_info['material_counts']
            material_units = []
            material_names = []
            for i in material_ids:
                cur.execute("select name, unit from base_materials_pool where id = '{}';".format(i))
                tmp = cur.fetchone()
                material_names.append(tmp[0])
                material_units.append(tmp[1])
            material_list = [dict(zip(target_2, (x, '{}{}'.format(y * product_info['target'], z))))
                             for x, y, z in zip(material_names, material_counts, material_units)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = dict()
        del product_info['material_ids']
        del product_info['material_counts']
        result['product_info'] = product_info
        result['material_list'] = material_list

        if product_info['state'] not in ['5', '6', '7']:
            postgresql.disconnect_postgresql(conn)
            return Response(result, status=status.HTTP_200_OK)

        # finish process
        sql_4 = """
            select 
                t1.process_step as id, 
                t1.start_time as start, 
                t1.end_time as end,  
                t1.take_time, 
                t1.good, 
                t1.ng, 
                t1.remark, 
                t1.time, 
                t2.name as creator 
            from 
                base_product_task_processes t1 
            left join 
                user_info t2 
                on t1.creator = t2.phone 
            where 
                t1.product_task_id = '{}'
                and t1.factory = '{}';"""
        sql_5 = """
            select 
                t2.process_step, 
                t3.name 
            from 
                base_product_task t1 
            left join 
                base_product_processes t2 
                on t1.product_id = t2.product_id 
            left join 
                base_processes t3 
                on t2.process_id = t3.id 
            where t1.id = '{}' 
                and t2.factory = '{}';"""
        target_0 = ['id', 'start', 'end', 'take_time', 'good', 'ng', 'remark', 'time', 'creator']
        try:
            cur.execute(sql_4.format(Id, factory_id))
            tmp = cur.fetchall()
            q_list = [dict(zip(target_0, i)) for i in tmp]
            cur.execute(sql_5.format(Id, factory_id))
            tmp = cur.fetchall()
            q_all = dict()
            q_all['process_ids'] = [i[0] for i in tmp]
            q_all['process_names'] = [i[1] for i in tmp]
            q_all['plan'] = product_info['target']
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not q_all:
            process, finish = [], {}
        else:
            process = []
            for ids, names in zip(q_all['process_ids'], q_all['process_names']):
                tmp = dict()
                for i in q_list:
                    if i.get('id') == ids:
                        tmp.update(i)
                        tmp['name'] = names
                        process.append(tmp)
                        break
                else:
                    process.append({'id': ids, 'name': names})
            if len(q_list) == len(q_all['process_ids']) and len(q_all['process_ids']) != 0:
                finish = {'ng': [], 'start': [], 'end': [], 'take_time': []}
                for i in process:
                    finish['ng'].append(i.get('ng', 0))
                    finish['start'].append(i.get('start'))
                    finish['end'].append(i.get('end', 0))
                    finish['take_time'].append(i.get('take_time', 0))
                finish['ng'] = sum(finish['ng'])
                finish['start'] = min(finish['start']) if finish['start'] else 0
                finish['end'] = max(finish['end']) if finish['end'] else 0
                finish['take_time'] = sum(finish['take_time'])
                finish['good'] = process[-1].get('good', 0)
            else:
                finish = dict()

        if product_info['state'] == '5':
            total_good = finish['good'] if finish else 0
            if total_good >= product_info['target']:
                state = 0
            elif total_good > 0:
                state = 1
            else:
                state = 2
        sql_6 = "select id from base_material_return where product_task_id = '{}';"
        sql_7 = "select id from base_material_supplement where product_task_id = '{}';"

        try:
            related = dict()
            cur.execute(sql_6.format(Id))
            related['return'] = [i[0] for i in cur.fetchall()]
            cur.execute(sql_7.format(Id))
            related['supplement'] = [i[0] for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result['process'] = process
        result['finish'] = finish
        result['related'] = related
        if product_info['state'] == '5':
            result['total_good'] = total_good
            result['state'] = state

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductTaskProcessStatsId(APIView):
    """产品工序统计详情 product/task/process/stats/{id}"""
    permission_classes = [ProductPermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_0 = """
            select 
                t1.process_step as id, 
                t1.start_time as start, 
                t1.end_time as end, 
                t1.take_time, 
                t1.good, 
                t1.ng, 
                t1.remark, 
                t1.time, 
                t2.name as creator 
            from 
                base_product_task_processes t1 
            left join 
                user_info t2 
                on t1.creator = t2.phone 
            where 
                t1.product_task_id = '{}' 
                and t1.factory = '{}';"""
        sql_1 = """
            select 
                t2.process_step, 
                t3.name, 
                t1.target_count 
            from 
                base_product_task t1 
            left join 
                base_product_processes t2 
                on t1.product_id = t2.product_id 
            left join 
                base_processes t3 
                on t2.process_id = t3.id 
            where 
                t1.id = '{}' 
                and t2.factory = '{}';"""
        target_0 = ['id', 'start', 'end', 'take_time', 'good', 'ng', 'remark', 'time', 'creator']
        target_1 = ['process_ids', 'process_names', 'plan']
        try:
            cur.execute(sql_0.format(Id, factory_id))
            tmp = cur.fetchall()
            q_list = [dict(zip(target_0, i)) for i in tmp]
            cur.execute(sql_1.format(Id, factory_id))
            tmp = cur.fetchall()
            if tmp:
                q_all = dict()
                q_all['process_ids'] = [i[0] for i in tmp]
                q_all['process_names'] = [i[1] for i in tmp]
                q_all['plan'] = tmp[0][2]
            else:
                q_all = dict()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not q_all:
            return Response({"res": 1, "errmsg": "no process"}, status=status.HTTP_200_OK)
        if len(q_all['process_ids']) != len(q_all['process_names']):
            return Response({"res": 1, "errmsg": "no finish"}, status=status.HTTP_200_OK)

        process = []
        for ids, names in zip(q_all['process_ids'], q_all['process_names']):
            tmp = dict()
            for i in q_list:
                if i.get('id') == ids:
                    tmp.update(i)
                    tmp['name'] = names
                    process.append(tmp)
            if not process:
                process.append({'id': ids, 'name': names})

        finish = {'ng': [], 'start': [], 'end': [], 'take_time': []}
        for i in process:
            finish['ng'].append(i.get('ng') or 0)
            finish['start'].append(i.get('start') or 0)
            finish['end'].append(i.get('end') or 0)
            finish['take_time'].append(i.get('take_time') or 0)

        finish['ng'] = sum(finish['ng'])
        finish['start'] = min(finish['start']) if finish['start'] else 0
        finish['end'] = max(finish['end']) if finish['end'] else 0
        finish['take_time'] = sum(finish['take_time'])

        finish['plan'] = q_all['plan']
        finish['good'] = process[-1]['good']

        # rty_list
        process = sorted(process, key=lambda i: -i['good'] / (i['good'] + i['ng']))
        rty_list = [{'id': i['id'],
                     'name': i['name'],
                     'digit': '{:.2f}%'.format(i['good'] / (i['good'] + i['ng']) * 100 if (i['good'] + i['ng']) else 0)} for i in process]
        # ngs_list
        process = sorted(process, key=lambda i: -i['ng'])
        ngs_list = [{'id': i['id'], 'name': i['name'], 'digit': str(i['ng'])} for i in process]
        # time_list
        process = sorted(process, key=lambda i: -i['take_time'])
        time_list = [{'id': i['id'], 'name': i['name'], 'digit': str(i['take_time'])} for i in process]

        finish['rty'] = rty_list
        finish['ngs'] = ngs_list
        finish['time'] = time_list

        postgresql.disconnect_postgresql(conn)
        return Response(finish, status=status.HTTP_200_OK)


class ProductTaskProcessTPId(APIView):
    """get 获取生产工序进度详情 product/task/process/{task_id}/{process_step}"""
    """post 提交生产工序进度详情 product/task/process/{task_id}/{process_step}"""
    """put 修改生产工序进度详情 product/task/process/{task_id}/{process_step}"""
    """delete 删除生产工序进度详情 product/task/process/{task_id}/{process_step}"""
    permission_classes = [ProductPermission]

    def get(self, request, task_id, process_step):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = """
            select 
                start_time as start, 
                end_time as end, 
                take_time, 
                good, 
                ng, 
                remark, 
                time 
            from 
                base_product_task_processes 
            where 
                product_task_id = '%s' 
                and process_step = '%s' 
                and factory = '%s';""" % (task_id, process_step, factory_id)
        target = ['start', 'end', 'take_time', 'good', 'ng', 'remark', 'time']

        try:
            cur.execute(sql)
            tmp = cur.fetchone()
            result = dict(zip(target, tmp)) if tmp else dict()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)

    def post(self, request, task_id, process_step):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        user_id = request.redis_cache["user_id"]
        start = request.data.get('start')
        end = request.data.get('end')
        good = request.data.get('good', 0)
        ng = request.data.get('ng', 0)
        remark = request.data.get('remark', '')
        takes = request.data.get('take_time')
        Time = int(time.time())

        sql = """
            insert into 
                base_product_task_processes 
                (product_task_id, process_step, start_time, end_time, take_time, good, ng, remark, time, creator, factory) 
            values 
                ('{0}', '{1}', {2}, {3}, {4}, {5}, {6}, '{7}', {8}, '{9}', '{10}');"""

        try:
            cur.execute(sql.format(task_id, process_step, start, end, takes, good, ng, remark, Time, user_id, factory_id))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def put(self, request, task_id, process_step):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        start = request.data.get('start')
        end = request.data.get('end')
        good = request.data.get('good')
        ng = request.data.get('ng')
        remark = request.data.get('remark', '')
        takes = request.data.get('take_time')
        Time = int(time.time())

        sql = """
            update 
                base_product_task_processes 
            set 
                start_time = {0}, 
                end_time = {1}, 
                take_time = {2}, 
                good = {3}, 
                ng = {4}, 
                remark = '{5}', 
                time = {6} 
            where 
                product_task_id = '{7}' 
                and process_step = '{8}' 
                and factory = '{9}';""".format(start, end, takes, good, ng, remark, Time, task_id, process_step, factory_id)
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request, task_id, process_step):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_0 = "select state from base_product_task where id = '{}';"
        sql_1 = "delete from base_product_task_processes where product_task_id = '%s' and process_step = '%s' " \
                "and factory = '%s';" % (task_id, process_step, factory_id)

        try:
            cur.execute(sql_0.format(task_id))
            state = cur.fetchone()[0]
            if state != '3':
                cur.execute(sql_1)
                conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class ProductTaskSplitId(APIView):
    """get: 获取拆分后的生产任务单 product/task/split/{id}"""
    """post: 执行拆分生产任务单 product/task/split/{id}"""
    permission_classes = [ProductPermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_1 = """
            select 
                t1.id, 
                t2.name, 
                t2.unit, 
                target_count, 
                t1.material_ids, 
                t1.material_counts
            from 
                base_product_task t1 
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id 
            where 
                t1.id = '{}';"""
        sql_2 = """
            select
                coalesce(actual, 0) + coalesce(on_road, 0) - coalesce(prepared, 0) - coalesce(safety, 0) as available_count
            from 
                base_materials_storage
            where
                factory = '{}'
                and material_id = '{}';"""
        target = ['id', 'name', 'unit', 'target', 'material_ids', 'material_counts']
        try:
            cur.execute(sql_1.format(Id))
            tmp = cur.fetchone() or []
            result = dict(zip(target, tmp))
            # 计算当前库存可生产产品个数
            counts = []
            for x, y in zip(result['material_ids'], result['material_counts']):
                cur.execute(sql_2.format(factory_id, x))
                available_count = cur.fetchone()[0]
                available_count = 0 if available_count < 0 else available_count
                # 暂时设为整数吧
                counts.append(available_count // y)
            count = min(counts) if counts else 0
            del result['material_ids']
            del result['material_counts']
            result['split_count'] = count
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)

    def post(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        seq_id = request.redis_cache["seq_id"]
        factory_id = request.redis_cache["factory_id"]
        target = request.data.get('target')
        split_count = request.data.get('split_count')
        Time = int(time.time())

        # 创建子单id
        kids_id = [generate_module_uuid('02', factory_id, seq_id) for i in range(2)]

        sql_1 = "insert into base_product_parent_task select * from base_product_task where id = '{}';"
        sql_2 = "update base_product_task set id = '{}', target_count = {}, time = {} where id = '{}';"
        sql_3 = "insert into base_product_task select * from base_product_parent_task where id = '{}';"
        sql_4 = "insert into base_product_relation values('{}', '{}', {});"
        sql_5 = "select order_id, material_ids, material_counts from base_product_task where id = '{}';"
        sql_6 = """
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
        sql_7 = """
            update
                base_materials_storage
            set 
                prepared = prepared + {}
            where
                material_id = '{}' 
                and factory = '{}';"""
        sql_8 = "select loss_coefficient from base_materials where id = '{}' and factory = '{}';"
        sql_9 = "update base_product_task set purchase_state = '1' where id = '{}';"
        try:
            # 将父单复制到父单表
            cur.execute(sql_1.format(Id))
            # 更新原父单为子单一
            cur.execute(sql_2.format(kids_id[0], split_count, Time, Id))
            # 将父单复制回去
            cur.execute(sql_3.format(Id))
            # 更新父单为子单二
            cur.execute(sql_2.format(kids_id[1], target - split_count, Time, Id))
            # 添加关系记录
            for i in kids_id:
                cur.execute(sql_4.format(i, Id, Time))
            # 预分配库存
            cur.execute(sql_5.format(kids_id[0]))
            tmp = cur.fetchone()
            order_id, material_ids, material_counts = tmp
            for x, y in zip(material_ids, material_counts):
                cur.execute(sql_6.format(x, split_count * y, factory_id, split_count, kids_id[0], Time))
                cur.execute(sql_7.format(split_count * y, x, factory_id))
            # 创建领料单
            create_picking_list(cur, order_id, kids_id[0], '', factory_id, 0, seq_id)
            # 判断是否已经创建采购单
            cur.execute("select purchase_state from base_product_task where id = '{}';".format(kids_id[1]))
            purchase_state = cur.fetchone()[0]
            if purchase_state == '0':
                # 创建采购单
                purchase_counts = list()
                for x, y in zip(material_ids, material_counts):
                    cur.execute(sql_8.format(x, factory_id))
                    loss = cur.fetchone()
                    loss_coefficient = loss[0] if loss else 0
                    purchase_count = (target - split_count) * y / (1 - loss_coefficient)
                    purchase_counts.append({'id': x, 'count': purchase_count})
                create_purchase(cur, factory_id, seq_id, order_id, purchase_counts, kids_id[1])
                # 更新采购状态
                cur.execute(sql_9.format(kids_id[1]))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductTaskDoneId(APIView):
    """完工入库 product/task/done/{id}"""
    permission_classes = [ProductPermission]

    def post(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        user_id = request.redis_cache["user_id"]
        seq_id = request.redis_cache["seq_id"]
        factory_id = request.redis_cache["factory_id"]
        complete_count = request.data.get('complete_count')
        Time = int(time.time())

        sql_0 = "select order_id from base_product_task where id = '{}';"
        sql_1 = "update base_product_task set state = '{}', complete_time = {}, complete_count = {} where id = '{}';"

        try:
            cur.execute(sql_0.format(Id))
            order_id = cur.fetchone()[0]
            cur.execute(sql_1.format(ProductStateFour.done.value, Time, complete_count, Id))
            # 完工入库单
            create_completed_storage(cur, order_id, Id, factory_id, seq_id)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 推送：已完工-未入库
        # message = get_message_data_product(cur, Id, user_id, '1')
        message = {'resource': 'PyProductTaskDoneId',
                   'type': 'POST',
                   'params': {"fac": factory_id, "id": Id, "state": "1", "user_id": user_id}}
        rabbitmq = UtilsRabbitmq()
        rabbitmq.send_message(json.dumps(message))
        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductProductMaterialList(APIView):
    """产品列表 product/product/list"""
    """物料列表 product/material/list"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        # 0：不根据类目分组；1：根据类目分组
        category_state = request.query_params.get('category', '0')

        sql = """
            select 
                t1.id, 
                t2.name, 
                t3.id, 
                t3.name
            from 
                {} t1 
            left join 
                base_materials_pool t2 on 
                t1.id = t2.id 
            left join 
                base_material_category_pool t3 on 
                t2.category_id = t3.id
            where 
                factory = '{}'
            order by 
                t1.time desc;"""
        target = ['id', 'name', 'category_id', 'category_name']
        # 判断类型
        if Type == 'product':
            table = 'base_products'
        else:
            table = 'base_materials'

        try:
            cur.execute(sql.format(table, factory_id))
            if category_state == '0':
                result = [dict(zip(target[:2], i)) for i in cur.fetchall()]
            else:
                tmp = cur.fetchall()
                pre_result = dict()
                for i in tmp:
                    if i[2] in pre_result:
                        pre_result[i[2]].append(i)
                    else:
                        pre_result[i[2]] = [i]
                result = list()
                for i in pre_result:
                    result.append({'name': pre_result[i][0][3], 'list': [dict(zip(target[:3], j)) for j in pre_result[i]]})
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductProductMaterialDetailId(APIView):
    """产品详情 product/product/detail/{id}"""
    """物料详情 product/material/detail/{id}"""
    """修改产品售价 product/product/detail/{id}"""
    """修改物料成本价和最低采购量 product/material/detail/{id}"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request, Id, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        if Type == 'product':
            sql_1 = """
                    select 
                        t1.id, 
                        t1.name, 
                        t1.unit, 
                        t2.price, 
                        t2.time, 
                        t1.category_id,
                        t2.loss_coefficient, 
                        t3.safety
                    from 
                        base_materials_pool t1 
                    left join 
                        base_products t2 
                        on t1.id = t2.id 
                        and t2.factory = '{0}'
                    left join 
                        base_products_storage t3 
                        on t1.id = t3.product_id 
                        and t3.factory = '{0}'
                    where 
                        t1.id = '{1}';"""
            target = ['id', 'name', 'unit', 'price', 'time', 'category_id', 'loss_coefficient', 'safety']
        else:
            sql_1 = """
                    select 
                        t1.id, 
                        t1.name, 
                        t1.unit, 
                        t2.price, 
                        t2.time, 
                        t1.category_id, 
                        t2.lowest_count, 
                        t2.loss_coefficient, 
                        t3.safety 
                    from 
                        base_materials_pool t1 
                    left join 
                        base_materials t2 
                        on t1.id = t2.id 
                        and t2.factory = '{0}' 
                    left join 
                        base_materials_storage t3 
                        on t1.id = t3.material_id 
                        and t3.factory = '{0}'
                    where 
                        t1.id = '{1}';"""
            target = ['id', 'name', 'unit', 'price', 'time', 'category_id', 'lowest_count', 'loss_coefficient', 'safety']
        sql_2 = """
                WITH RECURSIVE cte AS (
                    SELECT A.id,
                        CAST (A.name AS VARCHAR(50)) AS name_full_path
                    FROM
                        base_material_category_pool A
                    WHERE
                        A.parent_id is null
                    UNION ALL
                        SELECT
                            K.id,
                            CAST (
                                C.name_full_path || '>' || K.name AS VARCHAR (50)
                            ) AS name_full_path
                        FROM
                            base_material_category_pool K
                        INNER JOIN cte C ON C.id = K.parent_id
                ) SELECT
                    *
                FROM
                    cte where id = '{}';"""
        try:
            cur.execute(sql_1.format(factory_id, Id))
            tmp = cur.fetchone()
            if tmp:
                result = dict(zip(target, tmp))
                cur.execute(sql_2.format(result['category_id']))
                result['category'] = cur.fetchone()[1]
            else:
                result = dict()

        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)

    def put(self, request, Id, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        price = request.data.get('price')
        loss_coefficient = request.data.get('loss_coefficient', 0)
        safety = request.data.get('safety')

        if Type == 'product':
            sql_1 = "update base_products set price = {}, loss_coefficient = {} " \
                    "where id = '{}' and factory = '{}';".format(price, loss_coefficient, Id, factory_id)
            sql_2 = "update base_products_storage set safety = {} " \
                    "where product_id = '{}' and factory = '{}';".format(safety, Id, factory_id)
        else:
            lowest_count = request.data.get('lowest_count')
            sql_1 = "update base_materials set price = {}, lowest_count = {}, loss_coefficient = {} " \
                    "where id = '{}' and factory = '{}';".format(price, lowest_count, loss_coefficient, Id, factory_id)
            sql_2 = "update base_materials_storage set safety = {} " \
                    "where material_id = '{}' and factory = '{}';".format(safety, Id, factory_id)
        try:
            cur.execute(sql_1)
            cur.execute(sql_2)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductProductMaterialSearch(APIView):
    """搜索产品/物料 product/materials/search"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        name = request.query_params.get('name')

        sql_1 = "select id, name, category_id from base_materials_pool where name like '%{}%';".format(name)
        sql_2 = """
                WITH RECURSIVE cte AS (
                    SELECT A.id,
                        CAST (A.name AS VARCHAR(50)) AS name_full_path
                    FROM
                        base_material_category_pool A
                    WHERE
                        A.parent_id is null
                    UNION ALL
                        SELECT
                            K.id,
                            CAST (
                                C.name_full_path || '>' || K.name AS VARCHAR (50)
                            ) AS name_full_path
                        FROM
                            base_material_category_pool K
                        INNER JOIN cte C ON C.id = K.parent_id
                ) SELECT
                    *
                FROM
                    cte where id = '{}';"""
        target = ['id', 'name', 'category_id']

        try:
            if name:
                cur.execute(sql_1)
                result = [dict(zip(target, i)) for i in cur.fetchall()]
                for i in result:
                    cur.execute(sql_2.format(i['category_id']))
                    i['category_name'] = cur.fetchone()[1]
            else:
                result = list()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductProductMaterialNew(APIView):
    """添加产品 product/product/new"""
    """添加物料 product/material/new"""
    permission_classes = [ProductPermission]

    # 已测
    def post(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        category_id = request.data.get('category_id', '07aa367b-d978-4948-ae80-575979f31689')
        Id = request.data.get('id')
        name = request.data.get('name')
        unit = request.data.get('unit')
        Time = int(time.time())

        # 判断name是否重复
        sql_0 = "select count(1) from base_materials_pool where category_id = '{}' and name = '{}';"
        # 判断id是否重复
        sql_1 = "select count(1) from base_materials_pool where id = '{}';"
        # 在总池中添加
        sql_2 = "insert into base_materials_pool(id, name, unit, category_id, time) values('{}', '{}', '{}', '{}', {});"

        if Type == 'product':
            type_name = '产品'
            sql_3 = "insert into base_products(id, factory, time) values('{}', '{}', {});"
            sql_4 = "select count(1) from base_products where id = '{}' and factory = '{}';"
            sql_5 = "insert into base_products_storage(product_id, factory, time, actual, pre_product, prepared) " \
                    "values('{}', '{}', {}, 0, 0, 0);"
        else:
            type_name = '物料'
            sql_3 = "insert into base_materials(id, factory, time) values('{}', '{}', {});"
            sql_4 = "select count(1) from base_materials where id = '{}' and factory = '{}';"
            sql_5 = "insert into base_materials_storage(material_id, factory, time, actual, on_road, prepared) " \
                    "values('{}', '{}', {}, 0, 0, 0);"
        try:
            if not Id:
                cur.execute(sql_0.format(category_id, name))
                tmp = cur.fetchone() or (0,)
                if tmp[0] == 1:
                    return Response({"res": 1, "errmsg": '{}名称已存在！'.format(type_name)}, status=status.HTTP_200_OK)
                Id = create_id(sql_1)
                cur.execute(sql_2.format(Id, name, unit, category_id, Time))
            else:
                cur.execute(sql_4.format(Id, factory_id))
                tmp = cur.fetchone() or (0,)
                if tmp[0] == 1:
                    return Response({"res": 1, "errmsg": '添加的{}已存在！'.format(type_name)}, status=status.HTTP_200_OK)
            cur.execute(sql_3.format(Id, factory_id, Time))
            cur.execute(sql_5.format(Id, factory_id, Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0, "id": Id}, status=status.HTTP_200_OK)


class ProductProcesslist(APIView):
    """工序列表 product/process/list"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        sql = """
            select 
                id, 
                name 
            from 
                base_processes 
            where 
                factory = '{}' 
                and del = '{}' 
            order by 
                time desc;"""
        target = ['id', 'name']

        try:
            cur.execute(sql.format(factory_id, DelState.del_no.value))
            result = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductProcessModify(APIView):
    """修改工序 product/process/{id}"""
    """删除工序 product/process/{id}"""
    permission_classes = [ProductPermission]

    # 已测
    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        name = request.data.get('name')

        sql_0 = "select count(1) from base_processes where factory = '{}' and name = '{}' and del = '{}';"
        sql_1 = "update base_processes set name = '{}' where id = '{}';".format(name, Id)

        try:
            cur.execute(sql_0.format(factory_id, name, DelState.del_no.value))
            if cur.fetchone()[0] == 0:
                cur.execute(sql_1.format(name, Id))
                conn.commit()
            else:
                return Response({"res": 1, "errmsg": '该工序名称已存在！'}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = "update base_processes set del = '{}' where id = '{}' and factory = '{}';".format(DelState.del_yes.value,
                                                                                                Id, factory_id)
        # todo 当工序不被被使用且状态为1时从数据库中删除
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductProcessNew(APIView):
    """新增工序 product/process/new"""
    permission_classes = [ProductPermission]

    # 已测
    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        name = request.data.get('name')
        Time = int(time.time())

        sql_0 = "select count(1) from base_processes where id = '{}';"
        sql_1 = "select count(1) from base_processes where factory = '{}' and name = '{}';"
        sql_2 = "insert into base_processes(id, name, factory, time) values('{}', '{}', '{}', {});"

        try:
            # 生成唯一id
            Id = create_id(sql_0)
            # 验证名称是否重复
            cur.execute(sql_1.format(factory_id, name))
            if cur.fetchone()[0] == 0:
                cur.execute(sql_2.format(Id, name, factory_id, Time))
                conn.commit()
            else:
                return Response({"res": 1, "errmsg": '该工序名称已存在！'}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductPbList(APIView):
    """工序/BOM列表 product/pb/list"""
    permission_classes = [ProductPermission]

    # 已测
    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        # row = request.query_params.get('row', ROW)
        # page = request.query_params.get('page', 1)
        #
        # limit = int(row)
        # offset = int(row) * (int(page) - 1)

        # 有工序的产品总数
        # sql_0 = "select count(1) from base_product_processes where factory = '{}';"
        # 有工序的产品列表
        #  limit {}
        sql_1 = """
            select 
                t1.*, 
                t2.name
            from (
                select
                    product_id, 
                    count(process_id) as count 
                from 
                    base_product_processes
                where 
                    factory = '{}' 
                group by 
                    product_id) t1
            left join 
                base_materials_pool t2 
                on t1.product_id = t2.id;"""
        # 没有工序的产品列表
        sql_2 = """
            select 
                t1.id, 
                0 as count, 
                t2.name
            from 
                base_products t1 
            left join 
                base_materials_pool t2 
                on t1.id = t2.id 
            left join 
                base_product_processes t3
                on t1.id = t3.product_id 
                and t1.factory = t3.factory
            where 
                t1.factory = '{}' 
                and t3.product_id is null;"""
        target = ['id', 'count', 'name']

        try:
            # cur.execute(sql_0.format(factory_id))
            # total = cur.fetchone()[0]
            # if total - offset >= limit:
            #     cur.execute(sql_1.format(factory_id, DelState.del_no.value, offset, limit))
            #     list_1 = [dict(zip(target, i)) for i in cur.fetchall()]
            #     list_2 = list()
            # elif total - offset > 0:
            #     cur.execute(sql_1.format(factory_id, DelState.del_no.value, offset, limit))
            #     list_1 = [dict(zip(target, i)) for i in cur.fetchall()]
            #     cur.execute(sql_2.format(factory_id, offset, limit - (total - offset)))
            #     list_2 = [dict(zip(target, i)).update({'count': 0}) for i in cur.fetchall()]
            # else:
            #     cur.execute(sql_2.format(factory_id, offset, limit))
            #     list_1 = list()
            #     list_2 = [dict(zip(target, i)).update({'count': 0}) for i in cur.fetchall()]
            cur.execute(sql_1.format(factory_id))
            list_1 = [dict(zip(target, i)) for i in cur.fetchall()]
            cur.execute(sql_2.format(factory_id))
            list_2 = [dict(zip(target, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        result = {'yes': list_1, 'no': list_2}

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductPbId(APIView):
    """获取工序/BOM详情 product/pb/{id}"""
    """修改工序/BOM详情 product/pb/{id}"""
    """新增工序/BOM详情 product/pb/{id}"""
    """删除工序/BOM详情 product/pb/{id}"""
    # 已测
    permission_classes = [ProductPermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = """
            select 
                t1.product_id, 
                t1.process_id, 
                t3.name, 
                t3.unit, 
                t1.process_step, 
                t2.name, 
                material_ids, 
                material_counts 
            from 
                base_product_processes t1 
            left join 
                base_processes t2 
                on t1.process_id = t2.id 
            left join 
                base_materials_pool t3
                on t1.product_id = t3.id
            where 
                t1.product_id = '{}' 
                and t1.factory = '{}';"""
        target = ['id', 'process_id', 'name', 'unit', 'process_step', 'process_name', 'material_ids', 'material_counts']

        try:
            cur.execute(sql.format(Id, factory_id))
            pre_result = [dict(zip(target, i)) for i in cur.fetchall()]
            result = dict()
            if pre_result:
                result['id'] = pre_result[0]['id']
                result['name'] = pre_result[0]['name']
                result['unit'] = pre_result[0]['unit']
                result['process'] = []
            for i in pre_result:
                material_names = list()
                material_units = list()
                for j in i['material_ids']:
                    cur.execute("select name, unit from base_materials_pool where id = '{}';".format(j))
                    tmp = cur.fetchone()
                    material_names.append(tmp[0])
                    material_units.append(tmp[1])
                material_list = [{'id': x, 'name': y, 'count': z, 'unit': t}
                                 for x, y, z, t in zip(i['material_ids'], material_names,
                                                       i['material_counts'], material_units)]
                item = dict()
                item['process_id'] = i['process_id']
                item['process_step'] = i['process_step']
                item['process_name'] = i['process_name']
                item['material_list'] = material_list
                result['process'].append(item)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        process_step = request.data.get('process_step')
        materials = request.data.get('materials')

        sql = """
            update 
                base_product_processes 
            set 
                material_ids = '{}', 
                material_counts = '{}' 
            where 
                product_id = '{}' 
                and factory = '{}' 
                and process_step = '{}';"""
        try:
            material_ids = '{' + ','.join(j['id'] for j in materials) + '}'
            material_counts = '{' + ','.join(str(j['count']) for j in materials) + '}'
            cur.execute(sql.format(material_ids, material_counts, Id, factory_id, process_step))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)

    def post(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        process_id = request.data.get('process_id')
        materials = request.data.get('materials')
        Time = int(time.time())

        sql_0 = """
            select 
                process_step 
            from 
                base_product_processes 
            where 
                factory = '{}' 
                and product_id = '{}';"""
        sql_1 = """
            insert into
                base_product_processes(factory, product_id, process_step, process_id, material_ids, material_counts, time)
            values('{}', '{}', '{}', '{}', '{}', '{}', {})"""
        try:
            cur.execute(sql_0.format(factory_id, Id))
            tmp = cur.fetchall()
            if tmp:
                process_step = max([int(i[0]) for i in tmp])
            else:
                process_step = 0
            material_ids = '{' + ','.join(j['id'] for j in materials) + '}'
            material_counts = '{' + ','.join(str(j['count']) for j in materials) + '}'
            cur.execute(sql_1.format(factory_id, Id, process_step+1, process_id, material_ids, material_counts, Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        process_step = request.query_params.get('process_step')

        sql_0 = """
            select 
                process_step 
            from 
                base_product_processes 
            where 
                factory = '{}' 
                and product_id = '{}';"""
        sql_1 = """
            delete from 
                base_product_processes 
            where 
                factory = '{}' 
                and product_id = '{}' 
                and process_step = '{}';"""
        sql_2 = """
            update 
                base_product_processes 
            set 
                process_step = '{}' 
            where 
                factory = '{}' 
                and product_id = '{}' 
                and process_step = '{}';"""

        try:
            cur.execute(sql_0.format(factory_id, Id))
            tmp = cur.fetchall()
            cur.execute(sql_1.format(factory_id, Id, process_step))
            if tmp:
                # 字符串？
                process_steps = [int(i[0]) for i in tmp if int(i[0]) > int(process_step)]
            else:
                process_steps = list()
            for i in process_steps:
                cur.execute(sql_2.format(i - 1, factory_id, Id, i))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductMaterialSupplementList(APIView):
    """补料单列表 product/material/supplement/list"""
    permission_classes = [ProductPermission]

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        # t1.time跟t4.time应该是一样的
        sql_1 = """
                select 
                    t1.id,
                    t1.state,
                    t1.material_ids,
                    t1.material_counts,
                    case t1.state 
                        when '0' then t1.create_time 
                        when '1' then t4.waited_time 
                        when '2' then t4.picking_time 
                        else null
                    end as time,
                    t3.name
                from
                    base_material_supplement t1
                left join
                    base_product_task t2 
                    on t1.product_task_id = t2.id
                left join
                    base_materials_pool t3 
                    on t2.product_id = t3.id 
                left join
                    base_store_picking_list t4 
                    on t1.id = t4.supplement_id 
                where 
                    t1.factory = '{}' 
                    and t4.style = '1'
                order by
                    time desc;"""
        sql_2 = """
                select 
                    t1.name, 
                    unit, 
                    t2.name 
                from 
                    base_materials_pool t1 
                left join 
                    base_material_category_pool t2 
                    on t1.category_id = t2.id
                where 
                    t1.id = '{}';"""
        target = ['id', 'state', 'material_ids', 'material_counts', 'time', 'name']
        try:
            cur.execute(sql_1.format(factory_id))
            tmp_1 = [dict(zip(target, i)) for i in cur.fetchall()]

            result = {'0': list(), '1': list(), '2': list()}
            for i in tmp_1:
                material_names = list()
                material_units = list()
                material_categories = list()
                for j in i['material_ids']:
                    cur.execute(sql_2.format(j))
                    tmp = cur.fetchone()
                    material_names.append(tmp[0])
                    material_units.append(tmp[1])
                    material_categories.append(tmp[2])
                i['material'] = ';'.join('{}:{} {}{}'.format(x, y, z, t) for x, y, z, t in zip(material_categories,
                                                                                               material_names,
                                                                                               i['material_counts'],
                                                                                               material_units))
                del i['material_ids']
                del i['material_counts']

                if i['state'] == '0':
                    result['0'].append(i)
                elif i['state'] == '1':
                    result['1'].append(i)
                elif i['state'] == '2':
                    result['2'].append(i)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductMaterialSupplementDetailId(APIView):
    """补料单详情 product/material/supplement/detail/{id}"""
    permission_classes = [ProductPermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        sql = """
            select 
                t1.id,
                t1.state,
                t1.remark,
                coalesce(t2.name, ''),
                t2.phone,
                case t1.state
                    when '0' then t1.create_time 
                    when '1' then t3.waited_time 
                    when '2' then t3.picking_time 
                    else null 
                end as time,
                t1.material_ids,
                t1.material_counts,
                t4.order_id
            from 
                base_material_supplement t1
            left join
                user_info t2 
                on t1.creator = t2.user_id
            left join
                base_store_picking_list t3 
                on t1.id = t3.supplement_id
            left join
                base_product_task t4 
                on t4.id = t1.product_task_id
            where 
                t1.id = '{}';"""
        target = ['id', 'state', 'remark', 'creator', 'phone', 'time', 'material_ids', 'material_counts', 'order_id']
        sql_2 = "select name, unit from base_materials_pool where id = '{}';"
        try:
            cur.execute(sql.format(Id))
            result = dict(zip(target, cur.fetchone()))

            material_names = list()
            material_units = list()
            for j in result['material_ids']:
                cur.execute(sql_2.format(j))
                tmp = cur.fetchone()
                material_names.append(tmp[0])
                material_units.append(tmp[1])

            result['material'] = [{'name': x, 'count': '{}{}'.format(y, z)}
                                  for x, y, z in zip(material_names, result['material_counts'], material_units)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductMaterialReturnList(APIView):
    """退料单列表 product/material/return/list"""
    permission_classes = [ProductPermission]

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_1 = """
            select 
                t1.id, 
                t1.state, 
                t1.material_ids, 
                t1.material_counts, 
                case  
                    when t1.state = '0' then create_time 
                    else finish_time 
                end as time,
                t3.name
            from 
                base_material_return t1 
            left join
                base_product_task t2 
                on t1.product_task_id = t2.id
            left join
                base_materials_pool t3 
                on t2.product_id = t3.id
            where
                t1.factory = '{}' 
            order by 
                time desc;"""
        sql_2 = """
                select 
                    t1.name, 
                    unit, 
                    t2.name 
                from 
                    base_materials_pool t1 
                left join 
                    base_material_category_pool t2 on 
                    t1.category_id = t2.id
                where 
                    t1.id = '{}';"""
        target = ['id', 'state', 'material_ids', 'material_counts', 'time', 'name']

        try:
            cur.execute(sql_1.format(factory_id))
            tmp_1 = [dict(zip(target, i)) for i in cur.fetchall()]

            result = {'0': list(), '1': list()}
            for i in tmp_1:
                material_names = list()
                material_units = list()
                material_categories = list()
                for j in i['material_ids']:
                    cur.execute(sql_2.format(j))
                    tmp = cur.fetchone()
                    material_names.append(tmp[0])
                    material_units.append(tmp[1])
                    material_categories.append(tmp[2])
                i['material'] = ';'.join('{}:{} {}{}'.format(x, y, z, t) for x, y, z, t in zip(material_categories,
                                                                                               material_names,
                                                                                               i['material_counts'],
                                                                                               material_units))
                del i['material_ids']

                if i['state'] == '0':
                    result['0'].append(i)
                elif i['state'] == '1':
                    result['1'].append(i)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductMaterialReturnDetailId(APIView):
    """退料单详情 product/material/return/detail/{id}"""
    permission_classes = [StoreProductPermission]

    # todo 仓库部扫码的人也要有权限访问此接口
    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        user_id = request.redis_cache["user_id"]
        factory_id = request.redis_cache["factory_id"]

        sql_0 = "select t2.factory from base_material_return t1 left join base_product_task t2 on " \
                "t1.product_task_id = t2.id where t1.id = '{}';"

        sql_1 = """
            select 
                t1.id, 
                t1.state, 
                t1.material_ids, 
                t1.material_counts, 
                t1.creator, 
                t1.receiver, 
                case 
                    when t1.state = '0' then t1.create_time 
                    else t1.finish_time 
                end as time, 
                t1.remark,
                t2.order_id
            from 
                base_material_return t1 
            left join 
                base_product_task t2 
                on t1.product_task_id = t2.id 
            where 
                t1.id = '{}';"""
        target = ['id', 'state', 'material_ids', 'material_counts', 'creator',
                  'receiver', 'time', 'remark', 'order_id']
        sql_2 = "select coalesce(name, ''), phone from user_info where user_id = '{}';"
        sql_3 = "select name, unit from base_materials_pool where id = '{}';"

        try:
            # 判断退料单是否存在
            cur.execute(sql_1.format(Id))
            exist = cur.fetchone()
            if not exist:
                return Response({"res": 1, "errmsg": '该退料单不存在'}, status=status.HTTP_200_OK)
            # 判断查看详情人的信息是否有权查看
            cur.execute(sql_0.format(Id))
            share_factory = cur.fetchone()
            if share_factory:
                share_factory = share_factory[0]
            else:
                return Response({"res": 1, "errmsg": '出示人所属公司不存在！'}, status=status.HTTP_200_OK)
            if share_factory != factory_id:
                return Response({"res": 1, "errmsg": '所属公司不相同，无权查看退料单详情！'}, status=status.HTTP_200_OK)
            result = dict(zip(target, exist))
            # 获取创建者和签收者信息
            cur.execute(sql_2.format(result['creator']))
            tmp = cur.fetchone() or ('', '')
            result['creator_name'] = tmp[0]
            result['creator_phone'] = tmp[1]
            if result['receiver']:
                cur.execute(sql_2.format(result['receiver']))
                tmp = cur.fetchone() or ('', '')
                result['receiver_name'] = tmp[0]
                result['receiver_phone'] = tmp[1]
            del result['creator']
            del result['receiver']
            # 物料清单
            material_names = list()
            material_units = list()
            for j in result['material_ids']:
                cur.execute(sql_3.format(j))
                tmp = cur.fetchone()
                material_names.append(tmp[0])
                material_units.append(tmp[1])
            # 二维码字符串信息
            if result['state'] == '0':
                content = {"type": "8", "id": Id, "share": user_id}
                result['content'] = content
            result['material'] = [{'name': x, 'count': '{}{}'.format(y, z)}
                                  for x, y, z in zip(material_names, result['material_counts'], material_units)]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class ProductMaterialReturnId(APIView):
    """(扫码人)确认退料 product/material/return/{id}"""
    # todo 权限对吗......？
    permission_classes = [StorePermission]

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql_0 = "select t2.factory from base_material_return t1 left join base_product_task t2 on " \
                "t1.product_task_id = t2.id where t1.id = '{}';"

        sql = """
            select 
                coalesce(t2.name, ''), 
                coalesce(t3.name, ''),
                finish_time
            from 
                base_material_return t1 
            left join 
                user_info t2 
                on t1.creator = t2.user_id
            left join 
                user_info t3 
                on t1.receiver = t3.user_id 
            where 
                id = '{}';"""
        target = ['creator', 'receiver', 'time']
        try:
            cur.execute(sql_0.format(Id))
            share_factory = cur.fetchone()
            if share_factory:
                share_factory = share_factory[0]
            else:
                return Response({"res": 1, "errmsg": '出示人所属公司不存在！'}, status=status.HTTP_200_OK)
            if share_factory != factory_id:
                return Response({"res": 1, "errmsg": '所属公司不相同，无权查看退料详情！'}, status=status.HTTP_200_OK)
            cur.execute(sql.format(Id))
            tmp = cur.fetchone() or list()
            result = dict(zip(target, tmp))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        user_id = request.redis_cache["user_id"]
        share_id = request.data.get('share')
        Time = int(time.time())

        sql_0 = "select factory from user_info t1 left join factory_users t2 on t1.phone = t2.phone " \
                "where t1.user_id = '{}';"
        sql_5 = "select state from base_material_return where id = '{}';"

        sql_1 = "update base_material_return set state = '1', receiver = '{}', finish_time = {} where id = '{}';"
        sql_2 = "select material_ids, material_counts from base_material_return where id = '{}';"
        sql_3 = """
            insert into
                base_materials_log(
                    material_id, 
                    count, 
                    type, 
                    factory,  
                    source, 
                    source_id, 
                    time)
            values ('{}', {}, 'actual', '{}', '4', '{}', {});"""
        sql_4 = """
            update
                base_materials_storage
            set 
                actual = actual + {}
            where
                material_id = '{}' 
                and factory = '{}';"""
        try:
            cur.execute(sql_0.format(share_id))
            share_factory = cur.fetchone()
            if share_factory:
                share_factory = share_factory[0]
            else:
                return Response({"res": 1, "errmsg": '出示人所属公司不存在！'}, status=status.HTTP_200_OK)
            if share_factory != factory_id:
                return Response({"res": 1, "errmsg": '所属公司不相同，无法退料！'}, status=status.HTTP_200_OK)

            cur.execute(sql_5.format(Id))
            state = cur.fetchone()[0]
            if state == '1':
                return Response({"res": 1, "errmsg": '此退料单已退料'}, status=status.HTTP_200_OK)
            cur.execute(sql_1.format(user_id, Time, Id))
            # 退料后增加库存
            cur.execute(sql_2.format(Id))
            tmp = cur.fetchone()
            for x, y in zip(tmp[0], tmp[1]):
                cur.execute(sql_3.format(x, y, factory_id, Id, Time))
                cur.execute(sql_4.format(y, x, factory_id))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 推送：已退料
        # message = get_message_data_material(cur, product_task_id, user_id, '已退料')
        message = {'resource': 'PyProductMaterialReturnCreate',
                   'type': 'POST',
                   'params': {"fac": factory_id, "id": Id, "state": "2", "user_id": user_id}}
        rabbitmq = UtilsRabbitmq()
        rabbitmq.send_message(json.dumps(message))

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductMaterialSupplementCreate(APIView):
    """创建补料单 product/material/supplement/create"""
    """创建退料单 product/material/return/create"""
    permission_classes = [ProductPermission]

    def post(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        seq_id = request.redis_cache["seq_id"]
        factory_id = request.redis_cache["factory_id"]
        user_id = request.redis_cache["user_id"]
        remark = request.data.get('remark', '')
        materials = request.data.get('materials')
        product_task_id = request.data.get('id')
        Time = int(time.time())

        if Type == 'supplement':
            Id = generate_module_uuid('11', factory_id, seq_id)
            sql_1 = """
                insert into base_material_supplement(
                        id, 
                        material_ids, 
                        material_counts, 
                        creator, 
                        create_time, 
                        factory, 
                        remark, 
                        product_task_id
                    )
                values('{}','{}','{}','{}',{},'{}','{}','{}');"""
            sql_2 = "select order_id from base_product_task where id = '{}';"
            sql_3 = """
                insert into
                    base_materials_log(
                        material_id, 
                        count, 
                        type, 
                        factory, 
                        source, 
                        source_id, 
                        time)
                values ('{}', {}, 'prepared', '{}', '3', '{}', {});"""
            sql_4 = """
                update
                    base_materials_storage
                set 
                    prepared = prepared + {}
                where
                    material_id = '{}' 
                    and factory = '{}';"""
            sql_5 = "select loss_coefficient from base_materials where id = '{}' and factory = '{}';"
        else:
            Id = generate_module_uuid('10', factory_id, seq_id)
            sql_1 = """
                insert into base_material_return(
                        id, 
                        material_ids, 
                        material_counts, 
                        creator, 
                        create_time, 
                        factory, 
                        remark, 
                        product_task_id
                    )
                values('{}','{}','{}','{}',{},'{}','{}','{}');"""

        try:
            material_ids = '{' + ','.join(j['id'] for j in materials) + '}'
            material_counts = '{' + ','.join(str(j['count']) for j in materials) + '}'
            cur.execute(sql_1.format(Id, material_ids, material_counts, user_id, Time, factory_id, remark,
                                     product_task_id))

            if Type == 'supplement':
                flag = True
                purchase_list = list()

                # 判断库存是否充足
                for i in materials:
                    x, y = i['id'], i['count']
                    cur.execute("select coalesce(actual, 0) + coalesce(on_road, 0) - coalesce(prepared, 0) - "
                                "coalesce(safety, 0) as available_count from base_materials_storage "
                                "where factory = '{}' and material_id = '{}';".format(factory_id, x))
                    count = cur.fetchone()[0]
                    if count < y:
                        flag = False
                        purchase_list.append((x, y-count))
                # 获取订单id
                cur.execute(sql_2.format(product_task_id))
                order_id = cur.fetchone()[0]
                if flag:
                    # 预分配
                    for i in materials:
                        x, y = i['id'], i['count']
                        cur.execute(sql_3.format(x, y, factory_id, Id, Time))
                        cur.execute(sql_4.format(y, x, factory_id))
                    # 创建领料单
                    create_picking_list(cur, order_id, product_task_id, Id, factory_id, 1, seq_id)
                else:
                    # 创建采购单
                    purchase_counts = list()
                    for x, y in purchase_list:
                        cur.execute(sql_5.format(x, factory_id))
                        loss = cur.fetchone()
                        loss_coefficient = loss[0] if loss else 0
                        purchase_count = y / (1 - loss_coefficient)
                        purchase_counts.append({'id': x, 'count': purchase_count})
                    create_purchase(cur, factory_id, seq_id, order_id, purchase_counts, Id)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 推送： 新建补料/退料推送
        if Type == 'supplement':
            # message = get_message_data_material(cur, product_task_id, user_id, '新建补料单')
            message = {'resource': 'PyProductMaterialSupplementCreate',
                       'type': 'POST',
                       'params': {"fac": factory_id, "id": Id, "state": "1", "user_id": user_id}}

        else:
            # message = get_message_data_material(cur, product_task_id, user_id, '新建退料单')
            message = {'resource': 'PyProductMaterialReturnCreate',
                       'type': 'POST',
                       'params': {"fac": factory_id, "id": Id, "state": "1", "user_id": user_id}}
        rabbitmq = UtilsRabbitmq()
        rabbitmq.send_message(json.dumps(message))

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class ProductMaterialRSList(APIView):
    """产品物料列表 product/material/rs_list/{id}"""

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        sql_1 = """
            select 
                material_ids, 
                material_counts 
            from 
                base_product_task
            where 
                id = '{}';"""
        sql_2 = "select name from base_materials_pool where id = '{}';"""

        try:
            cur.execute(sql_1.format(Id))
            material_list = cur.fetchone()

            result = list()
            for x, y in zip(material_list[0], material_list[1]):
                material = dict()
                cur.execute(sql_2.format(x))
                name = cur.fetchone()[0]
                material['id'] = x
                material['count'] = y
                material['name'] = name
                result.append(material)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": '服务器异常'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)