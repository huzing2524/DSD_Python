# -*- coding: utf-8 -*-
import json
import logging
import time
import traceback

from django.utils.decorators import method_decorator
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from apps_utils import UtilsPostgresql, UtilsRabbitmq, AliOss, generate_module_uuid, today_timestamp
from permissions import OrderPermission, order_decorator, order_approval_decorator

from purchase.purchase_utils import update_purchase_state
from store.store_utils import update_invoice, create_invoice
from store.store_stock_utils import product_prepare_stock
from products.products_utils import create_product_task
from constants import PrimaryKeyType, OrderStatsState, OrderTrackType, OrderType

logger = logging.getLogger('django')


class OrderMain(APIView):
    """订单首页 /order/main"""
    permission_classes = [OrderPermission]

    def get(self, request):
        timestamp = int(time.time())
        factory_id = request.redis_cache["factory_id"]
        stime = request.query_params.get("start", 1)
        etime = request.query_params.get("end", timestamp)
        header_sql = "SELECT s1, s2, s3, s6 FROM get_order_state_counts('{}') AS (s1 integer, s2 integer," \
                     " s3 integer, s6 integer);".format(factory_id)
        deliver_sql = '''
        select
            state,
            count( 1 )
        from
            (
            select
                case
                    when ( to_timestamp(plan_arrival_time) < current_timestamp )
                    or actual_arrival_time > plan_arrival_time then '2'
                    when actual_arrival_time <= plan_arrival_time then '1'
                    else '3'
                end as state
            from
                base_orders
            where
                factory = '{0}'
                and state != '5'
                and state != '6'
                and create_time >= {1}
                and create_time < {2} ) t
        group by
            t.state ;'''.format(factory_id, stime, etime)

        order_amount_sql = '''
            select
                count(1),
                state,
                sum(count) as orders,
                sum(price)
            from
                (
                select
                    sum( price ) as price,
                    count( 1 ),
                    case
                        when count( 1 ) > 1 then '2'
                        else '1'
                    end as state
                from
                    (
                    select
                        t1.client_id,
                        t2.price
                    from
                        base_orders t1
                    left join (
                        select
                            order_id,
                            sum(unit_price * product_count) as price
                        from
                            base_order_products
                        group by order_id ) t2 on
                            t1.id = t2.order_id
                    where
                        t1.factory = '{0}'
                        and t1.state != '5'
                        and t1.create_time >= {1} 
                        and t1.create_time < {2} ) t
                group by
                    client_id ) t
            group by
                state;'''.format(factory_id, stime, etime)

        order_progress_sql = '''
            select
                count(1),
                state
            from
                (
                select
                    case
                        when t2.state @> array['2']:: varchar[] then '2'
                        when t2.state @> array['0']:: varchar[]
                        or t2.state @> array['1']:: varchar[] then '1'
                        when t2.state @> array['3']:: varchar[]
                        and t3.state isnull then '3'
                        when t3.state @> array['0']:: varchar[] then '3'
                    end as state
                from
                    base_orders t1
                left join (
                    select
                        order_id,
                        array_agg( state ) as state
                    from
                        base_product_task
                    group by
                        order_id ) t2 on
                    t1.id = t2.order_id
                left join (
                    select
                        order_id,
                        array_agg( state ) as state
                    from
                        base_store_invoice
                    group by
                        order_id ) t3 on
                    t1.id = t3.order_id
                where
                    t1.factory = '{0}'
                    and t1.create_time >= {1}
                    and t1.create_time < {2}
                    and ( ( t2.state notnull
                    and ( t2.state @> array['0'] :: varchar[]
                    or t3.state isnull ))
                    or t3.state @> array['0'] :: varchar[])) t
            group by
                state;'''.format(factory_id, stime, etime)

        order_status_sql = '''
            select
                state,
                count( 1 )
            from
                base_orders
            where
                factory = '{0}'
                and create_time >= {1}
                and create_time < {2}
            group by
                state'''.format(factory_id, stime, etime)
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(header_sql)
            res_header = cursor.fetchall()[0]
            header = {
                "approval": res_header[0],
                "deliver": res_header[1],
                "transit": res_header[2],
                "pause": res_header[3]
            }
            cursor.execute(deliver_sql)
            res_deliver = cursor.fetchall()
            deliver_stats = {
                "ontime": 0,
                "overdue": 0,
                "notdeliver": 0
            }
            for x in res_deliver:
                if x[0] == '1':
                    deliver_stats['ontime'] = x[1]
                elif x[0] == '2':
                    deliver_stats['overdue'] = x[1]
                else:
                    deliver_stats['notdeliver'] = x[1]

            cursor.execute(order_amount_sql)
            res_order_amount = cursor.fetchall()
            total_amount, totoal_orders = 0, 0
            order_amount = {
                "orders": 0,
                "new_c": 0,
                "regular_c": 0,
                "total_amount": 0
            }
            for x in res_order_amount:
                total_amount += x[3]
                totoal_orders += x[2]
                if x[1] == '1':
                    order_amount['new_c'] = x[0]
                else:
                    order_amount['regular_c'] = x[0]
            order_amount['orders'] = totoal_orders
            order_amount['total_amount'] = total_amount
            cursor.execute(order_progress_sql)
            res_order_progress = cursor.fetchall()
            order_progress = {
                "pending": 0,
                "producing": 0,
                "not_deliver": 0
            }
            for x in res_order_progress:
                if x[1] == '1':
                    order_progress['pending'] = x[0]
                elif x[1] == '2':
                    order_progress['producing'] = x[0]
                else:
                    order_progress['not_deliver'] = x[0]

            cursor.execute(order_status_sql)
            res_order_status = cursor.fetchall()
            order_status = {
                "s1": 0,
                "s2": 0,
                "s3": 0,
                "s4": 0,
                "s5": 0,
                "s6": 0
            }
            for x in res_order_status:
                order_status['s{0}'.format(x[0])] = x[1]

            return Response({"list": {'header': header,
                                      'deliver_stats': deliver_stats,
                                      'order_amount': order_amount,
                                      'order_progress': order_progress,
                                      'order_status': order_status}},
                            status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class OrderCRank(APIView):
    permission_classes = [OrderPermission]

    def get(self, request):
        start, end = today_timestamp()
        factory_id = request.redis_cache["factory_id"]
        stime = request.query_params.get("start", start)
        etime = request.query_params.get("end", end)
        if request.query_params.get("order", '1') == '1':
            order = 'desc'
        else:
            order = 'asc'

        sql = '''
            select
                t1.price,
                coalesce(t2.name, t3.name) as name
            from
                (
                select
                    client_id,
                    sum( price ) as price
                from
                    (
                    select
                        t1.client_id,
                        t2.price
                    from
                        base_orders t1
                    left join (
                        select
                            order_id,
                            sum( price ) as price
                        from
                            (
                            select
                                order_id,
                                unit_price * product_count as price
                            from
                                base_order_products ) t
                        group by
                            order_id ) t2 on
                        t1.id = t2.order_id
                    where
                        t1.factory = '{0}'
                        and create_time >= {1}
                        and create_time < {2}
                        and state != '5' ) t
                group by
                    client_id ) t1
            left join (select * from base_clients where factory = '{0}') t2 on
                t1.client_id = t2.id
            left join base_clients_pool t3 on
                t1.client_id = t3.id
            order by
                price {3}
            limit 10;'''.format(factory_id, stime, etime, order)
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(sql)
            res = cursor.fetchall()
            data = []
            for index, val in enumerate(res):
                temp = dict()
                temp['rn'] = index + 1
                temp['name'] = val[1]
                temp['amount'] = val[0]
                data.append(temp)

            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


def customer_name_dict(orders):
    dt = {}
    for x in orders:
        if dt.get(x[7]):
            continue
        else:
            dt[x[7]] = x[1]
    return dt


# 生成一个hash表，根据客户id记录对应是新客户还是老客户
# 1: 新客户 2: 老客户； 在多个订单中出现就是老客户
def customer_state_dict(orders):
    dt = {}
    for x in orders:
        if dt.get(x[7]):
            dt[x[7]] = '2'
        else:
            dt[x[7]] = '1'
    return dt


class OrderList(APIView):
    permission_classes = [OrderPermission]

    def get(self, request, list_type):
        """订单列表，包含了各个订单列表
        Args:
            list_type:
                1: 订单状态列表
                2: 订单交货率列表
                3: 订单金额列表
                4: 订单生产进度列表
                5: 客户贡献度列表
        """
        factory_id = request.redis_cache["factory_id"]

        # 1.订单状态sql
        sql1 = """
            select
                t1.id,
                coalesce(t3.name, t4.name) as client_name,
                t2.product_count,
                t2.product_name,
                t2.unit,
                t1.order_type,
                t1.create_time,
                t1.state
            from
                base_orders t1
            left join (
                select
                    order_id,
                    array_agg( product_id ) as product_id,
                    array_agg( product_count ) as product_count,
                    array_agg( name ) as product_name,
                    array_agg( unit ) as unit
                from
                    (
                    select
                        t1.product_id,
                        t1.product_count,
                        t1.order_id,
                        t2.name,
                        t2.unit
                    from
                        base_order_products t1
                    left join base_materials_pool  t2 on
                        t1.product_id = t2.id
                    where
                        t2.id notnull ) t
                group by
                    order_id ) t2 on
                t1.id = t2.order_id
            left join (select * from base_clients where factory = '{0}') t3 on
                t1.client_id = t3.id
            left join base_clients_pool t4 on
                t1.client_id = t4.id
            where
                t1.factory = '{0}'
            order by
                t1.state,
                t1.create_time desc; """.format(factory_id)

        # 2: 订单交货率列表
        sql2 = '''
            select
                t1.id,
                coalesce(t3.name, t4.name) as client_name,
                t2.product_count,
                t2.product_name,
                t2.unit,
                t1.order_type,
                t1.create_time,
                case
                    when to_timestamp(t1.plan_arrival_time) < current_timestamp
                    or t1.actual_arrival_time > t1.plan_arrival_time then '2'
                    else '1'
                end as state
            from
                base_orders t1
            left join (
                select
                    order_id,
                    array_agg( product_count ) as product_count,
                    array_agg( name ) as product_name,
                    array_agg( unit ) as unit
                from
                    (
                    select
                        t1.product_count,
                        t1.order_id,
                        t2.name,
                        t2.unit
                    from
                        base_order_products t1
                    left join base_materials_pool  t2 on
                        t1.product_id = t2.id
                    where
                        t2.id notnull ) t
                group by
                    order_id ) t2 on
                t1.id = t2.order_id
            left join (select * from base_clients where factory = '{0}') t3 on
                t1.client_id = t3.id
            left join base_clients_pool t4 on
                t1.client_id = t4.id
            where
                t1.factory = '{0}'
                and state != '5' and state != '6'
                and ( t1.actual_arrival_time != 0
                or to_timestamp(t1.plan_arrival_time) < current_timestamp )
            order by
                create_time desc;'''.format(factory_id)

        # 3: 订单金额列表, 5: 客户贡献度列表
        # 按客户分类， 订单数大于1为老客户
        sql3 = '''
            select
                t1.id,
                coalesce(t3.name, t4.name) as client_name,
                t2.product_count,
                t2.product_name,
                t2.unit,
                t1.order_type,
                t1.create_time,
                t3.id
            from
                base_orders t1
            left join (
                select
                    order_id,
                    array_agg( product_count ) as product_count,
                    array_agg( name ) as product_name,
                    array_agg( unit ) as unit
                from
                    (
                    select 
                        t1.product_count,
                        t1.order_id,
                        t2.name,
                        t2.unit
                    from
                        base_order_products t1
                    left join base_materials_pool  t2 on
                        t1.product_id = t2.id
                    where
                        t2.id notnull ) t
                group by
                    order_id ) t2 on
                t1.id = t2.order_id
            left join (select * from base_clients where factory = '{0}') t3 on
                t1.client_id = t3.id
            left join base_clients_pool t4 on
                t1.client_id = t4.id
            where
                t1.factory = '{0}'
            order by
                t1.create_time desc;'''.format(factory_id)

        # 4: 订单生产进度列表
        sql4 = '''
        select
            t1.id,
            coalesce(t3.name, t6.name) as client_name,
            t2.product_count,
            t2.product_name,
            t2.unit,
            t1.order_type,
            t1.create_time,
            case
                when t4.state @> array['2']:: varchar[] then '2'
                when t4.state @> array['0']:: varchar[]
                or t4.state @> array['1']:: varchar[] then '1'
                when t4.state @> array['3']:: varchar[]
                and t5.state isnull then '3'
                when t5.state @> array['0']:: varchar[] then '3'
            end as state
        from
            base_orders t1
        left join (
            select
                order_id,
                array_agg( product_count ) as product_count,
                array_agg( name ) as product_name,
                array_agg( unit ) as unit
            from
                (
                select
                    t1.product_count,
                    t1.order_id,
                    t2.name,
                    t2.unit
                from
                    base_order_products t1
                left join base_materials_pool  t2 on
                    t1.product_id = t2.id
                where
                    t2.id notnull ) t
            group by
                order_id ) t2 on
            t1.id = t2.order_id
        left join (select * from base_clients where factory = '{0}') t3 on
            t1.client_id = t3.id
        left join (
            select
                order_id,
                array_agg(state) as state
            from
                base_product_task
            group by
                order_id ) t4 on
            t1.id = t4.order_id
        left join (
            select
                order_id,
                array_agg(state) as state
            from
                base_store_invoice
            group by
                order_id ) t5 on
            t1.id = t5.order_id
        left join base_clients_pool t6 on
          t1.client_id = t6.id
        where
            t1.factory = '{0}'
            and ( ( t4.state notnull
            and ( t5.state @> array['0'] :: varchar[]
            or t5.state isnull ))
            or t5.state @> array['0'] :: varchar[])
        order by
            t1.create_time desc;'''.format(factory_id)

        sql_dict = {
            "1": sql1,
            "2": sql2,
            "3": sql3,
            "4": sql4,
            "5": sql3
        }

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(sql_dict.get(list_type, '1'))
            result = cursor.fetchall()
            data = {}
            # 判断客户是新客户还是老客户
            customer_states = {}
            if list_type == '3':
                customer_states = customer_state_dict(result)
            # 根据客户id获取客户名称
            elif list_type == '5':
                customer_states = customer_name_dict(result)
            for res in result:
                temp = {}
                if list_type == '3' or list_type == '5':
                    state = customer_states.get(res[7])
                else:
                    state = res[7]
                if not data.get(state):
                    data[state] = []
                temp['id'] = res[0]
                temp['order_type'] = res[5]
                temp['name'] = res[1] or ""
                products = ''
                if res[2]:
                    for i, val in enumerate(res[2]):
                        products += res[3][i] + ':' + str(round(val, 2)) + res[4][i] + ';'
                products = products.rstrip(",")
                temp['products'] = products
                temp['time'] = res[6] or 0
                data[state].append(temp)
            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class OrderDetail(APIView):
    """订单详情 order/detail"""

    @method_decorator(order_decorator)
    def get(self, request, order_id):
        if request.flag is False:
            return Response({"res": "1", "errmsg": "你没有权限"}, status=status.HTTP_403_FORBIDDEN)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(
            "select count(1) from base_orders where id = '{}';".format(order_id))
        order_check = cursor.fetchone()[0]
        if order_check == 0:
            return Response({"res": 1, "errmsg": "订单id查询不存在。"}, status=status.HTTP_200_OK)
        phone = request.redis_cache["phone"]
        permission = request.redis_cache["permission"]
        factory_id = request.redis_cache["factory_id"]

        order_sql = """
        select 
            t1.client_id,
            t1.state,
            t1.creator as creator_id,
            t4.name as creator,
            t1.collected,
            t1.plan_arrival_time,
            t1.actual_arrival_time,
            t1.remark,
            coalesce(t3.name, t6.name) as name,
            coalesce(t3.contacts, t6.contacts) as contacts,
            coalesce(t3.phone, t6.phone) as phone,
            coalesce(t3.position, t6.position) as position,
            coalesce(t3.region || t3.address, t6.region || t6.address) as address,
            t1.create_time,
            t1.order_type
        from 
            base_orders t1
        left join (select * from base_clients where factory = '{}') t3 on 
          t1.client_id = t3.id
        left join user_info t4 on 
          t1.creator = t4.user_id
        left join user_info t5 on 
          t1.approver = t5.user_id
        left join base_clients_pool t6 on
          t1.client_id = t6.id
           where t1.id = '{}';
        """.format(factory_id, order_id)

        products_sql = """
        select
            t1.*,
            coalesce(t2.name, '') as product_name,
            coalesce(t2.unit, '') as unit,
            coalesce( t3.name, '' ) as category_name
        from
            (
            select
            t1.product_id,
            t1.product_count,
            t1.unit_price,
            coalesce(t2.lowest_count,0),
            coalesce(t2.lowest_package,0)
        from
            base_order_products t1
        left join (
            select
                *
            from
                base_products
            where
                factory = '{}' ) t2 on
            t1.product_id = t2.id
        where
            t1.order_id = '{}') t1
        left join base_materials_pool  t2 on
            t1.product_id = t2.id
        left join base_material_category_pool t3 on
            t2.category_id = t3.id;
        """.format(factory_id, order_id)

        stats_sql = '''
            select
                t1.state,
                t1.remark,
                t1.optime as time,
                t2.phone,
                t2.name,
                t2.image
            from
                base_orders_stats t1
            left join user_info t2 on
                t1.operator = t2.user_id
            where
                t1.order_id = '{}'
            order by
                t1.time;'''.format(order_id)

        cursor.execute(order_sql)
        order_result = cursor.fetchall()
        cursor.execute(products_sql)
        products_result = cursor.fetchall()
        cursor.execute(stats_sql)
        stats_result = cursor.fetchall()
        data, products = {}, []
        for res in order_result:
            data["state"] = res[1] or ""

            data["creator_id"] = res[2] or ""
            data["creator"] = res[3] or ""
            data["collected"] = res[4] or 0
            data['plan_arrival_time'] = res[5] or 0
            data['actual_arrival_time'] = res[6] or 0
            data['order_type'] = res[14]
            data['client'] = {}
            data['client']['name'] = res[8] or ''
            data['client']['contact'] = res[9] or ''
            data['client']['phone'] = res[10] or ''
            data['client']['position'] = res[11] or ''
            data['client']['address'] = res[12] or ''
            data['client']["client_id"] = res[0] or ""

            if res[14] == OrderType.self.push:
                data['create_time'] = res[13]
                data['remark'] = res[7] or ''

        data['stats'] = []
        alioss = AliOss()
        for stat in stats_result:
            image = stat[5]
            if isinstance(image, memoryview):
                image = image.tobytes().decode()
            temp = {
                'state': stat[0],
                'remark': stat[1],
                'time': stat[2],
                'phone': stat[3],
                'name': stat[4],
                'image': alioss.joint_image(image)
            }
            data['stats'].append(temp)

            if "1" in permission:
                data["flag"] = "0"
            elif data["creator_id"] == phone:
                data["flag"] = "0"
            else:
                data["flag"] = "1"

        for res in products_result:
            di = dict()
            di["id"] = res[0] or ""
            di["count"] = round(res[1] if res[1] else 0, 2)
            di["unit_price"] = round(res[2] if res[2] else 0, 2)
            di["lowest_count"] = res[3] or 0
            di["lowest_package"] = res[4] or 0
            di["name"] = res[5] or ""
            di["unit"] = res[6] or ""
            di["category_name"] = res[7] or ""
            products.append(di)

        data["products"] = products
        return Response(data, status=status.HTTP_200_OK)

    @method_decorator(order_approval_decorator)
    def put(self, request, order_id):
        """修改订单状态
        1: 通过审批， 2：取消订单, 3: 终止订单, 4: 暂停订单，5：启动订单， 6：修改订单

        -- 订单状态 1: 待审批， 2：待发货, 3: 运输中, 4: 已送达，5：已取消, 6:已暂停

        审批订单 -> 判断产品库存，-> 库存不足 -> 创建生产任务单
                               -> 库存充足 -> 创建发货单
        取消，终止订单 -> 对应采购单取消，发货单取消
        """
        if request.flag is False:
            return Response({"res": "1", "errmsg": "你没有权限"}, status=status.HTTP_403_FORBIDDEN)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        order_state_sql = "select state from base_orders where id = '{}'".format(order_id)

        cursor.execute(order_state_sql)
        state_res = cursor.fetchone()
        state = request.data.get('state')
        remark = request.data.get('remark', '')
        if not state_res:
            return Response({"res": "1", "errmsg": "输入参数错误"}, status=status.HTTP_400_BAD_REQUEST)
        if state == '1' and state_res[0] != '1':
            return Response({"res": "1", "errmsg": "输入参数错误"}, status=status.HTTP_400_BAD_REQUEST)

        timestamp = int(time.time())

        factory_id = request.redis_cache["factory_id"]
        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        seq_id = request.redis_cache["seq_id"]
        if not user_id:
            user_id = request.redis_cache["phone"]

        update_state_dict = {
            '1': '2',
            '2': '5',
            '3': '5',
            '4': '6'
        }
        # 修改订单产品信息
        if state == '6':
            products_list = request.data.get("products", [])  # 列表
            client_sql = "select client_id, order_type from base_orders where id = '{}'".format(order_id)
            cursor.execute(client_sql)
            order_res = cursor.fetchone()
            client_id = order_res[0] or ''
            order_type = order_res[1]
            if order_type == '2':  # 订单类型 2：推送订单
                for product in products_list:
                    product_id = product["id"]
                    product_count = product["count"]
                    order_update_sql = "update base_order_products set product_count = {} " \
                                       "where order_id = '{}' and product_id = '{}'".format(product_count, order_id,
                                                                                            product_id)
                    cursor.execute(order_update_sql)
            else:  # 订单类型 1：自建订单
                delete_products = "delete from base_order_products where order_id = '{}'".format(order_id)
                cursor.execute(delete_products)
                for product in products_list:
                    product_id = product["id"]
                    product_count = product["count"]
                    price_sql = '''
                    select
                        coalesce( unit_price, 0.0)
                    from
                        base_client_products
                    where
                        factory_id = '{}'
                        and client_id = '{}'
                        and product_id = '{}'; '''.format(factory_id, client_id, product_id)
                    cursor.execute(price_sql)
                    price_val = cursor.fetchone()[0] or 0.0
                    order_products_sql = "insert into base_order_products (order_id, product_id, product_count, unit_price) " \
                                         " values ('{0}', '{1}', {2}, {3});".format(order_id, product_id, product_count,
                                                                                    price_val)
                    cursor.execute(order_products_sql)
            if remark:
                update_sql = "update base_orders set remark = '{}' where id = '{}'".format(remark, order_id)
                cursor.execute(update_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)

        if state != '5':
            update_state = update_state_dict[state]
            time_dict = {
                '1': "approval_time = {}, approver = '{}'".format(timestamp, user_id),  # fix bug: approver 类型: str
                '2': 'cancel_time = {0}'.format(timestamp),
                '3': 'cancel_time = {0}'.format(timestamp),
                '4': "pause_time = {0}, before_pause_state = state ".format(timestamp),
                '5': ''
            }
            cursor.execute(
                "update base_orders set state = '{}', {} where id = '{}' ".format(update_state, time_dict[state],
                                                                                  order_id))
            if state == '1':
                product_store = '''
                select
                    t1.product_id,
                    case
                        when ( 1 - t3.loss_coefficient ) = 0 then ( t1.product_count - ( coalesce( t2.actual,
                        0 ) + coalesce( t2.pre_product,
                        0 ) - coalesce( t2.prepared,
                        0 ) - coalesce( t2.safety,
                        0 )))
                        else ( t1.product_count - ( coalesce( t2.actual,
                        0 ) + coalesce( t2.pre_product,
                        0 ) - coalesce( t2.prepared,
                        0 ) - coalesce( t2.safety,
                        0 ))) / ( 1 - t3.loss_coefficient )
                    end as product_store_count,
                    t1.product_count
                from
                    base_order_products t1
                left join (
                    select
                        *
                    from
                        base_products_storage
                    where
                        factory = '{}' ) t2 on
                    t1.product_id = t2.product_id
                left join (
                    select
                        *
                    from
                        base_products
                    where
                        factory = '{}' ) t3 on
                    t1.product_id = t3.id
                where
                    t1.order_id = '{}';'''.format(factory_id, factory_id, order_id)

                cursor.execute(product_store)
                product_res = cursor.fetchall()

                order_sql = "select remark, plan_arrival_time from base_orders where id = '{}'".format(order_id)
                cursor.execute(order_sql)
                order_res = cursor.fetchall()[0]
                store_invoice_state = '1'
                product_ids = []
                product_counts = []
                for product in product_res:
                    product_ids.append(product[0])
                    product_counts.append(product[2])
                    if product[1] > 0:
                        # 生产计划完成时间暂时定位订单完成时间-1天
                        create_product_task(order_id=order_id, factory_id=factory_id, product_id=product[0],
                                            target_count=product[1], remark=order_res[0],
                                            plan_complete_time=order_res[1] - 24 * 60 * 60, seq_id=seq_id)
                        store_invoice_state = '0'

                # 创建发货单
                if store_invoice_state == '1':
                    create_invoice(cursor, order_id, '', factory_id, seq_id)
                update_purchase_state(cursor, factory_id, order_id, '3')
                # 预分配库存增加
                product_prepare_stock(cursor, factory_id, product_ids, product_counts, '4', order_id)

            elif state == '2' or state == '3':
                update_purchase_state(cursor, factory_id, order_id, '6')
                invoice_sql = "select id from base_store_invoice where order_id = '{}'".format(order_id)
                cursor.execute(invoice_sql)
                invoice_res = cursor.fetchone()
                if invoice_res:
                    update_invoice(invoice_res[0], '3', user_id, phone, factory_id, seq_id)
        elif state == "5":
            # 启动订单，恢复原来状态
            cursor.execute("update base_orders set state =  before_pause_state where id = '{}';".format(order_id))

        try:
            # 1: 通过审批， 2：取消订单, 3: 终止订单, 4: 暂停订单，5：启动订单
            # 订单通知状态 1: 订单审批通过， 2：订单已取消, 3: 订单已发货
            # 4: 订单已暂停，5：订单已启动, 6: 待审批订单， 7: 订单已送达
            stats_state_dict = {
                '1': OrderStatsState.approve.value,
                '2': OrderStatsState.cancel_inside.value,
                '3': OrderStatsState.cancel_inside.value,
                '4': OrderStatsState.pause.value,
                '5': OrderStatsState.resume.value
            }

            # 订单状态记录
            order_stats_sql = "insert into base_orders_stats (order_id, state, remark, operator, optime) values " \
                              "('{}', '{}', '{}', '{}', {})".format(order_id, stats_state_dict[state],
                                                                    remark, user_id, timestamp)
            cursor.execute(order_stats_sql)
            connection.commit()

            notice_state_dict = {
                '1': '1',
                '2': '2',
                '3': '2',
                '4': '4',
                '5': '5'
            }
            message = {'resource': 'PyOrderState', 'type': 'PUT',
                       'params': {'fac': factory_id, 'id': order_id, 'state': notice_state_dict[state]}}
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class OrderNew(APIView):
    """新建订单 order/new"""
    permission_classes = [OrderPermission]

    def post(self, request):
        products_list = request.data.get("products", [])  # 列表
        client_id = request.data.get("client_id", "")  # 客户ID
        timestamp = int(time.time())
        plan_arrival_time = request.data.get(
            "plan_arrival_time", timestamp)  # 交货日期
        remark = request.data.get("remark", "")  # 备注

        user_id = request.redis_cache["user_id"]
        factory_id = request.redis_cache["factory_id"]
        seq_id = request.redis_cache["seq_id"]

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        order_id = generate_module_uuid(PrimaryKeyType.order.value, factory_id, seq_id)
        try:
            order_sql = "insert into base_orders (id, factory, client_id, plan_arrival_time, create_time, " \
                        "creator, state, order_type) values ('{0}', '{1}', '{2}', {3}, {4}, '{5}', " \
                        "'1', '1')".format(order_id, factory_id, client_id, plan_arrival_time, timestamp, user_id)

            order_stats_sql = "insert into base_orders_stats (order_id, state, remark, operator, optime) values " \
                              "('{}', '{}', '{}', '{}', {})".format(order_id, '1', remark, user_id, timestamp)

            cursor.execute(order_sql)
            cursor.execute(order_stats_sql)
            for product in products_list:
                product_id = product["id"]
                product_count = product["count"]
                price_sql = '''
                select
                    coalesce( unit_price,
                    0 )
                from
                    base_client_products
                where
                    factory_id = '{}'
                    and client_id = '{}'
                    and product_id = '{}'; '''.format(factory_id, client_id, product_id)
                cursor.execute(price_sql)
                price_val = cursor.fetchone()[0] or ''
                order_products_sql = "insert into base_order_products (order_id, product_id, product_count, unit_price) " \
                                     " values ('{0}', '{1}', {2}, {3});".format(order_id, product_id, product_count,
                                                                                price_val)
                cursor.execute(order_products_sql)

            order_track_sql = "insert into base_order_track (order_id, type, val) VALUES ('{}','1',{});".format(
                order_id, int(OrderTrackType.create.value))
            cursor.execute(order_track_sql)

            connection.commit()

            message = {'resource': 'PyOrderState', 'type': 'PUT',
                       'params': {'fac': factory_id, 'id': order_id, 'state': '6'}}
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


# class OrderDelete(APIView):
#     """删除订单 order/del"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         order_id = request.data.get("id")
#         if not order_id:
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少订单id参数。"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#         try:
#             cursor.execute(
#                 "select count(1) from orders where id = '%s';" % order_id)
#             order_check = cursor.fetchone()[0]
#             if order_check == 0:
#                 return Response({"res": 1, "errmsg": "order_id doesn't exist. 该订单id不存在。"}, status=status.HTTP_200_OK)
#             cursor.execute("delete from orders where id = '%s';" % order_id)
#             cursor.execute(
#                 "delete from finance_logs where use_id = '%s';" % order_id)
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class OrderModify(APIView):
#     """修改订单 order/modify"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         timestamp = int(time.time())
#         order_id = request.data.get("id")  # 订单ID
#         products_list = request.data.get("products", [])  # 列表
#         client_id = request.data.get("client_id")  # 客户ID
#         deliver_time = request.data.get("deliver_time", 0)  # 交货日期
#         remark = request.data.get("remark", "")  # 备注
#         contract = request.data.get("contract")  # 合同照片
#         if not order_id:
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少订单id参数。"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#         alioss = AliOss()
#
#         cursor.execute(
#             "select count(1) from orders where id = '%s';" % order_id)
#         order_check = cursor.fetchone()[0]
#         if order_check == 0:
#             return Response({"res": 1, "errmsg": "order_id doesn't exist. 该订单id不存在。"}, status=status.HTTP_200_OK)
#
#         cursor.execute(
#             "select name from factory_clients where id = '%s';" % client_id)
#         client_name = cursor.fetchone()[0] or ""
#
#         try:
#             if contract:
#                 image_id, image_url = alioss.upload_image(contract)
#                 cursor.execute(
#                     "update orders set client_id = '%s', deliver_time = %d, remark = '%s', time = %d, contract = '%s' "
#                     "where id = '%s';" % (client_id, deliver_time, remark, timestamp, image_id, order_id))
#             else:
#                 cursor.execute(
#                     "update orders set client_id = '%s', deliver_time = %d, remark = '%s', time = %d where id = '%s';"
#                     % (client_id, deliver_time, remark, timestamp, order_id))
#             cursor.execute(
#                 "delete from order_products where order_id = '%s';" % order_id)
#
#             finace_count = 0
#             for product in products_list:
#                 finace_count += product["sell_price"]
#                 product_id = product["product_id"]
#                 product_count = product["product_count"]
#                 sell_price = product["sell_price"]
#                 order_products_sql = "insert into order_products (order_id, product_id, product_count, sell_price," \
#                                      " time) values ('%s', '%s', %d, %s, %d);" % (
#                                          order_id, product_id, product_count, str(sell_price), timestamp)
#                 cursor.execute(order_products_sql)
#             if finace_count >= 0:
#                 cursor.execute("update finance_logs set type = '%s', count = %d, time = %d where use_id = '%s';" % (
#                     client_name, finace_count, timestamp, order_id))
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class OrderTrack(APIView):
#     """订单追踪 order/track"""
#     permission_classes = [OrderPermission]
#
#     def get(self, request):
#         order_id = request.query_params.get("id")  # 订单id
#         # 订单追踪类型，all: 订单所有状态追踪, money: 订单收款情况追踪, products: 订单产品状态追踪
#         order_type = request.query_params.get("type")
#
#         condition = ""
#         if order_type == "all":
#             pass
#         elif order_type == "money":
#             condition += " and type = '%s' " % OrderTrackType.money.value
#         elif order_type == "products":
#             condition += " and type = '%s' " % OrderTrackType.products.value
#
#         track_sql = "select id, type, val, time from order_track where order_id = '%s'" % order_id + condition + \
#                     " order by time desc;"
#         products_sql = "select sum(product_count) as val from order_products where order_id = '%s' group by " \
#                        "order_id;" % order_id
#         money_sql = "select sum(sell_price) as val from order_products where order_id = '%s' group by " \
#                     "order_id;" % order_id
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             cursor.execute(track_sql)
#             track_result = cursor.fetchall()
#             cursor.execute(products_sql)
#             counts_result = cursor.fetchone()
#             counts_result = counts_result[0] if counts_result else 0
#             cursor.execute(money_sql)
#             money_result = cursor.fetchone()
#             money_result = money_result[0] if money_result else 0
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)
#
#         data, track_list = {}, []
#         for track in track_result:
#             di = dict()
#             di["id"] = track[0]
#             di["type"] = track[1]
#             di["val"] = track[2]
#             di["time"] = track[3]
#             track_list.append(di)
#
#         data["list"] = track_list
#         if order_type == "all":
#             pass
#         elif order_type == "money":
#             data["val"] = money_result
#         elif order_type == "products":
#             data["val"] = counts_result
#
#         return Response(data, status=status.HTTP_200_OK)


# class OrderDeliver(APIView):
#     """订单发货、未发货 order/deliver"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         order_id = request.data.get("id")  # 订单id
#         state = request.data.get("state", "0")  # 发货: 0, 未发货: 1
#         deliver_time = request.data.get(
#             "deliver_time", int(time.time()))  # 发货时间戳
#         remark = request.data.get("remark", "")  # 备注
#         if not order_id:
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少订单id参数。"}, status=status.HTTP_200_OK)
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         timestamp = int(time.time())
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select count(1) from orders where id = '%s';" % order_id)
#         order_check = cursor.fetchone()[0]
#         # print(order_check)
#         if order_check == 0:
#             return Response({"res": 1, "errmsg": "order_id doesn't exist. 该订单id不存在。"}, status=status.HTTP_200_OK)
#
#         cursor.execute("select state from orders where id = '%s';" % order_id)
#         order_state = cursor.fetchone()[0]
#
#         try:
#             if order_state == state:
#                 cursor.execute("update order_track set time = %d where order_id = '%s' and type = '%s';" % (
#                     deliver_time, order_id, OrderTrackType.deliver.value))
#             else:
#                 if order_state == "0":
#                     cursor.execute("delete from order_track where order_id = '%s' and type = '%s';" % (
#                         order_id, OrderTrackType.deliver.value))
#                     cursor.execute(
#                         "delete from products_log where use_id = '%s';" % order_id)
#                 else:
#                     message = {'resource': 'PyOrderDeliver', 'type': 'POST',
#                                'params': {'Fac': factory_id, 'OrderId': order_id}}
#                     # print("message=", message)
#                     rabbitmq = UtilsRabbitmq()
#                     rabbitmq.send_message(json.dumps(message))
#
#                     cursor.execute(
#                         "insert into order_track (order_id, type, val, time) values ('%s', '%s', '', %d);" % (
#                             order_id, OrderTrackType.deliver.value, timestamp))
#                     cursor.execute(
#                         "select product_id, product_count from order_products where order_id = '%s';" % order_id)
#                     products_result = cursor.fetchall()
#                     for product in products_result:
#                         product_id = product[0]
#                         product_count = product[1]
#                         cursor.execute("insert into products_log (id, factory, use_id, parent_type, product_id, count, "
#                                        "time) values ('%s', '%s', '%s', 'order', '%s', %d, %d);" % (
#                                            generate_uuid(), factory_id, order_id, product_id, product_count,
#                                            int(deliver_time)))
#
#             cursor.execute("update orders set state = '%s', deliver_time = %d, remark = '%s' where id = '%s';" % (
#                 state, deliver_time, remark, order_id))
#
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class OrderIncome(APIView):
#     """订单金额 order/income/{id}"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request, order_id):
#         """添加订单收款金额"""
#         if not order_id:  # 订单id
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少订单id参数。"}, status=status.HTTP_200_OK)
#         value = int(request.data.get("val"))  # 收款金额
#         timestamp = request.data.get("time")  # 时间戳
#         # print(order_id), print(value, type(value)), print(timestamp, type(timestamp))
#         if value < 0:
#             return Response({"res": 1, "errmsg": "order_id value less than 0! 订单id收款金额小于0！"}, status=status.HTTP_200_OK)
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select count(1) from orders where id = '%s';" % order_id)
#         id_check = cursor.fetchone()[0]
#         if id_check <= 0:
#             return Response({"res": 1, "errmsg": "order_id doesn't exist! 订单id不存在！"}, status=status.HTTP_200_OK)
#
#         try:
#             cursor.execute(
#                 "update orders set collected = collected + %d where id = '%s';" % (value, order_id))
#             cursor.execute("insert into order_track (order_id, type, val, time) values ('%s', %s, '%s', %d);" % (
#                 order_id, OrderTrackType.money.value, value, timestamp))
#             connection.commit()
#
#             message = {'resource': 'PyOrderIncome', 'type': 'POST',
#                        'params': {'Fac': factory_id, 'OrderId': order_id, "Val": value}}
#             # print("message=", message)
#             rabbitmq = UtilsRabbitmq()
#             rabbitmq.send_message(json.dumps(message))
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)
#
#     def put(self, request, track_id):
#         """修改订单收款金额"""
#         if not track_id or not track_id.isdigit():  # 收款记录id
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少收款记录id参数。"}, status=status.HTTP_200_OK)
#         value = int(request.data.get("val"))  # 收款金额
#         timestamp = request.data.get("time")  # 时间戳
#         # print(track_id, type(track_id)), print(value, type(value)), print(timestamp, type(timestamp))
#         if value < 0:
#             return Response({"res": 1, "errmsg": "order_id value less than 0! 订单id收款金额小于0！"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select count(1) from order_track where id = '%s';" % track_id)
#         id_check = cursor.fetchone()[0]
#         if id_check <= 0:
#             return Response({"res": 1, "errmsg": "order track_id doesn't exist! 订单收款记录id不存在！"},
#                             status=status.HTTP_200_OK)
#
#         track_id = int(track_id)
#         try:
#             cursor.execute(
#                 "select order_id, val from order_track where id = %d;" % track_id)
#             result = cursor.fetchone()
#             order_id, val = result[0], result[1] if result[1] else 0
#             # print(order_id, type(order_id)), print(val, type(val))
#             update_val = int(value) - int(val)
#             cursor.execute(
#                 "update orders set collected = collected + %d where id = '%s';" % (update_val, order_id))
#             cursor.execute(
#                 "update order_track set val = '%s', time = %d where id = %d;" % (value, timestamp, track_id))
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)
#
#     def delete(self, request, track_id):
#         """删除修改记录"""
#         if not track_id or not track_id.isdigit():  # 收款记录id
#             return Response({"res": 1, "errmsg": "lack of order_id. 缺少收款记录id参数。"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select count(1) from order_track where id = '%s';" % track_id)
#         id_check = cursor.fetchone()[0]
#         if id_check <= 0:
#             return Response({"res": 1, "errmsg": "order track_id doesn't exist! 订单收款记录id不存在！"},
#                             status=status.HTTP_200_OK)
#
#         track_id = int(track_id)
#         try:
#             cursor.execute(
#                 "select order_id, val from order_track where id = %d;" % track_id)
#             result = cursor.fetchone()
#             order_id, val = result[0], result[1] if result[1] else 0
#             # print(order_id, type(order_id)), print(val, type(val))
#             update_val = - int(val)
#             cursor.execute(
#                 "update orders set collected = collected + %d where id = '%s';" % (update_val, order_id))
#             cursor.execute("delete from order_track where id = %d;" % track_id)
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsList(APIView):
#     """客户列表 clients/list"""
#     permission_classes = [OrderPermission]
#
#     def get(self, request):
#         group = request.query_params.get("group")  # 分组id,请求所有客户时不带此参数
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         data = []
#
#         try:
#             if group:
#                 cursor.execute("select id, name, contacts, phone, position, region, address from factory_clients "
#                                "where factory = '%s' and group_id = '%s' order by name asc;" % (factory_id, group))
#             else:
#                 cursor.execute("select id, name, contacts, phone, position, region, address from factory_clients "
#                                "where factory = '%s' order by name asc;" % factory_id)
#
#             result = cursor.fetchall()
#             for res in result:
#                 di = dict()
#                 di["id"] = res[0] or ""
#                 di["name"] = res[1] or ""
#                 di["contacts"] = res[2] or ""
#                 di["phone"] = res[3] or ""
#                 di["position"] = res[4] or ""
#                 di["region"] = res[5] or ""
#                 di["address"] = res[6] or ""
#                 data.append(di)
#
#             return Response({"list": data}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsNew(APIView):
#     """新建客户 clients/new"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         name = request.data.get("name")  # 客户名称
#         contacts = request.data.get("contact")  # 联系人
#         client_phone = request.data.get("phone")  # 手机号
#         wechat = request.data.get("wechat", "")  # 微信号
#         salesman_id = request.data.get("salesman_id", "")  # 跟进业务员ID
#         group_id = request.data.get("group_id", "")  # 分组id
#         position = request.data.get("position", "")  # 职位
#         remark = request.data.get("remark", "")  # 备注
#         region = request.data.get("region", "")  # 客户地址
#         address = request.data.get("address", "")  # 详细地址
#
#         if not all([name, contacts, client_phone]):
#             return Response({"res": 1, "errmsg": "Lack of params name or contacts or phone! 缺少参数客户名称或联系人或客户手机号！"},
#                             status=status.HTTP_200_OK)
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             uuid = generate_uuid()
#             cursor.execute("insert into factory_clients values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, "
#                            "'%s', '%s', '%s', '%s', '%s');" % (
#                                uuid, factory_id, name, contacts, client_phone, wechat, position, remark,
#                                int(time.time()),
#                                group_id, salesman_id, phone, region, address))
#             connection.commit()
#
#             message = {'resource': 'PyClientsNew', 'type': 'POST',
#                        'params': {'Fac': factory_id, 'Name': name, 'Contacts': contacts, 'ClientID': uuid,
#                                   'Creator': phone}}
#             # print("message=", message)
#             rabbitmq = UtilsRabbitmq()
#             rabbitmq.send_message(json.dumps(message))
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsDetail(APIView):
#     """客户详情 clients/detail"""
#     permission_classes = [OrderPermission]
#
#     def get(self, request):
#         client_id = request.query_params.get("id")  # 客户id
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         sql = """
#         select
#           t1.*,
#           t2.name as group_name,
#           t3.name as salesman_name,
#           t4.name as creator
#         from
#           (
#           select
#             *
#           from
#             factory_clients
#           where
#             id = '%s'
#           ) t1
#         left join groups t2 on
#           t1.group_id = t2.id
#         left join salesman t3 on
#           t1.salesman_id = t3.id
#         left join user_info t4 on
#           t1.creator_id = t4.phone;
#         """ % client_id
#
#         try:
#             data = {}
#             cursor.execute(sql)
#             result = cursor.fetchone()
#
#             data["name"] = result[2] or ""
#             data["contacts"] = result[3] or ""
#             data["phone"] = result[4] or ""
#             data["wechat"] = result[5] or ""
#             data["position"] = result[6] or ""
#             data["remark"] = result[7] or ""
#             data["group_id"] = result[9] or ""
#             data["salesman_id"] = result[10] or ""
#             creator_id = result[11]
#             data["region"] = result[12] or ""
#             data["address"] = result[13] or ""
#             data["group_name"] = result[14] or ""
#             data["salesman_name"] = result[15] or ""
#
#             if "1" in permission:
#                 data["flag"] = "0"
#             elif creator_id == phone:
#                 data["flag"] = "0"
#             else:
#                 data["flag"] = "1"
#             return Response(data, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsDelete(APIView):
#     """删除客户 clients/del"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         client_id = request.data.get("id")  # 客户id
#         if not client_id:
#             return Response({"res": 1, "errmsg": "Lack of params client id! 缺少参数客户id！"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             cursor.execute(
#                 "delete from factory_clients where id = '%s';" % client_id)
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsModify(APIView):
#     """修改客户信息 clients/modify"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         client_id = request.data.get("id")  # 客户id
#         name = request.data.get("name")  # 客户名称
#         contacts = request.data.get("contact")  # 联系人
#         client_phone = request.data.get("phone")  # 手机号
#         wechat = request.data.get("wechat", "")  # 微信号
#         salesman_id = request.data.get("salesman_id", "")  # 跟进业务员ID
#         group_id = request.data.get("group_id", "")  # 分组id
#         position = request.data.get("position", "")  # 职位
#         remark = request.data.get("remark", "")  # 备注
#         region = request.data.get("region", "")  # 客户地址
#         address = request.data.get("address", "")  # 详细地址
#         if not all([client_id, name, contacts, client_phone]):
#             return Response({"res": 1, "errmsg": "Lack of params! 缺少参数！"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select count(1) from factory_clients where id = '%s';" % client_id)
#         id_check = cursor.fetchone()[0]
#         if id_check <= 0:
#             return Response({"res": 1, "errmsg": "This id doesn't exist! 此id不存在！"}, status=status.HTTP_200_OK)
#
#         try:
#             cursor.execute("update factory_clients set name = '%s', contacts = '%s', phone = '%s', wechat = '%s',"
#                            " position = '%s', remark = '%s', time = %d, group_id = '%s', salesman_id = '%s', "
#                            "region = '%s', address = '%s' where id = '%s';" % (name, contacts, client_phone, wechat,
#                                                                                position, remark, int(
#                 time.time()),
#                                                                                group_id, salesman_id, region, address,
#                                                                                client_id))
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsGroupList(APIView):
#     """客户分组列表 clients/group/list"""
#     permission_classes = [OrderPermission]
#
#     def get(self, request):
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select id, name from groups where factory = '%s' order by name asc;" % factory_id)
#         result = cursor.fetchall()
#         data = []
#         for res in result:
#             di = dict()
#             di["id"] = res[0] or ""
#             di["name"] = res[1] or ""
#             data.append(di)
#
#         return Response({"list": data}, status=status.HTTP_200_OK)


# class ClientsGroupNew(APIView):
#     """新建客户分组 clients/group/new"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         name = request.data.get("name")  # 客户分组名称
#         if not name:
#             return Response({"res": 1, "errmsg": "Lack of params client group name! 缺少参数客户分组名称！"},
#                             status=status.HTTP_200_OK)
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute("select count(1) from groups where factory = '%s' and name = '%s';" % (
#             factory_id, name))
#         name_check = cursor.fetchone()[0]
#         if name_check >= 1:
#             return Response({"res": 1, "errmsg": "This name is already exist in current factory! 此名称已存在于当前工厂！"},
#                             status=status.HTTP_200_OK)
#
#         try:
#             cursor.execute("insert into groups values ('%s', '%s', '%s', %d);" % (
#                 generate_uuid(), factory_id, name, int(time.time())))
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsGroupDelete(APIView):
#     """删除客户分组 clients/group/del"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         client_group_id = request.data.get("id")  # 客户分组id
#         if not client_group_id:
#             return Response({"res": 1, "errmsg": "Lack of params client group id! 缺少参数客户分组id！"},
#                             status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             cursor.execute("delete from groups where id = '%s';" %
#                            client_group_id)
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsSalesmanList(APIView):
#     """业务员列表 clients/salesman/list"""
#     permission_classes = [OrderPermission]
#
#     def get(self, request):
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         cursor.execute(
#             "select id, name from salesman where factory = '%s' order by name asc;" % factory_id)
#         result = cursor.fetchall()
#         data = []
#         for res in result:
#             di = dict()
#             di["id"] = res[0] or ""
#             di["name"] = res[1] or ""
#             data.append(di)
#
#         return Response({"list": data}, status=status.HTTP_200_OK)


# class ClientsSalesmanNew(APIView):
#     """新建跟进业务员 clients/salesman/new"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         name = request.data.get("name")  # 业务员姓名
#         if not name:
#             return Response({"res": 1, "errmsg": "Lack of params name! 缺少参数业务员姓名！"}, status=status.HTTP_200_OK)
#
#         phone = request.redis_cache["phone"]
#         factory_id = request.redis_cache["factory_id"]
#         permission = request.redis_cache["permission"]
#         # print(phone, factory_id, permission)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             cursor.execute("insert into salesman values ('%s', '%s', '%s', %d);" % (
#                 generate_uuid(), factory_id, name, int(time.time())))
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


# class ClientsSalesmanDelete(APIView):
#     """删除业务员 clients/salesman/del"""
#     permission_classes = [OrderPermission]
#
#     def post(self, request):
#         salesman_id = request.data.get("id")  # 业务员id
#         if not salesman_id:
#             return Response({"res": 1, "errmsg": "Lack of params salesman id! 缺少参数业务员id！"}, status=status.HTTP_200_OK)
#
#         pgsql = UtilsPostgresql()
#         connection, cursor = pgsql.connect_postgresql()
#
#         try:
#             cursor.execute(
#                 "delete from salesman where id = '%s';" % salesman_id)
#             connection.commit()
#
#             return Response({"res": 0}, status=status.HTTP_200_OK)
#         except Exception as e:
#             logger.error(e)
#             return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
#         finally:
#             pgsql.disconnect_postgresql(connection)


class Products(APIView):
    permission_classes = [OrderPermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        client_id = request.query_params.get('client_id', '')
        sql = '''
            select
                    t2.id,
                    t2.name,
                    t2.unit,
                    t1.unit_price,
                    t3.lowest_package,
                    t3.lowest_count
                from
                    (
                    select
                        product_id,
                        unit_price
                    from
                        base_client_products
                    where
                        factory_id = '{0}'
                        and client_id = '{1}' ) t1
                left join base_materials_pool t2 on
                    t1.product_id = t2.id
                left join (
                    select
                        id,
                        lowest_package,
                        lowest_count
                    from
                        base_products
                    where
                        factory = '{0}' ) t3 on
                    t1.product_id = t3.id'''.format(factory_id, client_id)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(sql)
        result = cursor.fetchall()
        data = []
        for res in result:
            temp = dict()
            temp['id'] = res[0]
            temp['name'] = res[1]
            temp['unit'] = res[2]
            temp['unit_price'] = res[3]
            temp['lowest_package'] = res[4]
            temp['lowest_count'] = res[5]
            data.append(temp)
        return Response({"list": data}, status=status.HTTP_200_OK)
