# -*- coding: utf-8 -*-
import logging
import time
import traceback
import json

from django.utils.decorators import method_decorator
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, UtilsRabbitmq, generate_module_uuid, AliOss  # pylint: disable=redefined-builtin
from constants import PrimaryKeyType
from permissions import PurchasePermission, PurchaseApprovalPermission, purchase_decorator, purchase_approval_decorator
from store.store_stock_utils import material_on_road
from order.order_utils import create_order

logger = logging.getLogger('django')


class PurchaseMain(APIView):
    permission_classes = [PurchasePermission]

    def get(self, request):
        timestamp = int(time.time())
        factory_id = request.redis_cache["factory_id"]
        stime = request.query_params.get("start", 1)
        etime = request.query_params.get("end", timestamp)

        header_sql = '''
        select
            state,
            count(1)
        from
            base_purchases
        where
            factory = '{0}'
        group by
            state;'''.format(factory_id)

        deliver_sql = '''
            select
                state,
                count(1)
            from
                (
                select
                    case
                        when ( to_timestamp( t1.plan_arrival_time ) < current_timestamp )
                        or t1.actual_arrival_time > t1.plan_arrival_time then '2'
                        when t1.actual_arrival_time <= t1.plan_arrival_time then '1'
                        else '3'
                    end as state
                from
                    base_orders t1 left join base_purchases t2 on t1.purchase_id = t2.id
                where
                    t2.factory = '{0}'
                    and t1.state != '5'
                    and t1.state != '6'
                    and t2.create_time >= {1}
                    and t2.create_time < {2} ) t
            group by
                t.state ;'''.format(factory_id, stime, etime)

        purchase_amount_sql = '''
            select
                count(1),
                state,
                sum(count) as orders,
                sum(price)
            from
                (
                select
                    supplier_id,
                    sum( price ) as price,
                    count( 1 ),
                    case
                        when count( 1 ) > 1 then '2'
                        else '1'
                    end as state
                from
                    (
                    select
                        t1.supplier_id,
                        t2.price
                    from
                        base_purchases t1
                    left join (
                        select
                            purchase_id,
                            sum( price ) as price
                        from
                            (
                            select
                                purchase_id,
                                unit_price * product_count as price
                            from
                                base_purchase_materials ) t
                        group by
                            purchase_id ) t2 on
                        t1.id = t2.purchase_id
                    where
                        t1.factory = '{0}'
                        and t1.state != '6'
                        and t1.create_time >= {1}
                        and t1.create_time < {2} ) t
                group by
                    supplier_id ) t
            group by
                state;'''.format(factory_id, stime, etime)

        purchase_progress_sql = '''
                select
                    state,
                    count(1)
                from
                    base_purchases
                where
                    factory = '{}'
                    and create_time >= {}
                    and create_time < {}
                group by
                    state;'''.format(factory_id, stime, etime)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(header_sql)
            res_header = cursor.fetchall()

            header = {
                "approval": 0,
                "confirm": 0,
                "deliver": 0,
                "transit": 0
            }

            for x in res_header:
                if x[0] == '1':
                    header['approval'] = x[1]
                elif x[0] == '2':
                    header['confirm'] = x[1]
                elif x[0] == '3':
                    header['deliver'] = x[1]
                elif x[0] == '4':
                    header['transit'] = x[1]

            progress = {
                "approval": 0,
                "deliver": 0,
                "transit": 0
            }
            cursor.execute(purchase_progress_sql)
            res_progress = cursor.fetchall()

            for x in res_progress:
                if x[0] == '1':
                    progress['approval'] = x[1]
                elif x[0] == '3':
                    progress['deliver'] = x[1]
                elif x[0] == '4':
                    progress['transit'] = x[1]

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

            cursor.execute(purchase_amount_sql)
            res_purchase_amount = cursor.fetchall()
            total_amount, total_purchases = 0, 0
            purchase_amount = {
                "purchases": 0,
                "new_c": 0,
                "regular_c": 0,
                "total_amount": 0
            }
            for x in res_purchase_amount:
                total_amount += x[3] or 0
                total_purchases += x[2] or 0
                if x[1] == '1':
                    purchase_amount['new_c'] = x[0]
                else:
                    purchase_amount['regular_c'] = x[0]
            purchase_amount['purchases'] = total_purchases
            purchase_amount['total_amount'] = total_amount

            return Response({"list": {'header': header,
                                      'deliver_stats': deliver_stats,
                                      'purchase_amount': purchase_amount,
                                      'purchase_progress': progress}},
                            status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PurchaseList(APIView):

    def customer_state_dict(self, orders):
        dt = {}
        for x in orders:
            if dt.get(x[6]):
                dt[x[6]] = '2'
            else:
                dt[x[6]] = '1'
        return dt

    def customer_name_dict(self, orders):
        dt = {}
        for x in orders:
            if dt.get(x[6]):
                continue
            else:
                dt[x[6]] = x[1]
        return dt

    def get(self, request, list_type):
        """订单列表，包含了各个订单列表
        Args:
            list_type:
                1: 采购单状态列表
                2: 采购到货率
                3: 采购金额,新老供应商
                4: 供应商列表

        """

        factory_id = request.redis_cache["factory_id"]
        state_sql = """
        select
            t1.id,
            t3.name,
            coalesce(t2.product_count, '{}') as product_count,
            coalesce(t2.product_name, '{}') as prodcut_name,
            coalesce(t2.unit, '{}') as unit,
            t1.create_time,
            t1.state
        from
            base_purchases t1
        left join (
            select
                purchase_id,
                array_agg(product_id) as product_id,
                array_agg(product_count) as product_count,
                array_agg(name) as product_name,
                array_agg(unit) as unit
            from
                (
                select
                    t1.product_id,
                    t1.product_count,
                    t1.purchase_id,
                    t2.name,
                    t2.unit
                from
                    base_purchase_materials t1
                left join base_materials_pool t2 on
                    t1.product_id = t2.id
                where
                    t2.id notnull ) t
            group by
                purchase_id ) t2 on
            t1.id = t2.purchase_id
        left join (select * from base_suppliers where factory = '{}') t3 on
            t1.supplier_id = t3.id
        where
            t1.factory = '{}'
        order by
            t1.state,
            t1.create_time desc;
        """.format('{}', '{}', '{}', factory_id, factory_id)

        state_123_sql = """
                select
                    t1.id,
                    t3.name,
                    coalesce(t2.product_count, '{}') as product_count,
                    coalesce(t2.product_name, '{}') as prodcut_name,
                    coalesce(t2.unit, '{}') as unit,
                    t1.create_time,
                    t1.state
                from
                    base_purchases t1
                left join (
                    select
                        purchase_id,
                        array_agg(product_id) as product_id,
                        array_agg(product_count) as product_count,
                        array_agg(name) as product_name,
                        array_agg(unit) as unit
                    from
                        (
                        select
                            t1.product_id,
                            t1.product_count,
                            t1.purchase_id,
                            t2.name,
                            t2.unit
                        from
                            base_purchase_materials t1
                        left join base_materials_pool t2 on
                            t1.product_id = t2.id
                        where
                            t2.id notnull ) t
                    group by
                        purchase_id ) t2 on
                    t1.id = t2.purchase_id
                left join (select * from base_suppliers where factory = '{}') t3 on
                    t1.supplier_id = t3.id
                where
                    t1.factory = '{}' and (t1.state = '1' or t1.state = '2' or t1.state = '3')
                order by
                    t1.state,
                    t1.create_time desc;
                """.format('{}', '{}', '{}', factory_id, factory_id)

        deliver_sql = '''
            select
                t2.id,
                coalesce( t4.name,
                '' ) as name,
                coalesce(t3.product_count, '{}') as product_count,
                coalesce(t3.product_name, '{}') as prodcut_name,
                coalesce(t3.unit, '{}') as unit,
                t2.create_time,
                case
                    when to_timestamp(t1.plan_arrival_time) < current_timestamp
                    or t1.actual_arrival_time > t1.plan_arrival_time then '2'
                    else '1'
                end as state
            from
                base_orders t1 left join base_purchases t2 on t2.id = t1.purchase_id
            left join (
                select
                    purchase_id,
                    array_agg( product_id ) as product_id,
                    array_agg( product_count ) as product_count,
                    array_agg( name ) as product_name,
                    array_agg( unit ) as unit
                from
                    (
                    select
                        t1.product_id,
                        t1.product_count,
                        t1.purchase_id,
                        t2.name,
                        t2.unit
                    from
                        base_purchase_materials t1
                    left join base_materials_pool  t2 on
                        t1.product_id = t2.id
                    where
                        t2.id notnull ) t
                group by
                    purchase_id ) t3 on
                t1.id = t3.purchase_id
            left join base_clients t4 on
                t2.supplier_id = t4.id
            where
                t2.factory = '{}'
                and t2.state != '6'
                and ( t1.actual_arrival_time != 0
                or to_timestamp(t1.plan_arrival_time) < current_timestamp )
            order by
                create_time desc;'''.format('{}', '{}', '{}', factory_id)

        supplier_sql = '''
            select
                t1.id,
                t3.name,
                coalesce(t2.product_count, '{}') as product_count,
                coalesce(t2.product_name, '{}') as prodcut_name,
                coalesce(t2.unit, '{}') as unit,
                t1.create_time,
                t3.id as supplier_id
            from
                base_purchases t1
            left join (
                select
                    purchase_id,
                    array_agg(product_id) as product_id,
                    array_agg(product_count) as product_count,
                    array_agg(name) as product_name,
                    array_agg(unit) as unit
                from
                    (
                    select
                        t1.product_id,
                        t1.product_count,
                        t1.purchase_id,
                        t2.name,
                        t2.unit
                    from
                        base_purchase_materials t1
                    left join base_materials_pool  t2 on
                        t1.product_id = t2.id
                    where
                        t2.id notnull ) t
                group by
                    purchase_id ) t2 on
                t1.id = t2.purchase_id
            left join (select * from base_suppliers where factory = '{}') t3 on
                t1.supplier_id = t3.id
            where
                t1.factory = '{}'
            order by
                t1.create_time desc;'''.format('{}', '{}', '{}', factory_id, factory_id)

        # print(sql)
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            sql_dict = {
                "1": state_sql,
                "2": deliver_sql,
                "3": supplier_sql,
                "4": supplier_sql,
                "5": state_123_sql
            }
            cursor.execute(sql_dict[list_type])
            result = cursor.fetchall()
            customer_states = {}
            if list_type == '3':
                customer_states = self.customer_state_dict(result)
            elif list_type == '4':
                customer_states = self.customer_name_dict(result)
            data = {}
            for res in result:
                temp = {}
                if list_type == '3' or list_type == '4':
                    state = customer_states.get(res[6])
                else:
                    state = res[6]
                if not data.get(state):
                    data[state] = []
                temp['id'] = res[0]
                temp['name'] = res[1]
                products = ''
                for i, val in enumerate(res[2]):
                    products += res[3][i] + ':' + str(round(val, 2)) + res[4][i] + ';'
                temp['products'] = products
                temp['time'] = res[5]
                data[state].append(temp)
            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PurchaseDetail(APIView):
    """采购单详情"""

    @method_decorator(purchase_decorator)
    def get(self, request, purchase_id):
        if request.flag is False:
            return Response({"res": 1, "errmsg": "你没有权限！"}, status=status.HTTP_403_FORBIDDEN)

        factory_id = request.redis_cache["factory_id"]
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        cursor.execute(
            "select count(1) from base_purchases where id = '{}';".format(purchase_id))
        order_check = cursor.fetchone()[0]
        if order_check == 0:
            return Response({"res": 1, "errmsg": "采购单id查询不存在。"}, status=status.HTTP_200_OK)
        phone = request.redis_cache["phone"]
        permission = request.redis_cache["permission"]
        purchase_sql = """        
            select
                t1.supplier_id,
                t1.state,
                t1.creator as creator_id,
                t4.name as creator,
                t1.remark,
                t3.name as name,
                t3.contacts,
                t3.phone,
                t3.position,
                ( t3.region || t3.address ) as address,
                t1.create_time,
                t5.name as approver,
                t5.phone as approver_phone,
                t1.approval_time,
                t1.plan_arrival_time,
                t6.name as canceler,
                t6.phone as cancceler_phone,
                t1.cancel_time,
                t1.cancel_remark,
                t1.arrival_time,
                t5.image,
                t6.image
            from
                base_purchases t1
            left join (select * from base_suppliers where factory = '{}') t3 on
                t1.supplier_id = t3.id
            left join user_info t4 on
                t1.creator = t4.user_id
            left join user_info t5 on
                t1.approver = t5.user_id
            left join user_info t6 on
                t1.canceler = t6.user_id
            where
                t1.id = '{}';
        """.format(factory_id, purchase_id)

        products_sql = """
        select
            t1.*,
            coalesce(t2.name, '') as product_name,
            coalesce(t2.unit, '') as unit,
            coalesce( t3.name, '' ) as category_name
        from
            (
            select
                product_id,
                product_count,
                unit_price
            from
                base_purchase_materials
            where
                purchase_id = '{0}' ) t1
        left join base_materials_pool t2 on
            t1.product_id = t2.id
        left join base_material_category_pool t3 on
            t2.category_id = t3.id
        """.format(purchase_id)

        try:
            cursor.execute(purchase_sql)
            purchase_result = cursor.fetchall()
            cursor.execute(products_sql)
            products_result = cursor.fetchall()

            # print("order_result=", order_result), print("products_result=", products_result)

            data, products = {}, []
            for res in purchase_result:
                data["state"] = res[1] or ""
                data["creator_id"] = res[2] or ""
                data["creator"] = res[3] or ""
                data['remark'] = res[4] or ''
                data['arrival_time'] = res[19] or 0
                data['supplier'] = {}
                data["supplier"]['id'] = res[0] or ""
                data['supplier']['name'] = res[5] or ''
                data['supplier']['contact'] = res[6] or ''
                data['supplier']['phone'] = res[7] or ''
                data['supplier']['position'] = res[8] or ''
                data['supplier']['address'] = res[9] or ''
                data['plan_arrival_time'] = res[14] or 0
                alioss = AliOss()
                if (res[1] != '1' and res[1] != '6') and res[1]:
                    data['approve'] = {}
                    data['approve']['name'] = res[11]
                    data['approve']['phone'] = res[12]
                    data['approve']['time'] = res[13]
                    image = res[20]
                    if isinstance(image, memoryview):
                        image = image.tobytes().decode()
                    data['approve']['image'] = alioss.joint_image(image)

                if res[1] == '6':
                    data['cancel'] = {}
                    data['cancel']['name'] = res[15]
                    data['cancel']['phone'] = res[16]
                    data['cancel']['time'] = res[17]
                    data['cancel']['remark'] = res[18]
                    image = res[21]
                    if isinstance(image, memoryview):
                        image = image.tobytes().decode()
                    data['cancel']['image'] = alioss.joint_image(image)

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
                di["name"] = res[3] or ""
                di["unit"] = res[4] or ""
                di["category_name"] = res[5] or ""
                products.append(di)

            data["materials"] = products
            return Response(data, status=status.HTTP_200_OK)

        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    @method_decorator(purchase_approval_decorator)
    def put(self, request, purchase_id):
        """修改采购状态
        1: 通过审批， 2：取消采购

        审批采购单 -> 判断供应商是否在线，-> 在线 -> 给供应商创建订单
                                      -> 不在线 -> 不处理

        取消采购单 -> 对应采购单取消
        """
        if request.flag is False:
            return Response({"res": 1, "errmsg": "你没有权限！"}, status=status.HTTP_403_FORBIDDEN)

        factory_id = request.redis_cache["factory_id"]
        seq_id = request.redis_cache["seq_id"]
        user_id = request.redis_cache["user_id"]
        if not user_id:
            user_id = request.redis_cache["phone"]
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        timestamp = int(time.time())
        state = request.data.get('state')
        supplier_id = request.data.get('supplier_id', '')
        materials = request.data.get('materials', [])
        plan_arrival_time = request.data.get('plan_arrival_time')
        remark = request.data.get('remark', '')
        rabbitmq = UtilsRabbitmq()

        try:
            if state == '1':

                if supplier_id:
                    supplier_id = ", supplier_id = '{}'".format(supplier_id)
                if plan_arrival_time:
                    plan_arrival_time = ", plan_arrival_time = {}".format(plan_arrival_time)
                else:
                    plan_arrival_time = ''
                if remark:
                    remark = ", remark = '{}'".format(remark)

                purchase_sql = "update base_purchases set state = '2', approval_time = {}, approver = '{}' {} {} {}" \
                               " where id = '{}' ".format(timestamp, user_id, supplier_id, plan_arrival_time,
                                                          remark, purchase_id)
                cursor.execute(purchase_sql)
                material_ids = []
                material_counts = []
                if materials:
                    for material in materials:
                        material_ids.append(material['id'])
                        material_counts.append(material['count'])
                        puchase_material_sql = "update base_purchase_materials set product_count = {} " \
                                               "where purchase_id = '{}' and product_id = '{}' ".format(
                            material['count'], purchase_id, material['id'])
                        cursor.execute(puchase_material_sql)
                else:
                    materials_sql = "select product_id, product_count from base_purchase_materials " \
                                    "where purchase_id = '{}'".format(purchase_id)
                    cursor.execute(materials_sql)
                    materials = cursor.fetchall()
                    for material in materials:
                        material_ids.append(material[0])
                        material_counts.append(material[1])

                # 增加在途库存
                material_on_road(cursor, factory_id, material_ids, material_counts, '3', purchase_id)
                # 检查供应商是否在
                check_client_sql = "select t2.id, t2.seq_id from base_purchases t1 left join factorys t2 on t1.supplier_id = t2.id  " \
                                   "where t1.id = '{}' and t2.id notnull;".format(purchase_id)

                cursor.execute(check_client_sql)
                supplier_res = cursor.fetchone()
                if supplier_res:
                    # 创建订单
                    create_order(cursor, factory_id, seq_id, purchase_id)
                    connection.commit()
                else:
                    connection.commit()

            elif state == '2':
                purchase_sql = "update base_purchases set state = '6', canceler = '{}', cancel_remark = '{}' " \
                               " ,cancel_time ={} where id = '{}' ".format(user_id, remark, timestamp, purchase_id)

                cursor.execute(purchase_sql)
                connection.commit()
                message = {'resource': 'PyPurchaseState', 'type': 'PUT',
                           'params': {'fac': factory_id, 'id': purchase_id, 'state': '5'}}
                rabbitmq.send_message(json.dumps(message))
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class PurchaseCRank(APIView):
    permission_classes = [PurchasePermission]

    def get(self, request):
        timestamp = int(time.time())
        factory_id = request.redis_cache["factory_id"]
        stime = request.query_params.get("start", timestamp)
        etime = request.query_params.get("end", timestamp)

        if request.query_params.get("order", '1') == '1':
            order = 'desc'
        else:
            order = 'asc'

        sql = '''
            select
                t1.price,
                t2.name
            from
                (
                select
                    supplier_id,
                    sum(price) as price
                from
                    (
                    select
                        t1.supplier_id,
                        t2.price
                    from
                        base_purchases t1
                    left join (
                        select
                            purchase_id,
                            sum(price) as price
                        from
                            (
                            select
                                purchase_id,
                                unit_price * product_count as price
                            from
                                base_purchase_materials ) t
                        group by
                            purchase_id ) t2 on
                        t1.id = t2.purchase_id
                    where
                        t1.factory = '{}'
                        and t1.create_time >= {}
                        and t1.create_time < {}
                        and t1.state != '6' ) t
                group by
                    supplier_id ) t1
            left join base_clients_pool t2 on
                t1.supplier_id = t2.id
            order by
                price {}
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

            return Response({"list": data},
                            status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
