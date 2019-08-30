# -*- coding: utf-8 -*-
# @Time   : 19-4-12 上午9:47
# @Author : huziying
# @File   : views_V350.py

import json
import logging
import time
import arrow
from django.utils.decorators import method_decorator
from isoweek import Week
from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db import connection

from apps_utils import today_timestamp, UtilsRabbitmq, year_timestamp, generate_module_uuid, AliOss
from constants import PrimaryKeyType
from permissions import StorePermission, store_decorator, store_approval_decorator
from store.store_utils import update_invoice, update_completed_storage, update_purchase_warehousing, update_picking_list

logger = logging.getLogger('django')


# 仓库部-----------------------------------------------------------------------------------------------------------------

"""
document: Connections and cursors 官方文档用法

https://docs.djangoproject.com/zh-hans/2.2/topics/db/sql/#connections-and-cursors

with connection.cursor() as c:
    c.execute(...)
或者
c = connection.cursor()
try:
    c.execute(...)
finally:
    c.close()
"""


class StoreMain(GenericAPIView):
    """仓库部首页 store/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        timestamp = today_timestamp()
        start = int(request.query_params.get("start", timestamp[0]))
        end = int(request.query_params.get("end", timestamp[1]))

        factory_id = request.redis_cache["factory_id"]
        # print(connection)
        cursor = connection.cursor()

        count_1 = """
        select
          count(1)
        from
          (
            select
              *
            from
              base_store_invoice
            where 
              factory = '%s'
          ) t1
        left join 
          base_orders t2 on t1.order_id = t2.id
        where 
          t2.del = '0' and t2.state != '5';
        """
        cursor.execute(count_1 % factory_id)
        invoice = cursor.fetchone()[0]
        cursor.execute("select count(1) from base_store_picking_list where factory = '%s';" % factory_id)
        picking_list = cursor.fetchone()[0]
        cursor.execute("select count(1) from base_store_completed_storage where factory = '%s';" % factory_id)
        completed_storage = cursor.fetchone()[0]
        cursor.execute("select count(1) from base_store_purchase_warehousing where factory = '%s';" % factory_id)
        purchase_warehousing = cursor.fetchone()[0]

        header = {"invoice": invoice, "picking_list": picking_list, "completed_storage": completed_storage,
                  "purchase_warehousing": purchase_warehousing}

        # 发货单
        sql_1 = """        
        select state,
               count(1)
        from (
               select t1.state
               from (
                      select case
                               when state = '0' then '0'
                               when state = '1' then '1'
                               when state = '2' then '2'
                               end as state,
                             order_id
                      from base_store_invoice
                      where factory = '%s' and time >= %d and time < %d
                    ) t1
                      left join
                    base_orders t2 on t1.order_id = t2.id
               where t2.del = '0'
                 and t2.state != '5'
             ) t
        group by state;
        """ % (factory_id, start, end)

        # 采购入库单
        sql_2 = """
        select
          state, count(1)
        from
          (
            select 
              case 
                when state = '0' then '0'
                when state = '1' then '1'
              end as state
            from
              base_store_purchase_warehousing
            where 
              factory = '%s' and time >= %d and time < %d
          ) t
        group by 
          state;
        """ % (factory_id, start, end)

        # 领料单
        sql_3 = """
        select
          state, count(1)
        from
          (
            select 
              case
                when state = '0' then '0'
                when state = '1' then '1'
                when state = '2' then '2'
              end as state
            from
              base_store_picking_list
            where 
              factory = '%s' and time >= %d and time < %d
          ) t
        group by 
          state;
        """ % (factory_id, start, end)

        # 仓库结存金额-产品
        sql_4 = """
        select
          sum(coalesce(t1.actual, 0) * coalesce(t2.price, 0))
        from
          (
            select 
              product_id, actual
            from
              base_products_storage
            where 
              factory = '%s' and time >= %d and time < %d
          ) t1
        left join base_products t2 on 
          t1.product_id = t2.id;
        """ % (factory_id, start, end)

        # 仓库结存金额-物料
        sql_5 = """
        select
          sum(coalesce(t1.actual, 0) * coalesce(t2.price, 0))
        from
          (
            select 
              material_id, actual
            from
              base_materials_storage
            where 
              factory = '%s' and time >= %d and time < %d
          ) t1
        left join base_materials t2 on 
          t1.material_id = t2.id;
        """ % (factory_id, start, end)

        try:
            data, storage_money = dict(), dict()
            invoice_dict = {"waited": 0, "invoice": 0, "done": 0}  # 发货单状态 0: 待发货，1: 已发货，2: 已送达
            purchase_warehousing_dict = {"not": 0, "done": 0}  # 采购入库状态，0: 未入库，1: 已入库
            picking_list_dict = {"prepared": 0, "waited": 0, "done": 0}  # 领料单状态，0: 待备料，1: 待领料，2: 已领料

            # 发货单
            cursor.execute(sql_1)
            result1 = cursor.fetchall()
            for res in result1:
                if res[0] == "0":
                    invoice_dict["waited"] = res[1]
                elif res[0] == "1":
                    invoice_dict["invoice"] = res[1]
                elif res[0] == "2":
                    invoice_dict["done"] = res[1]

            # 采购入库单
            cursor.execute(sql_2)
            result2 = cursor.fetchall()
            for res in result2:
                if res[0] == "0":
                    purchase_warehousing_dict["not"] = res[1]
                elif res[0] == "1":
                    purchase_warehousing_dict["done"] = res[1]

            # 领料单
            cursor.execute(sql_3)
            result3 = cursor.fetchall()
            for res in result3:
                if res[0] == "0":
                    picking_list_dict["prepared"] = res[1]
                elif res[0] == "1":
                    picking_list_dict["waited"] = res[1]
                elif res[0] == "2":
                    picking_list_dict["done"] = res[1]

            # 仓库结存金额-产品
            cursor.execute(sql_4)
            result4 = cursor.fetchone()
            products = round(result4[0] if result4[0] else 0, 2)

            # 仓库结存金额-物料
            cursor.execute(sql_5)
            result5 = cursor.fetchone()
            materials = round(result5[0] if result5[0] else 0, 2)

            storage_money["products"], storage_money["materials"], storage_money["total"] = products, materials, round(
                products + materials, 2)

            data = {"header": header, "invoice": invoice_dict, "purchase_warehousing": purchase_warehousing_dict,
                    "picking_list": picking_list_dict, "storage_money": storage_money}

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreInvoiceMain(APIView):
    """发货单首页 store/invoice/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        # 仓库统计分析 复用，带时间开始 截止参数
        start = request.query_params.get("start")
        end = request.query_params.get("end")

        if start and end:
            condition = " and time >= {} and time < {} ".format(start, end)
        else:
            condition = ""

        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        sql = """
        select
          t1.id, t1.order_id, t1.state, t1.time, coalesce(t1.deliver_time, 0) as deliver_time,
          t2.order_type, coalesce(t2.plan_arrival_time, 0) as plan_arrival_time, 
          coalesce(t2.actual_arrival_time, 0) as arrival_time,
          coalesce(t3.name, t4.name) as name
        from
          (
            select
              *
            from
              base_store_invoice
            where
              factory = '%s' and state != '3' and state = '%s' """ + condition + """
          ) t1
        left join 
          (select * from base_orders where del = '0') t2 on t1.order_id = t2.id
        left join
          (select * from base_clients where factory = '%s') t3 on t2.client_id = t3.id
        left join 
          base_clients_pool t4 on t2.client_id = t4.id
        order by 
          t1.time desc;
        """
        waited, deliver, done = [], [], []
        data_dict = {0: waited, 1: deliver, 2: done}
        temp_dict = {0: "waited", 1: "deliver", 2: "done"}

        try:
            for i in temp_dict:
                # 发货单状态 0: 未发货，1: 已发货，2: 已送达
                cursor.execute(sql % (factory_id, i, factory_id))
                result = cursor.fetchall()
                for res in result:
                    di, products = dict(), ""

                    di["id"] = res[0]
                    order_id = res[1]
                    di["deliver_time"] = res[4]
                    di["style"] = res[5]
                    di["plan_arrival_time"] = res[6]
                    di["arrival_time"] = res[7]
                    di["name"] = res[8]
                    products_sql = """
                    select
                      coalesce(t1.product_count, 0) as count,
                      coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit
                    from
                      (
                        select 
                          *
                        from 
                          base_order_products
                        where 
                          order_id = '%s'
                      ) t1
                    left join 
                      base_materials_pool t2 on t1.product_id = t2.id;
                    """ % order_id
                    cursor.execute(products_sql)
                    result2 = cursor.fetchall()
                    for re in result2:
                        dt = dict()
                        dt["count"] = round(re[0] if re[0] else 0, 2)
                        dt["product"] = re[1]
                        dt["unit"] = re[2]
                        products += str(dt["product"]) + ":" + str(dt["count"]) + str(dt["unit"]) + ";"
                    di["products"] = products.rstrip(";")
                    data_dict[i].append(di)

            return Response({"waited": waited, "deliver": deliver, "done": done}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreInvoiceDetail(APIView):
    """发货单详情-待发货/已发货/已送达 store/invoice/detail
    发货单状态 0: 未发货，1: 已发货，2: 已送达"""
    permission_classes = [StorePermission]

    def get(self, request):
        invoice_id = request.query_params.get("id")
        if not invoice_id:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        user_id = phone if not user_id else user_id

        alioss = AliOss()
        cursor = connection.cursor()

        cursor.execute(
            "select count(1) from base_store_invoice where factory = '%s' and id = '%s';" % (factory_id, invoice_id))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.id, t1.order_id, t1.state,
          t2.order_type, coalesce(t2.plan_arrival_time, 0) as plan_arrival_time, 
          coalesce(t2.collected, 0) as receipt, 
          coalesce(t1.remark, '') as remark, 
          coalesce(t3.name, t7.name) as company_name, 
          coalesce(t3.region || t3.address, t7.region || t7.address) as address,
          coalesce(t4.phone, '') as phone,
          coalesce(t5.name, '') as client_name,
          coalesce(t6.name, '') as deliver_person, coalesce(t6.phone, '') as deliver_phone, 
          coalesce(t1.deliver_time, 0) as deliver_time,
          t2.plan_arrival_time, t3.deliver_days,
          t6.image as deliver_image
        from
          (
            select
              *
            from
              base_store_invoice
            where
              id = '%s' and factory = '%s' and state != '3'
          ) t1
        left join 
          (select * from base_orders where del = '0') t2 on t1.order_id = t2.id
        left join 
          (select * from base_clients where factory = '%s') t3 on t2.client_id = t3.id
        left join 
          base_clients_pool t7 on t2.client_id = t7.id
        left join 
          (
            select 
              *
            from
              factory_users 
            where 
              '1' = any(rights)
          )t4 on t2.factory = t4.factory
        left join 
          user_info t5 on t4.phone = t5.phone
        left join 
          user_info t6 on t1.deliver_person = t6.user_id;
        """ % (invoice_id, factory_id, factory_id)

        product_sql = """
        select
          coalesce(t1.product_count, 0) as count, coalesce(t1.unit_price, 0) as price,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit, coalesce(t3.name, '') as category
        from
          (
            select 
              *
            from 
              base_order_products
            where 
              order_id = '%s'
          ) t1
        left join 
          base_materials_pool t2 on t1.product_id = t2.id
        left join 
          base_material_category_pool t3 on t2.category_id = t3.id;
        """
        search_sql = """
        select
          t2.id as purchase_warehousing_id
        from
          (
            select 
              *
            from
              base_store_invoice
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join base_store_purchase_warehousing t2 on
          t1.id = t2.invoice_id;
        """ % (factory_id, invoice_id)

        try:
            cursor.execute(sql)
            result1 = cursor.fetchone()

            if not result1:
                return Response({"res": 1, "errmsg": "此发货单无关联数据！"}, status=status.HTTP_200_OK)

            data, clients, time_ = dict(), dict(), dict()
            data["id"] = result1[0]
            order_id = result1[1]
            time_["state"] = result1[2]
            time_["predict_time"] = result1[4]
            data["time"] = time_
            clients["style"] = result1[3]
            clients["company_name"] = result1[7]
            clients["address"] = result1[8]
            clients["phone"] = result1[9]
            clients["client_name"] = result1[10]
            data["clients"] = clients
            data["deliver_person"] = result1[11]
            data["deliver_phone"] = result1[12]
            data["deliver_time"] = result1[13]
            # 发货单添加 预计发货时间 = 订单期望送达时间 - 送达天数
            order_plan_arrival_time, client_deliver_days = result1[14], result1[15] if result1[15] else 0  # 时间戳, 天数
            data["plan_deliver_time"] = arrow.get(order_plan_arrival_time).shift(
                days=-round(client_deliver_days, 1)).timestamp

            data["deliver_image"] = alioss.joint_image(result1[16].tobytes().decode()) if \
                isinstance(result1[16], memoryview) else alioss.joint_image(result1[16])

            cursor.execute(product_sql % order_id)
            result2 = cursor.fetchall()
            # print(result2)
            total_money, products, products_list = 0, dict(), list()
            for res in result2:
                di = dict()
                di["count"] = round(res[0] if res[0] else 0, 2)
                di["price"] = round(res[1] if res[1] else 0, 2)
                di["name"] = res[2]
                di["unit"] = res[3]
                di["category"] = res[4]
                di["money"] = round(float(di["count"]) * float(di["price"]), 2)
                total_money += di["money"]
                products_list.append(di)
            products["products_list"] = products_list
            products["total_money"] = total_money
            products["receipt"] = result1[5]
            products["remark"] = result1[6]
            data["products"] = products

            # 查找采购入库单
            cursor.execute(search_sql)
            purchase_warehousing_id = cursor.fetchone()[0]
            purchase_warehousing_id = purchase_warehousing_id if purchase_warehousing_id else ""

            # 生成二维码内容
            if time_["state"] != "0" and time_["state"] != "3":
                content = {"type": "1", "id": purchase_warehousing_id, "share": user_id}
                data["qr_code"] = content

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        """发货单-发货"""
        invoice_id = request.data.get("id")
        if not invoice_id:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        seq_id = request.redis_cache["seq_id"]
        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        cursor.execute(
            "select count(1) from base_store_invoice where id = '%s' and factory = '%s';" % (
                invoice_id, factory_id))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        cursor.execute(
            "select state from base_store_invoice where id = '%s' and factory = '%s';" % (invoice_id, factory_id))
        state_check = cursor.fetchone()[0]
        if state_check != "0":
            return Response({"res": 1, "errmsg": "状态错误，无法操作！"}, status=status.HTTP_200_OK)

        try:
            errmsg = update_invoice(invoice_id, "1", user_id, phone, factory_id, seq_id)
            if not errmsg:
                return Response({"res": 0}, status=status.HTTP_200_OK)
            else:
                return Response({"res": 1, "errmsg": errmsg}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreCompletedStorageMain(APIView):
    """完工入库单首页 store/completed_storage/main
    完工入库单状态 0: 未入库，1: 已入库"""
    permission_classes = [StorePermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        sql = """
        select
          t1.id, t1.time, coalesce(t1.completed_time, 0) as income_time,
          coalesce(t2.target_count, 0) as plan_count, coalesce(t2.complete_time, 0) as complete_time,
          coalesce(t3.name, '') as name, coalesce(t3.unit, '') as unit, coalesce(t4.name, '') as category
        from
          (
            select 
              id, order_id, product_task_id, state, time, completed_time
            from 
              base_store_completed_storage
            where 
              state = '%s' and factory = '%s'
          ) t1
        left join base_product_task t2 on t1.product_task_id = t2.id
        left join base_materials_pool t3 on t2.product_id = t3.id
        left join base_material_category_pool t4 on t3.category_id = t4.id
        order by 
          t1.time desc;
        """

        try:
            not_yet, done = [], []
            state_dict = {"0": not_yet, "1": done}

            for state in state_dict:
                # 完工入库单状态 0: 未入库，1: 已入库
                cursor.execute(sql % (state, factory_id))
                result = cursor.fetchall()
                for res in result:
                    di = dict()
                    di["id"] = res[0]
                    di["income_time"] = res[2]
                    di["plan_count"] = round(res[3] if res[3] else 0, 2)
                    di["complete_time"] = res[4]
                    di["name"] = res[5]
                    di["unit"] = res[6]
                    di["category"] = res[7]
                    state_dict[state].append(di)

            return Response({"not_yet": not_yet, "done": done}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreCompletedStorageDetail(APIView):
    """完工入库单详情 store/completed_storage/detail
    完工入库单状态 0: 未入库，1: 已入库"""
    permission_classes = [StorePermission]

    def get(self, request):
        completed_id = request.query_params.get("id")
        if not completed_id:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        user_id = phone if not user_id else user_id

        alioss = AliOss()
        cursor = connection.cursor()

        cursor.execute("select count(1) from base_store_completed_storage where factory = '%s' and id = '%s';" % (
            factory_id, completed_id))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        sql = """
        select
         t1.*,
         coalesce(t2.target_count, 0) as plan_count, coalesce(t2.complete_time, 0) as complete_time,
         coalesce(t3.name, '') as name, coalesce(t3.unit, '') as unit, coalesce(t4.name, '') as category,
         coalesce(t2.complete_count, 0) as complete_count,
         t2.id as product_id
        from
          (
            select 
              id, product_task_id, state, coalesce(completed_time, 0) as income_time, 
              coalesce(send_person, '') as send_person, coalesce(receive_person, '') as receive_person
            from 
              base_store_completed_storage
            where 
              id = '%s' and factory = '%s'
          ) t1
        left join base_product_task t2 on
          t1.product_task_id = t2.id
        left join base_materials_pool t3 on
          t2.product_id = t3.id
        left join base_material_category_pool t4 on
          t3.category_id = t4.id;
        """ % (completed_id, factory_id)

        try:
            data, product, completed_storage = dict(), dict(), dict()
            cursor.execute(sql)
            result = cursor.fetchone()
            data["id"], data["state"] = result[0], result[2]
            product["plan_count"], product["complete_time"], product["name"], product["unit"], product["category"], \
                product["complete_count"] = round(result[6] if result[6] else 0, 2), result[7], result[8], result[9], \
                result[10], result[11]
            completed_storage["income_time"] = result[3]
            send_user_id, receive_user_id = result[4], result[5]
            product["product_id"] = result[12]
            cursor.execute("select phone, name, coalesce(image, '') as image from user_info where user_id = '%s';"
                           % send_user_id)
            send_person = cursor.fetchone()
            completed_storage["send_phone"] = send_person[0] if send_person else ""
            completed_storage["send_person"] = send_person[1] if send_person else ""
            if send_person:
                completed_storage["send_image"] = alioss.joint_image(send_person[2].tobytes().decode()) if \
                    isinstance(send_person[2], memoryview) else alioss.joint_image(send_person[2])
            else:
                completed_storage["send_image"] = alioss.joint_image(None)
            cursor.execute("select phone, name, coalesce(image, '') as image from user_info where user_id = '%s';"
                           % receive_user_id)
            receive_person = cursor.fetchone()
            completed_storage["receive_phone"] = receive_person[0] if receive_person else ""
            completed_storage["receive_person"] = receive_person[1] if receive_person else ""
            if receive_person:
                completed_storage["receive_image"] = alioss.joint_image(receive_person[2].tobytes().decode()) if \
                    isinstance(receive_person[2], memoryview) else alioss.joint_image(receive_person[2])
            else:
                completed_storage["receive_image"] = alioss.joint_image(None)

            data["product"], data["completed_storage"] = product, completed_storage

            # 生成二维码内容
            if data["state"] == "0":
                content = {"type": "6", "id": data["id"], "share": user_id}
                data["qr_code"] = content

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        """完工入库单-入库操作"""
        completed_id = request.data.get("id")
        action = request.data.get("action")  # 操作类型 1: 返回二维码内容，记录接收人信息。2: 入库操作，更新状态。

        if not all([completed_id, action]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        seq_id = request.redis_cache["seq_id"]
        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)
        user_id = phone if not user_id else user_id

        cursor = connection.cursor()

        cursor.execute("select count(1) from base_store_completed_storage where id = '%s' and factory = '%s';" % (
            completed_id, factory_id))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        try:
            if action == "1":  # 返回二维码内容，记录接收人信息
                cursor.execute("update base_store_completed_storage set receive_person = '%s' where id = '%s';"
                               % (user_id, completed_id))
                connection.commit()
                return Response({"res": 0}, status=status.HTTP_200_OK)
            elif action == "2":  # 入库操作，更新状态
                errmsg = update_completed_storage(completed_id, "1", user_id, factory_id, seq_id)
                if errmsg:
                    return Response({"res": 1, "errmsg": errmsg}, status=status.HTTP_200_OK)
                else:
                    return Response({"res": 0}, status=status.HTTP_200_OK)
            else:
                return Response({"res": 1, "errmsg": "操作参数代号错误！"}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StorePurchaseWarehousingMain(APIView):
    """采购入库单 store/purchase_warehousing/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        # 仓库统计分析 复用，带时间开始 截止参数
        start = request.query_params.get("start")
        end = request.query_params.get("end")

        if start and end:
            condition = " and time >= {} and time < {} ".format(start, end)
        else:
            condition = ""

        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        sql = """
        select
          t1.id, coalesce(t1.income_time, 0) as income_time,
          t2.id as order_id, coalesce(t2.deliver_time, 0) as deliver_time, 
          coalesce(t4.name, '') as company_name
        from
          (
            select 
              id, order_id, state, time, income_time
            from 
              base_store_purchase_warehousing
            where 
              factory = '%s' and state = '%s'  """ + condition + """            
          ) t1
        left join (select * from base_orders where del = '0') t2 on
          t1.order_id = t2.id
        left join base_purchases t3 on
          t2.purchase_id = t3.id
        left join base_clients_pool t4 on
          t3.supplier_id = t4.id
        order by
          t1.time desc;
        """

        product_sql = """
        select
          coalesce(t1.product_count, 0) as count, 
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit
        from 
          (
            select
              *
            from
              base_order_products
            where 
              order_id = '%s'
          ) t1
        left join base_materials_pool t2 on 
          t1.product_id = t2.id;
        """

        try:
            not_yet, done = [], []
            state_dict = {0: not_yet, 1: done}
            for state in state_dict:
                # 采购入库状态，0: 未入库，1: 已入库
                cursor.execute(sql % (factory_id, state))
                # print(sql % (factory_id, state))
                result = cursor.fetchall()
                for res in result:
                    di, purchase = dict(), ""
                    di["id"] = res[0]
                    di["income_time"] = res[1]
                    order_id = res[2]
                    di["deliver_time"] = res[3]
                    di["company_name"] = res[4]
                    cursor.execute(product_sql % order_id)
                    result2 = cursor.fetchall()
                    for re in result2:
                        purchase += re[1] + ":" + str(round(re[0], 2)) + re[2] + ";"
                    di["purchase"] = purchase
                    state_dict[state].append(di)

            return Response({"0": not_yet, "1": done}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StorePurchaseWarehousingDetail(APIView):
    """采购入库单详情 store/purchase_warehousing/detail"""
    permission_classes = [StorePermission]

    def get(self, request):
        purchase_id = request.query_params.get("id")
        # print(purchase_id)
        if not purchase_id:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]

        alioss = AliOss()
        cursor = connection.cursor()

        sql_check = "select count(1) from base_store_purchase_warehousing where id = '%s' and factory = '%s';" % \
                    (purchase_id, factory_id)

        sql = """
        select
          t1.id, t1.state, coalesce(t1.income_time, 0) as income_time,
          t2.id as order_id, t2.order_type,
          coalesce(t4.name,'') as company_name, coalesce(t6.name, '') as name, coalesce(t6.phone, '') as phone, 
          coalesce(t4.region || t4.address, '') as address,
          coalesce(t7.phone, '') as phone, coalesce(t7.name, '') as income_person, t7.image
        from
          (
            select 
              *
            from 
              base_store_purchase_warehousing
            where 
              id = '%s' and factory = '%s'
          ) t1
        left join base_orders t2 on
          t1.order_id = t2.id
        left join base_purchases t3 on
          t2.purchase_id = t3.id
        left join base_clients_pool t4 on
          t3.supplier_id = t4.id
        left join (select * from factory_users where '1' = any(rights)) t5 on
          t2.factory = t5.factory
        left join user_info t6 on
          t5.phone = t6.phone
        left join user_info t7 on
          t1.income_person = t7.user_id;
        """

        product_sql = """
        select
          coalesce(t1.product_count, 0) as count, coalesce(t1.unit_price, 0) as price,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit, coalesce(t3.name, '') as category 
        from
          (
            select 
              *
            from 
              base_order_products
            where 
              order_id = '%s'  
          ) t1
        left join base_materials_pool t2 on
          t1.product_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id;
        """

        data, client, income, purchase_list, total_money = {}, {}, {}, [], 0

        try:
            cursor.execute(sql_check)
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

            cursor.execute(sql % (purchase_id, factory_id))
            result = cursor.fetchone()
            # print(sql % (purchase_id, factory_id))
            data["id"] = result[0]
            data["state"] = result[1]
            order_id = result[3]
            client["style"], client["company_name"], client["name"], client["phone"], client["address"] = \
                result[4], result[5], result[6], result[7], result[8]
            income["income_time"], income["phone"], income["income_person"], income["image"] = result[2], result[9], \
                                                                                               result[
                                                                                                   10], alioss.joint_image(
                result[11].tobytes().decode()) if isinstance(result[11], memoryview) \
                                                                                                   else alioss.joint_image(
                result[11])
            data["client"] = client
            data["income"] = income

            cursor.execute(product_sql % order_id)
            result2 = cursor.fetchall()
            for res in result2:
                di = dict()
                di["name"] = res[2]
                di["category"] = res[4]
                di["count"] = round(res[0], 2)
                di["unit"] = res[3]
                di["price"] = res[1]
                di["money"] = float(round(di["count"] * di["price"], 2))
                total_money += di["money"]
                purchase_list.append(di)

            data["purchase_list"] = purchase_list
            data["total_money"] = round(total_money, 2)

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        """采购入库单-入库 0: 未入库, 1: 已入库"""
        warehousing_id = request.data.get("id")
        if not warehousing_id:
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        cursor.execute("select count(1) from base_store_purchase_warehousing where id = '%s' and factory = '%s';" % (
            warehousing_id, factory_id))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        try:
            errmsg = update_purchase_warehousing(warehousing_id, "1", phone, factory_id)
            if errmsg:
                return Response({"res": 1, "errmsg": errmsg}, status=status.HTTP_200_OK)
            else:
                return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StorePickingListMain(APIView):
    """领料单首页 store/picking_list/main
    领料单状态，0: 未备料，1: 待领料，2: 已领料"""
    permission_classes = [StorePermission]

    def get(self, request):
        # 仓库统计分析 复用，带时间开始 截止参数
        start = request.query_params.get("start")
        end = request.query_params.get("end")

        if start and end:
            condition = " and time >= {} and time < {} ".format(start, end)
        else:
            condition = ""

        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        sql = """
        select
          t1.id, t1.product_task_id, t1.supplement_id, t1.style, coalesce(t1.time, 0) as time, 
          coalesce(t1.waited_time, 0) as waited_time, coalesce(t1.picking_time, 0) as picking_time     
        from
          (
            select 
              *
            from
              base_store_picking_list
            where 
              factory = '%s' and state = '%s' """ + condition + """
            order by
              time desc
          ) t1;
        """
        task_sql = """
        select
          coalesce(t1.target_count, 0) as target_count, t1.material_ids, t1.material_counts,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit, coalesce(t3.name, '') as category
        from
          (
            select
              *
            from
              base_product_task
            where
              factory = '%s' and id = '%s'
          ) t1
        left join base_materials_pool t2 on
          t1.product_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id;
        """
        supplement_sql = """
        select
          coalesce(t2.target_count, 0) as target_count, t1.material_ids, t1.material_counts,
          coalesce(t3.name, '') as name, coalesce(t3.unit, '') as unit, coalesce(t4.name, '') as category          
        from
          (
            select * from base_material_supplement where factory = '%s' and id = '%s'
          ) t1
        left join base_product_task t2 on
          t1.product_task_id = t2.id
        left join base_materials_pool t3 on
          t2.product_id = t3.id
        left join base_material_category_pool t4 on 
          t3.category_id = t4.id;
        """

        material_sql = """
        select 
          coalesce(t1.name, '') as name, coalesce(t1.unit, '') as unit, coalesce(t2.name, '') as category
        from 
          base_materials_pool t1
        left join base_material_category_pool t2 on
          t1.category_id = t2.id
        where 
          t1.id = '%s';
        """

        not_yet, prepared, done, materials = [], [], [], ""
        state_dict = {"0": not_yet, "1": prepared, "2": done}

        try:
            for state in state_dict:
                cursor.execute(sql % (factory_id, state))
                result = cursor.fetchall()
                for res in result:
                    di, ma_str = dict(), ""
                    di["id"] = res[0]
                    task_id, supplement_id = res[1], res[2]
                    di["style"] = res[3]
                    di["time"] = res[4]
                    di["waited_time"] = res[5]
                    di["picking_time"] = res[6]
                    # 领料单类型，0: 生产单直接创建，1:补料单创建
                    if di["style"] == "0":
                        cursor.execute(task_sql % (factory_id, task_id))
                    elif di["style"] == "1":
                        cursor.execute(supplement_sql % (factory_id, supplement_id))
                    result2 = cursor.fetchone()
                    material_ids, material_counts = result2[1] or [], result2[2] or []
                    di["target_count"], di["name"], di["unit"], di["category"] = result2[0], result2[3], result2[4], \
                                                                                 result2[5]
                    if di["style"] == "0":
                        # 生产单中的物料数量（单个产品）,要乘以产生的数量
                        material_counts = [i * di["target_count"] for i in material_counts]
                    # 补料单创建, 物料数量是用户填写的，不要相乘

                    combine_dict = dict(zip(material_ids or [], material_counts or []))
                    for material in combine_dict:
                        cursor.execute(material_sql % material)
                        result3 = cursor.fetchone()
                        ma_str += result3[2] + ":" + result3[0] + " " + str(round(combine_dict[material], 2)) + result3[
                            1] + ";"
                    di["materials"] = ma_str
                    state_dict[state].append(di)

            return Response(state_dict, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StorePickingListDetail(APIView):
    """领料单详情 store/picking_list/detail
    领料单状态，0: 未备料，1: 待领料，2: 已领料"""
    permission_classes = [StorePermission]

    def get(self, request):
        picking_id = request.query_params.get("id")
        if not picking_id:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        user_id = phone if not user_id else user_id

        alioss = AliOss()
        cursor = connection.cursor()

        check_sql = "select count(1) from base_store_picking_list where factory = '%s' and id = '%s';" % (
            factory_id, picking_id)
        cursor.execute(check_sql)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.id, t1.product_task_id, t1.supplement_id, t1.state, t1.style, coalesce(t1.picking_time, 0) as picking_time,
          coalesce(t2.name, '') as send_person, coalesce(t2.phone, '') as send_phone, 
          coalesce(t2.image, '') as send_image,
          coalesce(t3.name, '') as accept_person, coalesce(t3.phone, '') as accept_phone, 
          coalesce(t3.image, '') as accept_image
        from
          (
            select 
              *
            from 
              base_store_picking_list
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join user_info t2 on
          t1.send_person = t2.user_id
        left join user_info t3 on
          t1.receive_person = t3.user_id;
        """
        task_sql = """
        select
          t1.id, coalesce(t1.target_count, 0) as target_count, t1.material_ids, t1.material_counts,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit, coalesce(t3.name, '') as category
        from
          (
            select
              *
            from
              base_product_task
            where
              factory = '%s' and id = '%s'
          ) t1
        left join base_materials_pool t2 on
          t1.product_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id;
        """
        supplement_sql = """
        select
          t2.id, coalesce(t2.target_count, 0) as target_count, t1.material_ids, t1.material_counts,
          coalesce(t3.name, '') as name, coalesce(t3.unit, '') as unit, coalesce(t4.name, '') as category          
        from
          (
            select * from base_material_supplement where factory = '%s' and id = '%s'
          ) t1
        left join base_product_task t2 on
          t1.product_task_id = t2.id
        left join base_materials_pool t3 on
          t2.product_id = t3.id
        left join base_material_category_pool t4 on 
          t3.category_id = t4.id;
        """

        material_sql = """
        select
          coalesce(name, '') as name, coalesce(unit, '') as unit
        from
          base_materials_pool
        where 
          id = '%s';
        """

        plan_picking_time = """
        select
          t2.plan_arrival_time, t3.deliver_days, coalesce(t5.process_time, 0) as process_time
        from
          (
            select 
              *
            from 
              base_store_picking_list
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join 
          base_orders t2 on t1.order_id = t2.id
        left join 
          (select * from base_clients where factory = '%s') t3 on t2.client_id = t3.id
        left join 
          base_product_task t4 on t1.product_task_id = t4.id
        left join 
          (
            select 
              product_id, sum(unit_time) as process_time 
            from 
              base_product_processes 
            where factory = '%s' 
            group by product_id
          ) t5 on t4.product_id = t5.product_id;
        """ % (factory_id, picking_id, factory_id, factory_id)

        try:
            data, materials_list = dict(), list()
            cursor.execute(sql % (factory_id, picking_id))
            result1 = cursor.fetchone()

            data["id"], data["state"], data["style"], data["picking_time"], data["send_person"], data["send_phone"], \
                data["accept_person"], data["accept_phone"], = result1[0], result1[3], result1[4], result1[5], \
                result1[6], result1[7], result1[9], result1[10]
            data["send_image"] = alioss.joint_image(result1[8].tobytes().decode()) \
                if isinstance(result1[8], memoryview) else alioss.joint_image(result1[8])
            data["accept_image"] = alioss.joint_image(result1[11].tobytes().decode()) \
                if isinstance(result1[11], memoryview) else alioss.joint_image(result1[11])

            task_id, supplement_id = result1[1], result1[2]

            # 领料单添加 期望领料时间 = 订单期望送达时间 - 送达天数 - 生产用时(分钟)
            cursor.execute(plan_picking_time)
            result_time = cursor.fetchone()
            # print(result_time)
            plan_arrival_time, deliver_days, process_time = result_time
            data["expect_picking_time"] = arrow.get(plan_arrival_time).shift(
                days=-round(deliver_days if deliver_days else 0, 1)).timestamp - int(process_time * 60)

            # 领料单类型，0: 生产单直接创建，1:补料单创建
            if data["style"] == "0":
                cursor.execute(task_sql % (factory_id, task_id))
            elif data["style"] == "1":
                cursor.execute(supplement_sql % (factory_id, supplement_id))
            else:
                return Response({"res": 1, "errmsg": "领料单类型错误！"}, status=status.HTTP_200_OK)
            result2 = cursor.fetchone()
            data["product_id"], data["target_count"], data["name"], data["unit"], data["category"] = result2[0], \
                result2[1], result2[4], result2[5], result2[6]
            material_ids, material_counts = result2[2] or [], result2[3] or []
            if data["style"] == "0":
                # 生产单中的物料数量（单个产品）,要乘以产生的数量
                material_counts = [i * data["target_count"] for i in material_counts]
            # 补料单创建, 物料数量是用户填写的，不要相乘

            combine_dict = dict(zip(material_ids, material_counts))
            for material_id in combine_dict:
                cursor.execute(material_sql % material_id)
                material_result = cursor.fetchone()
                materials_list.append({"name": material_result[0], "count": combine_dict[material_id],
                                       "unit": material_result[1]})
            data["materials_list"] = materials_list

            # 二维码json内容
            if data["state"] != "0":
                content = {"type": "7", "id": picking_id, "share": user_id}
                data["qr_code"] = content

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        """领料单状态，0: 未备料，1: 待领料，2: 已领料"""
        picking_id = request.data.get("id")
        state = request.data.get("state")  # 1: 设置为待领料, 2: 设置为已领料
        action = request.data.get("action")  # 操作类型 1: 返回二维码内容，记录接收人信息。2: 入库操作，更新状态

        if not all([picking_id, state]):
            return Response({"res": 1, "errmsg": "缺少参数"}, status=status.HTTP_200_OK)

        if state not in ["1", "2"]:
            return Response({"res": 1, "errmsg": "状态代号错误！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        user_id = phone if not user_id else user_id
        cursor = connection.cursor()

        try:
            content = update_picking_list(picking_id, state, user_id, factory_id, action)
            return Response(content, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreStorageMain(APIView):
    """库存主页 store/storage/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        # 仓库统计分析 复用，带时间开始 截止参数
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        choice = request.query_params.get("choice", "all")  # 某个仓库id

        if start and end:
            condition = " and t1.time >= {} and t1.time < {} ".format(start, end)
        else:
            condition = ""
        if choice == "all":
            condition_2 = ""
        else:
            condition_2 = " and t4.uuid = '{}' ".format(choice)

        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        materials_sql = """
        select
          array_agg(t1.material_id),
          array_agg(coalesce(t1.actual, 0) + coalesce(t1.on_road, 0) - coalesce(t1.prepared, 0) - 
          coalesce(t1.safety, 0)) as available,
          array_agg(coalesce(t2.name, '')) as name, array_agg(coalesce(t2.unit, '')) as unit,
          coalesce(t3.name, '') as category,
          array_agg(coalesce(t4.name, '')) as store_name
        from
          base_materials_storage t1
        left join base_materials_pool t2 on
          t1.material_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id
        left join (select * from base_multi_storage where factory = '%s') t4 on
          t1.uuid = t4.uuid
        where t1.factory = '%s' """ % (factory_id, factory_id) + condition + condition_2 + """
        group by t3.id
        order by 
          category desc;
        """

        # materials_log_sql = """
        # select
        #   coalesce(time, 0) as recent
        # from
        #   base_materials_log
        # where
        #   material_id = '%s' and type = 'actual' and count < 0
        # order by
        #   time desc
        # limit 1;
        # """

        products_sql = """
        select
          array_agg(t1.product_id),
          array_agg(coalesce(t1.actual, 0) + coalesce(t1.pre_product, 0) - coalesce(t1.prepared, 0) - 
          coalesce(t1.safety, 0)) as available,
          array_agg(coalesce(t2.name, '')) as name, array_agg(coalesce(t2.unit, '')) as unit,
          coalesce(t3.name, '') as category,
          array_agg(coalesce(t4.name, '')) as store_name
        from
          base_products_storage t1
        left join base_materials_pool t2 on
          t1.product_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id
        left join (select * from base_multi_storage where factory = '%s') t4 on
          t1.uuid = t4.uuid
        where t1.factory = '%s' """ % (factory_id, factory_id) + condition + condition_2 + """
        group by t3.id
        order by 
          category desc;
        """

        # products_log_sql = """
        # select
        #   coalesce(time, 0) as recent
        # from
        #   base_products_log
        # where
        #   product_id = '%s' and type = 'actual' and count < 0
        # order by
        #   time desc
        # limit 1;
        # """

        products, materials, not_category_products, not_category_materials = [], [], [], []
        try:
            cursor.execute(materials_sql)
            materials_result = cursor.fetchall()
            materials_result = [list(ma) for ma in materials_result]
            # print(materials_result)
            for ma in materials_result:
                di = dict()
                li = list()
                di["category"] = ma[4]
                temp = list(zip(ma[0], ma[1], ma[2], ma[3], ma[5]))
                for te in temp:
                    dt = dict()
                    dt["id"] = te[0]
                    # cursor.execute(materials_log_sql % dt["id"])
                    # recent = cursor.fetchone()
                    # dt["recent"] = recent[0] if recent else 0
                    dt["available"] = round(te[1] if te[1] else 0, 2)
                    dt["name"] = te[2]
                    dt["unit"] = te[3]
                    dt["store_name"] = te[4]
                    li.append(dt)
                di["materials"] = li
                if di["category"] == "其他":
                    not_category_materials.append(di)
                else:
                    materials.append(di)

            cursor.execute(products_sql)
            products_result = cursor.fetchall()
            products_result = [list(pr) for pr in products_result]
            for pr in products_result:
                di = dict()
                li = list()
                di["category"] = pr[4]
                temp = list(zip(pr[0], pr[1], pr[2], pr[3], pr[5]))
                for te in temp:
                    dt = dict()
                    dt["id"] = te[0]
                    # cursor.execute(products_log_sql % dt["id"])
                    # recent = cursor.fetchone()
                    # dt["recent"] = recent[0] if recent else 0
                    dt["available"] = round(te[1] if te[1] else 0, 2)
                    dt["name"] = te[2]
                    dt["unit"] = te[3]
                    dt["store_name"] = te[4]
                    li.append(dt)
                di["products"] = li
                if di["category"] == "其他":
                    not_category_products.append(di)
                else:
                    products.append(di)

            return Response({"0": {"category": products, "not_category": not_category_products},
                             "1": {"category": materials, "not_category": not_category_materials}},
                            status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreStorageDetail(APIView):
    """库存详情页-物料详情/产品详情 store/storage/detail/{type}"""
    permission_classes = [StorePermission]

    def get(self, request, type_):
        id_ = request.query_params.get("id")  # 产品id/物料id
        date = request.query_params.get("date", "3")  # 1: 月， 2: 年, 3：周

        user_id = request.redis_cache["user_id"]
        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        if date == "1":
            condition = " to_char(to_timestamp(time), 'YYYY-MM') as date "
        elif date == "2":
            condition = " to_char(to_timestamp(time), 'YYYY') as date "
        elif date == "3":
            condition = " to_char(to_timestamp(time), 'iyyy-IW') as date "
        else:
            return Response({"res": 1, "errmsg": "时间类型错误！"}, status=status.HTTP_200_OK)

        product_id_check = "select count(1) from base_products_storage where product_id = '%s' and factory = '%s';" % (
            id_, factory_id)
        material_id_check = "select count(1) from base_materials_storage where material_id = '%s' and factory = '%s';" \
                            % (id_, factory_id)
        product_sql = """
        select 
          coalesce(t1.actual, 0) as actual, coalesce(t1.pre_product, 0) as pre_product, 
          coalesce(t1.prepared, 0) as prepared, coalesce(t1.safety, 0) as safety, 
          (coalesce(t1.actual, 0) + coalesce(t1.pre_product, 0) - coalesce(t1.prepared, 0) - 
          coalesce(t1.safety, 0)) as available,
          t2.id, coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit,
          coalesce(t3.name, '') as category,
          coalesce(t4.name, '') as store
        from
          base_products_storage t1
        left join base_materials_pool t2 on
          t1.product_id = t2.id
        left join base_material_category_pool t3 on 
          t2.category_id = t3.id
        left join base_multi_storage t4 on
          t1.uuid = t4.uuid
        where 
          t1.factory = '%s' and t1.product_id = '%s';
        """
        product_log_sql = """
        select 
          date, array_agg(id), array_agg(type), array_agg(count), array_agg(time), array_agg(source), array_agg(source_id)
        from (
           select product_id                             as id,
                  type,
                  coalesce(count, 0)                     as count,
                  coalesce(time, 0)                      as time,
                  source,
                  source_id, """ + condition + """
           from base_products_log
           where factory = '%s'
             and product_id = '%s'
             and (
               type = 'actual' or
               type = 'store_check'
             )
           order by time desc) t
        group by date;
        """
        product_inout_sql = """
        select 
          coalesce(sum(case when count < 0 then count end), 0) as outgoing,
          coalesce(sum(case when count > 0 then count end), 0) as incoming
        from 
          base_products_log
        where 
          type = 'actual' and factory = '%s' and product_id = '%s' and time >= %d and time < %d;
        """

        material_sql = """
        select
          coalesce(t1.actual, 0) as actual, coalesce(t1.on_road, 0) as on_road, coalesce(t1.prepared, 0) as prepared, 
          coalesce(t1.safety, 0) as safety, 
          (coalesce(t1.actual, 0) + coalesce(t1.on_road, 0) - coalesce(t1.prepared, 0) - coalesce(t1.safety, 0)) 
          as available,
          t2.id, coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit,
          coalesce(t3.name, '') as category,
          coalesce(t4.name, '') as store
        from
          base_materials_storage t1
        left join base_materials_pool t2 on
          t1.material_id = t2.id
        left join base_material_category_pool t3 on
          t2.category_id = t3.id
        left join base_multi_storage t4 on
          t1.uuid = t4.uuid
        where 
          t1.factory = '%s' and t1.material_id = '%s';
        """
        material_log_sql = """
        select 
          date, array_agg(id), array_agg(type), array_agg(count), array_agg(time), array_agg(source), array_agg(source_id)
        from (
               select material_id                            as id,
                      type,
                      coalesce(count, 0)                     as count,
                      coalesce(time, 0)                      as time,
                      source, 
                      source_id,  """ + condition + """
               from base_materials_log
               where factory = '%s'
                 and material_id = '%s'
                 and (
                   type = 'actual' or
                   type = 'store_check'
                 )
               order by time desc) t
        group by date;
        """
        material_inout_sql = """
        select 
          coalesce(sum(case when count < 0 then count end), 0) as outgoing,
          coalesce(sum(case when count > 0 then count end), 0) as incoming
        from 
          base_materials_log
        where 
          type = 'actual' and factory = '%s' and material_id = '%s' and time >= %d and time < %d;
        """

        # print("product_log_sql=", product_log_sql % (factory_id, id_))
        # print("material_log_sql=", material_log_sql % (factory_id, id_))
        properties, data = {}, []
        try:
            if type_ == "product":
                cursor.execute(product_id_check)
                id_check = cursor.fetchone()[0]
                if id_check <= 0:
                    return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)
                cursor.execute(product_sql % (factory_id, id_))
                result = cursor.fetchone()
                properties["actual"], properties["pre_product"], properties["prepared"], properties["safety"], \
                    properties["available"], properties["id"], properties["name"], properties["unit"], properties[
                    "category"] = round(result[0], 2), round(result[1], 2), round(result[2], 2), round(result[3], 2), \
                    round(result[4], 2), result[5], result[6], result[7], result[8]
                properties["store"] = result[9]
                cursor.execute(product_log_sql % (factory_id, id_))
                result2 = cursor.fetchall()
                for res in result2:
                    di = dict()
                    di["flag"] = False
                    history = list()
                    date_split = res[0]
                    # 1: 月, 2: 年, 3：周
                    if date == "1" or date == "3":
                        di["year"] = date_split.split("-")[0]
                        di["mon_or_week"] = date_split.split("-")[1]
                    else:
                        di["year"] = date_split

                    if date == "1":
                        date_time = arrow.get(date_split)
                        start, end = date_time.timestamp, date_time.shift(months=1).timestamp
                    elif date == "2":
                        start, end = year_timestamp(int(date_split))
                    else:
                        w = Week(int(di["year"]), int(di["mon_or_week"]))
                        monday, sunday = w.monday(), w.sunday()
                        start, end = arrow.get(monday).timestamp, arrow.get(sunday).timestamp

                    cursor.execute(product_inout_sql % (factory_id, id_, start, end))
                    in_out = cursor.fetchone()
                    # print(in_out)
                    if in_out:
                        di["out"], di["in"] = abs(round(in_out[0], 2)), round(in_out[1], 2)
                    else:
                        di["out"], di["in"] = 0, 0

                    temp_list = list(zip(res[1], res[2], res[3], res[4], res[5], res[6]))
                    # print(temp_list)
                    for temp in temp_list:
                        dt = dict()
                        dt["id"] = temp[0]
                        if temp[4] == "0":
                            dt["parent_type"] = "产品入库"
                        elif temp[4] == "1":
                            dt["parent_type"] = "产品出库"
                        elif temp[4] == "2":
                            dt["parent_type"] = "库存盘点"
                        elif temp[4] == "3":
                            dt["parent_type"] = "生产任务单"
                        dt["count"] = temp[2]
                        dt["time"] = temp[3]
                        dt["source"] = temp[4]
                        dt["source_id"] = temp[5]
                        history.append(dt)
                    di["history"] = history
                    data.append(di)
                properties["data"] = data

                # 二维码json内容
                content = {"type": "10", "id": id_, "share": user_id}
                properties["qr_code"] = content

            elif type_ == "material":
                cursor.execute(material_id_check)
                id_check = cursor.fetchone()[0]
                if id_check <= 0:
                    return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)
                cursor.execute(material_sql % (factory_id, id_))
                result = cursor.fetchone()
                properties["actual"], properties["on_road"], properties["prepared"], properties["safety"], properties[
                    "available"], properties["id"], properties["name"], properties["unit"], properties["category"] = \
                    round(result[0], 2), round(result[1], 2), round(result[2], 2), round(result[3], 2), \
                    round(result[4], 2), result[5], result[6], result[7], result[8]
                properties["store"] = result[9]
                cursor.execute(material_log_sql % (factory_id, id_))
                result2 = cursor.fetchall()
                for res in result2:
                    di = dict()
                    di["flag"] = False
                    history = list()
                    date_split = res[0]
                    # 1: 月, 2: 年, 3：周
                    if date == "1" or date == "3":
                        di["year"] = date_split.split("-")[0]
                        di["mon_or_week"] = date_split.split("-")[1]
                    else:
                        di["year"] = date_split

                    if date == "1":
                        date_time = arrow.get(date_split)
                        start, end = date_time.timestamp, date_time.shift(months=1).timestamp
                    elif date == "2":
                        start, end = year_timestamp(int(date_split))
                    else:
                        w = Week(int(di["year"]), int(di["mon_or_week"]))
                        monday, sunday = w.monday(), w.sunday()
                        start, end = arrow.get(monday).timestamp, arrow.get(sunday).timestamp

                    cursor.execute(material_inout_sql % (factory_id, id_, start, end))
                    in_out = cursor.fetchone()
                    # print(in_out)
                    if in_out:
                        di["out"], di["in"] = abs(round(in_out[0], 2)) or 0, round(in_out[1], 2) or 0
                    else:
                        di["out"], di["in"] = 0, 0

                    temp_list = list(zip(res[1], res[2], res[3], res[4], res[5], res[6]))
                    # print(temp_list)
                    for temp in temp_list:
                        dt = dict()
                        dt["id"] = temp[0]
                        if temp[4] == "0":
                            dt["parent_type"] = "物料入库"
                        elif temp[4] == "1":
                            dt["parent_type"] = "物料出库"
                        elif temp[4] == "2":
                            dt["parent_type"] = "库存盘点"
                        elif temp[4] == "3":
                            dt["parent_type"] = "生产任务单"
                        elif temp[4] == "4":
                            dt["parent_type"] = "退料单"
                        dt["count"] = temp[2]
                        dt["time"] = temp[3]
                        dt["source"] = temp[4]
                        dt["source_id"] = temp[5]
                        history.append(dt)
                    di["history"] = history
                    data.append(di)
                properties["data"] = data

                # 二维码json内容
                content = {"type": "9", "id": id_, "share": user_id}
                properties["qr_code"] = content
            else:
                return Response({"res": 1, "errmsg": "类型错误！"}, status=status.HTTP_200_OK)

            return Response(properties, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreCheckMain(APIView):
    """库存盘点主页 store/check/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        sql = """
        select
          t1.id, (t1.after - t1.before) as count, t1.more_less, coalesce(t1.remark, '') as remark,
          coalesce(t1.time, 0) as time, coalesce(t1.approval_time, 0) as approval_time,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit
        from
          (
            select 
              *
            from 
              base_storage_check
            where 
              factory = '%s' and state = '%s'
          ) t1
        left join base_materials_pool t2 on
          t1.material_id = t2.id
        order by 
          time desc;
        """
        waited, approval_pass, approval_refuse = [], [], []
        state_dict = {"0": waited, "1": approval_pass, "2": approval_refuse}

        try:
            for state in state_dict:
                cursor.execute(sql % (factory_id, state))
                result = cursor.fetchall()
                for res in result:
                    di = dict()
                    di["id"] = res[0]
                    di["count"] = abs(res[1])
                    di["more_less"] = res[2]
                    di["remark"] = res[3]
                    di["time"] = res[4]
                    di["approval_time"] = res[5]
                    di["name"] = res[6]
                    di["unit"] = res[7]
                    state_dict[state].append(di)

            return Response({"0": waited, "1": approval_pass, "2": approval_refuse}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreCheckDetail(APIView):
    """库存盘点详情 store/check/detail"""

    @method_decorator(store_decorator)
    def get(self, request):
        id_ = request.query_params.get("id")
        if not id_:
            return Response({"res": 1, "errmsg": "缺少参数id！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        alioss = AliOss()
        cursor = connection.cursor()

        cursor.execute("select count(1) from base_storage_check where id = '%s';" % id_)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.material_id, t1.id, (t1.after - t1.before) as update, t1.state, t1.more_less, 
          coalesce(t1.remark, '') as remark, coalesce(t1.time, 0) as check_time, 
          coalesce(t1.approval_time, 0) as approval_time,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit,
          coalesce(t3.name, '') as check_person, t3.phone as check_phone,
          coalesce(t4.name, '') as approval_person, coalesce(t4.phone, '') as approval_phone,
          coalesce(t5.name, '') as category,
          t3.image, t4.image,
          coalesce(t1.reason, '') as reason
        from
          (
            select 
              *
            from 
              base_storage_check
            where 
              factory = '%s' and id = '%s'
          ) t1
        left join base_materials_pool t2 on 
          t1.material_id = t2.id
        left join user_info t3 on
          t1.creator = t3.user_id
        left join user_info t4 on
          t1.approval = t4.user_id
        left join base_material_category_pool t5 on
          t2.category_id = t5.id;
        """
        data = {}
        try:
            cursor.execute(sql % (factory_id, id_))
            result = cursor.fetchone()
            material_id = result[0]
            data["id"], data["update"], data["state"], data["more_less"], data["remark"], data["check_time"], \
                data["approval_time"], data["name"], data["unit"], data["check_person"], data["check_phone"], \
                data["approval_person"], data["approval_phone"], data["category"] = result[1], round(result[2], 2), \
                result[3], result[4], result[5], result[6], result[7], result[8], result[9], result[10], result[11], \
                result[12], result[13], result[14]
            data["check_image"] = alioss.joint_image(result[15].tobytes().decode()) if \
                isinstance(result[15], memoryview) else alioss.joint_image(result[15])
            data["approval_image"] = alioss.joint_image(result[16].tobytes().decode()) if \
                isinstance(result[16], memoryview) else alioss.joint_image(result[16])
            data["reason"] = result[17]
            cursor.execute(
                "select count(1) from base_products where factory = '%s' and id = '%s';" % (factory_id, material_id))
            products_check = cursor.fetchone()[0]
            cursor.execute(
                "select count(1) from base_materials where factory = '%s' and id = '%s';" % (factory_id, material_id))
            materials_check = cursor.fetchone()[0]
            if products_check >= 1:  # 产品
                cursor.execute(
                    "select price from base_products where factory = '%s' and id = '%s';" % (factory_id, material_id))
            elif materials_check >= 1:  # 物料
                cursor.execute(
                    "select price from base_materials where factory = '%s' and id = '%s';" % (factory_id, material_id))
            price = cursor.fetchone()[0]
            price = round(price if price else 0, 2)
            money = round(data["update"] * price, 2)
            data["price"], data["money"] = price, money

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    @method_decorator(store_approval_decorator)
    def post(self, request):
        """库存盘点 审批通过/不通过 需要审批人的权限
        0: 待审批，1: 审批通过(已审批)，2: 审批未通过(已审批)"""

        if request.flag is False:
            return Response({"res": "1", "errmsg": "你没有权限"}, status=status.HTTP_403_FORBIDDEN)

        id_ = request.data.get("id")  # 库存盘点单号id
        state = request.data.get("state")
        remark = request.data.get("remark", "")

        if not all([id_, state]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)
        if state not in ["1", "2"]:
            return Response({"res": 1, "errmsg": "状态代号错误！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        rabbitmq = UtilsRabbitmq()
        cursor = connection.cursor()

        cursor.execute(
            "select count(1) from base_storage_check where factory = '%s' and id = '%s';" % (factory_id, id_))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)
        cursor.execute(
            "select state, material_id, coalesce(after - before, 0) as update, type from base_storage_check where "
            "factory = '%s' and id = '%s';" % (factory_id, id_))
        result = cursor.fetchone()
        state_check, material_id, update, type_ = result[0], result[1], result[2], result[3]

        if state_check != "0":
            return Response({"res": 1, "errmsg": "状态错误！"}, status=status.HTTP_200_OK)

        timestamp = arrow.now().timestamp
        sql = """
        update
          base_storage_check
        set
          state = '%s', approval = '%s', approval_time = %d, remark = '%s'
        where 
          factory = '%s' and id = '%s'
        """ % (state, phone, timestamp, remark, factory_id, id_)

        update_products_sql = """
        update
          base_products_storage
        set
          actual = actual + %d
        where 
          factory = '%s' and product_id = '%s'
        """ % (update, factory_id, material_id)
        products_log_sql = """
        insert into
          base_products_log (product_id, type, count, source, source_id, factory, time) 
        values 
          ('%s', '%s', %f, '%s', '%s', '%s', %d);
        """ % (material_id, 'store_check', update, '2', id_, factory_id, timestamp)

        update_materials_sql = """
        update
          base_materials_storage
        set
          actual = actual + %d
        where 
          factory = '%s' and material_id = '%s'
        """ % (update, factory_id, material_id)
        materials_log_sql = """
        insert into
          base_materials_log (material_id, type, count, source, source_id, factory, time) 
        values 
          ('%s', '%s', %f, '%s', '%s', '%s', %d);
        """ % (material_id, 'store_check', update, '2', id_, factory_id, timestamp)

        try:
            cursor.execute(sql)  # 改变盘点单状态

            # 发送RabbitMQ消息
            if state == "1":  # 库存盘点审批通过
                message = {'resource': 'PyStoreCheck', 'type': 'POST',
                           'params': {'fac': factory_id, 'id': id_, 'state': '2'}}

                # 添加记录，增减库存
                if type_ == "product":  # 产品
                    cursor.execute(update_products_sql)
                    cursor.execute(products_log_sql)
                elif type_ == "material":  # 物料
                    cursor.execute(update_materials_sql)
                    cursor.execute(materials_log_sql)
            else:  # 库存盘点审批不通过
                message = {'resource': 'PyStoreCheck', 'type': 'POST',
                           'params': {'fac': factory_id, 'id': id_, 'state': '3'}}
            rabbitmq.send_message(json.dumps(message))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreCheckNew(APIView):
    """新增库存盘点 store/check/new/{type}"""
    permission_classes = [StorePermission]

    def post(self, request, type_):
        id_ = request.data.get("id")
        update = request.data.get("update")
        reason = request.data.get("reason")
        if not all([type_, id_, update, reason]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)
        if type_ not in ["material", "product"]:
            return Response({"res": 1, "errmsg": "参数type错误！"}, status=status.HTTP_200_OK)
        if update < 0:
            return Response({"res": 1, "errmsg": "盘点数量不能为负数！"}, status=status.HTTP_200_OK)
        if not isinstance(update, float) and not isinstance(update, int):
            return Response({"res": 1, "errmsg": "盘点数量只为为数字！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        seq_id = request.redis_cache["seq_id"]
        user_id = phone if not user_id else user_id
        cursor = connection.cursor()

        storage_check_sql = """
        insert into 
          base_storage_check (id, factory, material_id, type, before, after, more_less, creator, time, reason)
        values 
          ('%s', '%s', '%s', '%s', %f, %f, '%s', '%s', %d, '%s');
        """

        try:
            serial_number = generate_module_uuid(PrimaryKeyType.storage_check.value, factory_id, seq_id)
            timestamp = int(time.time())
            if type_ == "material":
                cursor.execute("select coalesce(actual, 0) as actual from base_materials_storage where factory = '%s' "
                               "and material_id = '%s';" % (factory_id, id_))
            elif type_ == "product":
                cursor.execute("select coalesce(actual, 0) as actual from base_products_storage where factory = '%s' "
                               "and product_id = '%s';" % (factory_id, id_))
            else:
                return Response({"res": 1, "errmsg": "类型错误！"}, status=status.HTTP_200_OK)

            before = cursor.fetchone()
            if before:
                before = before[0]
            else:
                return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)
            more_less = "0" if update > before else "1"

            cursor.execute(storage_check_sql % (serial_number, factory_id, id_, type_, before, update, more_less,
                                                user_id, timestamp, reason))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreTemporaryPurchaseMain(APIView):
    """临时申购 store/temporary_purchase/main"""
    permission_classes = [StorePermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        waited, approval, canceled = [], [], []
        state_dict = {"0": waited, "1": approval, "2": canceled}

        sql = """
        select
          id, coalesce(remark, '') as remark, coalesce(time, 0) as time, coalesce(approval_time, 0) as approval_time
        from
          base_store_temporary_purchase
        where 
          factory = '%s' and state = '%s'
        order by 
          time desc;
        """
        materials_sql = """
        select
          t1.count,
          coalesce(t2.name, '') as name, coalesce(t2.unit, '') as unit
        from
          (
            select
              material_id, count
            from
              base_store_temporary_purchase_materials
            where 
              purchase_id = '%s'
          ) t1
        left join base_materials_pool t2 on 
          t1.material_id = t2.id
        """

        try:
            for state in state_dict:
                cursor.execute(sql % (factory_id, state))
                result = cursor.fetchall()
                for res in result:
                    di, products = dict(), ""
                    di["id"] = res[0]
                    di["remark"] = res[1]
                    di["create_time"] = res[2]
                    di["approval_time"] = res[3]
                    cursor.execute(materials_sql % res[0])
                    materials = cursor.fetchall()
                    for ma in materials:
                        products += ma[1] + ":" + str(round(ma[0], 2)) + ma[2] + ";"

                    di["products"] = products
                    state_dict[state].append(di)

            return Response(state_dict, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreTemporaryPurchaseDetail(APIView):
    """临时申购详情 store/temporary_purchase/detail"""

    @method_decorator(store_decorator)
    def get(self, request):
        if request.flag is False:
            return Response({"res": "1", "errmsg": "你没有权限"}, status=status.HTTP_403_FORBIDDEN)

        id_ = request.query_params.get("id")
        if not id_:
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        alioss = AliOss()
        cursor = connection.cursor()

        cursor.execute("select count(1) from base_store_temporary_purchase where factory = '%s' and id = '%s';" % (
            factory_id, id_))
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        sql = """
        select
          t1.id, t1.state, coalesce(t1.remark, '') as remark, coalesce(t1.time, 0) as time, 
          coalesce(t1.approval_time, 0) as approval_time, 
          coalesce(t2.name, '') as creator, coalesce(t2.phone, '') as phone,
          coalesce(t3.name, '') as approval, coalesce(t3.phone, '') as approval_phone,
          coalesce(t1.reason, '') as reason, t2.image, t3.image
        from
          base_store_temporary_purchase t1
        left join user_info t2 on
          t1.creator = t2.user_id
        left join user_info t3 on 
          t1.approval = t3.user_id
        where 
          t1.factory = '%s' and t1.id = '%s';
        """ % (factory_id, id_)

        materials_sql = """
        select
          t1.count,
          coalesce(t3.name, '') as name, coalesce(t3.unit, '') as unit,
          coalesce(t4.name, '') as category,
          coalesce(t2.price, 0) as price
        from
          base_store_temporary_purchase_materials t1
        left join 
          (select * from base_materials where factory = '%s') t2 on t1.material_id = t2.id
        left join 
          base_materials_pool t3 on t2.id = t3.id
        left join 
          base_material_category_pool t4 on t3.category_id = t4.id
        where 
          t1.purchase_id = '%s';
        """ % (factory_id, id_)

        try:
            data, products, total_money = dict(), list(), 0
            cursor.execute(sql)
            result1 = cursor.fetchone()
            data["id"], data["state"], data["remark"], data["time"], data["approval_time"], data["creator"], \
            data["phone"], data["approval"], data["approval_phone"], data["reason"] = result1[0], result1[1], \
                                                                                      result1[2], result1[3], result1[
                                                                                          4], result1[5], result1[6], \
                                                                                      result1[7], result1[8], result1[9]
            data["creator_image"] = alioss.joint_image(result1[10].tobytes().decode()) if \
                isinstance(result1[10], memoryview) else alioss.joint_image(result1[10])
            data["approval_image"] = alioss.joint_image(result1[11].tobytes().decode()) \
                if isinstance(result1[11], memoryview) else alioss.joint_image(result1[11])

            cursor.execute(materials_sql)
            result2 = cursor.fetchall()
            for res in result2:
                di = dict()
                di["count"] = res[0]
                di["name"] = res[1]
                di["unit"] = res[2]
                di["category"] = res[3]
                di["price"] = res[4]
                di["money"] = round(di["count"] * di["price"], 2)
                total_money += di["money"]
                products.append(di)
            data["products"], data["total_money"] = products, total_money

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    @method_decorator(store_approval_decorator)
    def post(self, request):
        """临时申购-审批 通过/不通过
        0: 待审批, 1: 审批通过, 2: 审批不通过, 3: 已取消"""

        if request.flag is False:
            return Response({"res": "1", "errmsg": "你没有权限"}, status=status.HTTP_403_FORBIDDEN)

        id_ = request.data.get("id")
        state = request.data.get("state")
        remark = request.data.get("remark", "")
        if not all([id_, state]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)
        if state not in ["1", "2", "3"]:
            return Response({"res": 1, "errmsg": "状态代号错误！"}, status=status.HTTP_200_OK)

        seq_id = request.redis_cache["seq_id"]
        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        user_id = phone if not user_id else user_id

        rabbitmq = UtilsRabbitmq()
        cursor = connection.cursor()

        cursor.execute("select count(1) from base_store_temporary_purchase where id = '%s';" % id_)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

        cursor.execute("select state from base_store_temporary_purchase where id = '%s';" % id_)
        state_check = cursor.fetchone()[0]
        if state_check != "0":
            return Response({"res": 1, "errmsg": "状态不是待审批，无法操作！"}, status=status.HTTP_200_OK)

        update_sql = """
        update
          base_store_temporary_purchase
        set
          purchase_id = '%s', state = '%s', approval = '%s', approval_time = %d, remark = '%s'
        where
          id = '%s';
        """

        materials_sql = """
        select
          t2.material_id, coalesce(t2.count, 0) as count
        from
          base_store_temporary_purchase t1
        left join base_store_temporary_purchase_materials t2 on
          t1.id = t2.purchase_id
        where 
          t1.factory = '%s' and t1.id = '%s';
        """

        try:
            serial_number = generate_module_uuid(PrimaryKeyType.purchase.value, factory_id, seq_id)  # 采购单sn
            timestamp = arrow.now().timestamp

            if state == "1":  # 临时申购单审批通过，生成采购单
                cursor.execute(update_sql % (serial_number, '1', user_id, timestamp, remark, id_))
                cursor.execute(materials_sql % (factory_id, id_))
                result = cursor.fetchall()
                materials = [dict(zip(["id", "count"], res)) for res in result]
                # print(materials)

                from purchase.purchase_utils import create_purchase
                create_purchase(cursor, factory_id, seq_id, "", materials)

                message = {'resource': 'PyTemporaryPurchase', 'type': 'POST',
                           'params': {'fac': factory_id, 'id': id_, 'state': '2'}}
                rabbitmq.send_message(json.dumps(message))
            elif state == "2":  # 临时申购单审批不通过
                cursor.execute(update_sql % ('', '2', user_id, timestamp, remark, id_))
                message = {'resource': 'PyTemporaryPurchase', 'type': 'POST',
                           'params': {'fac': factory_id, 'id': id_, 'state': '3'}}
                rabbitmq.send_message(json.dumps(message))
            else:  # 取消
                cursor.execute(update_sql % ('', '3', user_id, timestamp, remark, id_))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class StoreTemporaryPurchaseNew(APIView):
    """新建临时申购 store/temporary_purchase/new"""
    permission_classes = [StorePermission]

    def post(self, request):
        reason = request.data.get("reason", "")
        materials = request.data.get("materials", [])

        seq_id = request.redis_cache["seq_id"]
        user_id = request.redis_cache["user_id"]
        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        user_id = phone if not user_id else user_id

        cursor = connection.cursor()

        sql = """
        insert into
          base_store_temporary_purchase (id, factory, state, reason, creator, time) 
        values 
          ('%s', '%s', '0', '%s', '%s', %d);
        """
        materil_sql = """
        insert into
          base_store_temporary_purchase_materials (purchase_id, material_id, count) 
        values 
          ('%s', '%s', %d);
        """

        try:
            serial_number = generate_module_uuid(PrimaryKeyType.temporary_purchase.value, factory_id, seq_id)

            cursor.execute(sql % (serial_number, factory_id, reason, user_id, int(time.time())))
            for material in materials:
                cursor.execute(materil_sql % (serial_number, material["id"], material["count"]))
            connection.commit()

            # 发送RabbitMQ消息
            rabbitmq = UtilsRabbitmq()
            message = {'resource': 'PyTemporaryPurchase', 'type': 'POST',
                       'params': {'fac': factory_id, 'id': serial_number, 'state': '1'}}
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class GenerateModuleUuid(APIView):
    """生成id store/generate_module_uuid"""

    def get(self, request):
        module_type = request.query_params.get("module_type")
        factory_id = request.query_params.get("factory_id")
        seq_id = request.query_params.get("seq_id")
        sn = generate_module_uuid(module_type, factory_id, seq_id)

        return Response(sn, status=status.HTTP_200_OK)
