# -*- coding: utf-8 -*-
import datetime
import json
import logging
import time

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, generate_uuid, UtilsRabbitmq
from constants import StoreNoticeMsgEnum
from material.material_utils import year_timestamp, week_timestamp
from order.order_utils import make_time

logger = logging.getLogger('django')


# 采购部-----------------------------------------------------------------------------------------------------------------
class MaterialMain(APIView):
    """物料采购管理首页数据列表 material/main
    putin = '1' 采购中
    putin = '0' 采购已完成
    """

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        sql = """
        select
          t1.*,
          COALESCE(t2.name, '') as name,
          t2.unit
        from
          (
          select 
            id,
            material_type_id,
            material_count as count,
            putin,
            buy_time as time
          from 
            purchase
          where factory = '%s') t1
        left join material_types t2 on 
          t1.material_type_id = t2.id order by time desc;
        """ % factory_id
        # print(sql)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        doing, done = [], []
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            for res in result:
                # print(res)
                di = dict()
                di["id"] = res[0]
                di["count"] = res[2]
                di["time"] = res[4]
                di["name"] = res[5]
                di["unit"] = res[6]
                if res[3] == "1":
                    doing.append(di)
                elif res[3] == "0":
                    done.append(di)
                else:
                    pass
            return Response({"doing": doing, "done": done}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialSummary(APIView):
    """物料管理首页 material/summary"""

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
            t1.id,
            t1.name,
            t1.unit,
            case when t.sum is null then 0 else t.sum end as count,
            t1.category_id,
            COALESCE(t2.name, '') as category_name,
            t1.low_limit
        from
          (
          select 
            material_type_id,
            sum(material_count)
          from 
            materials_log where factory = '%s' group by material_type_id
          ) t 
        right join material_types t1 on
          t.material_type_id = t1.id 
        left join material_categories t2 on
          t1.category_id = t2.id where t1.factory = '%s';
        """ % (factory_id, factory_id)
        # print(sql)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            data, low_stocks, parent_list, category_id_check, temp, multi_dict = {}, [], [], [], [], {}
            for res in result:
                di = dict()
                if res[3] < 0:
                    di["id"] = res[0]
                    di["name"] = res[1]
                    di["unit"] = res[2]
                    di["count"] = res[3]
                    low_stocks.append(di)
                else:
                    category_id_check.append(res[4])
                    temp.append(res)

            category_id_check = list(set(category_id_check))
            # print(category_id_check), print(temp)
            for category_id in category_id_check:
                li = list()
                for te in temp:
                    if te[4] == category_id:
                        li.append(te)
                        multi_dict[category_id] = li

            # print("multi_dict=", multi_dict)
            for multi in multi_dict:
                di = dict()
                child_list = list()
                di["category_id"] = multi
                di["category_name"] = multi_dict[multi][0][5]
                for mul in multi_dict[multi]:
                    dc = dict()
                    dc["id"] = mul[0]
                    dc["name"] = mul[1]
                    dc["unit"] = mul[2]
                    dc["count"] = mul[3]
                    if mul[3] >= mul[6]:
                        dc["stock_state"] = "0"
                    else:
                        dc["stock_state"] = "1"
                    child_list.append(dc)
                    di["list"] = child_list
                parent_list.append(di)

            data["low_stocks"], data["list"] = low_stocks, parent_list

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialList(APIView):
    """物料数据列表 material/list"""

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
          t1.*,
          COALESCE(t2.name, '') as name,
          t2.unit,
          t3.type,
          t3.parent_type
        from
          (
          select
            id,
            material_type_id,
            material_count as count,
            putin,
            buy_time as time
          from 
            purchase
          where factory = '%s') t1
        left join material_types t2 on 
          t1.material_type_id = t2.id 
        left join materials_log t3 on
          t1.id = t3.use_id
        order by time desc;
        """ % factory_id
        # print(sql)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            data, child_list = {}, []

            for res in result:
                di = dict()
                di["id"] = res[0]
                di["count"] = res[2]
                di["putin"] = res[3]
                di["time"] = res[4]
                di["name"] = res[5]
                di["unit"] = res[6]
                di["type"] = res[7] or ""
                di["parent_type"] = res[8] or ""
                child_list.append(di)

            data["year"] = datetime.datetime.now().year
            data["month"] = datetime.datetime.now().month
            data["list"] = child_list

            # print(data)
            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialDetail(APIView):
    """采购明细 material/detail"""

    def get(self, request):
        purchase_id = request.query_params.get("id")  # 明细id
        purchase_type = request.query_params.get("type", "id")  # id(默认): 根据taskid获取，notice: 根据通知id获取
        if not purchase_id:
            return Response({"res": 1, "errmsg": "lack of purchase_id! 缺少采购明细id！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
          t.material_type_id as type_id,
          t.material_count as count,
          t.total_price,
          t.buy_time as time,
          t.buyer,
          t.remark,
          t.putin,
          t.putin_time,
          t.creator as creator_id,
          t1.name,
          t1.unit,
          t3.name as creator,
          COALESCE(t2.contacts, '') as contacts,
          COALESCE(t2.id, '') as supplier,
          COALESCE(t2.name, '') as supplier_name,
          COALESCE(t2.phone, '') as phone,
          COALESCE(t2.position, '') as position
        from 
          purchase t
        left join material_types t1 on 
          t.material_type_id = t1.id
        left join factory_supplier t2 on 
          t.supplier = t2.id 
        left join user_info t3 on 
          t.creator = t3.phone
        """

        if purchase_type == "id":
            sql += " where t.id = '%s';" % purchase_id
        elif purchase_type == "notice":
            sql += " where t.notice_id = '%s';" % purchase_id
        else:
            return Response({"res": 0, "errmsg": "Type code error! 类型代号错误！"}, status=status.HTTP_200_OK)
        # print(sql)

        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            data = dict()
            data["type_id"] = result[0]
            data["count"] = result[1]
            data["total_price"] = round(result[2], 2)
            data["unit_price"] = round(result[2] / result[1], 2)
            data["time"] = result[3]
            data["buyer"] = result[4]
            data["remark"] = result[5] or ""
            data["putin"] = result[6]  # 0: 已入库, 1: 未入库
            data["putin_time"] = result[7]
            data["creator_id"] = result[8]
            data["name"] = result[9]
            data["unit"] = result[10]
            data["creator"] = result[11] if result[11] else phone
            data["contacts"] = result[12]
            data["supplier"] = result[13]
            data["supplier_name"] = result[14]
            data["phone"] = result[15]
            data["position"] = result[16]
            # 0: 可读写, 1: 只可读
            if "1" in permission:
                data["flag"] = "0"
            elif result[8] == phone:
                data["flag"] = "0"
            else:
                data["flag"] = "1"

            # print(result), print(data)
            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialNew(APIView):
    """新增采购物料 V3.3.0有改动 material/new"""

    def post(self, request):
        type_id = request.data.get("type_id", "")  # 物料类型ID
        count = int(request.data.get("count", 0))  # 物料数量
        unit_price = int(request.data.get("unit_price", 0))  # 单价
        buy_time = request.data.get("time", 0)  # 采购时间
        buyer = request.data.get("buyer", "")  # 采购人
        supplier_id = request.data.get("supplier_id", "")  # 供应商ID
        remark = request.data.get("remark", "")  # 备注
        notice_id = request.data.get("notice_id", "")  # 消息通知id

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_types where id = '%s';" % type_id)
        type_id_check = cursor.fetchone()[0]
        # print("type_id_check=", type_id_check)
        if type_id_check == 0:
            return Response({"res": 1, "errmsg": "type_id doesn't exists! 物料类型ID不存在！"}, status=status.HTTP_200_OK)
        if count <= 0:
            return Response({"res": 1, "errmsg": "count require more than 0! 物料数量需要大于0！"}, status=status.HTTP_200_OK)
        if unit_price <= 0:
            return Response({"res": 1, "errmsg": "unit_price require more than 0! 单价需要大于0！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        try:
            uuid = generate_uuid()
            timestamp = int(time.time())
            total_price = count * unit_price
            purchase_sql = "insert into purchase (id, factory, material_type_id, material_count, total_price, buyer," \
                           " buy_time, supplier, remark, time, creator, notice_id) values ('%s', '%s', '%s', %s, %s," \
                           " '%s', %d, '%s', '%s', %d, '%s', '%s');" % (uuid, factory_id, type_id, str(count),
                                                                        str(total_price), buyer, buy_time, supplier_id,
                                                                        remark, timestamp, phone, notice_id)
            finance_logs_sql = "insert into finance_logs (factory, use_id, type, count, time, parent_type) values " \
                               "('%s', '%s', '采购', %s, %d, 'material');" % (
                                   factory_id, uuid, str(-total_price), timestamp)
            notice_sql = "update store_notice set state = '%s' where id = '%s';" % (
                StoreNoticeMsgEnum.msg_done.value, notice_id)

            # print(purchase_sql), print(finance_logs_sql)
            cursor.execute(purchase_sql)
            cursor.execute(finance_logs_sql)
            cursor.execute(notice_sql)

            connection.commit()

            message = {'resource': 'PyMaterialNew', 'type': 'POST',
                       'params': {'Fac': factory_id, 'UID': uuid, 'Count': count, 'TotalPrice': total_price,
                                  'TypeID': type_id, "UserID": phone}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialDelete(APIView):
    """删除采购单 material/del"""

    def post(self, request):
        purchase_id = request.data.get("id")  # 采购id
        if not purchase_id:
            return Response({"res": 1, "errmsg": "param need purchase_id! 需要采购ID！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select count(1) from purchase where id = '%s';" % purchase_id)
            purchase_id_check = cursor.fetchone()[0]
            if purchase_id_check == 0:
                return Response({"res": 1, "errmsg": "purchase_id doesn't exist! 采购ID不存在，无法删除！"},
                                status=status.HTTP_200_OK)

            cursor.execute("delete from purchase where id = '%s';" % purchase_id)
            cursor.execute("delete from materials_log where use_id = '%s';" % purchase_id)
            cursor.execute("delete from finance_logs where use_id = '%s';" % purchase_id)
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialModify(APIView):
    """修改采购单 material/modify"""

    def post(self, request):
        purchase_id = request.data.get("id")  # 采购id
        type_id = request.data.get("type_id")  # 物料类型ID
        count = int(request.data.get("count"))  # 物料数量
        unit_price = int(request.data.get("unit_price"))  # 单价
        buy_time = request.data.get("time")  # 采购时间
        buyer = request.data.get("buyer")  # 采购人
        supplier_id = request.data.get("supplier_id")  # 供应商ID
        remark = request.data.get("remark")  # 备注
        putin = request.data.get("putin")  # 0: 已入库 1: 未入库
        if not purchase_id:
            return Response({"res": 1, "errmsg": "param need purchase_id! 需要采购ID！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from purchase where id = '%s';" % purchase_id)
        purchase_id_check = cursor.fetchone()[0]
        cursor.execute("select count(1) from material_types where id = '%s';" % type_id)
        type_id_check = cursor.fetchone()[0]
        if purchase_id_check <= 0:
            return Response({"res": 1, "errmsg": "purchase_id doesn't exist! 采购ID不存在！"}, status=status.HTTP_200_OK)
        if type_id_check <= 0:
            return Response({"res": 1, "errmsg": "type_id doesn't exist! 物料类型ID不存在！"}, status=status.HTTP_200_OK)
        if count <= 0:
            return Response({"res": 1, "errmsg": "count require more than 0! 物料数量需要大于0！"}, status=status.HTTP_200_OK)
        if unit_price <= 0:
            return Response({"res": 1, "errmsg": "unit_price require more than 0! 单价需要大于0！"}, status=status.HTTP_200_OK)

        try:
            total_price = count * unit_price
            timestamp = int(time.time())
            cursor.execute("update purchase set material_type_id = '%s', material_count = %s, total_price = %s, "
                           "buy_time = %d, remark = '%s', time = %d, buyer = '%s', supplier = '%s' where id = '%s';"
                           % (type_id, str(count), str(total_price), buy_time, remark, timestamp, buyer, supplier_id,
                              purchase_id))
            cursor.execute("update finance_logs set count = %s, time = %d where use_id = '%s';" % (
                -total_price, timestamp, purchase_id))
            cursor.execute("select putin from purchase where id = '%s';" % purchase_id)
            origin_putin_state = cursor.fetchone()[0]
            if origin_putin_state == "0":
                cursor.execute("update materials_log set material_type_id = '%s', material_count = %s, time = %d "
                               "where use_id = '%s';" % (type_id, count, timestamp, purchase_id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialTypeNew(APIView):
    """新增物料类型 material/type/new"""

    def post(self, request):
        name = request.data.get("name")  # 物料类型名称
        unit = request.data.get("unit")  # 物料单位
        category_id = request.data.get("category_id", "")  # 物料类别ID
        if not all([name, unit]):
            return Response({"res": 1, "errmsg": "lack of params name or unit! 缺少物料类型名称或物料单位！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_types where factory = '%s' and name = '%s';" % (factory_id, name))
        name_check = cursor.fetchone()[0]
        # print(name_check)
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "material_type already exist! 物料类型名称已存在！"}, status=status.HTTP_200_OK)
        # if not len(unit):
        #     return Response({"res": 1, "errmsg": "unit length need more than 0! 请填写单位！"}, status=status.HTTP_200_OK)
        try:
            uuid = generate_uuid()
            cursor.execute("insert into material_types (id, factory, name, unit, time, category_id, creator_id) "
                           "values ('%s', '%s', '%s', '%s', %d, '%s', '%s');" % (
                               uuid, factory_id, name, unit, int(time.time()), category_id, phone))
            connection.commit()
            # todo create_qrcode创建二维码，但是手机上看不到这个功能......

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialTypeModify(APIView):
    """修改物料类型信息 material/type/modify"""

    def post(self, request):
        id = request.data.get("id")  # 物料id
        name = request.data.get("name")  # 物料类型名称
        unit = request.data.get("unit")  # 物料单位
        category_id = request.data.get("category_id", "")  # 物料类别ID
        if not all([id, name, unit]):
            return Response({"res": 1, "errmsg": "lack of params! 缺少参数！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_types where factory = '%s' and id = '%s';" % (factory_id, id))
        name_check = cursor.fetchone()[0]
        if name_check == 0:
            return Response({"res": 1, "errmsg": "material_type_id doesn't exist! 物料id不存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("update material_types set name = '%s', unit = '%s', time = %d, category_id = '%s' "
                           "where id = '%s';" % (name, unit, int(time.time()), category_id, id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialTypes(APIView):
    """物料类型列表 material/types"""

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select id, name, unit from material_types where factory = '%s' order by name;" % factory_id)
            result = cursor.fetchall()
            types_list = []
            for res in result:
                di = dict()
                di["id"] = res[0]
                di["name"] = res[1]
                di["unit"] = res[2]
                types_list.append(di)

            return Response({"list": types_list}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialStatus(APIView):
    """物料统计 material/stats"""

    def get(self, request):
        material_id = request.query_params.get("id")  # 物料id
        statistics_type = request.query_params.get("type", "1")  # 1: 按月， 2: 按年, 3：按周

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        if statistics_type not in ["1", "2", "3"]:
            return Response({"res": 1, "errmsg": "type errors! 时间类型代号选择错误！"}, status=status.HTTP_200_OK)

        log_sql = """
        select
          t1.use_id as id,
          t1.material_count as count,
          t1.type,
          COALESCE(t3.buy_time, t1.time) as time,
          t2.count as cost,
          t1.parent_type,
          t3.putin 
        from 
          materials_log t1
        left join finance_logs t2 on 
          t1.use_id = t2.use_id 
        left join purchase t3 on 
          t1.use_id = t3.id
        where t1.factory = '%s' and t1.material_type_id = '%s'
        order by t1.time desc;
        """ % (factory_id, material_id)

        purchase_sql = """
        select
            id,
            material_count as count,
            '采购' as type,
            time,
            total_price as cost,
            'material' as parent_type,
            putin
        from
          purchase
        where factory = '%s' and putin = '1' and material_type_id = '%s';
        """ % (factory_id, material_id)

        material_sql = """
        select 
            t1.*,
            COALESCE(t2.name, '') as category_name
        from
          (
          select 
            name,
            unit,
            category_id
          from 
            material_types
          where id = '%s'
          ) t1
        left join material_categories t2 on 
          t1.category_id = t2.id;
        """ % material_id
        # print(log_sql), print(purchase_sql), print(material_sql)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            result, data = {}, []
            cursor.execute(log_sql)
            log_result = cursor.fetchall()
            cursor.execute(purchase_sql)
            purchase_result = cursor.fetchall()
            cursor.execute(material_sql)
            material_result = cursor.fetchone()
            if material_result:
                result["name"] = material_result[0]
                result["unit"] = material_result[1]
                result["category_id"] = material_result[2]
                result["category_name"] = material_result[3]
            else:
                result["name"], result["unit"], result["category_id"], result["category_name"] = "", "", "", ""

            putin, notputin, total, cost, orders = 0, 0, 0, 0, 0  # 已入库物料数量，未入库物料数量，总数量(相加)，采购数量
            add_list = log_result + purchase_result
            new_list = []

            for new in add_list:
                di = dict()
                di["id"] = new[0] or ""
                di["count"] = new[1] or 0
                di["time"] = new[3]
                di["cost"] = new[4] or 0
                di["parent_type"] = new[5] or ""
                di["putin"] = new[6] or ""
                new_list.append(di)

            # 0: 已入库 1: 未入库
            res = []
            for i in new_list:
                if i["parent_type"] == "material":
                    cost += abs(i["cost"])

                if i["putin"] == "1":
                    notputin += i["count"]
                else:
                    putin += i["count"]

                if statistics_type == "1":
                    key = time.strftime("%Y_%m", time.localtime(i["time"]))
                elif statistics_type == "3":
                    year, week = datetime.datetime.fromtimestamp(i["time"]).isocalendar()[:2]
                    key = "{}_{}".format(year, week)
                else:
                    key = time.strftime("%Y", time.localtime(i["time"]))

                for j in res:
                    if j[0] == key:
                        j[1].append(i)
                        break
                else:
                    res.append([key, [i]])

            for i in res:
                date = i[0].split("_")
                if len(date) == 1:
                    tmp = {"year": date[0], "list": i[1]}
                else:
                    tmp = {"year": date[0], "mon_or_week": date[1], "list": i[1]}

                count, orders, cost = 0, 0, 0  # 总量, 采购数, 总支出
                for j in i[1]:
                    if j["count"] > 0:
                        count += j["count"]
                    if j["parent_type"] == "material":
                        orders += 1
                    cost += j["cost"] if j["cost"] else 0
                summary = {"count": count, "cost": cost, "orders": orders}
                tmp.update(summary)
                data.append(tmp)

            # print(new_list), print(putin, notputin, total, cost, orders)
            result["count"], result["notputin"], result["total"], result["data"] = int(abs(putin)), int(notputin), int(
                abs(putin)) + int(notputin), data

            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialBill(APIView):
    """每月财务数据列表 material/bill"""

    def get(self, request):
        time_type = request.query_params.get("type", "1")  # 1: 按月， 2: 按年
        year = request.query_params.get("year", datetime.datetime.now().year)  # 年份
        month = request.query_params.get("month", datetime.datetime.now().month)  # 月份
        # print("time_type=", time_type, "year=", year, "month=", month)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        if time_type == "1":
            start_time, end_time = make_time(int(year), int(month))
        elif time_type == "2":
            start_time, end_time = year_timestamp(int(year))
        else:
            return Response({"res": 1, "errmsg": "time_type code errors! 时间年月代号错误！"}, status=status.HTTP_200_OK)
        # print("start_time=", time.localtime(start_time)), print("end_time=", time.localtime(end_time))

        try:
            sql = """
            select
              name,
              sum(total_price) as cost
            from
              (
              select 
                t2.name,
                t1.total_price
              from 
                purchase t1
              left join material_types t2 on 
                t1.material_type_id = t2.id
              where 
                t1.factory = '%s' and t1.buy_time > %d and t1.buy_time <= %d
              ) t 
            group by 
              name
            order by 
              cost
            desc;
            """ % (factory_id, start_time, end_time)
            # print(sql)

            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            bill_list, sum_price = [], 0
            for res in result:
                sum_price += res[1]
            for res in result:
                di = dict()
                di["name"] = res[0]
                di["cost"] = res[1]
                di["rate"] = ("%.2f" % (res[1] / sum_price * 100)) + "%"
                bill_list.append(di)

            return Response({"list": bill_list}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialPutin(APIView):
    """采购单入库 material/putin"""

    def post(self, request):
        purchase_id = request.data.get("id")  # 采购id
        putin = request.data.get("putin", "0")  # 0: 已入库 1: 未入库
        putin_time = request.data.get("time", int(time.time()))  # 入库时间
        if not purchase_id:
            return Response({"res": 1, "errmsg": "param need purchase_id! 需要采购id！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from purchase where id = '%s';" % purchase_id)
        id_check = cursor.fetchone()[0]
        # print(id_check)
        if id_check == 0:
            return Response({"res": 1, "errmsg": "purchase_id doesn't exist! 采购id不存在"}, status=status.HTTP_200_OK)

        try:
            cursor.execute(
                "select material_type_id, material_count, putin from purchase where id = '%s';" % purchase_id)
            result = cursor.fetchone()
            material_type_id, material_count, putin_state = result[0], result[1], result[2]
            if putin_state == putin:
                pass
            else:
                if putin == "0":
                    cursor.execute("insert into materials_log (id, factory, use_id, type, material_type_id, "
                                   "material_count, time, parent_type) values ('%s', '%s', '%s', '采购', '%s', %s, %d, "
                                   "'material')" % (generate_uuid(), factory_id, purchase_id, material_type_id,
                                                    str(material_count), int(time.time())))
                else:
                    cursor.execute("delete from materials_log where use_id = '%s';" % purchase_id)

            cursor.execute("update purchase set putin = '%s', putin_time = %d where id = '%s';"
                           % (putin, putin_time, purchase_id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialCategories(APIView):
    """物料类别列表 material/categories"""

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select id, name from material_categories where factory = '%s';" % factory_id)
            result = cursor.fetchall()
            categories_list = []
            for res in result:
                di = dict()
                di["id"] = res[0]
                di["name"] = res[1]
                categories_list.append(di)

            return Response({"list": categories_list}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialCategoryNew(APIView):
    """新建物料类别 material/category/new"""

    def post(self, request):
        name = request.data.get("name")  # 物料类别名称
        if not name:
            return Response({"res": 1, "errmsg": "lack of category_name! 缺少物料类别名称！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(
            "select count(1) from material_categories where factory = '%s' and name = '%s';" % (factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "category_name already exist! 物料类别名称已存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("insert into material_categories values ('%s', '%s', '%s', %d);" % (
                generate_uuid(), factory_id, name, int(time.time())))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialCategoryDelete(APIView):
    """删除物料类别 material/category/del"""

    def post(self, request):
        id = request.data.get("id")  # 物料类别ID
        if not id:
            return Response({"res": 1, "errmsg": "lack of category_name! 缺少物料类别名称！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(
            "select count(1) from material_categories where factory = '%s' and id = '%s';" % (factory_id, id))
        id_check = cursor.fetchone()[0]
        # print("id_check=", id_check)
        if id_check == 0:
            return Response({"res": 1, "errmsg": "category_id doesn't exist! 物料类别id不存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("delete from material_categories where id = '%s';" % id)
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class MaterialCategoryModify(APIView):
    """修改物料类别名称 material/category/modify"""

    def post(self, request):
        id = request.data.get("id")  # 物料类别ID
        name = request.data.get("name")  # 物料类别名称
        if not all([id, name]):
            return Response({"res": 1, "errmsg": "lack of params category_id or category_name! 缺少物料类别id或物料类别名称！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(
            "select count(1) from material_categories where factory = '%s' and id = '%s';" % (factory_id, id))
        id_check = cursor.fetchone()[0]
        # print("id_check=", id_check)
        if id_check == 0:
            return Response({"res": 1, "errmsg": "category_id doesn't exist! 物料类别id不存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute(
                "update material_categories set name = '%s', time = %d where id = '%s';" % (name, int(time.time()), id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierList(APIView):
    """供应商管理 列表展示 supplier/list"""

    def get(self, request):
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select id, name, contacts from factory_supplier where factory = '%s' "
                           "order by name desc;" % factory_id)
            result = cursor.fetchall()
            data = list()
            for res in result:
                di = dict()
                di["id"] = res[0] or ""
                di["name"] = res[1] or ""
                di["contacts"] = res[2] or ""
                data.append(di)

            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierNew(APIView):
    """供应商管理 新增供应商 supplier/new"""

    def post(self, request):
        name = request.data.get("name")  # 供应商名称
        contacts = request.data.get("contacts")  # 联系人
        supplier_phone = request.data.get("phone")  # 供应商手机号
        wechat = request.data.get("wechat", "")  # 微信号
        position = request.data.get("position", "")  # 职位
        remark = request.data.get("remark", "")  # 备注
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址
        if not all([name, contacts, supplier_phone]):
            return Response({"res": 1, "errmsg": "Lack of name or contacts or phone! 缺少参数供应商名称或联系人或电话号码！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("insert into factory_supplier values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, "
                           "'%s', '%s', '%s');" % (generate_uuid(), factory_id, name, contacts, supplier_phone, wechat,
                                                   position, remark, int(time.time()), region, address, phone))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierDetail(APIView):
    """供应商管理 详情 supplier/detail"""

    def get(self, request):
        supplier_id = request.query_params.get("id")  # 供应商id

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        try:
            sql = """
            select
              t1.*,
              coalesce(t2.name, '') as creator
            from
              factory_supplier t1
            left join user_info t2 on 
              t1.creator_id = t2.phone
            where 
              t1.id = '%s';
            """ % supplier_id
            data = {}
            cursor.execute(sql)
            result = cursor.fetchone()

            creator = result[12]
            if not creator:
                creator = "null"

            if "1" in permission:
                data["flag"] = 0
            elif creator == phone:
                data["flag"] = 0
            else:
                data["flag"] = "1"

            data["name"] = result[2] or ""
            data["contacts"] = result[3] or ""
            data["phone"] = result[4] or ""
            data["wechat"] = result[5] or ""
            data["position"] = result[6] or ""
            data["remark"] = result[7] or ""
            data["region"] = result[9] or ""
            data["address"] = result[10] or ""
            data["creator"] = creator

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierDelete(APIView):
    """供应商管理 删除 supplier/del"""

    def post(self, request):
        supplier_id = request.data.get("id")  # 供应商id
        if not supplier_id:
            return Response({"res": 1, "errmsg": "Lack of supplier id! 缺少参数供应商id！"})

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from factory_supplier where id = '%s';" % supplier_id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "Supplier id doesn't exist! 此供应商id不存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("delete from factory_supplier where id = '%s';" % supplier_id)
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierModify(APIView):
    """供应商管理 修改 supplier/modify"""

    def post(self, request):
        supplier_id = request.data.get("id")  # 供应商id
        name = request.data.get("name")  # 供应商名称
        contacts = request.data.get("contacts")  # 联系人
        supplier_phone = request.data.get("phone")  # 供应商手机号
        wechat = request.data.get("wechat", "")  # 微信号
        position = request.data.get("position", "")  # 职位
        remark = request.data.get("remark", "")  # 备注
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址

        if not all([supplier_id, name, contacts, supplier_phone]):
            return Response({"res": 1, "errmsg": "Lack of params! 缺少参数！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from factory_supplier where id = '%s';" % supplier_id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "Supplier id doesn't exist! 此供应商id不存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("update factory_supplier set name = '%s', contacts = '%s', phone = '%s', wechat = '%s',"
                           "position = '%s', remark = '%s', time = %d, region = '%s', address = '%s' where id = '%s';"
                           % (name, contacts, supplier_phone, wechat, position, remark, int(time.time()), region,
                              address, supplier_id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
