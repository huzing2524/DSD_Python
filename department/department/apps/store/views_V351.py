# -*- coding: utf-8 -*-
# @Time   : 19-5-15 上午11:27
# @Author : huziying
# @File   : views_V351.py

import logging
import arrow
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db import connection

from permissions import StorePermission

logger = logging.getLogger('django')


class MultiStorage(APIView):
    """库存列表-选择仓库 store/multi_storage/select"""
    permission_classes = [StorePermission]

    def get(self, request):
        """多仓库列表"""
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        store_sql = "select uuid, name from base_multi_storage where factory = '{}';".format(factory_id)
        material_sql = """
        select
          t1.uuid, t1.name, count(t2.material_id) as materials
        from
          (select * from base_multi_storage where factory = '{}') t1
        left join 
          (select * from base_materials_storage where factory = '{}') t2 on t1.uuid = t2.uuid
        group by 
          t1.uuid, t1.name;
        """.format(factory_id, factory_id)
        product_sql = """
        select
          t1.uuid, t1.name, count(t2.product_id) as products
        from
          (select * from base_multi_storage where factory = '{}') t1
        left join 
          (select * from base_products_storage where factory = '{}') t2 on t1.uuid = t2.uuid
        group by 
          t1.uuid, t1.name;
        """.format(factory_id, factory_id)

        store_uuid, multi = [], []
        target_1, target_2 = ["id", "name", "materials"], ["id", "name", "products"]
        try:
            cursor.execute(
                "select count(1) from base_multi_storage where factory = '{}' and uuid = 'default';".format(factory_id))
            default_check = cursor.fetchone()[0]
            if default_check <= 0:
                cursor.execute("insert into base_multi_storage (uuid, name, time, factory) values "
                               "('default', '默认仓库', {}, '{}') ;".format(arrow.now().timestamp, factory_id))
                connection.commit()

            cursor.execute(store_sql)
            result_1 = cursor.fetchall()
            store_uuid = dict(result_1)

            cursor.execute(material_sql)
            result_2 = cursor.fetchall()
            materials = [dict(zip(target_1, res)) for res in result_2]

            cursor.execute(product_sql)
            result_3 = cursor.fetchall()
            products = [dict(zip(target_2, res)) for res in result_3]

            for uuid in store_uuid:
                di = dict()
                materials_count, products_count = 0, 0
                for ma in materials:
                    if ma["id"] == uuid:
                        materials_count += ma["materials"]
                for pr in products:
                    if pr["id"] == uuid:
                        products_count += pr["products"]
                di["id"] = uuid
                di["name"] = store_uuid[uuid]
                di["materials"] = materials_count
                di["products"] = products_count
                multi.append(di)
            return Response(multi, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        """新建仓库"""
        name = request.data.get("name")
        if not name:
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)
        if len(name) > 30:
            return Response({"res": 1, "errmsg": "名称太长！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        cursor.execute(
            "select count(1) from base_multi_storage where factory = '{}' and name = '{}';".format(factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "名称已存在！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("insert into base_multi_storage (uuid, name, factory, time) VALUES (uuid_generate_v4(), "
                           "'{}', '{}', {});".format(name, factory_id, arrow.now().timestamp))

            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def put(self, request):
        """修改仓库名称"""
        id_ = request.data.get("id")
        name = request.data.get("name")
        if not all([id_, name]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        cursor.execute(
            "select count(1) from base_multi_storage where factory = '{}' and uuid = '{}';".format(factory_id, id_))
        id_check = cursor.fetchone()[0]
        if id_check == 0:
            return Response({"res": 1, "errmsg": "要修改的此仓库不存在！"}, status=status.HTTP_200_OK)

        # 同一工厂内 仓库名称不能重复
        cursor.execute(
            "select count(*) from base_multi_storage where factory = '{}' and name = '{}';".format(factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({'res': 1, 'errmsg': '仓库名称重复！'})

        try:
            cursor.execute("update base_multi_storage set name = '{}' where factory = '{}' and "
                           "uuid = '{}';".format(name, factory_id, id_))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def delete(self, request):
        """删除仓库"""
        id_ = request.query_params.get("id")
        if not id_:
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        try:
            cursor.execute(
                "select count(1) from base_multi_storage where factory = '{}' and uuid = '{}';".format(factory_id, id_))
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "此id不存在！"}, status=status.HTTP_200_OK)

            cursor.execute(
                "delete from base_multi_storage where factory = '{}' and uuid = '{}';".format(factory_id, id_))
            cursor.execute(
                "update base_materials_storage set uuid = 'default' where factory = '{}' and uuid = '{}';".format(
                    factory_id, id_))
            cursor.execute(
                "update base_products_storage set uuid = 'default' where factory = '{}' and uuid = '{}';".format(
                    factory_id, id_))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class MultiStorageMove(APIView):
    """库存详情-仓库转移 store/multi_storage/move"""

    def put(self, request):
        store_id = request.data.get("store_id")  # 仓库id
        id_ = request.data.get("id")  # 物料/产品id
        type_ = request.data.get("type")  # 物料: material, 产品: product

        if not all([store_id, id_, type_]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        cursor = connection.cursor()

        try:
            cursor.execute(
                "select count(1) from base_multi_storage where factory = '{}' and uuid = '{}';".format(factory_id,
                                                                                                       store_id))
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "此仓库不存在！"}, status=status.HTTP_200_OK)

            if type_ == "material":
                cursor.execute(
                    "update base_materials_storage set uuid = '{}' where factory = '{}' and material_id = '{}';".format(
                        store_id, factory_id, id_))
            elif type_ == "product":
                cursor.execute(
                    "update base_products_storage set uuid = '{}' where factory = '{}' and product_id = '{}';".format(
                        store_id, factory_id, id_))
            else:
                return Response({"res": 1, "errmsg": "type参数错误！"}, status=status.HTTP_200_OK)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "服务器错误！"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()
