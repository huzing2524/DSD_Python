# -*- coding: utf-8 -*-

import datetime
import json
import logging
import re
import time
import jwt

from django.conf import settings
from django_redis import get_redis_connection
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db import connection

from apps_utils import AliOss, UtilsRabbitmq
from constants import RIGHTS_DICT, FACTORY_HBYL, FACTORY_WHBY, HBYL_RIGHTS, WHBY_RIGHTS
from permissions import ManagementPermission

logger = logging.getLogger('django')

# 权限管理---------------------------------------------------------------------------------------------------------------

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


class GenerateToken(APIView):
    """generate/token
    Token:
        过期时间 365 days * 10
        添加user_id字段
    localStorage.setItem('Authorization', 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6IjEzMjI3MTQyNzE2IiwidXNlcl9pZCI6IjRiZmRiNDdiLWI4ODktNDBhYS1hYmVlLTVmNjcxYmEwZGRiYiIsImV4cCI6MTkwNjg4MjIxMn0.Z1SdEnatiWXbzwcxWrT9f3Ardnya-pDO9H2bHkKgBKw')
    """
    permission_classes = []

    def get(self, request):
        phone = request.query_params.get("phone")
        user_id = request.query_params.get("user_id")
        payload = {"username": phone, "user_id": user_id,
                   "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)}
        jwt_token = jwt.encode(payload, settings.JWT_SECRET_KEY)
        print("jwt_token=", jwt_token)

        return Response(jwt_token, status=status.HTTP_200_OK)


class RightsInfo(APIView):
    """权限信息 rights/info"""
    permission_classes = [ManagementPermission]

    def get(self, request):
        phone = request.query_params.get("phone")  # 手机号码
        if not re.match("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$", phone):
            return Response({"res": 1, "errmsg": "bad phone number format! 电话号码格式错误"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]

        alioss = AliOss()
        cursor = connection.cursor()

        sql = """
        select 
          t.rights,
          t.phone,
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image
        from
          (
          select 
            *
          from 
            factory_users
          where 
            factory = '%s' and phone = '%s'
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone;
        """ % (factory_id, phone)

        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            # print(result)
            data = {}
            if result:
                data["rights"] = result[0]
                data["phone"] = result[1]
                data["name"] = result[2]
                data["image"] = alioss.joint_image(result[3].tobytes().decode()) if isinstance(result[3], memoryview) \
                    else alioss.joint_image(result[3])

                return Response(data, status=status.HTTP_200_OK)
            else:
                return Response({"res": 1, "errmsg": "此号码不存在！"}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class RightsList(APIView):
    """权限展示列表 rights/list"""
    permission_classes = [ManagementPermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()
        alioss = AliOss()

        sql = """
        select 
          t.rights,
          t.phone,
          COALESCE(t1.name, '') as name,
          COALESCE(t1.image, '') as image
        from
          (
          select 
            *
          from 
            factory_users
          where 
            factory = '%s'
          ) t
        left join 
          user_info t1 on 
        t.phone = t1.phone;
        """ % factory_id
        # print(sql)
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            # print(result)
            master, admins = [], []  # 超级管理员，普通用户
            for res in result:
                di, desc = dict(), ""
                di["rights"] = res[0]
                for r in res[0]:
                    if r in RIGHTS_DICT:
                        desc += RIGHTS_DICT[r] + ","
                di["desc"] = desc.rstrip(",")
                di["phone"] = res[1]
                di["name"] = res[2]
                if isinstance(res[3], memoryview):
                    temp = res[3].tobytes().decode()
                    image_url = alioss.joint_image(temp)
                    di["image"] = image_url
                elif isinstance(res[3], str):
                    image_url = alioss.joint_image(res[3])
                    di["image"] = image_url
                if "1" in res[0]:
                    master.append(di)
                else:
                    admins.append(di)

            return Response({"master": master, "admins": admins}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class RightsNew(APIView):
    """新增人员 rights/new"""
    permission_classes = [ManagementPermission]

    def post(self, request):
        new_phone = request.data.get("phone")  # 新增手机号
        rights = request.data.get("rights", [])  # 权限代号列表 ['1','2', '3']
        if not all([new_phone, rights]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        rights = list(set(rights))  # 去重
        if not re.match("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$", new_phone):
            return Response({"res": 1, "errmsg": "电话号码格式错误"}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        cursor.execute("select count(1) from user_info where phone = '{}';".format(new_phone))
        new_phone_check = cursor.fetchone()[0]
        if new_phone_check <= 0:
            return Response({"res": 1, "errmsg": "该号码未注册，无法添加！"}, status=status.HTTP_200_OK)

        cursor.execute("select factory from factory_users where phone = '%s';" % new_phone)
        factory = cursor.fetchone()
        # print(factory)
        if factory:
            if factory[0] == factory_id:
                return Response({"res": 1, "errmsg": "该号码已存在于当前工厂！"}, status=status.HTTP_200_OK)
            else:
                return Response({"res": 1, "errmsg": "该号码已存在于其它工厂！"}, status=status.HTTP_200_OK)

        cursor.execute("select user_id from user_info where phone = '{}';".format(new_phone))
        new_user_id = cursor.fetchone()[0]

        try:
            cursor.execute(
                "insert into factory_users (phone, rights, factory, time) values ('%s', '{%s}', '%s', %d)" % (
                    new_phone, ','.join(rights), factory_id, int(time.time())))

            # 删除新增人员之前的权限(之前加入体验版然后退出，后来加入某个工厂，缓存没有删除，会出错)
            redis_conn = get_redis_connection("default")
            redis_conn.hdel(new_phone, "permission", "factory_id", "seq_id", "user_id")

            # 发送消息通知
            message = {'resource': 'PyRightsNew', 'type': 'POST',
                       'params': {'Fac': factory_id, 'Phone': new_phone, "User": new_user_id}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class RightsModify(APIView):
    """修改人员权限 rights/modify"""
    permission_classes = [ManagementPermission]

    def put(self, request):
        new_phone = request.data.get("phone")  # 手机号
        new_rights = request.data.get("rights", [])  # 权限代号列表 ['1','2', '3']
        if not all([new_phone, new_rights]):
            return Response({"res": 1, "errmsg": "缺少参数！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        if phone == new_phone:
            return Response({"res": 1, "errmsg": "不能修改自身权限！"}, status=status.HTTP_200_OK)
        # 修改权限不能添加超级管理员的权限
        if "1" in new_rights:
            new_rights.remove("1")

        new_rights = list(set(new_rights))  # 去重

        try:
            cursor.execute("select count(1) from factory_users where phone = '%s';" % new_phone)
            phone_check = cursor.fetchone()[0]
            if phone_check <= 0:
                return Response({"res": 1, "errmsg": "该号码的用户不存在！"}, status=status.HTTP_200_OK)

            cursor.execute("update factory_users set rights = '{%s}' where phone = '%s' and factory = '%s';" % (
                ','.join(new_rights), new_phone, factory_id))

            connection.commit()

            # new_phone用户的权限发生变化，删除Redis的缓存，在中间件中重新读取数据库获取权限
            redis_conn = get_redis_connection("default")
            redis_conn.hdel(new_phone, "permission", "factory_id", "seq_id", "user_id")

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class RightsDelete(APIView):
    """删除人员权限 rights/del"""
    permission_classes = [ManagementPermission]

    def delete(self, request):
        new_phone = request.query_params.get("phone")  # 要删除的手机号
        if not new_phone:
            return Response({"res": 1, "errmsg": "缺少参数，无法删除！"}, status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        cursor = connection.cursor()

        if phone == new_phone:
            return Response({"res": 1, "errmsg": "不能删除自身权限！"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("select count(1) from factory_users where phone = '%s';" % new_phone)
            phone_check = cursor.fetchone()[0]
            if phone_check <= 0:
                return Response({"res": "1", "errmsg": "此号码不存在！"}, status=status.HTTP_200_OK)

            cursor.execute("select count(*) from factory_users where phone = '%s' and factory = '%s' and "
                           "'1' = ANY(rights);" % (new_phone, factory_id))
            result = cursor.fetchone()[0]
            if result >= 1:
                return Response({"res": 1, "errmsg": "该号码是超级管理员，不能删除权限！"}, status=status.HTTP_200_OK)

            cursor.execute("delete from factory_users where phone = '%s' and factory = '%s';" %
                           (new_phone, factory_id))
            connection.commit()

            # new_phone用户被删除，删除Redis的缓存
            redis_conn = get_redis_connection("default")
            redis_conn.hdel(new_phone, "permission", "factory_id", "seq_id", "user_id")

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class RightsOrg(APIView):
    """获取应用列表 rights/orgs"""
    permission_classes = [ManagementPermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        if factory_id == FACTORY_HBYL:
            return Response({'res': 0, 'data': {'label': '治疗仪管理', 'rights': HBYL_RIGHTS}}, status=status.HTTP_200_OK)
        if factory_id == FACTORY_WHBY:
            return Response({'res': 0, 'data': {'label': '数据管理', 'rights': WHBY_RIGHTS}}, status=status.HTTP_200_OK)
        return Response({'res': 1, 'data': {}}, status=status.HTTP_200_OK)
