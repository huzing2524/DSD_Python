# -*- coding: utf-8 -*-
import re
import jwt
from django.conf import settings
from django.http import HttpResponse
from django.utils.deprecation import MiddlewareMixin
from rest_framework import status
from django_redis import get_redis_connection

from apps_utils import UtilsPostgresql
from constants import REDIS_CACHE


def _redis_pool_number():
    """输出redis连接池数量"""
    r = get_redis_connection("default")  # Use the name you have defined for Redis in settings.CACHES
    connection_pool = r.connection_pool
    print("Created connections so far: %d" % connection_pool._created_connections)


class JwtTokenMiddleware(MiddlewareMixin):
    def process_request(self, request):
        # print(request.path)
        token = request.META.get("HTTP_AUTHORIZATION")
        if token:
            try:
                token = token.split(" ")[-1]
                # print(token)
                payload = jwt.decode(token, key=settings.JWT_SECRET_KEY, verify=True)
                if "username" in payload and "exp" in payload:
                    # print("payload=", payload)
                    REDIS_CACHE["phone"] = payload["username"]
                    REDIS_CACHE["user_id"] = payload["user_id"] if "user_id" in payload else payload["username"]
                    request.redis_cache = REDIS_CACHE
                    # print("request.redis_cache=", request.redis_cache)
                else:
                    raise jwt.InvalidTokenError
            except jwt.ExpiredSignatureError:
                return HttpResponse("jwt token expired", status=status.HTTP_401_UNAUTHORIZED)
            except jwt.InvalidTokenError:
                return HttpResponse("Invalid jwt token", status=status.HTTP_401_UNAUTHORIZED)
        else:
            return HttpResponse("lack of jwt token", status=status.HTTP_401_UNAUTHORIZED)

    def process_response(self, request, response):
        return response


class RedisMiddleware(MiddlewareMixin):
    """Redis读取缓存, hash类型
    key: "13212345678"
    field: value
    {"user_id": "xxxxxx"}
    {"factory_id": "QtfjtzpNcM9DuGgR6e"},
    {"permission": "3,4,5,6,7,8"}
    """

    def process_request(self, request):
        phone = request.redis_cache["phone"]
        user_id = request.redis_cache["user_id"]
        if not user_id:
            user_id = phone
        conn = get_redis_connection("default")
        # print(conn.hvals(phone))
        if phone.isdigit():
            factory_id = conn.hget(phone, "factory_id")
            permission = conn.hget(phone, "permission")
            seq_id = conn.hget(phone, "seq_id")

            if not factory_id or not permission or not seq_id:
                pgsql = UtilsPostgresql()
                connection, cursor = pgsql.connect_postgresql()
                pl = conn.pipeline()
                sql = '''
                    select
                        t2.rights,
                        t2.factory,
                        t3.seq_id
                    from
                        user_info t1
                    left join factory_users t2 on
                        t1.user_id = t2.phone
                    left join factorys t3 on
                        t2.factory = t3.id
                    where
                        t1.user_id = '{}';'''.format(user_id)
                cursor.execute(sql)
                result = cursor.fetchone()
                if result:
                    permission, factory_id, seq_id = ",".join(result[0]), result[1], result[2]
                    pl.hset(phone, "permission", permission)
                    pl.hset(phone, "factory_id", factory_id)
                    pl.hset(phone, "seq_id", seq_id)
                    pl.execute()
                else:
                    permission, factory_id = "", ""

            request.redis_cache["factory_id"] = factory_id
            request.redis_cache["permission"] = permission
            request.redis_cache["seq_id"] = seq_id
        else:
            return None

    def process_response(self, request, response):
        return response
