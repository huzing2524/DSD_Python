import json
import logging
import time
import traceback

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from apps_utils import UtilsPostgresql, generate_sql_uuid

logger = logging.getLogger('django')


class ClientGroup(APIView):
    """订单首页 /order/main"""

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        sql = "select id, name from base_client_groups where factory = '{0}' order by name desc".format(factory_id)
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(sql)
            res = cursor.fetchall()
            data = []
            for x in res:
                temp = dict()
                temp['id'] = x[0]
                temp['name'] = x[1]
                data.append(temp)

            return Response({"list": data},
                            status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        timestamp = int(time.time())
        name = request.data.get("name")  # 客户分组名称
        if not name:
            return Response({"res": 1, "errmsg": "缺少客户分组名称！"},
                            status=status.HTTP_200_OK)
        factory_id = request.redis_cache["factory_id"]
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        cursor.execute("select count(1) from groups where factory = '%s' and name = '%s';" % (
            factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "名称已存在！"},
                            status=status.HTTP_200_OK)

        try:
            delete_sql = "insert into base_client_groups (factory, name, time) values ('{0}', '{1}', {2})".format(
                factory_id,
                name,
                timestamp)
            cursor.execute(delete_sql)
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        cursor.execute(
            "select count(1) from base_client_groups where id = '{0}';".format(id))

        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "记录不存在！"},
                            status=status.HTTP_200_OK)
        try:
            cursor.execute("delete from order_track where id = '{0}';".format(id))
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class Clients(APIView):
    """客户"""

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        data = []
        try:
            sql = '''
            select
                t1.id,
                t1.name,
                t1.industry,
                coalesce( t3.products,
                '' ) as products,
                t1.phone, t1.contacts, t1.position, t1.region, t1.address
            from
                base_clients t1
            left join factorys t2 on
                t1.id = t2.id
            left join (
                select
                    t1.factory_id,
                    t1.client_id,
                    string_agg( t2."name", ',' ) as products
                from
                    base_client_products t1
                left join base_materials_pool t2 on
                    t1.product_id = t2.id
                where
                    t1.factory_id = '{0}'
                group by
                    t1.factory_id,
                    t1.client_id ) t3 on
                t1.id = t3.client_id
            where
                t1.factory = '{0}';'''.format(factory_id)
            cursor.execute(sql)
            result = cursor.fetchall()
            for res in result:
                di = dict()
                di["id"] = res[0] or ""
                di["name"] = res[1] or ""
                di["industry"] = res[2] or ""
                di["products"] = res[3] or ""
                di["phone"] = res[4] or ""
                di["contact"] = res[5] or ""
                di["position"] = res[6] or ""
                di["region"] = res[7] or ""
                di["address"] = res[8] or ""
                data.append(di)
            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        timestamp = int(time.time())
        name = request.data.get("name")  # 客户名称
        contacts = request.data.get("contact")  # 联系人
        phone = request.data.get("phone")  # 手机号]
        group_id = request.data.get("group_id")  # 分组id
        position = request.data.get("position", "")  # 职位
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址
        industry = request.data.get("industry", "")  # 行业
        products = request.data.get("products", [])  #
        deliver_days = request.data.get("deliver_days", 0)  # 送达天数

        if not all([name, contacts, phone]):
            return Response({"res": 1, "errmsg": "请检查输入参数！"},
                            status=status.HTTP_200_OK)

        user_phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            client_id = generate_sql_uuid()
            if group_id:
                sql = "insert into base_clients (id, factory, name, contacts, phone, position, create_time, group_id, " \
                      "creator, region, address, industry, deliver_days) values ('{}', '{}', '{}','{}','{}', '{}', {}, '{}', '{}', '{}'," \
                      " '{}', '{}')".format(client_id, factory_id, name, contacts, phone, position, timestamp, group_id,
                                            user_phone, region,
                                            address, industry, deliver_days)
            else:
                sql = "insert into base_clients (id, factory, name, contacts, phone, position, create_time, " \
                      "creator, region, address, industry, deliver_days) values ('{}', '{}', '{}','{}','{}', '{}'," \
                      " {}, '{}', '{}', '{}', '{}', {})".format(client_id, factory_id, name, contacts,
                                                                phone, position, timestamp,
                                                                user_phone, region, address, industry, deliver_days)

            pool_sql = "insert into base_clients_pool (id, name, contacts, phone, position, create_time, " \
                       "region, address, industry, verify) values ('{}', '{}', '{}','{}','{}', {}, '{}', '{}', '{}', " \
                       "'0') on conflict (name) do nothing".format(client_id, name, contacts, phone, position,
                                                                   timestamp, region,
                                                                   address, industry)
            cursor.execute(pool_sql)
            cursor.execute(sql)

            for x in products:
                client_product_sql = "insert into base_client_products (factory_id, client_id, product_id, unit_price) " \
                                     "values ('{}', '{}', '{}', {})".format(factory_id, client_id, x['id'],
                                                                            x['unit_price'])
                cursor.execute(client_product_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request):
        client_id = request.data.get("id")  # 客户id
        if not client_id:
            return Response({"res": 1, "errmsg": " 输入参数错误！"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute(
                "delete from base_clients where id = '%s';" % client_id)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class ClientSearch(APIView):
    def get(self, request):
        name = request.query_params.get('name')
        if not name:
            return Response({"res": 1, "errmsg": "输入参数不能为空"}, status=status.HTTP_200_OK)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute("select id, name from base_clients_pool where name like %s order by name asc limit 5;",
                           ['%' + name + '%'])
            res = cursor.fetchall()
            data = []
            for x in res:
                temp = dict()
                temp['id'] = x[0]
                temp['name'] = x[1]
                data.append(temp)
            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class ClientSave(APIView):
    def post(self, request, client_id):
        user_phone = request.redis_cache["phone"]
        factory_id = request.redis_cache["factory_id"]
        products = request.data.get("products", [])
        deliver_days = request.data.get("deliver_days", 0)

        pgsql = UtilsPostgresql()
        timestamp = int(time.time())
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select *  from base_clients where id = '{}' and factory = '{}';".format(client_id, factory_id))
        res = cursor.fetchone()
        if res:
            return Response({"res": 1, "errmsg": "已添加该过改客户！"}, status=status.HTTP_200_OK)

        cursor.execute(
            "select id, name, phone, contacts, position, region, address, coalesce(industry, '') as industry"
            "  from base_clients_pool where id = '{0}';".format(client_id))

        res = cursor.fetchall()
        if len(res) <= 0:
            return Response({"res": 1, "errmsg": "记录不存在！"}, status=status.HTTP_200_OK)

        try:
            client = res[0]
            sql = "insert into base_clients (id, factory, name, contacts, phone, position, creator, region, address, " \
                  "industry, create_time, deliver_days) values ('{0}', '{1}','{2}','{3}', '{4}', '{5}', '{6}', '{7}', " \
                  "'{8}', '{9}', {10}, {11})".format(client[0], factory_id, client[1], client[3], client[2], client[4],
                                                     user_phone, client[5], client[6], client[7], timestamp,
                                                     deliver_days)
            cursor.execute(sql)
            for x in products:
                client_product_sql = "insert into base_client_products (factory_id, client_id, product_id, unit_price) " \
                                     "values ('{}', '{}', '{}', {})".format(factory_id, client_id, x['id'],
                                                                            x['unit_price'])
                cursor.execute(client_product_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def get(self, request, client_id):
        factory_id = request.redis_cache["factory_id"]
        type = request.query_params.get('type')
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        products = []
        if type == '1':
            sql = "select name, phone, contacts, position, region, address, industry, deliver_days from base_clients" \
                  " where id = '{}'".format(client_id)
            supplier_products = '''
                    select
                        t2.id,
                        t2.name,
                        t2.unit,
                        t1.unit_price,
                        t3.lowest_package,
                        t3.lowest_count
                    from
                        base_client_products t1
                    left join base_materials_pool t2 on
                        t1.product_id = t2.id
                    left join (select * from base_products where factory = '{0}') t3 on 
                        t1.product_id = t3.id
                    where
                        t1.factory_id = '{0}'
                        and t1.client_id = '{1}'; '''.format(factory_id, client_id)

            cursor.execute(supplier_products)
            products_res = cursor.fetchall()
            for x in products_res:
                temp = dict()
                temp['id'] = x[0] or ''
                temp['name'] = x[1] or ''
                temp['unit'] = x[2] or ''
                temp['unit_price'] = x[3] or 0
                temp['lowest_package'] = x[4] or 0
                temp['lowest_count'] = x[5] or 0
                products.append(temp)
        else:
            sql = "select name, phone, contacts, position, region, address, industry " \
                  "from base_clients_pool where id = '{}'".format(client_id)

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            di = dict()
            for res in result:
                di["id"] = client_id
                di["name"] = res[0] or ""
                di["contact"] = res[2] or ""
                di["phone"] = res[1] or ""
                di["position"] = res[3] or ""
                di['region'] = res[4]
                di['address'] = res[5]
                di["industry"] = res[6] or ""
                if len(res) >= 8:
                    di["deliver_days"] = res[7]
                else:
                    di["deliver_days"] = 0

            di['products'] = products

            return Response(di, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            traceback.print_exc()
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, client_id):
        name = request.data.get("name")  # 客户名称
        contacts = request.data.get("contact")  # 联系人
        phone = request.data.get("phone")  # 手机号]
        industry = request.data.get("industry")  # 分组id
        position = request.data.get("position", "")  # 职位
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址
        products = request.data.get("products", [])
        factory_id = request.redis_cache["factory_id"]
        deliver_days = request.data.get("deliver_days", 0)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            sql = "update base_clients set name = '{}', contacts = '{}', phone = '{}', position = '{}', " \
                  "region = '{}', address = '{}', industry = '{}', deliver_days = '{}' where id = '{}'" \
                  "".format(name, contacts, phone, position, region, address, industry, deliver_days, client_id)

            cursor.execute(sql)
            materials_del_sql = "delete from base_client_products where factory_id = '{}' and client_id = '{}'" \
                                ";".format(factory_id, client_id)
            cursor.execute(materials_del_sql)
            for x in products:
                supplier_materials_sql = "insert into base_client_products (factory_id, client_id, product_id, unit_price) " \
                                         "values ('{}', '{}', '{}', {})".format(factory_id, client_id, x['id'],
                                                                                x['unit_price'])
                cursor.execute(supplier_materials_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class ClientProductList(APIView):
    def get(self, request):
        factory_id = request.redis_cache["factory_id"]

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            suppliers_sql = '''
                select
                    t1.id,
                    t2.name,
                    t2.unit,
                    t1.price,
                    coalesce(t1.lowest_package, 0),
                    coalesce(t1.lowest_count, 0)
                from
                    base_products t1 left join base_materials_pool t2 on
                t1.id = t2.id
                where
                    t1.factory = '{0}' ;'''.format(factory_id)
            cursor.execute(suppliers_sql)
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
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
