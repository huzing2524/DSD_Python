import logging
import time
import traceback

from django.db import transaction
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, generate_sql_uuid
from permissions import PurchasePermission

logger = logging.getLogger('django')


class Supplier(APIView):
    """客户 purchase/supplier/<str:supplier_id>"""
    permission_classes = [PurchasePermission]

    def get(self, request, supplier_id):
        """
        :param request:
        :param supplier_id: 如果type:1 则是 base_suppliers -> id type:2, base_clients_pool -> id
        :return:
        """
        factory_id = request.redis_cache["factory_id"]
        type = request.query_params.get('type')
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        materials = []
        if type == '1':
            sql = "select name, phone, contacts, position, region, address, industry, deliver_days from base_suppliers " \
                  "where id = '{}' ".format(supplier_id)

            supplier_materials = '''
            select
                t2.id,
                t2.name,
                t2.unit,
                t1.unit_price,
                t1.lowest_package,
                t1.lowest_count
            from
                base_supplier_materials t1
            left join base_materials_pool t2 on
                t1.material_id = t2.id
            where
                t1.factory_id = '{}'
                and t1.supplier_id = '{}'; '''.format(factory_id, supplier_id)
            cursor.execute(supplier_materials)
            materials_res = cursor.fetchall()
            for x in materials_res:
                temp = dict()
                temp['id'] = x[0] or ''
                temp['name'] = x[1] or ''
                temp['unit'] = x[2] or ''
                temp['unit_price'] = x[3] or 0
                temp['lowest_package'] = x[4] or 0
                temp['lowest_count'] = x[5] or 0
                materials.append(temp)
        else:
            sql = "select name, phone, contacts, position, region, address, industry from base_clients_pool  " \
                  "where id = '{}'".format(supplier_id)
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            di = dict()
            for res in result:
                di["id"] = supplier_id
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
            di['materials'] = materials
            return Response(di, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request, supplier_id):
        """从客户资源库中添加为供应商"""
        user_id = request.redis_cache["user_id"]
        factory_id = request.redis_cache["factory_id"]
        materials = request.data.get("materials", [])
        deliver_days = request.data.get("deliver_days", 0)
        if not factory_id:
            return Response({"res": 1, "errmsg": "请输入正确的请求参数！"}, status=status.HTTP_400_BAD_REQUEST)
        if factory_id == supplier_id:
            return Response({"res": 1, "errmsg": "不能添加自己为供应商！"}, status=status.HTTP_400_BAD_REQUEST)

        pgsql = UtilsPostgresql()
        timestamp = int(time.time())
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute(
            "select *  from base_suppliers where id = '{}' and factory = '{}';".format(supplier_id, factory_id))
        res = cursor.fetchone()
        if res:
            return Response({"res": 1, "errmsg": "已经添加过该供应商！"}, status=status.HTTP_200_OK)

        cursor.execute(
            "select id, name, phone, contacts, position, region, address, industry  from base_clients_pool "
            "where id = '{0}';".format(supplier_id))

        res = cursor.fetchall()
        if len(res) <= 0:
            return Response({"res": 1, "errmsg": "记录不存在！"}, status=status.HTTP_200_OK)

        try:
            client = res[0]
            sql = "insert into base_suppliers (id, factory, name, contacts, phone, position, creator, " \
                  "region, address, industry, create_time, deliver_days) values ('{0}', '{1}','{2}','{3}', '{4}'," \
                  " '{5}', '{6}', '{7}', '{8}', '{9}', {10}, {11})".format(client[0], factory_id, client[1], client[3],
                                                                           client[2], client[4],
                                                                           user_id, client[5], client[6], client[7],
                                                                           timestamp,
                                                                           deliver_days)
            cursor.execute(sql)
            for x in materials:
                material_lowest_sql = "select lowest_package, lowest_count from base_products where id = '{}'" \
                                      " and factory = '{}' ".format(x['id'], factory_id)

                cursor.execute(material_lowest_sql)
                res = cursor.fetchone()

                supplier_materials_sql = '''
                insert into 
                    base_supplier_materials (factory_id, supplier_id, material_id, unit_price, lowest_package, 
                    lowest_count) 
                values 
                    ('{}', '{}', '{}', {}, {}, {})'''.format(factory_id, supplier_id, x['id'], x['unit_price'],
                                                             res[0] or 0, res[1] or 0)

                cursor.execute(supplier_materials_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, supplier_id):
        name = request.data.get("name")  # 客户名称
        contacts = request.data.get("contact")  # 联系人
        phone = request.data.get("phone")  # 手机号
        industry = request.data.get("industry")  # 分组id
        position = request.data.get("position", "")  # 职位
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址
        materials = request.data.get("materials", [])
        deliver_days = request.data.get("deliver_days", 0)

        factory_id = request.redis_cache["factory_id"]

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            sql = "update base_suppliers set name = '{}', contacts = '{}', phone = '{}', position = '{}', " \
                  "region = '{}', address = '{}', industry = '{}', deliver_days = {} where id =" \
                  " '{}'".format(name, contacts, phone, position, region, address, industry, deliver_days, supplier_id)
            cursor.execute(sql)
            materials_del_sql = "delete from base_supplier_materials where factory_id = '{}' and supplier_id = '{}'" \
                                ";".format(factory_id, supplier_id)
            cursor.execute(materials_del_sql)
            search_sql = "select lowest_count, lowest_package from base_products where factory = '{}' and id = '{}';"
            for x in materials:
                cursor.execute(search_sql.format(factory_id, x['id']))
                search_res = cursor.fetchone()
                lowest_count, lowest_package = search_res[0] or 0, search_res[1] or 0

                supplier_materials_sql = '''
                insert into 
                    base_supplier_materials (factory_id, supplier_id, material_id, unit_price, lowest_count, 
                    lowest_package) 
                values ('{}', '{}', '{}', {}, {}, {})'''.format(factory_id, supplier_id, x['id'], x['unit_price'],
                                                                lowest_count, lowest_package)
                cursor.execute(supplier_materials_sql)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierNew(APIView):
    """purchase/supplier 添加供应商"""
    permission_classes = [PurchasePermission]

    def post(self, request):
        timestamp = int(time.time())
        name = request.data.get("name")  # 客户名称
        contact = request.data.get("contact")  # 联系人
        phone = request.data.get("phone")  # 手机号]
        industry = request.data.get("industry")  # 分组id
        position = request.data.get("position", "")  # 职位
        region = request.data.get("region", "")  # 客户地址
        address = request.data.get("address", "")  # 详细地址
        materials = request.data.get("materials", [])
        deliver_days = request.data.get("deliver_days", 0)

        # print(request.data)

        if not all([name, contact, phone]):
            return Response({"res": 1, "errmsg": "请检查输入参数！"}, status=status.HTTP_200_OK)

        user_id = request.redis_cache["user_id"]
        factory_id = request.redis_cache["factory_id"]

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            name_check_sql = "select count(1) from base_clients_pool where name = '{}'".format(name)
            cursor.execute(name_check_sql)
            count = cursor.fetchone()[0]
            if count >= 1:
                return Response({"res": 1, "errmsg": "该供应商已存在，请搜索添加!"}, status=status.HTTP_200_OK)

            supplier_id = generate_sql_uuid()
            sql = "insert into base_suppliers (id, factory, name, contacts, phone, position, create_time, " \
                  "creator, region, address, industry, deliver_days) values ('{0}', '{1}','{2}','{3}', '{4}', '{5}'," \
                  " {6}, '{7}', '{8}', '{9}', '{10}', {11})".format(supplier_id, factory_id, name, contact,
                                                                    phone, position, timestamp,
                                                                    user_id, region,
                                                                    address, industry, deliver_days)

            pool_sql = "insert into base_clients_pool ( name, contacts, phone, position, create_time, region," \
                       " address, industry, id, verify) values ('{0}', '{1}','{2}','{3}', {4}, '{5}'," \
                       " '{6}', '{7}', '{8}', '0') on conflict (name) do nothing".format(name, contact, phone,
                                                                                         position, timestamp, region,
                                                                                         address,
                                                                                         industry,
                                                                                         supplier_id)

            with transaction.atomic():
                cursor.execute(pool_sql)
                cursor.execute(sql)
                for x in materials:
                    supplier_materials_sql = '''
                    insert into base_supplier_materials (factory_id, supplier_id, material_id, unit_price, 
                    lowest_package, lowest_count) values ('{}', '{}', '{}', {}, {}, {})
                    '''.format(factory_id, supplier_id, x['id'], x['unit_price'], x['lowest_package'], x['lowest_count'])
                    cursor.execute(supplier_materials_sql)
                return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierSearch(APIView):
    """purchase/supplier/search"""
    permission_classes = [PurchasePermission]

    def get(self, request):
        name = request.query_params.get('name', '')
        if not name:
            return Response({"list": []}, status=status.HTTP_200_OK)
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(
                "select id, name from base_clients_pool where name like %s order by name limit 5;",
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


class SupplierList(APIView):
    """purchase/supplier/list"""
    permission_classes = [PurchasePermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            suppliers_sql = '''
            select
                t1.id,
                t1.name,
                t1.industry,
                t1.create_time as time,
                case
                    when t2.id isnull then '2'
                    else '1'
                end as state,
                coalesce(t3.materials, '') as materials
            from
                base_suppliers t1
            left join factorys t2 on
                t1.factory = t2.id
            left join (
                select
                    t1.factory_id,
                    t1.supplier_id,
                    array_agg(t1.material_id) as material_ids,
                    string_agg( t2."name", ',' ) as materials
                from
                    base_supplier_materials t1
                left join base_materials_pool t2 on
                    t1.material_id = t2.id
                where
                    t1.factory_id = '{}'
                group by
                    t1.factory_id,
                    t1.supplier_id ) t3 on
                t1.id = t3.supplier_id
            where
                t1.factory = '{}' order by t1.name;'''.format(factory_id, factory_id)
            cursor.execute(suppliers_sql)
            result = cursor.fetchall()
            data = []
            for res in result:
                temp = dict()
                temp['id'] = res[0]
                temp['name'] = res[1]
                temp['industry'] = res[2]
                temp['materials'] = res[5]
                temp['state'] = res[4]
                data.append(temp)
            return Response({"list": data}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class SupplierMaterialList(APIView):
    """purchase/supplier/materials"""
    permission_classes = [PurchasePermission]

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]
        supplier_id = request.query_params.get('id')
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            if not supplier_id:
                suppliers_sql = '''
                    select
                        t1.id,
                        t2.name,
                        t2.unit
                    from
                        base_products t1
                    left join base_materials_pool t2 on
                        t1.id = t2.id
                    where
                        t1.factory = '{}';'''.format(factory_id)
                cursor.execute(suppliers_sql)

                result = cursor.fetchall()
                data = []
                for res in result:
                    temp = dict()
                    temp['id'] = res[0]
                    temp['name'] = res[1]
                    temp['unit'] = res[2]
                    data.append(temp)
                return Response({"list": data}, status=status.HTTP_200_OK)
            else:
                count_sql = """
                select count(*)
                from base_supplier_materials
                where factory_id = '{0}'
                  and supplier_id = '{1}';
                """.format(factory_id, supplier_id)
                cursor.execute(count_sql)
                count = cursor.fetchone()[0]
                if count:
                    materials_sql = '''
                    select t1.material_id,
                           t1.lowest_count,
                           t1.lowest_package,
                           t2.name,
                           t2.unit
                    from (
                             select *
                             from base_supplier_materials
                             where factory_id = '{0}'
                               and supplier_id = '{1}') t1
                             left join base_materials_pool t2 on
                        t1.material_id = t2.id;
                    '''.format(factory_id, supplier_id)
                    cursor.execute(materials_sql)
                    materials_res = cursor.fetchall()
                    disable = []
                    able = []
                    target1, target2 = ['id', 'name', 'unit'], ['id', 'lowest_count', 'lowest_package', 'name', 'unit']
                    for material in materials_res:
                        if not material[0]:
                            disable.append(dict(zip(target1, (material[0], material[-2], material[-1]))))
                        else:
                            able.append(dict(zip(target2, material)))
                    return Response({'able': able, 'disable': disable}, status=status.HTTP_200_OK)

                else:
                    return Response({"able": {}, 'disable': {}}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
