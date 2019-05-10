# -*- coding: utf-8 -*-
import json
import logging
import time
import datetime

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, UtilsRabbitmq, generate_uuid
from constants import StoreTypeEnum, StoreNoticeMsgEnum, StoreNoticeEnum, PRODUCT_MATERIAL_DICT

logger = logging.getLogger('django')


# 仓库部-----------------------------------------------------------------------------------------------------------------


class StoreMainType(APIView):
    """仓库管理首页 store/main/{type}"""

    def get(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        if Type == 'product':
            sql = """
            select
                t1.id,
                t1.name,
                t1.unit,
                t1.category_id,
                t1.low_limit,
                t1.notice_state,
                coalesce( t2.name,
                '' ) as category_name,
                coalesce( t3.count,
                0 ) as count,
                coalesce( t4.process_ids,
                '{}' ) as process,
                coalesce( t5.time,
                0 ) as time
            from
                products t1
            left join product_categories t2 on
                t1.category_id = t2.id
            left join (
                select
                    product_id,
                    sum(count) as count
                from
                    products_log
                where
                    parent_type = 'incoming'
                    or parent_type = 'outgoing'
                    or parent_type = 'store_check'
                    or parent_type = 'init'
                    or parent_type = 'order'
                    or parent_type = 'product'
                group by
                    product_id ) t3 on
                t1.id = t3.product_id
            left join product_processes t4 on
                t1.id = t4.product_id
            left join (
                select
                    distinct on
                    (product_id) product_id,
                    time,
                    use_id,
                    factory
                from
                    products_log
                where
                    factory = '%s'
                    and parent_type = 'order'
                order by
                    product_id,
                    time desc ) t5 on
                t1.id = t5.product_id
            where
                t1.factory = '%s';""" % (factory_id, factory_id)
            not_deliver = """
            select
                count( 1 )
            from
                orders t1
            left join (
                select
                    order_id,
                    array_agg(product_count) as product_count,
                    array_agg(name) as product_name,
                    array_agg(unit) as unit
                from
                    (
                    select
                        t1.product_count,
                        t1.order_id,
                        t2.name,
                        t2.unit
                    from
                        order_products t1
                    left join products t2 on
                        t1.product_id = t2.id
                    where
                        t2.factory = '{0}' ) t
                group by
                    order_id ) t2 on
                t1.id = t2.order_id
            where
                t1.factory = '{0}'
                and t2.order_id notnull
                and t1.state = '1';""".format(factory_id)
            target = ['id', 'name', 'unit', 'category_id', 'low_limit', 'notice_state', 'category_name', 'count',
                      'process', 'time']
            try:
                cur.execute(sql)
                product_list = [dict(zip(target, i)) for i in cur.fetchall()]
                cur.execute(not_deliver)
                notdeliver_count = cur.fetchone()[0]
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            low_stock = []
            all_list = []

            for i in product_list:
                # low_stock
                if len(i['process']):
                    i['process'] = '1'
                else:
                    i['process'] = '0'
                category_id = i['category_id']
                category_name = i['category_name']
                del i['category_id']
                del i['category_name']
                if i['notice_state'] == StoreNoticeEnum.done.value:
                    tmp_sql = "select id as notice_id from store_notice where item_id = '%s' order by create_time " \
                              "desc limit 1;" % i['id']
                    cur.execute(tmp_sql)
                    tmp = cur.fetchone()
                    if tmp:
                        i.update({'notice_id': tmp[0]})
                if i['count'] > i['low_limit']:
                    i['stock_state'] = '0'
                    del i['notice_state']
                else:
                    low_stock.append(i.copy())
                    i['stock_state'] = '1'
                # all_list
                for j in all_list:
                    if j['category_name'] == category_name:
                        j['list'].append(i)
                        break
                else:
                    all_list.append({'category_id': category_id, 'category_name': category_name, 'list': [i]})

            result = {'low_stocks': low_stock, 'list': all_list, 'not_deliver': notdeliver_count}
            postgresql.disconnect_postgresql(conn)
            return Response(result, status=status.HTTP_200_OK)
        elif Type == 'material':
            sql = """
            select
                t1.id,
                t1.name,
                t1.unit,
                case
                    when t.sum is null then 0
                    else t.sum
                end as count,
                t1.category_id,
                t1.low_limit,
                t1.notice_state,
                coalesce( t2.name,
                '' ) as category_name,
                coalesce( t3.time,
                0 ) as time
            from
                (
                select
                    material_type_id,
                    sum(material_count)
                from
                    materials_log
                where
                    ( parent_type = 'incoming'
                    or parent_type = 'outgoing'
                    or parent_type = 'store_check'
                    or parent_type = 'init'
                    or parent_type = 'order'
                    or parent_type = 'product' )
                group by
                    material_type_id ) t
            right join material_types t1 on
                t.material_type_id = t1.id
            left join material_categories t2 on
                t1.category_id = t2.id
            left join (
                select
                    distinct on
                    (material_type_id) material_type_id,
                    time
                from
                    materials_log
                where
                    factory = '{0}'
                    and parent_type = 'outgoing'
                order by
                    material_type_id,
                    time desc ) t3 on
                t1.id = t3.material_type_id
            where
                t1.factory = '{0}';""".format(factory_id)
            not_deliver = """
            select
                count(1)
            from
                product_task t1
            left join (
                select
                    use_id
                from
                    materials_log
                where
                    parent_type = 'product'
                group by
                    use_id ) t2 on
                t1.id = t2.use_id
            where
                t1.factory = '%s'
                and t1.prepare_state = '%s'
                and t2.use_id notnull;""" % (factory_id, PRODUCT_MATERIAL_DICT['material_not'])
            target = ['id', 'name', 'unit', 'count', 'category_id', 'low_limit', 'notice_state', 'category_name',
                      'time']
            try:
                cur.execute(sql)
                material_list = [dict(zip(target, i)) for i in cur.fetchall()]
                cur.execute(not_deliver)
                notdeliver_count = cur.fetchone()[0]
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            low_stock = []
            all_list = []
            for i in material_list:
                category_id = i['category_id']
                category_name = i['category_name']
                del i['category_id']
                del i['category_name']
                if i['notice_state'] == StoreNoticeEnum.done.value:
                    tmp_sql = "select id as notice_id from store_notice where item_id = '%s' order by create_time " \
                              "desc limit 1;" % i['id']
                    cur.execute(tmp_sql)
                    tmp = cur.fetchone()
                    if tmp:
                        i.update({'notice_id': tmp[0]})
                # low_stock
                if i['count'] > i['low_limit']:
                    i['stock_state'] = '0'
                    del i['notice_state']
                else:
                    i['stock_state'] = '1'
                    low_stock.append(i)
                # all_list
                for j in all_list:
                    if j['category_name'] == category_name:
                        j['list'].append(i)
                        break
                else:
                    all_list.append({'category_id': category_id, 'category_name': category_name, 'list': [i]})

            result = {'low_stocks': low_stock, 'list': all_list, 'not_deliver': notdeliver_count}
            postgresql.disconnect_postgresql(conn)
            return Response(result, status=status.HTTP_200_OK)
        else:
            postgresql.disconnect_postgresql(conn)
            return Response({'res': 1, 'errmsg': 'invalid type'}, status=status.HTTP_200_OK)


class StoreStatsType(APIView):
    """产品/物料统计 store/stats/{type}"""

    def get(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        Id = request.query_params.get('id', 'id')
        time_type = request.query_params.get('type', '1')

        if Type == 'product':
            log_sql = """
            select
                use_id as id, count, time, parent_type
            from
                products_log
            where
                product_id = '%s'
                and ( parent_type = 'incoming'
                or parent_type = 'outgoing'
                or parent_type = 'store_check'
                or parent_type = 'init'
                or parent_type = 'order'
                or parent_type = 'product' ) order by time desc;""" % Id
            sql = """
            select
                t1.*,
                coalesce( t2.name,
                '' ) as category_name
            from
                (
                select
                    name,
                    unit,
                    low_limit,
                    category_id,
                    number,
                    spec
                from
                    products
                where
                    id = '%s' ) t1
            left join product_categories t2 on
                t1.category_id = t2.id;""" % Id
        elif Type == 'material':
            log_sql = """
            select
                use_id as id,
                material_count as count,
                time,
                parent_type
            from
                materials_log
            where
                material_type_id = '%s'
                and ( parent_type = 'incoming'
                or parent_type = 'outgoing'
                or parent_type = 'store_check'
                or parent_type = 'init'
                or parent_type = 'product' ) order by time desc;""" % Id
            sql = """
            select
                t1.*,
                coalesce( t2.name,
                '' ) as category_name
            from
                (
                select
                    name,
                    unit,
                    low_limit,
                    category_id,
                    number,
                    spec
                from
                    material_types
                where
                    id = '%s' ) t1
            left join material_categories t2 on
                t1.category_id = t2.id;""" % Id
        else:
            postgresql.disconnect_postgresql(conn)
            return Response({'res': 1, 'errmsg': 'invalid type'}, status=status.HTTP_200_OK)
        target_1 = ['id', 'count', 'time', 'parent_type']
        target_2 = ['name', 'unit', 'low_limit', 'category_id', 'number', 'spec', 'category_name']
        try:
            cur.execute(log_sql)
            log = [dict(zip(target_1, i)) for i in cur.fetchall()]
            cur.execute(sql)
            tmp = cur.fetchone() or {}
            material = dict(zip(target_2, tmp))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        res = []
        stock = 0
        for i in log:
            # date
            if time_type == '1':
                key = time.strftime('%Y_%m', time.localtime(i['time']))
            elif time_type == '3':
                year, week = datetime.datetime.fromtimestamp(i['time']).isocalendar()[:2]
                key = '{}_{}'.format(year, week)
            else:
                key = time.strftime('%Y', time.localtime(i['time']))

            for j in res:
                if j[0] == key:
                    j[1].append(i)
                    break
            else:
                res.append([key, [i]])
            stock += i['count']

        # data
        data = []
        for i in res:
            date = i[0].split('_')
            if len(date) == 1:
                tmp = {'year': date[0], 'list': i[1]}
            else:
                tmp = {'year': date[0], 'mon_or_week': date[1].lstrip('0'), 'list': i[1]}

            incoming = 0
            outgoing = 0
            for j in i[1]:
                if j['parent_type'] == 'incoming':
                    incoming += j['count']
                elif j['parent_type'] == 'outgoing':
                    outgoing += -j['count']
            summary = {'in': incoming, 'out': outgoing}
            tmp.update(summary)
            data.append(tmp)

        result = {'data': data, 'count': stock}
        result.update(material)
        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class StoreCheckType(APIView):
    """产品/物料盘点 store/check/{type}"""

    def post(self, request, Type):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        phone = request.redis_cache['username']
        Id = request.data.get('id')
        update = request.data.get('update')
        Time = request.data.get('time')
        remark = request.data.get('remark', '')
        # 文档中多一个actual的参数, 但是代码中没有接收，而是去数据库查了

        if Type == 'product':
            sql = "select coalesce(sum(count), 0) as sum from products_log where product_id = '%s' and (parent_type =" \
                  " 'incoming' or parent_type = 'outgoing' or parent_type = 'store_check' or parent_type = 'init' or" \
                  " parent_type = 'order' );" % Id
            log_sql = "insert into products_log(id, factory, use_id, parent_type, product_id, count, time) values(" \
                      "'{}', '{}', '{}', 'store_check', '{}', {}, {})"
            check_log = "insert into product_check (id, factory, product_id, actual, update, check_time, remark, " \
                        "creator_id, time) values('{}', '{}', '{}', {}, {}, {}, '{}', '{}', {})"
        elif Type == 'material':
            sql = "select coalesce(sum(material_count), 0) as sum from materials_log where material_type_id = '%s' " \
                  "and (parent_type = 'incoming' or parent_type = 'outgoing' or parent_type = 'store_check' or parent" \
                  "_type = 'init' or parent_type = 'order' );" % Id
            log_sql = "insert into materials_log(id, factory, use_id, parent_type, material_type_id, material_count," \
                      " time) values('{}', '{}', '{}', 'store_check', '{}', {}, {})"
            check_log = "insert into material_check (id, factory, material_type_id, actual, update, check_time, " \
                        "remark, creator_id, time) values('{}', '{}', '{}', {}, {}, {}, '{}', '{}', {})"
        else:
            postgresql.disconnect_postgresql(conn)
            return Response({'res': 1, 'errmsg': 'invalid type'}, status=status.HTTP_200_OK)

        uuid = generate_uuid()
        try:
            cur.execute(sql)
            actural = cur.fetchone()[0]
            cur.execute(check_log.format(uuid, factory_id, Id, actural, update, Time, remark, phone, int(time.time())))
            cur.execute(log_sql.format(uuid, factory_id, uuid, Id, update - actural, Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreCheckTypeId(APIView):
    """产品/物料盘点详情 store/check/{type}/{id}"""

    # 没找到
    def get(self, request, Type, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()
        if Type == 'product':
            sql = """
            select
                t2.name,
                t2.unit,
                t1.actual,
                t1.update,
                t1.time,
                t1.remark,
                coalesce( t3.name,
                '' ) as creator
            from
                product_check t1
            left join products t2 on
                t1.product_id = t2.id
            left join user_info t3 on
                t1.creator_id = t3.phone where t1.id = '%s';""" % Id
        elif Type == 'material':
            sql = """
            select
                coalesce(t2.name, '') as name,
                t2.unit,
                t1.actual,
                t1.update,
                t1.time,
                t1.remark,
                coalesce( t3.name,
                '' ) as creator
            from
                material_check t1
            left join material_types t2 on
                t1.material_type_id = t2.id
            left join user_info t3 on
                t1.creator_id = t3.phone where t1.id = '%s';""" % Id
        else:
            postgresql.disconnect_postgresql(conn)
            return Response({'res': 1, 'errmsg': 'invalid type'}, status=status.HTTP_200_OK)
        target = ['name', 'unit', 'actual', 'update', 'time', 'remark', 'creator']
        correct = ['name', 'unit', 'remark', 'creator']
        try:
            cur.execute(sql)
            tmp = cur.fetchone()
            result = dict(zip(target, tmp)) if tmp else {}
            if result:
                for i in correct:
                    if result[i] is None:
                        result[i] = ''
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class StoreProductOutgoing(APIView):
    """产品出库 store/product/outgoing"""

    # 被干掉
    def post(self, request):
        rabbitmq = UtilsRabbitmq()
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        phone = request.redis_cache['username']
        products = request.data.get('products', [])
        order_id = request.data.get('order_id', None)
        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')
        outgoing_id = generate_uuid()

        id_list = [i['id'] for i in products]
        count_list = [i['count'] for i in products]

        if order_id is not None:
            order_id = repr(order_id)
        else:
            order_id = 'NULL'

        insert_sql = "insert into product_outgoing(id, factory, products, counts, order_id, time, remark, creator_id)" \
                     " values('{}', '{}', '{}', '{}', {}, {}, '{}', '{}');"
        products_log = "insert into products_log (id, factory, use_id, parent_type, product_id, count, time) " \
                       "values('{}', '{}', '{}', 'outgoing', '{}', {}, {});"
        try:
            for i in products:
                uuid = generate_uuid()
                cur.execute(products_log.format(uuid, factory_id, outgoing_id, i['id'], -i['count'], Time))
            product_id = '{' + ','.join(id_list) + '}'
            product_count = '{' + ','.join(str(i) for i in count_list) + '}'
            cur.execute(insert_sql.format(outgoing_id, factory_id, product_id, product_count, order_id, Time, remark,
                                          phone))
            conn.commit()
            # send_message
            message = {'resource': 'PyStoreProductOutgoing',
                       'type': 'POST',
                       'params': {'Fac': factory_id, 'UID': outgoing_id, 'ProductIds': id_list,
                                  'ProductCounts': count_list, 'Username': phone}}
            rabbitmq.send_message(json.dumps(message))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductOutgoingId(APIView):
    """get 获取产品出库信息 store/product/outgoing/{id}"""
    """put 产品出库信息修改 store/product/outgoing/{id}"""
    """delete 产品出库信息删除 store/product/outgoing/{id}"""

    # 被干掉
    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_outgoing where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        rights = request.redis_cache["permission"].split(',')

        sql = "select t1.*, t2.name as creator from (select * from product_outgoing where id = '%s') t1 left join " \
              "user_info t2 on t1.creator_id = t2.phone;" % Id
        product_name = "select name, unit from products where id = '%s';"
        order_info = """
        select
            t1.*,
            t3.name as client_name,
            t4.name as creator
        from
            (
            select
                order_id,
                array_agg(product_count) as product_count,
                array_agg(name) as product_name,
                array_agg(unit) as unit
            from
                (
                select
                    t1.product_count,
                    t1.order_id,
                    t2.name,
                    t2.unit
                from
                    order_products t1
                left join products t2 on
                    t1.product_id = t2.id
                where
                    t1.order_id = '{}') t
            group by
                order_id ) t1
        left join orders t2 on
            t1.order_id = t2.id
        left join factory_clients t3 on
            t2.client_id = t3.id
        left join user_info t4 on
            t2.creator = t4.phone;"""
        target_0 = ['id', 'factory', 'products', 'counts', 'order_id', 'time', 'remark', 'creator_id']
        target_1 = ['order_id', 'product_count', 'product_name', 'unit', 'client_name', 'creator']
        try:
            cur.execute(sql)
            record = dict(zip(target_0, cur.fetchone()))
            cur.execute(order_info.format(record['order_id']))
            tmp = cur.fetchone() or []
            order = dict(zip(target_1, tmp))
            if order:
                ps = [{'product_name': i[0], 'unit': i[1], 'product_counts': i[2]}
                      for i in zip(order['product_name'], order['product_count'], order['unit'])]
                del order['product_name']
                del order['product_count']
                del order['unit']
                order['products'] = ps

            ns = []
            us = []
            for i in record['products']:
                cur.execute(product_name % i)
                tmp = cur.fetchone() or ('', '')
                ns.append(tmp[0] or '')
                us.append(tmp[1] or '')
            del record['factory']
            del record['order_id']
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        # erlang中count是整数类型，文档中是字符串
        products = [{'id': i[0], 'count': i[1], 'name': i[2], 'unit': i[3]}
                    for i in zip(record['products'], record['counts'], ns, us)]

        record['products'] = products
        del record['counts']
        record.update({'order': order})
        # flag
        if '1' in rights:
            flag = '0'
        elif record['creator_id'] == phone:
            flag = '0'
        elif not record['creator_id'] and phone:
            flag = '0'
        else:
            flag = '1'
        record['flag'] = flag

        postgresql.disconnect_postgresql(conn)
        return Response(record, status=status.HTTP_200_OK)

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_outgoing where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        products = request.data.get('products', [])
        order_id = request.data.get('order_id', None)
        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')

        product_id = []
        product_count = []
        for i in products:
            product_id.append(i['id'])
            product_count.append(i['count'])

        if order_id is not None:
            order_id = repr(order_id)
        else:
            order_id = 'NULL'

        update_sql = "update product_outgoing set products = '{}', counts = '{}', order_id = {}, time = {}, remark" \
                     " = '{}' where id = '{}';"
        delete_sql = "delete from products_log where use_id = '%s';" % Id
        products_log = "insert into products_log (id, factory, use_id, parent_type, product_id, count, time) " \
                       "values('{}', '{}', '{}', 'outgoing', '{}', {}, {});"

        try:
            cur.execute(delete_sql)
            for x, y in zip(product_id, product_count):
                uuid = generate_uuid()
                cur.execute(products_log.format(uuid, factory_id, Id, x, -y, Time))
            product_id = '{' + ','.join(product_id) + '}'
            product_count = '{' + ','.join(str(i) for i in product_count) + '}'
            cur.execute(update_sql.format(product_id, product_count, order_id, Time, remark, Id))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_outgoing where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        sql = "delete from product_outgoing where id = '%s';" % Id
        log_sql = "delete from products_log where use_id = '%s';" % Id
        try:
            cur.execute(sql)
            cur.execute(log_sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductIncoming(APIView):
    """产品入库 store/product/incoming"""

    def post(self, request):
        rabbitmq = UtilsRabbitmq()
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        phone = request.redis_cache['username']
        products = request.data.get('products', [])
        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')
        incoming_id = generate_uuid()

        id_list = [i['id'] for i in products]
        count_list = [i['count'] for i in products]

        insert_sql = "insert into product_incoming (id, factory, products, counts, time, remark, creator_id) " \
                     "values('{}', '{}', '{}', '{}', {}, '{}', '{}');"
        products_log = "insert into products_log (id, factory, use_id, parent_type, product_id, count, time) " \
                       "values('{}', '{}', '{}', 'incoming', '{}', {}, {});"
        try:
            for i in products:
                uuid = generate_uuid()
                cur.execute(products_log.format(uuid, factory_id, incoming_id, i['id'], i['count'], Time))
            product_id = '{' + ','.join(id_list) + '}'
            product_count = '{' + ','.join(str(i) for i in count_list) + '}'
            cur.execute(insert_sql.format(incoming_id, factory_id, product_id, product_count, Time, remark, phone))
            conn.commit()
            # send_message
            message = {'resource': 'PyStoreProductIncoming',
                       'type': 'POST',
                       'params': {'Fac': factory_id, 'UID': incoming_id, 'ProductIds': id_list,
                                  'ProductCounts': count_list, 'Username': phone}}
            rabbitmq.send_message(json.dumps(message))
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductIncomingId(APIView):
    """get 获取产品入库信息 store/product/incoming/{id}"""
    """put 产品入库信息修改 store/product/incoming/{id}"""
    """delete 产品入库信息删除 store/product/incoming/{id}"""

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_incoming where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        rights = request.redis_cache["permission"].split(',')

        sql = "select t1.*, t2.name as creator from (select * from product_incoming where id = '%s') t1 left join " \
              "user_info t2 on t1.creator_id = t2.phone;" % Id
        product_name = "select name, unit from products where id = '%s';"
        target_0 = ['id', 'factory', 'products', 'counts', 'time', 'remark', 'creator_id']
        try:
            cur.execute(sql)
            record = dict(zip(target_0, cur.fetchone()))
            ns = []
            us = []
            for i in record['products']:
                cur.execute(product_name % i)
                tmp = cur.fetchone() or ('', '')
                ns.append(tmp[0] or '')
                us.append(tmp[1] or '')
            del record['factory']
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        products = [{'id': i[0], 'count': i[1], 'name': i[2], 'unit': i[3]}
                    for i in zip(record['products'], record['counts'], ns, us)]
        record['products'] = products
        del record['counts']
        # flag
        if '1' in rights:
            flag = '0'
        elif record['creator_id'] == phone:
            flag = '0'
        elif not record['creator_id'] and phone:
            flag = '0'
        else:
            flag = '1'
        record['flag'] = flag

        postgresql.disconnect_postgresql(conn)
        return Response(record, status=status.HTTP_200_OK)

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_incoming where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        products = request.data.get('products', [])
        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')

        product_id = []
        product_count = []
        for i in products:
            product_id.append(i['id'])
            product_count.append(i['count'])

        update_sql = "update product_incoming set products = '{}', counts = '{}', time = {}, remark = '{}' " \
                     "where id = '{}';"
        delete_sql = "delete from products_log where use_id = '%s';" % Id
        products_log = "insert into products_log (id, factory, use_id, parent_type, product_id, count, time) " \
                       "values('{}', '{}', '{}', 'incoming', '{}', {}, {});"

        try:
            cur.execute(delete_sql)
            for x, y in zip(product_id, product_count):
                uuid = generate_uuid()
                cur.execute(products_log.format(uuid, factory_id, Id, x, y, Time))
            product_id = '{' + ','.join(product_id) + '}'
            product_count = '{' + ','.join(str(i) for i in product_count) + '}'
            cur.execute(update_sql.format(product_id, product_count, Time, remark, Id))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from product_incoming where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        sql = "delete from product_incoming where id = '%s';" % Id
        log_sql = "delete from products_log where use_id = '%s';" % Id
        try:
            cur.execute(sql)
            cur.execute(log_sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductMgmt(APIView):
    """新建产品名称 store/product/mgmt"""

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        phone = request.redis_cache['username']
        name = request.data.get('name')
        unit = request.data.get('unit', '')
        materials = request.data.get('materials', [])
        is_default = request.data.get('is_default', '1')
        category_id = request.data.get('category_id', '')
        init_count = request.data.get('init_count', 0)
        low_limit = request.data.get('low_limit', 0)
        number = request.data.get('number', '')
        spec = request.data.get('spec', [])

        spec = '{' + ','.join(spec) + '}'

        # name_check
        sql = "select count(1) from products where factory = '%s' and name = '%s';" % (factory_id, name)
        try:
            cur.execute(sql)
            check = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if check != 0:
            return Response({"res": 1, "errmsg": 'product name already exist'}, status=status.HTTP_200_OK)

        material_sql = "insert into product_materials (product_id, material_type_id, count, time) " \
                       "values ('{}', '{}', {}, {});"
        insert_sql = "insert into products (id, factory, name, is_default, time, unit, category_id, creator_id, " \
                     "low_limit, number, spec) values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}')"
        default = "update products set is_default = '0' where factory = '%s' and is_default = '1'" % factory_id
        products_log = "insert into products_log(id, factory, use_id, parent_type, product_id, count, time) " \
                       "values('{0}', '{1}', '{0}', 'init', '{0}', {2}, {3});"

        uuid = generate_uuid()
        Time = int(time.time())
        try:
            if is_default == '1':
                cur.execute(default)
            cur.execute(products_log.format(uuid, factory_id, init_count, Time))
            cur.execute(insert_sql.format(uuid, factory_id, name, is_default, Time, unit, category_id, phone,
                                          low_limit, number, spec))
            for i in materials:
                if i['count']:
                    cur.execute(material_sql.format(uuid, i['id'], i['count'], Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductMgmtId(APIView):
    """get 获取产品详情 store/product/mgmt/{id}"""
    """put 修改产品详情 store/product/mgmt/{id}"""
    """delete 删除产品 store/product/mgmt/{id}"""

    def get(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from products where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        rights = request.redis_cache["permission"].split(',')

        materials_sql = "select t1.*, t2.name, t2.unit from (select material_type_id as id, count from " \
                        "product_materials where product_id = '%s') t1 left join material_types t2 on t1.id = t2.id;" % Id
        product_sql = "select t.*,  COALESCE(t2.name, '') as creator, COALESCE(t3.name, '') as category_name from " \
                      "(select name, unit, is_default, low_limit, creator_id,category_id, number, spec from products " \
                      "where id = '%s') t left join user_info t2 on t.creator_id = t2.phone left join " \
                      "product_categories t3 on t.category_id = t3.id;" % Id
        target_0 = ['name', 'unit', 'is_default', 'low_limit', 'creator_id', 'category_id', 'number', 'spec', 'creator',
                    'category_name']
        target_1 = ['product_id', 'material_type_id', 'count', 'time']
        try:
            cur.execute(product_sql)
            order = dict(zip(target_0, cur.fetchone()))
            # flag
            if '1' in rights:
                flag = '0'
            elif order['creator_id'] == phone:
                flag = '0'
            elif not order['creator_id'] and phone:
                flag = '0'
            else:
                flag = '1'
            order['flag'] = flag

            cur.execute(materials_sql)
            materials = [dict(zip(target_1, i)) for i in cur.fetchall()]
            order['materials'] = materials
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response(order, status=status.HTTP_200_OK)

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from products where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        factory_id = request.redis_cache["factory_id"]
        name = request.data.get('name')
        unit = request.data.get('unit', '')
        # 原代码中没有对材料进行修改， 但我感觉应该可以修改，添加在注释中
        materials = request.data.get('materials', [])
        is_default = request.data.get('is_default', '1')
        category_id = request.data.get('category_id', '')
        low_limit = request.data.get('low_limit', 0)
        number = request.data.get('number', '')
        spec = request.data.get('spec', [])
        spec = '{' + ','.join(spec) + '}'

        # check
        sql = "select id, name, is_default from products where factory = '%s' and name = '%s';" % (factory_id, name)
        try:
            cur.execute(sql)
            tmp = cur.fetchone()
            if tmp and tmp[0] != Id:
                return Response({"res": 1, "errmsg": 'product name already exist'}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        update = "update products set name = '{}', is_default = '{}', time = {}, unit = '{}', category_id = '{}', " \
                 "low_limit = '{}', number = '{}', spec = '{}' where id = '{}';"
        default = "update products set is_default = '0' where factory = '%s' and is_default = '1';" % factory_id
        # delete_sql = "delete from product_materials where product_id = '%s';" % Id
        # material_sql = "insert into product_materials (product_id, material_type_id, count, time) " \
        #                "values ('{}', '{}', {}, {});"
        try:
            if is_default == '1':
                cur.execute(default)
            Time = int(time.time())
            cur.execute(update.format(name, is_default, Time, unit, category_id, low_limit, number, spec, Id))
            # cur.execute(delete_sql)
            # for i in materials:
            #     if i['count']:
            #         cur.execute(material_sql.format(Id, i['id'], i['count'], Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if len(Id):
            check_sql = "select count(1) from products where id = '%s';" % Id
            try:
                cur.execute(check_sql)
                tmp = cur.fetchone()[0]
                check_result = (tmp == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id is not exist'}, status=status.HTTP_200_OK)

        sql = "delete from products where id = '%s';" % Id

        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductCategory(APIView):
    """产品类别列表 store/product/category"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]

        sql = "select id, name from product_categories where factory = '%s';" % factory_id
        target = ['id', 'name']
        try:
            cur.execute(sql)
            tmp = cur.fetchall()
            if tmp:
                result = [dict(zip(target, i)) for i in tmp]
            else:
                result = {}
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'list': result}, status=status.HTTP_200_OK)

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        name = request.data.get('name')

        # check
        if not name:
            return Response({'res': 1, 'errmsg': "name can't be empty"}, status=status.HTTP_200_OK)
        if len(name) > 20:
            return Response({'res': 1, 'errmsg': "name too long"}, status=status.HTTP_200_OK)
        sql = "select count(1) from product_categories where factory = '%s' and name = '%s';" % (factory_id, name)
        try:
            cur.execute(sql)
            tmp = cur.fetchone()[0]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if tmp != 0:
            return Response({'res': 1, 'errmsg': "name already exist"}, status=status.HTTP_200_OK)

        save_sql = "insert into product_categories values('{}', '{}', '{}', {});"
        uuid = generate_uuid()
        Time = int(time.time())
        try:
            cur.execute(save_sql.format(uuid, factory_id, name, Time))
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreProductCategoryId(APIView):
    """修改产品类别详情 store/product/category/{id}"""

    # 没有修改
    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        name = request.data.get('name')

        # check
        if Id:
            sql = "select count(1) from product_categories where id = '%s';" % Id
            try:
                cur.execute(sql)
                check_result = (cur.fetchone()[0] == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id not exist'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        sql = "update product_categories set name = '%s' where id = '%s';" % (name, Id)
        name_exist = "select id from product_categories where factory = '%s' and name = '%s';" % (factory_id, name)

        try:
            cur.execute(name_exist)
            tmp = cur.fetchone()
            if tmp and tmp[0] != Id:
                return Response({"res": 1, "errmsg": 'name already exist'},
                                status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)

    def delete(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # check
        if Id:
            sql = "select count(1) from product_categories where id = '%s';" % Id
            try:
                cur.execute(sql)
                check_result = (cur.fetchone()[0] == 1)
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            check_result = False
        if not check_result:
            return Response({"res": 1, "errmsg": 'id not exist'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        delete_sql = "delete from product_categories where id = '%s';" % Id
        try:
            cur.execute(delete_sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)


class StoreOrders(APIView):
    def get(self, request):
        """仓库部-待发货订单列表(待完成, 已完成) store/orders"""
        product_id = request.query_params.get("id")  # 产品id, 多个id用空格连接组成一个字符串

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
            t1.id,
            t1.deliver_time,
            t1.state,
            COALESCE(t2.product_name, '{}') as product_name,
            COALESCE(t2.product_count, '{}') as product_count,
            COALESCE(t2.unit, '{}') as unit,
            t3.name as client_name,
            t4.name as creator
        from
            orders t1
        left join (
            select
                order_id,
                array_agg(product_count) as product_count,
                array_agg(name) as product_name,
                array_agg(unit) as unit
            from
                (
                select
                    t1.product_count,
                    t1.order_id,
                    t2.name,
                    t2.unit
                from
                    order_products t1
                left join products t2 on
                    t1.product_id = t2.id where t2.factory = '%s') t
            group by
                order_id
            ) t2 on
            t1.id = t2.order_id
        left join factory_clients t3 on
            t1.client_id = t3.id
        left join user_info t4 on
            t1.creator = t4.phone where t1.factory = '%s';
        """ % (factory_id, factory_id)
        # print(sql)

        try:
            # state = "1" 未完成, state = "0" 已完成
            doing, done = list(), list()  # 未完成，已完成
            cursor.execute(sql)
            result = cursor.fetchall()
            for res in result:
                di, temp, products = dict(), list(), list()
                if res[2] == "1":
                    di["id"] = res[0]
                    di["deliver_time"] = res[1]
                    di["client_name"] = res[6]
                    temp = list(zip(res[3], res[4], res[5]))
                    # print(temp)
                    for t in temp:
                        dt = dict()
                        dt["product_name"] = t[0]
                        dt["product_counts"] = str(t[1])
                        dt["unit"] = t[2]
                        products.append(dt)
                    di["products"] = products
                    doing.append(di)
                elif res[2] == "0":
                    di["id"] = res[0]
                    di["deliver_time"] = res[1]
                    di["client_name"] = res[6]
                    temp = list(zip(res[3], res[4], res[5]))
                    # print(temp)
                    for t in temp:
                        dt = dict()
                        dt["product_name"] = t[0]
                        dt["product_counts"] = str(t[1])
                        dt["unit"] = t[2]
                        products.append(dt)
                    di["products"] = products
                    done.append(di)

            return Response({"doing": doing, "done": done}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreMaterialCategory(APIView):
    """物料类别列表 store/material/category/{id}"""

    def get(self, request):
        """获取物料类别列表"""
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            category_list = list()
            cursor.execute("select id, name from material_categories where factory = '%s';" % factory_id)
            result = cursor.fetchall()
            for res in result:
                di = dict()
                di["id"] = res[0]
                di["name"] = res[1]
                category_list.append(di)

            return Response({"list": category_list}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        """新建物料类别"""
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        name = request.data.get("name")  # 新建物料名称
        if not name or len(name) > 20:
            return Response({"res": 1, "errmsg": "lack of name or length is too long! 缺少物料名称或物料名称长度大于20！"},
                            status=status.HTTP_200_OK)
        cursor.execute(
            "select count(1) from material_categories where factory = '%s' and name = '%s';" % (factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "material category name conflict! 物料名称已存在！"},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            cursor.execute("insert into material_categories values ('%s', '%s', '%s', %d);" % (
                generate_uuid(), factory_id, name, int(time.time())))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, id):
        """修改物料类别详情"""
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        name = request.data.get("name")  # 新建物料名称
        if not name or len(name) > 20:
            return Response({"res": 1, "errmsg": "lack of name or length is too long! 缺少物料名称或物料名称长度大于20！"},
                            status=status.HTTP_200_OK)
        cursor.execute("select id from material_categories where factory = '%s' and name = '%s';" % (factory_id, name))
        name_check = cursor.fetchone()
        # print("name_check=", name_check)
        if name_check:
            return Response({"res": 1, "errmsg": "material category name already exist! 物料类别名称已存在！"},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            cursor.execute("update material_categories set name = '%s' where id = '%s';" % (name, id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        """删除物料类别"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("delete from material_categories where id = '%s';" % id)
            connection.commit()
            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreMaterialManagement(APIView):
    """物料管理 store/material/mgmt/{id}"""

    def get(self, request, id):
        """获取物料详情"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        sql = """
        select
          t1.*,
          coalesce(t2.name, '') as category_name,
          coalesce(t3.name, '') as creator
        from
          (
          select
            id,
            name,
            unit,
            category_id,
            low_limit,
            creator_id,
            number,
            spec
          from
            material_types
          where
            id = '%s'
          ) t1
        left join material_categories t2 on
          t1.category_id = t2.id
        left join user_info t3 on
          t1.creator_id = t3.phone;
        """ % id
        # print(sql)
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            # print(result)

            data = dict()
            data["name"] = result[1]
            data["unit"] = result[2]
            data["category_id"] = result[3]
            data["number"] = result[6]
            data["spec"] = result[7]
            data["category_name"] = result[8]
            if "1" in permission:
                data["flag"] = "0"
            elif result[5] == phone:
                data["flag"] = "0"
            else:
                data["flag"] = "1"

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        """新建物料名称"""
        name = request.data.get("name")  # 物料名称
        unit = request.data.get("unit", "")  # 物料数量单位
        category_id = request.data.get("category_id", "")  # 物料类别id
        spec = request.data.get("spec", [])  # 规格说明 list ["内存:64g", "颜色:黑"]
        number = request.data.get("number")  # 编号
        init_count = request.data.get("init_count", 0)  # 初始数量
        low_limit = request.data.get("low_limit", 0)  #

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_types where factory = '%s' and name = '%s';" % (factory_id, name))
        name_check = cursor.fetchone()[0]
        if name_check >= 1:
            return Response({"res": 1, "errmsg": "material type already exist! 该物料类型已存在！"},
                            status=status.HTTP_400_BAD_REQUEST)
        try:
            uuid, timestamp = generate_uuid(), int(time.time())
            cursor.execute("insert into material_types (id, factory, name, unit, time, category_id, low_limit, "
                           "creator_id, number, spec) VALUES ('%s', '%s', '%s', '%s', %d, '%s', %s, '%s', '%s', "
                           "'{%s}');" % (uuid, factory_id, name, unit, timestamp, category_id, str(low_limit), phone,
                                         number, ','.join(spec)))
            cursor.execute("insert into materials_log (id, factory, use_id, parent_type, material_type_id,"
                           " material_count, time) VALUES ('%s', '%s', '%s', 'init', '%s', %s, %d);" % (
                               uuid, factory_id, uuid, uuid, str(init_count), timestamp))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, id):
        """修改物料详情"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        name = request.data.get("name")  # 物料名称
        unit = request.data.get("unit", "")  # 物料数量单位
        category_id = request.data.get("category_id", "")  # 物料类别id
        spec = request.data.get("spec", [])  # 规格说明 list ["内存:64g", "颜色:黑"]
        number = request.data.get("number")  # 编号
        low_limit = request.data.get("low_limit", 0)  #

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute(
                "select id from material_types where factory = '%s' and name = '%s';" % (factory_id, name))
            name_check = cursor.fetchone()
            # print("name_check=", name_check)

            if name_check:
                if name_check[0] != id:
                    return Response({"res": 1, "errmsg": "material type already exist! 该物料类型已存在！"},
                                    status=status.HTTP_400_BAD_REQUEST)

            cursor.execute("update material_types set name = '%s', unit = '%s', time = %d, category_id = '%s', "
                           "low_limit = %s, number = '%s', spec = '{%s}' where id = '%s';" % (
                               name, unit, int(time.time()), category_id, str(low_limit), number, ','.join(spec), id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        """删除物料"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()
        try:
            cursor.execute("select count(1) from material_types where id = '%s';" % id)
            id_check = cursor.fetchone()[0]
            if id_check == 0:
                return Response({"res": 1, "errmsg": "material type id doesn't exist, can't delete! 物料类型id不存在，不能删除！"},
                                status=status.HTTP_400_BAD_REQUEST)
            cursor.execute("delete from material_types where id = '%s';" % id)
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreMaterialIncoming(APIView):
    """物料入库相关 store/material/incoming/{id}"""

    def get(self, request, id):
        """获取物料入库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material id! 缺少物料id！"}, status=status.HTTP_200_OK)

        item_id = request.query_params.get("item_id")

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        incoming_sql = """
        select
          t1.*,
          t2.name as creator
        from
          (
          select
            *
          from
            material_incoming
          where
            id = '%s'
          ) t1
        left join user_info t2 on
          t1.creator_id = t2.phone;
        """ % id

        if not item_id:
            material_sql = """
            select
              coalesce(unit, '') as unit,
              coalesce(name, '') as name
            from
              material_types
            where
              id = '%s';
            """
        else:
            material_sql = """
            select
              t1.item_count as count,
              t1.time,
              t2.name,
              t2.unit,
              t2.id
            from
              item_in_out t1
            left join material_types t2 on
              t1.item_id = t2.id
            where
              t1.in_out_id = '%s' and t1.item_id = '%s';
            """
        # print("incoming_sql=", incoming_sql), print("material_sql=", material_sql)

        try:
            cursor.execute(incoming_sql)
            incoming_result = cursor.fetchone()
            if not incoming_result:
                return Response({"res": 1, "errmsg": "material incoming id doesn't exist! 此物料入库记录id不存在！"},
                                status=status.HTTP_200_OK)

            incoming_list = incoming_result[2]
            # print(incoming_result), print(incoming_list)
            temp_list, materials = list(), list()

            if not item_id:
                for material_id in incoming_list:
                    cursor.execute(material_sql % material_id)
                    material_result = cursor.fetchone()
                    # print(material_result)
                    temp_list.append(list(material_result))
                # print(temp_list)
                result = list(zip(incoming_result[2], incoming_result[3], temp_list))
                # print(result)
                for res in result:
                    di = dict()
                    di["id"] = res[0] or ""
                    di["count"] = res[1] or 0
                    di["unit"] = res[2][0] or ""
                    di["name"] = res[2][1] or ""
                    materials.append(di)
            else:
                cursor.execute(material_sql % (id, item_id))
                material_result = cursor.fetchone()
                # print(material_result)
                di = dict()
                di["count"] = material_result[0] or 0
                di["name"] = material_result[2] or ""
                di["unit"] = material_result[3] or ""
                di["id"] = material_result[4] or ""
                materials.append(di)

            if "1" in permission:
                flag = "0"
            elif incoming_result[6] == phone:
                flag = "0"
            else:
                flag = "1"

            data = {"id": incoming_result[0], "materials": materials, "time": incoming_result[4],
                    "remark": incoming_result[5] or "", "flag": flag, "creator": incoming_result[7] or ""}

            return Response(data, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        """物料入库"""
        materials = request.data.get("materials", [])  # 列表 [{'id': '9f120N5ITjATRc9S2S', 'count': 220}......]
        timestamp = request.data.get("time", int(time.time()))  # 发货时间
        remark = request.data.get("remark", "")  # 备注信息
        # print("materials=", materials)
        if not materials:
            return Response({"res": 1, "errmsg": "material list is empty! 物料列表为空，无法添加入库！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        id_list, count_list = list(), list()
        uuid = generate_uuid()

        sql = """
        update
          material_types set
          notice_state = '%s'
        where
          id = '%s'
          and low_limit <
          (
            select
              sum( material_count )
            from
              materials_log
            where
              material_type_id = '%s'
              and ( parent_type = 'incoming'
              or parent_type = 'outgoing'
              or parent_type = 'store_check'
              or parent_type = 'init'
              or parent_type = 'order'
              or parent_type = 'product' )
            group by
              material_type_id
          );
        """

        try:
            for mat in materials:
                # print(mat)
                id_list.append(mat["id"])
                count_list.append(str(int(mat["count"])))  # 数量可能是小数，数据库字段类型是integer
                cursor.execute("insert into materials_log (id, factory, use_id, parent_type, material_type_id, "
                               "material_count, time) VALUES ('%s', '%s', '%s', 'incoming', '%s', %s, %d);" % (
                                   generate_uuid(), factory_id, uuid, mat["id"], str(int(mat["count"])), timestamp))
                cursor.execute("insert into item_in_out (in_out_id, item_id, item_count, time) VALUES "
                               "('%s', '%s', %d, %d);" % (uuid, mat["id"], int(mat["count"]), timestamp))
                cursor.execute(sql % (StoreNoticeEnum.not_yet.value, mat["id"], mat["id"]))

            # print("id_list=", id_list), print("count_list=", count_list)
            cursor.execute("insert into material_incoming (id, factory, time, remark, creator_id) VALUES "
                           "('%s', '%s', %d, '%s', '%s');" % (uuid, factory_id, timestamp, remark, phone))
            connection.commit()

            message = {'resource': 'PyStoreMaterialIncoming', 'type': 'POST',
                       'params': {'Fac': factory_id, 'UID': uuid, 'MaterialIds': id_list, 'MaterialCounts': count_list,
                                  'Creator': phone}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, id):
        """修改物料入库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        materials = request.data.get("materials", [])  # 列表 [{'id': '9f120N5ITjATRc9S2S', 'count': 220}......]
        timestamp = request.data.get("time", int(time.time()))  # 发货时间
        remark = request.data.get("remark", "")  # 备注信息
        # print("materials=", materials)
        if not materials:
            return Response({"res": 1, "errmsg": "material list is empty! 物料列表为空，无法添加入库！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_incoming where id = '%s';" % id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "material incoming id doesn't exist! 物料入库记录id不存在，无法修改！"},
                            status=status.HTTP_200_OK)

        id_list, count_list = list(), list()
        sql = """
        update
          material_types set
          notice_state = '%s'
        where
          id = '%s'
          and low_limit <
          (
            select
              sum( material_count )
            from
              materials_log
            where
              material_type_id = '%s'
              and ( parent_type = 'incoming'
              or parent_type = 'outgoing'
              or parent_type = 'store_check'
              or parent_type = 'init'
              or parent_type = 'order'
              or parent_type = 'product' )
            group by
              material_type_id
          );
        """

        try:
            for mat in materials:
                # print(mat)
                id_list.append(mat["id"])
                count_list.append(str(int(mat["count"])))
                cursor.execute(
                    "delete from materials_log where use_id = '%s' and material_type_id = '%s';" % (id, mat["id"]))
                cursor.execute("insert into materials_log (id, factory, use_id, parent_type, material_type_id, "
                               "material_count, time) VALUES ('%s', '%s', '%s', 'incoming', '%s', %s, %d);" % (
                                   generate_uuid(), factory_id, id, mat["id"], str(int(mat["count"])), timestamp))
                cursor.execute("update item_in_out set item_count = %d, time = %d where in_out_id = '%s' and "
                               "item_id = '%s';" % (int(mat["count"]), timestamp, id, mat["id"]))
                cursor.execute(sql % (StoreNoticeEnum.not_yet.value, mat["id"], mat["id"]))

            # print("id_list=", id_list), print("count_list=", count_list)
            cursor.execute("update material_incoming set remark = '%s' where id = '%s';" % (remark, id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        """删除物料入库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_400_BAD_REQUEST)

        item_id = request.query_params.get("item_id")

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select count(1) from material_incoming where id = '%s';" % id)
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "This material type id doesn't exist! 此物料类别id不存在，无法删除！"},
                                status=status.HTTP_400_BAD_REQUEST)

            cursor.execute("delete from item_in_out where in_out_id = '%s' and item_id = '%s';" % (id, item_id))
            cursor.execute(
                "delete from materials_log where use_id = '%s' and material_type_id = '%s';" % (id, item_id))

            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreMaterialOutgoing(APIView):
    """物料出库相关 store/material/outgoing/{id}"""

    def get(self, request, id):
        """获取物料出库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material outgoing id! 缺少物料出库id！"}, status=status.HTTP_200_OK)

        item_id = request.query_params.get("item_id")

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_outgoing where id = '%s';" % id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "material incoming id doesn't exist! 物料入库记录id不存在，无法修改！"},
                            status=status.HTTP_200_OK)

        outgoing_sql = """
        select
          t1.*,
          t2.name as creator
        from
          (
          select
            *
          from
            material_outgoing
          where
            id = '%s'
          ) t1
        left join user_info t2 on
          t1.creator_id = t2.phone;
        """ % id

        if not item_id:
            material_sql = """
            select
              unit,
              name
            from
              material_types
            where
              id = '%s';
            """
        else:
            material_sql = """
            select
              t1.item_count as count,
              t1.time,
              t2.name,
              t2.unit,
              t2.id
            from
              item_in_out t1
            left join material_types t2 on
              t1.item_id = t2.id
            where
              t1.in_out_id = '%s' and t1.item_id = '%s';
            """
        # print("outgoing_sql=", outgoing_sql), print("material_sql=", material_sql)

        try:
            cursor.execute(outgoing_sql)
            outgoing_result = cursor.fetchone()
            if not outgoing_result:
                return Response({"res": 1, "errmsg": "material outgoing id doesn't exist! 此物料出库记录id不存在！"},
                                status=status.HTTP_200_OK)
            outgoing_list = outgoing_result[2]
            # print(outgoing_result), print(outgoing_list)
            temp_list, materials = list(), list()

            if not item_id:
                for material_id in outgoing_list:
                    cursor.execute(material_sql % material_id)
                    material_result = cursor.fetchone()
                    # print(material_result)
                    temp_list.append(list(material_result))
                # print(temp_list)
                result = list(zip(outgoing_result[2], outgoing_result[3], temp_list))
                # print(result)
                for res in result:
                    di = dict()
                    di["id"] = res[0] or ""
                    di["count"] = res[1] or 0
                    di["unit"] = res[2][0] or ""
                    di["name"] = res[2][1] or ""
                    materials.append(di)
            else:
                cursor.execute(material_sql % (id, item_id))
                material_result = cursor.fetchone()
                # print(material_result)
                di = dict()
                di["count"] = material_result[0] or 0
                di["name"] = material_result[2] or ""
                di["unit"] = material_result[3] or ""
                di["id"] = material_result[4] or ""
                materials.append(di)

            if "1" in permission:
                flag = "0"
            elif outgoing_result[6] == phone:
                flag = "0"
            else:
                flag = "1"

            data = {"id": outgoing_result[0], "materials": materials, "time": outgoing_result[4],
                    "remark": outgoing_result[5], "flag": flag, "creator": outgoing_result[7] or ""}

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request):
        """物料出库"""
        materials = request.data.get("materials", [])  # 列表 [{'id': '9f120N5ITjATRc9S2S', 'count': 220}......]
        timestamp = request.data.get("time", int(time.time()))  # 发货时间
        remark = request.data.get("remark", "")  # 备注信息
        # print("materials=", materials)
        if not materials:
            return Response({"res": 1, "errmsg": "material list is empty! 物料列表为空，无法添加入库！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        id_list, count_list = list(), list()
        uuid = generate_uuid()

        try:
            for mat in materials:
                # print(mat)
                id_list.append(mat["id"])
                count_list.append(str(int(mat["count"])))  # 数量可能是小数，数据库字段类型是integer
                cursor.execute("insert into materials_log (id, factory, use_id, parent_type, material_type_id, "
                               "material_count, time) VALUES ('%s', '%s', '%s', 'outgoing', '%s', %s, %d);" % (
                                   generate_uuid(), factory_id, uuid, mat["id"], str(-int(mat["count"])), timestamp))
                cursor.execute("insert into item_in_out (in_out_id, item_id, item_count, time) VALUES "
                               "('%s', '%s', %d, %d);" % (uuid, mat["id"], int(mat["count"]), timestamp))

            # print("id_list=", id_list), print("count_list=", count_list)
            cursor.execute("insert into material_outgoing (id, factory, time, remark, creator_id) VALUES "
                           "('%s', '%s', %d, '%s', '%s');" % (uuid, factory_id, timestamp, remark, phone))
            connection.commit()

            message = {'resource': 'PyStoreMaterialOutgoing', 'type': 'POST',
                       'params': {'Fac': factory_id, 'UID': uuid, 'MaterialIds': id_list, 'MaterialCounts': count_list,
                                  'Creator': phone}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def put(self, request, id):
        """修改物料出库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"}, status=status.HTTP_200_OK)

        materials = request.data.get("materials", [])  # 列表 [{'id': '9f120N5ITjATRc9S2S', 'count': 220}......]
        timestamp = request.data.get("time", int(time.time()))  # 发货时间
        remark = request.data.get("remark", "")  # 备注信息
        # print("materials=", materials)
        if not materials:
            return Response({"res": 1, "errmsg": "material list is empty! 物料列表为空，无法添加入库！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        cursor.execute("select count(1) from material_outgoing where id = '%s';" % id)
        id_check = cursor.fetchone()[0]
        if id_check <= 0:
            return Response({"res": 1, "errmsg": "material incoming id doesn't exist! 物料入库记录id不存在，无法修改！"},
                            status=status.HTTP_200_OK)

        id_list, count_list = list(), list()

        try:
            for mat in materials:
                # print(mat)
                id_list.append(mat["id"])
                count_list.append(str(int(mat["count"])))
                cursor.execute(
                    "delete from materials_log where use_id = '%s' and material_type_id = '%s';" % (id, mat["id"]))
                cursor.execute("update item_in_out set item_count = %d, time = %d where in_out_id = '%s' and "
                               "item_id = '%s';" % (int(mat["count"]), timestamp, id, mat["id"]))
                cursor.execute("insert into materials_log (id, factory, use_id, parent_type, material_type_id, "
                               "material_count, time) VALUES ('%s', '%s', '%s', 'outgoing', '%s', %s, %d);" % (
                                   generate_uuid(), factory_id, id, mat["id"], str(-int(mat["count"])), timestamp))

            # print("id_list=", id_list), print("count_list=", count_list)
            cursor.execute("update material_outgoing set remark = '%s' where id = '%s';" % (remark, id))

            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def delete(self, request, id):
        """删除物料出库信息"""
        if not id:
            return Response({"res": 1, "errmsg": "lack of material category id! 缺少物料类别id！"},
                            status=status.HTTP_200_OK)

        item_id = request.query_params.get("item_id")

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            cursor.execute("select count(1) from material_outgoing where id = '%s';" % id)
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "This material type id doesn't exist! 此物料类别id不存在，无法删除！"},
                                status=status.HTTP_200_OK)

            cursor.execute("delete from item_in_out where in_out_id = '%s' and item_id = '%s';" % (id, item_id))
            cursor.execute("delete from materials_log where use_id = '%s' and material_type_id = '%s';" % (id, item_id))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreNotice(APIView):
    """仓库部 消息通知 store/notice/{type}/{id}"""

    def get(self, request, type, id):
        notice_sql = """
        select
          t1.item_id,
          t1.count,
          t1.pick_time,
          t1.incoming_time,
          t1.remark,
          t1.create_time,
          t1.low_limit,
          t1.stock,
          t1.state,
          t2.name as creator
        from
          store_notice t1
        left join user_info t2 on
          t1.creator = t2.phone
        where t1.id = '%s';
        """ % id

        material_info_sql = """
        select
          t1.name,
          t2.name as category,
          t1.unit
        from
          material_types t1
        left join material_categories t2 on
          t1.category_id = t2.id
        where t1.id = '%s';
        """

        product_info_sql = """
        select
          t1.name,
          coalesce(t2.name, '') as category,
          t1.unit
        from
          products t1
        left join product_categories t2 on
          t1.category_id = t2.id
        where
          t1.id = '%s';
        """

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        if id:
            cursor.execute("select count(1) from store_notice where id = '%s';" % id)
            id_check = cursor.fetchone()[0]
            if id_check <= 0:
                return Response({"res": 1, "errmsg": "This id doesn't exist! 此通知id不存在！"}, status=status.HTTP_200_OK)
        else:
            return Response({"res": 1, "errmsg": "Lack of id! 缺少参数id！"}, status=status.HTTP_200_OK)

        try:
            data = {}

            cursor.execute(notice_sql)
            result = cursor.fetchone()
            data["item_id"] = result[0]
            data["count"] = result[1]
            data["pick_time"] = result[2]
            data["incoming_time"] = result[3]
            data["remark"] = result[4]
            data["create_time"] = result[5]
            data["low_limit"] = result[6]
            data["stock"] = result[7]
            data["state"] = result[8]
            data["creator"] = result[9]

            if type == "material":
                cursor.execute(material_info_sql % result[0])
            elif type == "product":
                cursor.execute(product_info_sql % result[0])
            else:
                return Response({"res": 1, "errmsg": "Type code error! 类型代号错误！"}, status=status.HTTP_200_OK)

            temp = cursor.fetchone()
            data["name"] = temp[0] or ""
            data["category"] = temp[1] or ""
            data["unit"] = temp[2] or ""

            return Response(data, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)

    def post(self, request, type, id):
        count = request.data.get("count", 0)  # 数量
        pick_time = request.data.get("pick_time", 0)  # 领料时间
        incoming_time = request.data.get("incoming_time", 0)  # 预计入库时间
        remark = request.data.get("remark", "")  # 备注

        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]
        permission = request.redis_cache["permission"]
        # print(phone, factory_id, permission)

        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        uuid = generate_uuid()
        timestamp = int(time.time())

        material_stock_sql = """
        select
          t1.low_limit,
          t2.stock
        from
          material_types t1
        left join
          (
          select
            material_type_id,
            sum(material_count) as stock
          from
            materials_log
          where
            (
            parent_type = 'incoming'
            or parent_type = 'outgoing'
            or parent_type = 'store_check'
            or parent_type = 'init'
            or parent_type = 'order'
            or parent_type = 'product'
            )
          group by material_type_id
          )
        t2 on
          t1.id = t2.material_type_id
        where
          t1.id = '%s';
        """ % id

        product_stock_sql = """
        select
          t1.low_limit,
          t2.stock
        from
          products t1
        left join
          (
          select
            product_id,
            sum(count) as stock
          from
            products_log
          where
            (
            parent_type = 'incoming'
            or parent_type = 'outgoing'
            or parent_type = 'store_check'
            or parent_type = 'init'
            or parent_type = 'order'
            or parent_type = 'product'
            )
          group by product_id
          )
        t2 on
          t1.id = t2.product_id
        where
          t1.id = '%s';
        """ % id

        material_or_product_sql = """
        insert into
          store_notice (id, factory_id, type, item_id, count, stock, low_limit, pick_time, incoming_time,
          remark, create_time, creator)
        values
          ('%s', '%s', '%s', '%s', %s, %s, %s, %d, %d, '%s', %d, '%s');
        """

        if not id or not type:
            return Response({"res": 1, "errmsg": "Lack of id or type! 缺少参数id或类型type！"}, status=status.HTTP_200_OK)

        try:
            if type == "material":
                cursor.execute("select count(1) from material_types where id = '%s' and notice_state = '%s';" % (
                    id, StoreNoticeEnum.not_yet.value))
                id_check = cursor.fetchone()[0]
                if id_check <= 0:
                    return Response({"res": 1, "errmsg": "This id doesn't exist! 此id不存在！"}, status=status.HTTP_200_OK)

                cursor.execute(material_stock_sql)
                material_stock = cursor.fetchone()
                if material_stock:
                    low_limit = material_stock[0] if material_stock[0] else 0
                    stock = material_stock[1] if material_stock[1] else 0
                else:
                    low_limit, stock = 0, 0

                cursor.execute(material_or_product_sql % (uuid, factory_id, StoreTypeEnum.material.value, id,
                                                          str(count), str(stock), str(low_limit), pick_time,
                                                          incoming_time, remark, timestamp, phone))
                cursor.execute("update material_types set notice_state = '%s' where id = '%s';" %
                               (StoreNoticeMsgEnum.msg_done.value, id))
            elif type == "product":
                cursor.execute("select count(1) from products where id = '%s' and notice_state = '%s';" % (
                    id, StoreNoticeEnum.not_yet.value))
                id_check = cursor.fetchone()[0]
                if id_check <= 0:
                    return Response({"res": 1, "errmsg": "This id doesn't exist! 此id不存在！"}, status=status.HTTP_200_OK)

                cursor.execute(product_stock_sql)
                product_stock = cursor.fetchone()
                low_limit = product_stock[0] if product_stock else 0
                stock = product_stock[1] if product_stock else 0

                cursor.execute(material_or_product_sql % (uuid, factory_id, StoreTypeEnum.product.value, id,
                                                          str(count), str(stock), str(low_limit), pick_time,
                                                          incoming_time, remark, timestamp, phone))
                cursor.execute(
                    "update products set notice_state = '%s' where id = '%s';" % (
                        StoreNoticeMsgEnum.msg_done.value, id))
            else:
                return Response({"res": 1, "errmsg": "Type code error! 类型代号错误！"}, status=status.HTTP_200_OK)

            # connection.commit()

            message = {'resource': 'PyStoreNotice', 'type': 'POST',
                       'params': {'Fac': factory_id, 'Type': type, 'ItemId': id, 'Count': count, 'UID': uuid,
                                  'Creator': phone}}
            # print("message=", message)
            rabbitmq = UtilsRabbitmq()
            rabbitmq.send_message(json.dumps(message))

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)


class StoreMaterialPrepare(APIView):
    """待备物料列表 store/material/prepare"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        factory_id = request.redis_cache["factory_id"]
        Type = request.query_params.get('type', '1')
        page = request.query_params.get('page', '1')

        start = (int(page) - 1) * 10

        sql_1 = """
        select
            t1.prepare_state,
            count(1)
        from
            product_task t1
        left join (
            select
                use_id,
                array_agg( material_type_id ),
                array_agg( material_count )
            from
                materials_log
            where
                parent_type = 'product'
            group by
                use_id ) t2 on
            t1.id = t2.use_id
        where
            t1.factory = '%s'
            and t2.use_id notnull group by t1.prepare_state;""" % factory_id

        sql_2 = """
        select
            *
        from
            (
            select
                coalesce( t4.name,
                '' ) as product_category,
                t3.name as product_name,
                t3.unit as product_unit,
                t1.target_count,
                t2.material_type_id,
                t2.material_count,
                t2.material_name,
                t2.material_unit,
                t2.material_category,
                t2.low_limit,
                t2.stock_count,
                t1.start_time,
                t1.id,
                t5.name as creator,
                row_number() over (
            order by
                t1.start_time desc ) as rn
            from
                product_task t1
            left join (
                select
                    t1.use_id,
                    array_agg(t1.material_type_id) as material_type_id,
                    array_agg(t1.material_count) as material_count ,
                    array_agg(t2.name) as material_name,
                    array_agg(t2.unit) as material_unit,
                    array_agg(t3.name) as material_category,
                    array_agg( t4.low_limit ) as low_limit,
                    array_agg( t4.count ) as stock_count
                from
                    materials_log t1
                left join material_types t2 on
                    t1.material_type_id = t2.id
                left join material_categories t3 on
                    t2.category_id = t3.id
                left join (
                    select
                        t1.material_type_id,
                        t1.count,
                        t2.low_limit
                    from
                        (
                        select
                            material_type_id,
                            sum(material_count) as count
                        from
                            materials_log
                        where
                            ( parent_type = 'incoming'
                            or parent_type = 'outgoing'
                            or parent_type = 'store_check'
                            or parent_type = 'init'
                            or parent_type = 'order'
                            or parent_type = 'product' )
                        group by
                            material_type_id ) t1
                    left join material_types t2 on
                        t1.material_type_id = t2.id
                    where
                        t2.id notnull ) t4 on
                    t1.material_type_id = t4.material_type_id
                where
                    t1.parent_type = 'product'
                group by
                    use_id ) t2 on
                t1.id = t2.use_id
            left join products t3 on
                t1.product_id = t3.id
            left join product_categories t4 on
                t3.category_id = t4.id
            left join user_info t5 on
                t1.creator = t5.phone
            where
                t1.factory = '{}' and t1.prepare_state = '{}'
                and t2.use_id notnull
            order by
                t1.time desc ) t
        where
            t.rn > {}
        limit {};""".format(factory_id, Type, start, 10)

        target_1 = ['prepare_state', 'count']
        target_2 = ['product_category', 'product_name', 'product_unit', 'target_count', 'material_type_id',
                    'material_count', 'material_name', 'material_unit', 'material_category', 'low_limit',
                    'stock_count', 'start_time', 'id', 'creator', 'rn']

        try:
            cur.execute(sql_1)
            state_list = [dict(zip(target_1, i)) for i in cur.fetchall()]
            cur.execute(sql_2)
            pre_result = [dict(zip(target_2, i)) for i in cur.fetchall()]
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        for i in pre_result:
            materials = list()
            for j in zip(i['material_type_id'], i['material_count'], i['material_name'], i['material_unit'],
                         i['material_category'], i['stock_count'], i['low_limit']):
                tmp = dict()
                tmp['id'] = j[0]
                tmp['count'] = -j[1]
                tmp['material_name'] = j[2]
                tmp['material_unit'] = j[3]
                tmp['material_category'] = j[4]
                if j[5] is not None and j[6] is not None and j[1] is not None:
                    tmp['low_stock'] = 0 if j[5] - j[6] + j[1] > 0 else 1
                else:
                    tmp['low_stock'] = 1
                materials.append(tmp)
            del i['material_type_id']
            del i['material_count']
            del i['material_name']
            del i['material_unit']
            del i['material_category']
            del i['stock_count']
            del i['low_limit']
            i['materials'] = materials

        result = {PRODUCT_MATERIAL_DICT['material_not']: 0,
                  PRODUCT_MATERIAL_DICT['material_ing']: 0,
                  PRODUCT_MATERIAL_DICT['material_done']: 0, }
        for i in state_list:
            if i['prepare_state'] in result:
                result[i['prepare_state']] = i['count']

        result['list'] = pre_result
        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class StoreMaterialPrepareId(APIView):
    """修改备料状态 store/material/prepare/{id}"""

    def put(self, request, Id):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        Type = request.data.get('type')

        if Type not in PRODUCT_MATERIAL_DICT.values():
            postgresql.disconnect_postgresql(conn)
            return Response({'res': 1, 'errmsg': 'wrong material prepare type'}, status=status.HTTP_200_OK)

        sql = "update product_task set prepare_state = '{}' where id = '{}';".format(Type, Id)
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({'res': 0}, status=status.HTTP_200_OK)