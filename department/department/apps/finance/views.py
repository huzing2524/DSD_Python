# -*- coding: utf-8 -*-
import time
import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from apps_utils import UtilsPostgresql, UtilsRabbitmq, generate_uuid
from finance.finance_utils import correct_time

logger = logging.getLogger('django')

# todo 这部分接口的方法很奇怪，都是post
# 财务部-----------------------------------------------------------------------------------------------------------------


class FinanceSummary(APIView):
    """每月财务数据总览 /finance/summary"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        start_year = request.query_params.get('year', '2018')
        start_month = request.query_params.get('month', '8')

        start_year, start_month = int(start_year), int(start_month)
        end_year, end_month = correct_time(start_year, start_month+1)
        start_timestamp = time.mktime(time.strptime('%d/%d' % (start_year, start_month), '%Y/%m'))
        end_timestamp = time.mktime(time.strptime('%d/%d' % (end_year, end_month), '%Y/%m')) - 1
        # 从缓存中获取factory_id
        factory_id = request.redis_cache["factory_id"]

        costsql = "select sum(count) as count from finance_logs where factory = '%s' and time >= %f and time < %f" \
                  " and count < 0;" % (factory_id, start_timestamp, end_timestamp)
        financeincomesql = "select sum(count) as count from finance_logs where factory = '%s' and time >= %f and " \
                           "time < %f and count > 0;" % (factory_id, start_timestamp, end_timestamp)
        notpaysql = "select sum(t2.sell_price - t1.collected) as count from orders t1 left join(select sum(sell" \
                    "_price) as sell_price, order_id from order_products group by order_id ) t2 on t1.id = t2.order_" \
                    "id where t1.factory = '%s' and t1.time >= %f and t1.time < %f and t1.collected < t2.sell_price;"\
                    % (factory_id, start_timestamp, end_timestamp)
        ordersql = "select count(1) from finance_logs where factory = '%s' and time >= %f and time < %f;" %\
                   (factory_id, start_timestamp, end_timestamp)

        try:
            cur.execute(costsql)
            cost = cur.fetchone()[0] or 0
            cur.execute(financeincomesql)
            sales = cur.fetchone()[0] or 0
            cur.execute(notpaysql)
            not_pay = cur.fetchone()[0] or 0
            cur.execute(ordersql)
            orders = cur.fetchone()[0] or 0
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        cost = cost if cost else 0
        sales = sales if sales else 0

        profit = sales + cost
        if profit > 0:
            profit_rate = '%.2f' % (profit / sales * 100) + '%' if sales else '0.00%'
        else:
            profit_rate = '0.00%'

        result = dict()
        result['cost'] = cost
        result['sales'] = sales
        result['not_pay'] = not_pay
        result['orders'] = orders
        result['profit'] = profit
        result['profit_rate'] = profit_rate

        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class FinanceList(APIView):
    """每月财务数据列表 /finance/list"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # 从缓存中获取factory_id
        factory_id = request.redis_cache["factory_id"]

        sql = "select use_id as id, type as name, count, parent_type, time, case when count > 0 then '0' else '1' end" \
              " as type from finance_logs where factory = '%s';" % factory_id
        try:
            cur.execute(sql)
            tmp = cur.fetchall()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        tmp = sorted(tmp, key=lambda x: -x[4])

        result = []
        date_list = []
        for i in tmp:
            date = time.strftime("%Y-%m", time.localtime(i[4]))
            year, month = date.split('-')
            month = month.lstrip('0')

            if date in date_list:
                index = date_list.index(date)
                t = result[index]
            else:
                t = dict()
                t['in'] = 0
                t['out'] = 0
                t['year'] = year
                t['month'] = month
                t['list'] = []
                date_list.append(date)
                result.append(t)

            if i[2] > 0:
                t['in'] += i[2]
            else:
                t['out'] += -i[2]
            t['list'].append({'id': i[0], 'name': i[1] or '', 'count': i[2] or '', 'parent_type': i[3], 'time': i[4],
                              'type': i[5]})
        postgresql.disconnect_postgresql(conn)
        return Response({'data': result}, status=status.HTTP_200_OK)


class FinanceDetail(APIView):
    """记账详情 /finance/detail"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        Id = request.query_params.get('id', 'id')
        # 从缓存中获取rights
        phone = request.redis_cache["username"]
        rights = request.redis_cache["permission"].split(',')

        sql = "select t1.name, t.price as count, t.remark, t.finance_type_id as type_id, t.time, t1.type, COALESCE(" \
              "t2.name, '') as creator, t.creator_id from (select * from finance_output where id = '%s')t left join" \
              " finance_types t1 on t.finance_type_id = t1.id left join user_info t2 on t.creator_id = t2.phone;"\
              % Id
        # 记账名称, 记账数目, 备注, 记账id, 记账日期, 支出/收入, 创建人, 创建人手机号
        target = ['name', 'count', 'remark', 'type_id', 'time', 'type', 'creator', 'creator_id']
        try:
            cur.execute(sql)
            tmp = cur.fetchone()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if tmp:
            if '1' in rights:
                flag = '0'
            elif phone == tmp[-1]:
                flag = '0'
            elif not tmp[-1] and phone:
                flag = '0'
            else:
                flag = '1'

            tmp = [i if i is not None else '' for i in tmp]
            result = dict(zip(target, tmp))
            if result['count'] == '':
                result['count'] = 0
            else:
                result['count'] = abs(result['count'])
            result['flag'] = flag
        else:
            result = dict()
        postgresql.disconnect_postgresql(conn)
        return Response(result, status=status.HTTP_200_OK)


class FinanceNew(APIView):
    """新建记账 /finance/new"""

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        type_id = request.data.get('type_id', '')
        name = request.data.get('name', '')
        count = request.data.get('count')
        # TODO 文档中并没有时间参数, 而且为什么默认值为0？
        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')
        # 从缓存中获取factory_id
        phone = request.redis_cache["username"]
        factory_id = request.redis_cache["factory_id"]

        # check
        try:
            count = float(count)
        except Exception as e:
            return Response({"res": 1, "errmsg": "parameter 'count' error"}, status=status.HTTP_200_OK)
        if count < 0:
            return Response({"res": 1, "errmsg": "parameter 'count' error"}, status=status.HTTP_200_OK)
        type_sql = "select type from finance_types where id = '%s';" % type_id
        try:
            cur.execute(type_sql)
            tmp = cur.fetchone()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if not tmp or tmp[0] not in ['0', '1']:
            return Response({"res": 1, "errmsg": "parameter 'type_id' error"}, status=status.HTTP_200_OK)
        else:
            Type = tmp[0]
        uuid = generate_uuid()
        sql = "insert into finance_output (id, factory, finance_type_id, price, remark, time, creator_id) values" \
              " ('{0}', '{1}', '{2}', {3}, '{4}', {5}, '{6}');".format(uuid, factory_id, type_id, count, remark, Time,
                                                                       phone)
        financelog_sql = "insert into finance_logs (factory, use_id, type, count, time, parent_type) values ('{0}'," \
                         " '{1}', '{2}', {3}, {4}, 'finance');"
        try:
            if Type == '0':
                cur.execute(financelog_sql.format(factory_id, uuid, name, count, Time))
            elif Type == '1':
                cur.execute(financelog_sql.format(factory_id, uuid, name, -count, Time))
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class FinanceDel(APIView):
    """删除记账 /finance/del"""

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        Id = request.data.get('id')
        # 从缓存中获取factory_id
        factory_id = request.redis_cache["factory_id"]

        sql = "delete from finance_output where factory = '%s' and id = '%s';" % (factory_id, Id)
        financelog_sql = "delete from finance_logs where factory = '%s' and use_id = '%s';" % (factory_id, Id)

        try:
            cur.execute(sql)
            cur.execute(financelog_sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class FinanceModify(APIView):
    """修改记账 /finance/modify"""

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        Id = request.data.get('id', '')
        type_id = request.data.get('type_id', '')
        name = request.data.get('name', '')
        count = request.data.get('count', 0)

        Time = request.data.get('time', int(time.time()))
        remark = request.data.get('remark', '')

        # check 'count'
        try:
            count = float(count)
        except Exception as e:
            return Response({"res": 1, "errmsg": "parameter 'count' error"}, status=status.HTTP_200_OK)
        if count < 0:
            return Response({"res": 1, "errmsg": "parameter 'count' error"}, status=status.HTTP_200_OK)
        # check type-id
        try:
            type_sql = "select type from finance_types where id = '%s';" % type_id
            cur.execute(type_sql)
            tmp = cur.fetchall()
        except Exception as e:
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if len(tmp) != 1 or tmp[0][0] not in ['0', '1']:
            return Response({"res": 1, "errmsg": "parameter 'type_id' error"}, status=status.HTTP_200_OK)
        else:
            Type = tmp[0][0]
        # check finance-id
        try:
            finance_id_sql = "select count(1) from finance_output where id = '%s';" % Id
            cur.execute(finance_id_sql)
            tmp = cur.fetchone()[0]
        except Exception as e:
            return Response({"res": 1, "errmsg": "server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        if tmp != 1:
            return Response({"res": 1, "errmsg": "parameter 'id' error"}, status=status.HTTP_200_OK)

        sql = "update finance_output set finance_type_id = '{1}', price = {2}, remark = '{3}', time = {4} where id " \
              "= '{0}';".format(Id, type_id, count, remark, Time)
        log_sql = "update finance_logs set type = '{0}' , count = {1}, time = {2} where use_id = '{3}';"

        try:
            if Type == '0':
                cur.execute(log_sql.format(name, count, Time, Id))
            elif Type == '1':
                cur.execute(log_sql.format(name, -count, Time, Id))
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)


class FinanceTypes(APIView):
    """记账类型列表 /finance/types"""

    def get(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        # 从缓存中获取factory_id
        factory_id = request.redis_cache["factory_id"]

        sql = "select id, name, type from finance_types where factory = '%s' order by name desc;" % factory_id
        target = ['id', 'name', 'type']
        try:
            cur.execute(sql)
            tmp = cur.fetchall()
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        income = []
        outcome = []
        for i in tmp:
            if i[-1] == '0':
                income.append(dict(zip(target, i)))
            else:
                outcome.append(dict(zip(target, i)))

        postgresql.disconnect_postgresql(conn)
        return Response({'income': income, 'outcome': outcome}, status=status.HTTP_200_OK)


class FinanceTypeNew(APIView):
    """新增记账类型 /finance/type/new"""

    def post(self, request):
        postgresql = UtilsPostgresql()
        conn, cur = postgresql.connect_postgresql()

        name = request.data.get('name')
        Type = request.data.get('type')
        if Type != '0':
            Type = '1'
        # 从缓存中获取factory_id
        factory_id = request.redis_cache["factory_id"]

        sql = "select count(1) from finance_types where name = '%s' and factory = '%s';" % (name, factory_id)
        try:
            cur.execute(sql)
            tmp = cur.fetchone()[0]
        except Exception as e:
            return Response({"res": 1, "errmsg": 'query error'}, status=status.HTTP_200_OK)
        if tmp != 0:
            return Response({"res": 1, "errmsg": 'finance_type already exists'},
                            status=status.HTTP_200_OK)
        else:
            Time = int(time.time())
            uuid = generate_uuid()
            sql = "insert into finance_types (id, factory, name, time, type) values ('{0}', '{1}', '{2}', {3}, '{4}')" \
                  ";".format(uuid, factory_id, name, Time, Type)
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                logger.error(e)
                return Response({"res": 1, "errmsg": 'server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        postgresql.disconnect_postgresql(conn)
        return Response({"res": 0}, status=status.HTTP_200_OK)

