# -*- coding: utf-8 -*-
import datetime
import time

from analysis.retrieve_jcj import get_month_timestamp


def week_timestamp():
    """从本周当前某天到本周第一天的持续时间，时间戳"""
    num_in_week = datetime.datetime.now().weekday()
    dtime = datetime.datetime.now().strftime('%Y-%m-%d')
    today = datetime.datetime.strptime(dtime, '%Y-%m-%d')
    monday = today + datetime.timedelta(days=-num_in_week)
    timestamp = int(time.mktime(monday.timetuple()))
    return timestamp


def month_timestamp(num):
    """上个月第一天到上个月最后一天的持续时间，时间戳
    0: this month
    1: last month
    2: two month ago"""
    return get_month_timestamp(num)


def week_product(factory_id, cursor):
    """生产部 本周产品出库 分类型号、名称、数量排名前3"""
    
    timestamp = week_timestamp()  # 1545580800

    cursor.execute("select product_id, sum(count) from products_log where parent_type = 'product' and factory = '{}' and time > {} group by product_id order by sum(count) desc limit 3;".format(factory_id, timestamp))

    result_products_log = cursor.fetchall()
    # print("result_products_log=", result_products_log)

    week_product_list = []

    if result_products_log:
        for product in result_products_log:
            # print("product=", product)
            w_product = {}

            product_id = product[0] or None
            product_num = product[1] or 0

            w_product["count"] = abs(int(product_num))

            if product_id:
                cursor.execute("select name, unit, category_id from products where id='{}';".format(product_id))
                result_products = cursor.fetchall()

                product_name = result_products[0][0] if result_products else ""
                product_unit = result_products[0][1] if result_products else ""
                product_category_id = result_products[0][2] if result_products else None
                # print("product_name=", product_name)  # 999感冒灵
                # print("product_unit=", product_unit)  # 盒
                # print("product_category_id=", product_category_id)  # '9dF9r7e8NDdGQU76jQ'
                w_product["name"] = product_name
                w_product["unit"] = product_unit

                if product_category_id:
                    cursor.execute("select name from product_categories where id='{}'".format(product_category_id))
                    result_product_categories = cursor.fetchall()
                    # print("result_product_categories=", result_product_categories)  # [('口服',)], [('外用',)]

                    product_category_name = result_product_categories[0][0] if result_product_categories else None
                    # print("product_category_name=", product_category_name)  # 口服

                    w_product["category_name"] = product_category_name
                else:
                    w_product["category_name"] = ""

            else:
                continue

            week_product_list.append(w_product)

    # print(week_product_list)
    
    return week_product_list


def month_product(factory_id, cursor):
    """生产部 当前月份产品出库 分类型号、名称、数量排名前3"""
    
    start_timestamp, end_timestamp = month_timestamp(0)

    cursor.execute(
        "select product_id, sum(count) from products_log where parent_type = 'product' and factory = '{}' and time between {} and {} group by product_id "
        "order by sum(count) desc limit 3;".format(factory_id, start_timestamp, end_timestamp))

    result_products_log = cursor.fetchall()
    # print("result_products_log=", result_products_log)

    month_product_list = []

    if result_products_log:
        for product in result_products_log:
            # print("product=", product)
            m_product = {}

            product_id = product[0] or None
            product_num = product[1] or 0

            m_product["count"] = abs(int(product_num))

            if product_id:
                cursor.execute("select name, unit, category_id from products where id='{}';".format(product_id))
                result_products = cursor.fetchall()  # [('一次性口罩', 9deSVx9iUzI0tSKuK8), ('急支糖浆', 9dGjrURmx6lfeZbedc)]

                # if result_products:
                product_name = result_products[0][0] if result_products else ""
                product_unit = result_products[0][1] if result_products else ""
                product_category_id = result_products[0][2] if result_products else None
                # print("product_name=", product_name)  # 999感冒灵
                # print("product_unit=", product_unit)  # 盒
                # print("product_category_id=", product_category_id)  # '9dF9r7e8NDdGQU76jQ'
                m_product["name"] = product_name
                m_product["unit"] = product_unit

                if product_category_id:
                    cursor.execute("select name from product_categories where id='{}'".format(product_category_id))
                    result_product_categories = cursor.fetchall()
                    # print("result_product_categories=", result_product_categories)  # [('口服',)], [('外用',)]

                    product_category_name = result_product_categories[0][0] if result_product_categories else None
                    # print("product_category_name=", product_category_name)  # 口服

                    m_product["category_name"] = product_category_name
                else:
                    m_product["category_name"] = ""

            else:
                continue

            month_product_list.append(m_product)

    # print(month_product_list)
    return month_product_list


# def week_store(factory_id, cursor):
#     """仓库部 本周产品出库 分类型号、名称、数量排名前3"""
#
#     timestamp = week_timestamp()
#
#     cursor.execute("select product_id, sum(count) from products_log where parent_type = 'outgoing' and factory = '{}' and time > {} group by product_id order by sum(count) limit 3;".format(factory_id, timestamp))
#
#     result_store_log = cursor.fetchall()
#
#     week_store_list = []
#
#     if result_store_log:
#         for store in result_store_log:
#             w_store = {}
#
#             material_type_id = store[0] or None
#             material_count = store[1] or 0
#
#             w_store["count"] = abs(int(material_count))
#
#             if material_type_id:
#                 cursor.execute(
#                     "select name, unit, category_id from products where id='{}';".format(material_type_id))
#                 result_material_types = cursor.fetchall()
#
#                 # if result_material_types:
#                 name = result_material_types[0][0] if result_material_types else ""
#                 unit = result_material_types[0][1] if result_material_types else ""
#                 category_id = result_material_types[0][2] if result_material_types else None
#                 # print("name=", name)
#                 # print("unit=", unit)
#                 # print("category_id=", category_id)
#
#                 w_store["name"] = name
#                 w_store["unit"] = unit
#
#                 if category_id:
#                     cursor.execute("select name from product_categories where id='{}';".format(category_id))
#                     result_material_categories = cursor.fetchall()
#                     # print("result_material_categories=", result_material_categories)  # [('电池材料',)]
#
#                     category_name = result_material_categories[0][0] if result_material_categories else ""
#                     # print("category_name=", category_name)
#                     w_store["category_name"] = category_name
#                 else:
#                     w_store["category_name"] = ""
#
#             else:
#                 continue
#
#             week_store_list.append(w_store)
#
#     # print("week_store_list=", week_store_list)
#
#     return week_store_list


# def month_store(factory_id, cursor):
#     """仓库部 当前月份产品出库 分类型号、名称、数量排名前3"""
#
#     start_timestamp, end_timestamp = month_timestamp(0)
#     cursor.execute(
#         "select product_id, sum(count) from products_log where parent_type = 'outgoing' and factory = '{}' and time between {} and {} group by product_id order by sum(count) limit 3;".format(factory_id, start_timestamp, end_timestamp))
#
#     result_materials_log = cursor.fetchall()
#     # print(result_materials_log)
#
#     month_store_list = []
#
#     if result_materials_log:
#         for material in result_materials_log:
#             m_store = {}
#
#             material_type_id = material[0] or None
#             material_count = material[1] or 0
#
#             m_store["count"] = abs(int(material_count))
#
#             if material_type_id:
#                 cursor.execute(
#                     "select name, unit, category_id from products where id='{}';".format(material_type_id))
#                 result_material_types = cursor.fetchall()
#                 # print("result_material_types=", result_material_types)
#
#                 # if result_material_types:
#                 name = result_material_types[0][0] if result_material_types else ""
#                 unit = result_material_types[0][1] if result_material_types else ""
#                 category_id = result_material_types[0][2] if result_material_types else None
#
#                 m_store["name"] = name
#                 m_store["unit"] = unit
#
#                 if category_id:
#                     cursor.execute("select name from product_categories where id='{}';".format(category_id))
#                     result_material_categories = cursor.fetchall()
#                     # print("result_material_categories=", result_material_categories)  # [('电池材料',)]
#
#                     category_name = result_material_categories[0][0] if result_material_categories else ""
#                     # print("category_name=", category_name)
#                     m_store["category_name"] = category_name
#                 else:
#                     m_store["category_name"] = ""
#
#             else:
#                 continue
#
#             month_store_list.append(m_store)
#
#     # print("month_store_list=", month_store_list)  # [{'count': -1111}, {'count': -110}, {'count': -10}]
#
#     return month_store_list


def week_store(factory_id, cursor):
    """仓库部 本周产品出库 分类型号、名称、数量排名前3"""

    timestamp = week_timestamp()
    sql = """
    select
        t1.id,
        t1.deliver_time,
        t1.state,
        COALESCE(t2.product_name) as product_name,
        COALESCE(t2.product_count) as product_count,
        COALESCE(t2.unit) as unit,
        t3.name as category_name
    from
        orders t1
    left join (
        select
          order_id,
          product_count as product_count,
          name as product_name,
          unit as unit,
          category_id
        from
            (
            select
                t1.product_count,
                t1.order_id,
                t2.name,
                t2.unit,
                t2.category_id
            from
                order_products t1
            left join products t2 on
                t1.product_id = t2.id where t2.factory = '%s') t
            ) t2 on
        t1.id = t2.order_id
    left join product_categories t3 on 
        t3.id = t2.category_id
    where 
        t1.factory = '%s' and t1.state = '0' and t1.deliver_time >= %d
    order by
        product_count desc
    limit 3;
    """ % (factory_id, factory_id, timestamp)
    # print(sql)

    cursor.execute(sql)
    result = cursor.fetchall()
    # print(result)

    week_store_list = []
    for res in result:
        w_store = dict()
        w_store["name"] = res[3] or ""
        w_store["count"] = res[4] or 0
        w_store["unit"] = res[5] or ""
        w_store["category_name"] = res[6] or ""
        week_store_list.append(w_store)

    # print("week_store_list=", week_store_list)
    return week_store_list


def month_store(factory_id, cursor):
    """仓库部 当前月份产品出库 分类型号、名称、数量排名前3"""

    start_timestamp, end_timestamp = month_timestamp(0)

    sql = """
    select
        t1.id,
        t1.deliver_time,
        t1.state,
        COALESCE(t2.product_name) as product_name,
        COALESCE(t2.product_count) as product_count,
        COALESCE(t2.unit) as unit,
        t3.name as category_name
    from
        orders t1
    left join (
        select
          order_id,
          product_count as product_count,
          name as product_name,
          unit as unit,
          category_id
        from
            (
            select
                t1.product_count,
                t1.order_id,
                t2.name,
                t2.unit,
                t2.category_id
            from
                order_products t1
            left join products t2 on
                t1.product_id = t2.id where t2.factory = '%s') t
            ) t2 on
        t1.id = t2.order_id
    left join product_categories t3 on 
        t3.id = t2.category_id
    where 
        t1.factory = '%s' and t1.state = '0' and t1.deliver_time >= %d and t1.deliver_time <= %d
    order by
        product_count desc
    limit 3;
    """ % (factory_id, factory_id, start_timestamp, end_timestamp)
    # print(sql)

    cursor.execute(sql)
    result = cursor.fetchall()
    # print(result)

    month_store_list = []
    for res in result:
        m_store = dict()
        m_store["name"] = res[3] or ""
        m_store["count"] = res[4] or 0
        m_store["unit"] = res[5] or ""
        m_store["category_name"] = res[6] or ""
        month_store_list.append(m_store)

    # print("month_store_list=", month_store_list)
    return month_store_list


def main(factory_id, cursor):
    """组装结果"""
    hzy = {}
    # ------仓库部------
    store = {}
    # 仓库部 本周产品出库
    week_store_list = week_store(factory_id, cursor)
    store["w"] = week_store_list

    # 仓库部 上个月产品出库
    month_store_list = month_store(factory_id, cursor)
    store["m"] = month_store_list

    hzy["store"] = store

    # ------生产部------
    product = {}
    # 生产部 本周产品出库
    week_product_list = week_product(factory_id, cursor)
    # 生产部 上个月产品出库
    month_product_list = month_product(factory_id, cursor)

    product["w"] = week_product_list
    product["m"] = month_product_list

    hzy["product"] = product
    # print("hzy=", hzy)
    return hzy

