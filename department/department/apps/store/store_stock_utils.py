# -*- coding: utf-8 -*-

"""
库存情况更新
"""
import logging

logger = logging.getLogger('django')


def product_prepare_stock(cursor, factory_id, product_ids, counts, source_type, source_id):
    """
    产品预分配库存的增减
    :param cursor:
    :param factory_id:
    :param product_ids: 产品id数组
    :param counts: [1,2,3] 对应产品数量
    :param source_type: 0: 入库-完工入库单, 1: 出库-发货单, 2: 库存盘点， 4:订单
    :param source_id: 对应类型id
    :return:
    """
    # logger.info(counts)
    # logger.info(product_ids)
    for index, product_id in enumerate(product_ids):
        log_sql = """
                insert into base_products_log
                  (product_id, type, count, source, source_id, factory, time)
                values
                  ('{}', 'prepared', {}, '{}','{}','{}', extract(epoch from now())::integer);
                """.format(product_id, counts[index], source_type, source_id, factory_id)
        storage_sql = """
                update
                  base_products_storage
                set
                  prepared = prepared + {}
                where
                  factory = '{}' and product_id = '{}';
                """.format(counts[index], factory_id, product_id)
        cursor.execute(log_sql)
        cursor.execute(storage_sql)


def material_on_road(cursor, factory_id, material_ids, counts, source_type, source_id):
    """
    物料在途库存的增减
    :param cursor:
    :param factory_id:
    :param material_ids: 物料id 数组
    :param counts:       物料对应数量
    :param source_type:  0: 入库-采购入库单, 1: 出库-领料单, 2: 库存盘点, 3: 采购单
    :param source_id:
    :return:
    """
    for index, material_id in enumerate(material_ids):
        log_sql = """
                insert into  base_materials_log 
                (material_id, type, count, source, source_id, factory, time) 
                values
                  ('{}', 'on_road', {}, '{}','{}','{}', extract(epoch from now())::integer);
                """.format(material_id, counts[index], source_type, source_id, factory_id)
        storage_sql = """
                update
                  base_materials_storage
                set
                  on_road = on_road + {}
                where
                  factory = '{}' and material_id = '{}';
                """.format(counts[index], factory_id, material_id)
        cursor.execute(log_sql)
        cursor.execute(storage_sql)
