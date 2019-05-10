# -*- coding: utf-8 -*-
from enum import Enum, unique

BG_QUEUE_NAME = "DSD-ERL-Backend"  # RabbitMQ routing_key

# 海滨医疗factory_id
FACTORY_HBYL = 'hbyl'
HBYL_RIGHTS = '10'
RIGHTS_LIST = [
    {"key": "1", "value": "超级管理员"},
    {"key": "2", "value": "高级管理员"},
    {"key": "3", "value": "订单"},
    {"key": "4", "value": "财务部"},
    {"key": "5", "value": "采购"},
    {"key": "6", "value": "客户管理"},
    {"key": "7", "value": "生产"},
    {"key": "8", "value": "权限管理"},
    {"key": "9", "value": "仓库"},
    {"key": "33", "value": "订单审批人"},
    {"key": "55", "value": "采购审批人"},
    {"key": "99", "value": "仓库审批人"}
]

BASE_RIGHTS_LIST = [
    {'订单': [{'审批人': '33', '普通管理员': '3'}]},
    {'采购': [{'审批人': '55', '普通管理员': '5'}]},
    {'生产': [{'普通管理员': '7'}]},
    {'仓库': [{'审批人': '33', '普通管理员': '3'}]},
    {'权限管理': [{'普通管理员': '8'}]}
]

# 审批人的权限包含普通管理员
RIGHTS_DICT = {
    "1": "超级管理员",
    "2": "高级管理员",
    "3": "订单",
    "4": "财务部",
    "5": "采购",
    "6": "客户管理",
    "7": "生产",
    "8": "权限管理",
    "9": "仓库",
    "33": "订单审批人",
    "55": "采购审批人",
    "99": "仓库审批人"
}

EDIT_RIGHTS_LIST = [
    {"key": "3", "value": "订单"},
    {"key": "4", "value": "财务部"},
    {"key": "5", "value": "采购"},
    {"key": "6", "value": "客户管理"},
    {"key": "7", "value": "生产"},
    {"key": "8", "value": "权限管理"},
    {"key": "9", "value": "仓库"},
    {"key": "33", "value": "订单审批人"},
    {"key": "55", "value": "采购审批人"},
    {"key": "99", "value": "仓库审批人"}
]

REDIS_CACHE = {'phone': '',
               'user_id': '',
               'factory_id': '',
               'permission': ''}


@unique
class PrimaryKeyType(Enum):
    """单号数字编号"""

    order = "01"
    product = "02"
    purchase = "03"
    invoice = "04"  # 发货单
    picking_list = "05"  # 领料单
    completed_storage = "06"  # 完工入库单
    purchase_warehousing = "07"  # 采购入库单
    storage_check = "08"  # 库存盘点单
    temporary_purchase = "09"  # 临时申购单
    return_materials = "10"  # 退料单
    supplement_materials = "11"  # 补料单


@unique
class OrderTrackType(Enum):
    """1: 创建订单, 2: 生产, 3: 收款, 4: 交货, 5: 完成"""

    create = "1"
    products = "2"
    money = "3"
    deliver = "4"
    finish = "5"


@unique
class RightsEnumerate(Enum):
    """权限枚举类"""

    super_administrator = "1"
    senior_administrator = "2"
    order = "3"
    finance = "4"
    material = "5"
    client_management = "6"
    products = "7"
    permission = "8"
    store = "9"


INDUSTRY_PLUS_LIST = [
    {"key": "1", "value": "数据可视化"},
    {"key": "2", "value": "数据分析"},
    {"key": "3", "value": "机器代替人工"},
    {"key": "4", "value": "机器远程控制"},
    {"key": "5", "value": "智能预测、预警"},
]

INDUSTRY_PLUS_SCORE_DICT = {
    "1": 10.34,  # 数据可视化
    "2": 15.45,  # 数据分析
    "3": 20.56,  # 机器替代人工
    "4": 25.67,  # 机器远程控制
    "5": 27.98  # 智能预测、预警
}

PRODUCT_TASK_DICT = {
    # 创建订单
    "task_create": "1",
    # 生产
    "task_product": "2",
    # 订单完成
    "task_finish": "3",
}

PRODUCT_MATERIAL_DICT = {
    # 未备料
    "material_not": "1",
    # 备料中
    "material_ing": "2",
    # 已备料
    "material_done": "3",
}


@unique
class StoreNoticeMsgEnum(Enum):
    """仓库消息通知"""

    msg_done = "1"  # 通知消息已处理
    msg_not = "0"  # 通知消息未处理


@unique
class StoreNoticeEnum(Enum):
    """仓库通知"""

    done = "1"  # 已通知
    not_yet = "0"  # 未通知


@unique
class StoreTypeEnum(Enum):
    """仓库通知类型，数据表store_notice.type字段的值"""

    product = "1"  # 产品
    material = "2"  # 物料


# 分页数量，默认值为10
ROW = 10


@unique
class ProductStateFour(Enum):
    """产品生产单状态，数据表base_product_task.state字段的值"""

    wait = '0'  # 待生产
    ready = '1'  # 待领料
    working = '2'  # 生产中
    done = '3'  # 已完工


@unique
class DelState(Enum):
    """数据删除状态"""

    del_no = '0'  # 未删除
    del_yes = '1'  # 已删除
