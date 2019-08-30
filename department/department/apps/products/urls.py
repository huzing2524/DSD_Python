# -*- coding: utf-8 -*-

from django.urls import path
from . import views

"""
str  匹配除了路径分隔符（/）之外的非空字符串，这是默认的形式
int  匹配正整数，包含0。
slug 匹配字母、数字以及横杠、下划线组成的字符串。
uuid 匹配格式化的uuid，如 075194d3-6885-417e-a8a8-6c931e272f00。
path 匹配任何非空字符串，包含了路径分隔符

如果上述的paths和converters还是无法满足需求，也可以使用正则表达式，这时应当使用 django.urls.re_path 函数
"""

urlpatterns = [
    # 生产部主页头部
    path("product/home/header", views.ProductHomeHeader.as_view()),
    # 生产部主页分析
    path("product/home/stats", views.ProductHomeStats.as_view()),
    # 生产完工率分析
    path("product/task/done/stats", views.ProductTaskDoneStats.as_view()),
    # 生产金额分析
    path("product/task/account/stats", views.ProductTaskAccountStats.as_view()),
    # 生产备料分析
    path("product/task/prepare/stats", views.ProductTaskPrepareStats.as_view()),
    # 生产需求分析
    path("product/task/demand/stats", views.ProductTaskDemandStats.as_view()),
    # 生产任务单列表
    path("product/task/list", views.ProductTaskList.as_view()),
    # 生产任务单详情
    path("product/task/detail/<str:Id>", views.ProductTaskDetailId.as_view()),
    # 产品工序统计详情
    path("product/task/process/stats/<str:Id>", views.ProductTaskProcessStatsId.as_view()),
    # 获取/提交/修改/删除生产工序进度详情
    path("product/task/process/<str:task_id>/<str:process_step>", views.ProductTaskProcessTPId.as_view()),
    # 获取/执行拆分后的生产任务单
    path("product/task/split/<str:Id>", views.ProductTaskSplitId.as_view()),
    # 完工入库
    path("product/task/done/<str:Id>", views.ProductTaskDoneId.as_view()),

    # 搜索产品/物料
    path("product/materials/search", views.ProductProductMaterialSearch.as_view()),
    # 产品列表
    path("product/product/list", views.ProductProductMaterialList.as_view(), {'Type': 'product'}),
    # 物料列表
    path("product/material/list", views.ProductProductMaterialList.as_view(), {'Type': 'material'}),
    # get：产品详情     put：修改产品售价
    path("product/product/detail/<str:Id>", views.ProductProductMaterialDetailId.as_view(), {'Type': 'product'}),
    # get：物料详情     put：修改物料成本价和最低采购量
    path("product/material/detail/<str:Id>", views.ProductProductMaterialDetailId.as_view(), {'Type': 'material'}),
    # 添加产品
    path("product/product/new", views.ProductProductMaterialNew.as_view(), {'Type': 'product'}),
    # 添加物料
    path("product/material/new", views.ProductProductMaterialNew.as_view(), {'Type': 'material'}),

    # 新增工序
    path("product/process/new", views.ProductProcessNew.as_view()),
    # 工序列表
    path("product/process/list", views.ProductProcesslist.as_view()),
    # put：修改工序     delete：删除工序
    path("product/process/<str:Id>", views.ProductProcessModify.as_view()),

    # 工序/BOM列表
    path("product/pb/list", views.ProductPbList.as_view()),
    # 获取/修改/删除/新增工序/BOM详情
    path("product/pb/<str:Id>", views.ProductPbId.as_view()),

    # 补料单列表
    path("product/material/supplement/list", views.ProductMaterialSupplementList.as_view()),
    # 补料单详情
    path("product/material/supplement/detail/<str:Id>", views.ProductMaterialSupplementDetailId.as_view()),
    # 退料单列表
    path("product/material/return/list", views.ProductMaterialReturnList.as_view()),
    # 退料单列详情
    path("product/material/return/detail/<str:Id>", views.ProductMaterialReturnDetailId.as_view()),
    # 创建补料单
    path("product/material/supplement/create", views.ProductMaterialSupplementCreate.as_view(), {'Type': 'supplement'}),
    # 创建退料单
    path("product/material/return/create", views.ProductMaterialSupplementCreate.as_view(), {'Type': 'return'}),
    # (扫码人)退料详情/确认退料 product/material/return/{id}
    path("product/material/return/<str:Id>", views.ProductMaterialReturnId.as_view()),
    # 产品物料列表 product/material/rs_list/{id}
    path("product/material/rs_list/<str:Id>", views.ProductMaterialRSList.as_view()),
]
