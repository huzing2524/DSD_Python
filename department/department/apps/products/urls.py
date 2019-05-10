# -*- coding: utf-8 -*-

from django.conf.urls import url
from . import views

urlpatterns = [
    # 生产部主页头部
    url(r"^product/home/header", views.ProductHomeHeader.as_view()),
    # 生产部主页分析
    url(r"^product/home/stats", views.ProductHomeStats.as_view()),
    # 生产完工率分析
    url(r"^product/task/done/stats", views.ProductTaskDoneStats.as_view()),
    # 生产金额分析
    url(r"^product/task/account/stats", views.ProductTaskAccountStats.as_view()),
    # 生产备料分析
    url(r"^product/task/prepare/stats", views.ProductTaskPrepareStats.as_view()),
    # 生产需求分析
    url(r"^product/task/demand/stats", views.ProductTaskDemandStats.as_view()),
    # 生产任务单列表
    url(r"^product/task/list", views.ProductTaskList.as_view()),
    # 生产任务单详情
    url(r"^product/task/detail/([^/]+)", views.ProductTaskDetailId.as_view()),
    # 产品工序统计详情
    url(r"^product/task/process/stats/([^/]+)", views.ProductTaskProcessStatsId.as_view()),
    # 获取/提交/修改/删除生产工序进度详情
    url(r"^product/task/process/([^/]+)/([^/]+)", views.ProductTaskProcessTPId.as_view()),
    # 获取/执行拆分后的生产任务单
    url(r"^product/task/split/([^/]+)", views.ProductTaskSplitId.as_view()),
    # 完工入库
    url(r"^product/task/done/([^/]+)", views.ProductTaskDoneId.as_view()),

    # 搜索产品/物料
    url(r"^product/materials/search", views.ProductProductMaterialSearch.as_view()),
    # 产品列表
    url(r"^product/product/list", views.ProductProductMaterialList.as_view(), {'Type': 'product'}),
    # 物料列表
    url(r"^product/material/list", views.ProductProductMaterialList.as_view(), {'Type': 'material'}),
    # get：产品详情     put：修改产品售价
    url(r"^product/product/detail/([^/]+)", views.ProductProductMaterialDetailId.as_view(), {'Type': 'product'}),
    # get：物料详情     put：修改物料成本价和最低采购量
    url(r"^product/material/detail/([^/]+)", views.ProductProductMaterialDetailId.as_view(), {'Type': 'material'}),
    # 添加产品
    url(r"^product/product/new", views.ProductProductMaterialNew.as_view(), {'Type': 'product'}),
    # 添加物料
    url(r"^product/material/new", views.ProductProductMaterialNew.as_view(), {'Type': 'material'}),

    # 新增工序
    url(r"^product/process/new", views.ProductProcessNew.as_view()),
    # 工序列表
    url(r"^product/process/list", views.ProductProcesslist.as_view()),
    # put：修改工序     delete：删除工序
    url(r"^product/process/([^/]+)", views.ProductProcessModify.as_view()),

    # 工序/BOM列表
    url(r"^product/pb/list", views.ProductPbList.as_view()),
    # 获取/修改/删除/新增工序/BOM详情
    url(r"^product/pb/([^/]+)", views.ProductPbId.as_view()),

    # 补料单列表
    url(r"^product/material/supplement/list", views.ProductMaterialSupplementList.as_view()),
    # 补料单详情
    url(r"^product/material/supplement/detail/([^/]+)", views.ProductMaterialSupplementDetailId.as_view()),
    # 退料单列表
    url(r"^product/material/return/list", views.ProductMaterialReturnList.as_view()),
    # 退料单列详情
    url(r"^product/material/return/detail/([^/]+)", views.ProductMaterialReturnDetailId.as_view()),
    # 创建补料单
    url(r"^product/material/supplement/create", views.ProductMaterialSupplementCreate.as_view(), {'Type': 'supplement'}),
    # 创建退料单
    url(r"^product/material/return/create", views.ProductMaterialSupplementCreate.as_view(), {'Type': 'return'}),
    # (扫码人)退料详情/确认退料 product/material/return/{id}
    url(r"^product/material/return/([^/]+)", views.ProductMaterialReturnId.as_view()),
    # 产品物料列表 product/material/rs_list/{id}
    url(r"^product/material/rs_list/([^/]+)", views.ProductMaterialRSList.as_view()),
]
