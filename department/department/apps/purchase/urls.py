# -*- coding: utf-8 -*-

from django.urls import path

from purchase import views, views_supplier

"""
str  匹配除了路径分隔符（/）之外的非空字符串，这是默认的形式
int  匹配正整数，包含0。
slug 匹配字母、数字以及横杠、下划线组成的字符串。
uuid 匹配格式化的uuid，如 075194d3-6885-417e-a8a8-6c931e272f00。
path 匹配任何非空字符串，包含了路径分隔符

如果上述的paths和converters还是无法满足需求，也可以使用正则表达式，这时应当使用 django.urls.re_path 函数
"""

urlpatterns = [
    path("purchase/main/crank", views.PurchaseCRank.as_view()),
    path("purchase/main", views.PurchaseMain.as_view()),
    path("purchase/list/<str:list_type>", views.PurchaseList.as_view()),
    path("purchase/supplier/search", views_supplier.SupplierSearch.as_view()),           # checked
    path("purchase/supplier/list", views_supplier.SupplierList.as_view()),               # checked
    path("purchase/supplier/materials", views_supplier.SupplierMaterialList.as_view()),  # checked
    path("purchase/supplier/<str:supplier_id>", views_supplier.Supplier.as_view()),      # checked
    path("purchase/supplier", views_supplier.SupplierNew.as_view()),                     # checked
    path("purchase/<str:purchase_id>", views.PurchaseDetail.as_view())
]
