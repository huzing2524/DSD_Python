# -*- coding: utf-8 -*-

from django.urls import path

from store import views_V350, views_V351

"""
str  匹配除了路径分隔符（/）之外的非空字符串，这是默认的形式
int  匹配正整数，包含0。
slug 匹配字母、数字以及横杠、下划线组成的字符串。
uuid 匹配格式化的uuid，如 075194d3-6885-417e-a8a8-6c931e272f00。
path 匹配任何非空字符串，包含了路径分隔符

如果上述的paths和converters还是无法满足需求，也可以使用正则表达式，这时应当使用 django.urls.re_path 函数
"""

urlpatterns = [
    # v3.5.0
    path("store/main", views_V350.StoreMain.as_view()),
    path("store/invoice/main", views_V350.StoreInvoiceMain.as_view()),
    path("store/invoice/detail", views_V350.StoreInvoiceDetail.as_view()),
    path("store/completed_storage/main", views_V350.StoreCompletedStorageMain.as_view()),
    path("store/completed_storage/detail", views_V350.StoreCompletedStorageDetail.as_view()),
    path("store/purchase_warehousing/main", views_V350.StorePurchaseWarehousingMain.as_view()),
    path("store/purchase_warehousing/detail", views_V350.StorePurchaseWarehousingDetail.as_view()),
    path("store/picking_list/main", views_V350.StorePickingListMain.as_view()),
    path("store/picking_list/detail", views_V350.StorePickingListDetail.as_view()),
    path("store/storage/main", views_V350.StoreStorageMain.as_view()),  # checked
    path("store/storage/detail/<str:type_>", views_V350.StoreStorageDetail.as_view()),
    path("store/check/main", views_V350.StoreCheckMain.as_view()),
    path("store/check/detail", views_V350.StoreCheckDetail.as_view()),
    path("store/check/new/<str:type_>", views_V350.StoreCheckNew.as_view()),
    path("store/temporary_purchase/main", views_V350.StoreTemporaryPurchaseMain.as_view()),
    path("store/temporary_purchase/detail", views_V350.StoreTemporaryPurchaseDetail.as_view()),
    path("store/temporary_purchase/new", views_V350.StoreTemporaryPurchaseNew.as_view()),
    path("store/generate_module_uuid", views_V350.GenerateModuleUuid.as_view()),
    # v3.5.1
    path("store/multi_storage/select", views_V351.MultiStorage.as_view()),    # checked
    path("store/multi_storage/move", views_V351.MultiStorageMove.as_view()),  # checked

]
