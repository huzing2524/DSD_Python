# -*- coding: utf-8 -*-

from django.conf.urls import url

from store import views_V350

urlpatterns = [
    # url(r"^store/main/(\w+)", views.StoreMainType.as_view()),
    # url(r"^store/stats/(\w+)", views.StoreStatsType.as_view()),
    # url(r"^store/check/(\w+)/(\w+)", views.StoreCheckTypeId.as_view()),
    # url(r"^store/check/(\w+)", views.StoreCheckType.as_view()),
    # url(r"^store/product/outgoing/(\w+)", views.StoreProductOutgoingId.as_view()),
    # url(r"^store/product/outgoing", views.StoreProductOutgoing.as_view()),
    # url(r"^store/product/incoming/(\w+)", views.StoreProductIncomingId.as_view()),
    # url(r"^store/product/incoming", views.StoreProductIncoming.as_view()),
    # url(r"^store/product/mgmt/(\w+)", views.StoreProductMgmtId.as_view()),
    # url(r"^store/product/mgmt", views.StoreProductMgmt.as_view()),
    # url(r"^store/product/category/(\w+)", views.StoreProductCategoryId.as_view()),
    # url(r"^store/product/category", views.StoreProductCategory.as_view()),
    # url(r"^store/orders", views.StoreOrders.as_view()),
    # url(r"^store/material/category/(\w+)", views.StoreMaterialCategory.as_view()),
    # url(r"^store/material/category", views.StoreMaterialCategory.as_view()),
    # url(r"^store/material/mgmt/(\w+)", views.StoreMaterialManagement.as_view()),
    # url(r"^store/material/mgmt", views.StoreMaterialManagement.as_view()),
    # url(r"^store/material/incoming/(\w+)", views.StoreMaterialIncoming.as_view()),
    # url(r"^store/material/incoming", views.StoreMaterialIncoming.as_view()),
    # url(r"^store/material/outgoing/(\w+)", views.StoreMaterialOutgoing.as_view()),
    # url(r"^store/material/outgoing", views.StoreMaterialOutgoing.as_view()),
    # url(r"^store/notice/(?P<type>\w+)/(?P<id>\w+)", views.StoreNotice.as_view()),
    # # v3.4
    # url(r"^store/material/prepare/(\w+)", views.StoreMaterialPrepareId.as_view()),
    # url(r"^store/material/prepare", views.StoreMaterialPrepare.as_view()),
    # v3.5.0
    url(r"^store/main", views_V350.StoreMain.as_view()),
    url(r"^store/invoice/main", views_V350.StoreInvoiceMain.as_view()),
    url(r"^store/invoice/detail", views_V350.StoreInvoiceDetail.as_view()),
    url(r"^store/completed_storage/main", views_V350.StoreCompletedStorageMain.as_view()),
    url(r"^store/completed_storage/detail", views_V350.StoreCompletedStorageDetail.as_view()),
    url(r"^store/purchase_warehousing/main", views_V350.StorePurchaseWarehousingMain.as_view()),
    url(r"^store/purchase_warehousing/detail", views_V350.StorePurchaseWarehousingDetail.as_view()),
    url(r"^store/picking_list/main", views_V350.StorePickingListMain.as_view()),
    url(r"^store/picking_list/detail", views_V350.StorePickingListDetail.as_view()),
    url(r"^store/storage/main", views_V350.StoreStorageMain.as_view()),
    url(r"^store/storage/detail/(\w+)", views_V350.StoreStorageDetail.as_view()),
    url(r"^store/check/main", views_V350.StoreCheckMain.as_view()),
    url(r"^store/check/detail", views_V350.StoreCheckDetail.as_view()),
    url(r"^store/check/new/(\w+)", views_V350.StoreCheckNew.as_view()),
    # url(r"^store/url/test/([a-z\d]{8}(-[a-z\d]{4}){3}-[a-z\d]{12})", views_V350.UrlTest.as_view()),
    # url(r"^store/url/test/([^/]+)", views_V350.UrlTest.as_view()),
    url(r"^store/temporary_purchase/main", views_V350.StoreTemporaryPurchaseMain.as_view()),
    url(r"^store/temporary_purchase/detail", views_V350.StoreTemporaryPurchaseDetail.as_view()),
    url(r"^store/temporary_purchase/new", views_V350.StoreTemporaryPurchaseNew.as_view()),
    url(r"^store/generate_module_uuid", views_V350.GenerateModuleUuid.as_view()),
]
