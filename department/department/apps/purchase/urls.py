# -*- coding: utf-8 -*-

from django.conf.urls import url

from purchase import views, views_supplier

urlpatterns = [
    url(r"^purchase/main/crank", views.PurchaseCRank.as_view()),
    url(r"^purchase/main", views.PurchaseMain.as_view()),
    url(r"^purchase/list/([^/]+)", views.PurchaseList.as_view()),
    url(r"^purchase/supplier/search", views_supplier.SupplierSearch.as_view()),
    url(r"^purchase/supplier/list", views_supplier.SupplierList.as_view()),
    url(r"^purchase/supplier/materials", views_supplier.SupplierMaterialList.as_view()),
    url(r"^purchase/supplier/([^/]+)", views_supplier.Supplier.as_view()),
    url(r"^purchase/supplier", views_supplier.SupplierNew.as_view()),
    url(r"^purchase/([^/]+)", views.PurchaseDetail.as_view())
]
