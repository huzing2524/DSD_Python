# -*- coding: utf-8 -*-

from django.conf.urls import url

from material import views

urlpatterns = [
    url(r"^material/main", views.MaterialMain.as_view()),
    url(r"^material/summary", views.MaterialSummary.as_view()),
    url(r"^material/list", views.MaterialList.as_view()),
    url(r"^material/detail", views.MaterialDetail.as_view()),
    url(r"^material/new", views.MaterialNew.as_view()),
    url(r"^material/del", views.MaterialDelete.as_view()),
    url(r"^material/modify", views.MaterialModify.as_view()),
    url(r"^material/type/new", views.MaterialTypeNew.as_view()),
    url(r"^material/type/modify", views.MaterialTypeModify.as_view()),
    url(r"^material/types", views.MaterialTypes.as_view()),
    url(r"^material/stats", views.MaterialStatus.as_view()),
    url(r"^material/bill", views.MaterialBill.as_view()),
    url(r"^material/putin", views.MaterialPutin.as_view()),
    url(r"^material/category/new", views.MaterialCategoryNew.as_view()),
    url(r"^material/category/del", views.MaterialCategoryDelete.as_view()),
    url(r"^material/category/modify", views.MaterialCategoryModify.as_view()),
    url(r"^material/categories", views.MaterialCategories.as_view()),
    url(r"^supplier/list", views.SupplierList.as_view()),
    url(r"^supplier/new", views.SupplierNew.as_view()),
    url(r"^supplier/detail", views.SupplierDetail.as_view()),
    url(r"^supplier/del", views.SupplierDelete.as_view()),
    url(r"^supplier/modify", views.SupplierModify.as_view()),
]
