# -*- coding: utf-8 -*-

from django.conf.urls import url
from . import views

urlpatterns = [
    url(r"^finance/summary", views.FinanceSummary.as_view()),
    url(r"^finance/list", views.FinanceList.as_view()),
    url(r"^finance/detail", views.FinanceDetail.as_view()),
    url(r"^finance/new", views.FinanceNew.as_view()),
    url(r"^finance/del", views.FinanceDel.as_view()),
    url(r"^finance/modify", views.FinanceModify.as_view()),
    url(r"^finance/types", views.FinanceTypes.as_view()),
    url(r"^finance/type/new", views.FinanceTypeNew.as_view()),
]
