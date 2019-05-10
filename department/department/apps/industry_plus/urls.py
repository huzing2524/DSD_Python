# -*- coding: utf-8 -*-

from django.conf.urls import url

from industry_plus import views

urlpatterns = [
    url(r"^industry_plus/relations", views.IndustryPlusRelations.as_view()),
    url(r"^industry_plus/score", views.IndustryPlusScore.as_view()),
    url(r"^industry_plus/factory/new", views.IndustryPlusFactoryNew.as_view()),
]
