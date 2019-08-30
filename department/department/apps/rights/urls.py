# -*- coding: utf-8 -*-

from django.urls import path

from rights import views

urlpatterns = [
    path("generate/token", views.GenerateToken.as_view()),
    path("rights/info", views.RightsInfo.as_view()),
    path("rights/list", views.RightsList.as_view()),
    path("rights/new", views.RightsNew.as_view()),
    path("rights/modify", views.RightsModify.as_view()),
    path("rights/del", views.RightsDelete.as_view()),
    path("rights/orgs", views.RightsOrg.as_view())
]
