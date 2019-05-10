# -*- coding: utf-8 -*-

from django.conf.urls import url

from rights import views

urlpatterns = [
    url(r"^generate/token", views.GenerateToken.as_view()),
    url(r"^rights/info", views.RightsInfo.as_view()),
    url(r"^rights/list", views.RightsList.as_view()),
    url(r"^rights/new", views.RightsNew.as_view()),
    url(r"^rights/modify", views.RightsModify.as_view()),
    url(r"^rights/del", views.RightsDelete.as_view()),
    url(r"^rights/orgs", views.RightsOrg.as_view())
]
