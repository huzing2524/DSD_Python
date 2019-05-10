# -*- coding: utf-8 -*-

from django.conf.urls import url

from analysis.views import Analysis

urlpatterns = [
    url(r'^api/v2/analysis', Analysis.as_view()),
]
