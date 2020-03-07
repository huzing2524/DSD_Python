"""department URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.urls import path, include
from django.contrib import admin

urlpatterns = [
    # path('admin/', admin.site.urls),
    path("", include("analysis.urls")),
    path("dsdpy/api/v3/", include("finance.urls")),
    path("dsdpy/api/v3/", include("material.urls")),
    path("dsdpy/api/v3/", include("purchase.urls")),
    path("dsdpy/api/v3/", include("order.urls")),
    path("dsdpy/api/v3/", include("predict.urls")),
    path("dsdpy/api/v3/", include("products.urls")),
    path("dsdpy/api/v3/", include("rights.urls")),
    path("dsdpy/api/v3/", include("store.urls")),
    path("dsdpy/api/v3/", include("industry_plus.urls")),
]
