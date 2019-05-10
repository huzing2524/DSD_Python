# -*- coding: utf-8 -*-

from django.conf.urls import url

from order import views, views_client

urlpatterns = [
    url(r"^order/clients/group/(\w+)", views_client.ClientGroup.as_view()),
    url(r"^order/clients/group", views_client.ClientGroup.as_view()),

    url(r"^order/clients/search", views_client.ClientSearch.as_view()),
    url(r"^order/clients/products", views_client.ClientProductList.as_view()),
    url(r"^order/clients/([^/]+)", views_client.ClientSave.as_view()),
    url(r"^order/clients", views_client.Clients.as_view()),

    url(r"^order/main/crank", views.OrderCRank.as_view()),
    url(r"^order/main", views.OrderMain.as_view()),
    url(r"^order/new", views.OrderNew.as_view()),
    url(r"^order/products", views.Products.as_view()),
    url(r"^order/list/(\w+)", views.OrderList.as_view()),
    url(r"^order/([^/]+)", views.OrderDetail.as_view()),

    # 注意：deliver和del有冲突
    # url(r"^order/deliver", views.OrderDeliver.as_view()),
    # url(r"^order/del", views.OrderDelete.as_view()),
    # url(r"^order/modify", views.OrderModify.as_view()),
    # url(r"^order/track", views.OrderTrack.as_view()),
    # url(r"^order/income/(\w+)", views.OrderIncome.as_view()),
    # url(r"^clients/list", views.ClientsList.as_view()),
    # url(r"^clients/new", views.ClientsNew.as_view()),
    # url(r"^clients/detail", views.ClientsDetail.as_view()),
    # url(r"^clients/del", views.ClientsDelete.as_view()),
    # url(r"^clients/modify", views.ClientsModify.as_view()),
    # url(r"^clients/group/list", views.ClientsGroupList.as_view()),
    # url(r"^clients/group/new", views.ClientsGroupNew.as_view()),
    # url(r"^clients/group/del", views.ClientsGroupDelete.as_view()),
    # url(r"^clients/salesman/list", views.ClientsSalesmanList.as_view()),
    # url(r"^clients/salesman/new", views.ClientsSalesmanNew.as_view()),
    # url(r"^clients/salesman/del", views.ClientsSalesmanDelete.as_view()),
]
