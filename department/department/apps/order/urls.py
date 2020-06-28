# -*- coding: utf-8 -*-

from django.urls import path

from order import views, views_client

"""
str  匹配除了路径分隔符（/）之外的非空字符串，这是默认的形式
int  匹配正整数，包含0。
slug 匹配字母、数字以及横杠、下划线组成的字符串。
uuid 匹配格式化的uuid，如 075194d3-6885-417e-a8a8-6c931e272f00。
path 匹配任何非空字符串，包含了路径分隔符

如果上述的paths和converters还是无法满足需求，也可以使用正则表达式，这时应当使用 django.urls.re_path 函数
"""

urlpatterns = [
    # path("order/clients/group/<str:id>", views_client.ClientGroup.as_view()),  # deleted
    # path("order/clients/group", views_client.ClientGroup.as_view()),           # deleted

    path("order/clients/search", views_client.ClientSearch.as_view()),         # checked
    path("order/clients/products", views_client.ClientProductList.as_view()),  # checked
    path("order/clients/<str:client_id>", views_client.ClientSave.as_view()),  # checked
    path("order/clients", views_client.Clients.as_view()),                     # checked

    path("order/main/crank", views.OrderCRank.as_view()),                      # checked
    path("order/main", views.OrderMain.as_view()),                             # checked
    path("order/new", views.OrderNew.as_view()),                               # checked
    path("order/products", views.Products.as_view()),                          # checked
    path("order/list/<str:list_type>", views.OrderList.as_view()),             # checked
    path("order/<str:order_id>", views.OrderDetail.as_view()),                 # checked

    # 注意：deliver和del有冲突
    # url("order/deliver", views.OrderDeliver.as_view()),
    # url("order/del", views.OrderDelete.as_view()),
    # url("order/modify", views.OrderModify.as_view()),
    # url("order/track", views.OrderTrack.as_view()),
    # url("order/income/(\w+)", views.OrderIncome.as_view()),
    # url("clients/list", views.ClientsList.as_view()),
    # url("clients/new", views.ClientsNew.as_view()),
    # url("clients/detail", views.ClientsDetail.as_view()),
    # url("clients/del", views.ClientsDelete.as_view()),
    # url("clients/modify", views.ClientsModify.as_view()),
    # url("clients/group/list", views.ClientsGroupList.as_view()),
    # url("clients/group/new", views.ClientsGroupNew.as_view()),
    # url("clients/group/del", views.ClientsGroupDelete.as_view()),
    # url("clients/salesman/list", views.ClientsSalesmanList.as_view()),
    # url("clients/salesman/new", views.ClientsSalesmanNew.as_view()),
    # url("clients/salesman/del", views.ClientsSalesmanDelete.as_view()),
]
