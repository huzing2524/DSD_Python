# -*- coding: utf-8 -*-
# @Time   : 19-4-30 下午7:58
# @Author : huziying
# @File   : permissions.py

from rest_framework.permissions import BasePermission


class SuperAdminPermission(BasePermission):
    """超级管理员权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "1" in permission:
            return True
        else:
            return False


class OrderPermission(BasePermission):
    """订单权限(普通部门权限和审批人都可以访问)"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "3" in permission or "33" in permission or "1" in permission:
            return True
        else:
            return False


class OrderApprovalPermission(BasePermission):
    """订单审批人权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "33" in permission or "1" in permission:
            return True
        else:
            return False


class PurchasePermission(BasePermission):
    """采购权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "5" in permission or "55" in permission or "1" in permission:
            return True
        else:
            return False


class PurchaseApprovalPermission(BasePermission):
    """采购审批人权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "55" in permission or "1" in permission:
            return True
        else:
            return False


class ProductPermission(BasePermission):
    """生产权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "7" in permission or "1" in permission:
            return True
        else:
            return False


class ManagementPermission(BasePermission):
    """权限管理权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "8" in permission or "1" in permission:
            return True
        else:
            return False


class StorePermission(BasePermission):
    """仓库权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "9" in permission or "99" in permission or "1" in permission:
            return True
        else:
            return False


class StoreApprovalPermission(BasePermission):
    """仓库审批人权限"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "99" in permission or "1" in permission:
            return True
        else:
            return False


class StoreProductPermission(BasePermission):
    """扫码跳转-仓库 生产"""

    # 无权限的显示信息
    message = "您没有权限查看！"

    def has_permission(self, request, view):
        permission = request.redis_cache["permission"]
        if "7" in permission or "9" in permission or "99" in permission or "1" in permission:
            return True
        else:
            return False


def store_decorator(func):
    """仓库普通权限-方法装饰器"""

    def wrapper(request, *args, **kwargs):
        permission = request.redis_cache["permission"]
        if "9" in permission or "1" in permission:
            request.flag = True
        else:
            request.flag = False
        return func(request, *args, **kwargs)

    return wrapper


def store_approval_decorator(func):
    """仓库审批权限-方法装饰器"""

    def wrapper(request, *args, **kwargs):
        permission = request.redis_cache["permission"]
        if "99" in permission or "1" in permission:
            request.flag = True
        else:
            request.flag = False
        return func(request, *args, **kwargs)

    return wrapper


def order_decorator(func):
    """订单普通权限-方法装饰器"""

    def wrapper(request, *args, **kwargs):
        permission = request.redis_cache["permission"]
        if "3" in permission or "1" in permission:
            request.flag = True
        else:
            request.flag = False
        return func(request, *args, **kwargs)

    return wrapper


def order_approval_decorator(func):
    """订单审批权限-方法装饰器"""

    def wrapper(request, *args, **kwargs):
        permission = request.redis_cache["permission"]
        if "33" in permission or "1" in permission:
            request.flag = True
        else:
            request.flag = False
        return func(request, *args, **kwargs)

    return wrapper


