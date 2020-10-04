# -*- coding: utf-8 -*-
import logging
import re
import time
import traceback

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from django.db import connection

from constants import INDUSTRY_PLUS_LIST, INDUSTRY_PLUS_SCORE_DICT

logger = logging.getLogger('django')


# 行业+-----------------------------------------------------------------------------------------------------------------


class IndustryPlusRelations(APIView):
    """获取 {代号: 打分项目}的对应关系 industry_plus/relations"""

    def get(self, request):
        return Response({"relations": INDUSTRY_PLUS_LIST}, status=status.HTTP_200_OK)


class IndustryPlusScore(APIView):
    """测测我的企业智能化程度, 评分 industry_plus/score"""

    def get(self, request):
        phone = request.redis_cache["phone"]

        cursor = connection.cursor()

        try:
            cursor.execute(
                "select company_name, intelligent_degree, score from industry_plus_test where phone = '%s';" % phone)
            result = cursor.fetchone()
            # print("result=", result)
            if result:
                cursor.execute("select count(*) from industry_plus_test where score < {} union all "
                               "select count(*) from industry_plus_test;".format(result[2]))
                union_res = cursor.fetchall()
                # print(union_res)
                less_count, total_count = union_res[0][0], union_res[1][0]
                beyond = float('%.4f' % (less_count / total_count))

                return Response({"company_name": result[0], "intelligent_degree": result[1], "score": result[2],
                                 "beyond": beyond}, status=status.HTTP_200_OK)
            else:
                return Response(status=status.HTTP_204_NO_CONTENT)
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()

    def post(self, request):
        company_name = request.data.get("company_name")  # 公司名称
        content_list = request.data.get("content_list", [])  # ['1', '2', '3', '4', '5']
        if not company_name:
            return Response({"res": 1, "errmsg": "lack of company_name or content_list! 缺少公司名称"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]

        # 去重处理
        content_list = sorted(list(set(content_list)))
        # print(content_list)

        score = 0.0
        for content in content_list:
            if content not in INDUSTRY_PLUS_SCORE_DICT:
                return Response({"res": 1, "errmsg": "content_list code error! 打分内容代号错误"},
                                status=status.HTTP_200_OK)
            score += INDUSTRY_PLUS_SCORE_DICT.get(content, 0)
        score = float("%.2f" % score)
        # print(score)

        cursor = connection.cursor()

        try:
            cursor.execute("delete from industry_plus_test where phone = '%s';" % phone)
            cursor.execute("insert into industry_plus_test (phone, company_name, intelligent_degree, score, time) "
                           "VALUES ('%s', '%s', '{%s}', %s, %d);" % (
                               phone, company_name, ','.join(content_list), score, int(time.time())))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()


class IndustryPlusFactoryNew(APIView):
    """进一步了解，新建企业信息 industry_plus/factory/new"""

    def post(self, request):
        company_name = request.data.get("company_name")  # 公司名称
        industry = request.data.get("industry", "")  # 所属行业
        region = request.data.get("region", "")  # 公司地区
        contact_name = request.data.get("contact_name")  # 联系人姓名
        contact_phone = request.data.get("contact_phone")  # 联系电话
        solve_problems = request.data.get("solve_problems", [])  # ['1', '2', '3', '4', '5']
        supplement = request.data.get("supplement", "")  # 其它补充描述

        if not all([company_name, contact_name, contact_phone]):
            return Response({"res": 1, "errmsg": "请填写完整信息，以便联系您！"},
                            status=status.HTTP_200_OK)

        phone = request.redis_cache["phone"]

        cursor = connection.cursor()

        if not re.match("^(13[0-9]|14[579]|15[0-3,5-9]|16[6]|17[0135678]|18[0-9]|19[89])\\d{8}$", contact_phone):
            return Response({"res": 1, "errmsg": "电话号码格式错误"}, status=status.HTTP_200_OK)

        try:
            cursor.execute("delete from industry_plus_factorys where phone = '%s';" % phone)

            cursor.execute("select count(1) from industry_plus_factorys where contact_phone = '%s';" % contact_phone)
            phone_check = cursor.fetchone()[0]
            if phone_check >= 1:
                return Response({"res": 1, "errmsg": "联系电话号码已存在！"},
                                status=status.HTTP_200_OK)

            cursor.execute("select count(1) from industry_plus_factorys where company_name = '%s';" % company_name)
            company_name_check = cursor.fetchone()[0]
            if company_name_check >= 1:
                return Response({"res": 1, "errmsg": "此公司名称已存在！"},
                                status=status.HTTP_200_OK)

            cursor.execute("insert into industry_plus_factorys (phone, company_name, industry, region, contact_name, "
                           "contact_phone, solve_problems, supplement, time) values ('%s', '%s', '%s', '%s', '%s', "
                           "'%s', '{%s}', '%s', %d);" % (
                               phone, company_name, industry, region, contact_name, contact_phone,
                               ','.join(solve_problems), supplement, int(time.time())))
            connection.commit()

            return Response({"res": 0}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "server error!"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            cursor.close()
