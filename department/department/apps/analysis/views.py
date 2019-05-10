# -*- coding: utf-8 -*-
import logging

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from analysis.retrieve_hzy import main
from analysis.retrieve_jcj import run
from apps_utils import UtilsPostgresql

logger = logging.getLogger('django')


class Analysis(APIView):
    """V2.4.0 分析功能 /api/v2/analysis"""

    def get(self, request):
        factory_id = request.redis_cache["factory_id"]

        result = {}
        pgsql = UtilsPostgresql()
        connection, cursor = pgsql.connect_postgresql()

        try:
            # ------市场部------------财务部------------采购部------
            jcj = run(factory_id, cursor)
            # print('jjj', jcj)
            result.update(jcj)

            # ------仓库部------------生产部------
            hzy = main(factory_id, cursor)
            result.update(hzy)

            # print('res', result)

            return Response(result, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(e)
            return Response({"res": 1, "errmsg": "Analysis data query occurred exception."},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            pgsql.disconnect_postgresql(connection)
