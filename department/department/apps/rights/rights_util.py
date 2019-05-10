# -*- coding: utf-8 -*-
import time


def add_apps_record(cursor, apps, factory_id, phone):
    for app in apps:
        cursor.execute("select type, start_time, end_time from tp_apps_order where factory_id = '%s' and app_id = '%s'"
                       " and state = '1' order by time desc limit 1;" % (factory_id, app))
        result = cursor.fetchone()
        # print(result)
        app_type, start_time, end_time = result[0], result[1], result[2]
        cursor.execute("insert into user_tp_apps (factory_id, phone, app_id, type, start_time, end_time, time) values "
                       "('%s', '%s', '%s', '%s', %d, %d, %d);" % (
                           factory_id, phone, app, app_type, start_time, end_time, int(time.time())))


def remove_apps_records(cursor, apps, phone):
    for app in apps:
        cursor.execute("delete from user_tp_apps where phone = '%s' and app_id = '%s';" % (phone, app))

