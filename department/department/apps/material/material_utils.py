# -*- coding: utf-8 -*-
import datetime
import time

from apps_utils import generate_uuid
from constants import RightsEnumerate


def send_notice_msg(cursor, factory_id, rights, phone, content):
    sql = "select coalesce(string_agg(phone, ','), '') as users from factory_users where factory = '%s' and" \
          " ('1' = ANY(rights) or '%s' = ANY(rights)) and phone <> '%s'" % (factory_id, rights, phone)
    # print(sql), print(content)
    cursor.execute(sql)
    users = cursor.fetchone()
    print(users)
    # todo 发送消息......


def save_message_record(cursor, connection, type1, type2, factory_id, phone, timestamp, title, content, item_id):
    uuid = generate_uuid()

    if type1 == "order" and type2 == "new":
        right = RightsEnumerate.store.value
    else:
        right = RightsEnumerate.material.value

    sql = "insert into messages (id, user_id, title, body, type, type2, item_id, related, time) values " \
          "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d)"
    user_sql = "select phone from factory_users where factory = '%s' and ('1' = ANY(rights) or '%s' = ANY(rights));" % \
               (factory_id, right)
    cursor.execute(user_sql)
    related = cursor.fetchone()
    print(sql), print(user_sql), print(related)

    for re in related:
        cursor.execute(sql % (uuid, phone, title, content, type1, type2, item_id, re, timestamp))
    connection.commit()
    # todo 发送消息......


def year_timestamp(year):
    """
    :param year: 某年
    :return: 指定年份的第一天起始时间戳，最后一天结束时间戳
    """
    start = datetime.datetime(year, 1, 1)
    end = datetime.datetime(year + 1, 1, 1) - datetime.timedelta(seconds=1)
    start_time = int(time.mktime(start.timetuple()))
    end_time = int(time.mktime(end.timetuple()))
    return start_time, end_time


def week_timestamp():
    # return the timestamp of monday
    num_in_week = datetime.datetime.now().weekday()
    dtime = datetime.datetime.now().strftime('%Y-%m-%d')
    today = datetime.datetime.strptime(dtime, '%Y-%m-%d')
    monday = today + datetime.timedelta(days=-num_in_week)
    start_timestamp = int(time.mktime(monday.timetuple()))
    end_timestamp = start_timestamp + 7*86400
    return start_timestamp, end_timestamp
