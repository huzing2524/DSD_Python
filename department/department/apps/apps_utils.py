# -*- coding: utf-8 -*-
import base64
import oss2
import pika
import shortuuid
import random
import datetime
import time

from itertools import islice
from django.db import connections
from django.conf import settings
from django_redis import get_redis_connection
from psycopg2.pool import AbstractConnectionPool

from constants import BG_QUEUE_NAME


def generate_module_uuid(module_type, factory_id, seq_id):
    """
    按照不同的单生成唯一ID
    :param module_type:  constants.PrimaryKeyType
    :param factory_id:   工厂id
    :param seq_id:       工厂序号 seq_id = request.redis_cache["seq_id"]
    :return: string
    """
    conn = get_redis_connection("default")
    num = conn.hget(factory_id, "primary_key")
    # print(num)
    if num:
        conn.hincrby(factory_id, "primary_key", 1)
    else:
        num = 1
        conn.hset(factory_id, "primary_key", 1)
        d = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
        expire = time.mktime(d.timetuple()) - int(time.time())
        conn.expire(factory_id, int(expire))
    return module_type + datetime.datetime.now().strftime("%Y%m%d%H%M") + str(seq_id).zfill(4) + str(num).zfill(4)


def generate_purchase_id():
    """生成采购单id, 字符串"""
    return '01' + datetime.datetime.now().strftime("%Y%m%d") + str(random.randint(1, 100001))


def generate_uuid():
    """生成18位短uuid，字符串"""
    # u22 = shortuuid.uuid()  # 22位uuid
    u18 = shortuuid.ShortUUID().random(length=18)  # 18位uuid
    return u18


class PostgresqlPool(AbstractConnectionPool):
    """Postgresql数据库连接池
    单例模式: 保证只创建一个对象
    注意数据库配置文件conf中时区设置
    """
    _instance = None

    def __new__(cls, *args, **kwargs):  # 创建对象
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)

        print('cls._instance', cls._instance, 'id--->', id(cls._instance))
        return cls._instance

    def __init__(self):  # 初始化对象
        super().__init__(minconn=5, maxconn=20, database=settings.POSTGRESQL_DATABASE, user=settings.POSTGRESQL_USER,
                         password=settings.POSTGRESQL_PASSWORD, host=settings.POSTGRESQL_HOST,
                         port=settings.POSTGRESQL_PORT)

    def connect_postgresql(self):
        connection = AbstractConnectionPool._getconn(self)
        cursor = connection.cursor()
        # print(connection)
        return connection, cursor

    def disconnect_postgresql(self, connection):
        AbstractConnectionPool._putconn(self, connection)


class UtilsPostgresql(object):
    """Postgresql数据库连接池"""

    def connect_postgresql(self):
        # connection = AbstractConnectionPool._getconn(self)
        # cursor = connection.cursor()
        # print(connection)
        db_conn = connections['default']
        cursor = db_conn.cursor()
        return db_conn, cursor

    def disconnect_postgresql(self, connection):
        # AbstractConnectionPool._putconn(self, connection)
        pass


class UtilsRabbitmq(object):
    """RabbitMQ消息发送"""
    host = settings.RABBITMQ_HOST
    port = settings.RABBITMQ_PORT
    vhost = '/'

    @classmethod
    def _connect_rabbitmq(cls):
        """连接rabbitmq"""
        try:
            parameters = pika.ConnectionParameters(
                host=cls.host, port=cls.port)
            connection = pika.BlockingConnection(parameters)
            return connection
        except Exception as e:
            raise e

    @classmethod
    def _disconnect_rabbitmq(cls, connection):
        """关闭连接"""
        connection.close()

    def send_message(self, message):
        """发送消息"""
        conn = self._connect_rabbitmq()
        channel = conn.channel()
        channel.basic_publish(
            exchange='', routing_key=BG_QUEUE_NAME, body=message)
        # print('send %s' % message)
        self._disconnect_rabbitmq(conn)

    def recieve_message(self):
        """接收消息"""
        conn = self._connect_rabbitmq()
        channel = conn.channel()
        channel.queue_declare(queue=BG_QUEUE_NAME)

        def callback(ch, method, properties, body):
            print('[x] recieved %r' % body)

        channel.basic_consume(callback, queue=BG_QUEUE_NAME, no_ack=True)
        channel.start_consuming()


class AliOss(object):
    """阿里云OSS 图片处理"""
    auth = oss2.Auth('LTAIpVVnnK7jBiAr', '1nDeqBqyUlZzI7njadkgFrpetstdkc')
    bucket = oss2.Bucket(
        auth, ' https://oss-cn-shenzhen.aliyuncs.com', 'dsd-images')

    def upload_image(self, image):
        """
        上传图片
        :return: 图片id(18位uuid)
        """
        if not image:
            return "", ""
        image_id = generate_uuid()
        image_url = "https://dsd-images.oss-cn-shenzhen.aliyuncs.com/{}"
        # print(type(image_id), type(image))
        try:
            if isinstance(image, str) and "," in image:
                image = base64.b64decode(image.split(",")[-1])
                # print(image[:10], type(image))
            else:
                image = ""
        except Exception:
            return "", ""
        else:
            full_image_id = settings.IMAGE_PATH + "/" + image_id + ".jpg"
            AliOss.bucket.put_object(full_image_id, image, headers={
                "Content-Type": "image/jpg"})

            image_url = image_url.format(full_image_id)
            # print(image_id, full_image_id, image_url)
            return image_id, image_url

    def joint_image(self, image_id):
        """拼接图片的完整url路径"""
        if not image_id:
            image_id = "default"
        image_url = "https://dsd-images.oss-cn-shenzhen.aliyuncs.com/{}"
        full_image_id = settings.IMAGE_PATH + "/" + \
                        image_id + ".jpg" if image_id else ''

        return image_url.format(full_image_id)

    def delete_image(self, objectname):
        """
        删除图片
        :return: class:`RequestResult <oss2.models.RequestResult>
        """
        resp = AliOss.bucket.delete_object(objectname)
        return resp

    def exist_image(self, objectname):
        """
        判断文件是否存在
        :return: 返回值为true表示文件存在，false表示文件不存在
        """
        objectname = settings.IMAGE_PATH + "/" + objectname + ".jpg"
        exist = AliOss.bucket.object_exists(objectname)
        return exist

    def list_images(self, num):
        """
        用于遍历文件
        :return: 图片列表
        """
        keys_list = []

        for b in islice(oss2.ObjectIterator(AliOss.bucket), num):
            keys_list.append(b.key)
        return keys_list


def generate_sql_uuid():
    pgsql = UtilsPostgresql()
    connection, cursor = pgsql.connect_postgresql()
    cursor.execute("select uuid_generate_v4();")
    uuid = cursor.fetchone()[0]
    pgsql.disconnect_postgresql(connection)
    return uuid


def correct_time(year, month):
    if month <= 0:
        return year - 1, month + 12
    elif month > 12:
        return year + 1, month - 12
    else:
        return year, month


def today_timestamp():
    """获取今天的起始、结束时间戳"""
    year, month, day = datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day
    start = datetime.datetime(year, month, day)
    end = start + datetime.timedelta(hours=24)
    start_timestamp = int(time.mktime(start.timetuple()))
    end_timestamp = int(time.mktime(end.timetuple()))
    return start_timestamp, end_timestamp


def month_timestamp(year, month):
    """
    :param year: 某年
    :param month: 某月
    :return: 指定月份的第一天起始时间戳，最后一天结束时间戳
    """
    ct1 = correct_time(year, month)
    start = datetime.datetime(ct1[0], ct1[1], 1)
    ct2 = correct_time(year, month + 1)
    end = datetime.datetime(ct2[0], ct2[1], 1) - datetime.timedelta(seconds=1)
    start_timestamp = int(time.mktime(start.timetuple()))
    end_timestamp = int(time.mktime(end.timetuple()))
    return start_timestamp, end_timestamp


def year_timestamp(year):
    """
    :param year: 某年
    :return: 指定年份的第一天起始时间戳，最后一天结束时间戳
    """
    start = datetime.datetime(year, 1, 1)
    end = datetime.datetime(year + 1, 1, 1) - datetime.timedelta(seconds=1)
    start_timestamp = int(time.mktime(start.timetuple()))
    end_timestamp = int(time.mktime(end.timetuple()))
    return start_timestamp, end_timestamp
