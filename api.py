# coding: utf-8
"""
Модуль Api Redis-соединения.
Введен для изоляции от текущей реализации драйвера.
"""
from redis import StrictRedis


class RedisApi(object):
    """
    Redis API class.
    """

    def __init__(self, host, port, db):
        self.__conn = StrictRedis(host=host, port=port, db=db)

    def push_queue_msg(self, queue, msg):
        """
        Запись сообщения в очередь.

        :param str queue: имя очереди.
        :param str msg: записываемое сообщение.
        :return:
        """
        return self.__conn.rpush(queue, msg)

    def pop_queue_msg(self, queue):
        """
        Получение сообщения из очереди.
        После получения сообщение удаляется.

        :param str queue: имя очереди.
        :return: str
        """
        return self.__conn.lpop(queue)

    def set_key_value(self, key, value, ex, nx):
        """
        Метод записи пары ключ-значение.

        :param key:
        :param value:
        :param ex: время жизни значения
        :param nx: устанавливает значение, если ключ еще не добавлен
        :return:
        """
        return self.__conn.set(key, value, ex=ex, nx=nx)

    def increment_value(self, key):
        """
        Инкрементирует значение ключа.
        По-умолчанию начинается с 1

        :param str key:
        :return:
        """
        return self.__conn.incr(key)
