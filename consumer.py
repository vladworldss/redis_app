# coding: utf-8
"""
Module of Consumer of Redis message queue.
Consumer handles of messages.
"""
from random import random

from settings import QUEUE_MSG, QUEUE_ERROR


class RedisConsumer(object):

    """
    Класс обработчика сообщений.
    """

    def __init__(self, redis_api, cid, logger):
        """
        Конструктор класса.

        :param redis_api: объект api для работы с очередью
        :param cid: id приложения.
        :param logger:
        """
        self.__api = redis_api
        self.cid = cid
        self.logger = logger

    #
    # __Private methods
    
    def __pop_msg(self):
        """
        Получение сообщения из очереди. После этого сообщение удаляется.
        :return: str
        """
        return self.__api.pop_queue_msg(QUEUE_MSG)

    def __push_bad_msg(self, msg):
        """
        Запись сообщения в очередь ошибок.

        :param str msg:
        """
        return self.__api.push_queue_msg(QUEUE_ERROR, msg)      

    def __all_bad_msg(self):
        """
        Генератор сообщений из очереди ошибок.

        :return: str
        """
        while True:
            msg = self.__api.pop_queue_msg(QUEUE_ERROR)
            if not msg:
                raise StopIteration
            yield msg.decode("utf-8")

    # 
    # __Public methods

    @staticmethod
    def is_bad_msg(msg, ok_prob=0.95):
        """
        Метод определяет, содержит ли сообщение ошибку.

        :param float ok_prob: probability of good msg
        """

        rand_prop = random()
        return bool(msg and (rand_prop >= ok_prob))

    def get_errors(self):
        """
        Метод получения ошибок и вывод их в консоль.
        После вывода сообщения удаляются из очереди.
        """
        for msg in self.__all_bad_msg():
            self.logger.error(f"CID={self.cid}: bad msg: {msg}")

    def handle(self):
        """
        Метод обработки сообщений.
        Происходит классификация сообщений.
        Если сообщение помечается как "ошибка", оно помещается в очередь ошибок.
        """
        msg = self.__pop_msg()
        if self.is_bad_msg(msg):
            self.__push_bad_msg(msg)
        elif msg:
            self.logger.info(f"CID={self.cid}: handling msg: {msg}")
