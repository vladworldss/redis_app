# coding: utf-8
"""
Модуль генератора сообщений.
"""
import time
from random import choice, randint
from string import ascii_letters

from settings import QUEUE_LOCK, LOCK_TIMEOUT, MSG_GEN_TIMEOUT, QUEUE_MSG


class RedisProducer(object):
    """
    Класс, публикующий сообщения в очередь.
    """

    def __init__(self, redis_api, cid, logger):
        """
        Конструктор класса.

        :param redis_api: api очереди.
        :param cid: id приложения
        :param logger:
        """
        self.__api = redis_api
        self.cid = cid
        self.logger = logger

    #
    # __Private methods

    def __lock_acquire(self, overwrite=True):
        """
        Захват блокировки.

        :param bool overwrite: повторное взятие блокировки.
        :return: bool
        """
        return self.__api.set_key_value(key=QUEUE_LOCK, value=self.cid, ex=LOCK_TIMEOUT, nx=overwrite)

    def __push_msg(self, msg):
        """
        Добавление сообщения в очередь.

        :param str msg:
        :return: bool
        """
        return self.__api.push_queue_msg(QUEUE_MSG, msg)

    def __generate_random_msg(self, min_length=10, max_lenght=30):
        """
        Генерирует сообщение произвольной длины.

        :param int min_length: минимальный размер сообщения.
        :param int max_lenght: максимальный размер сообщения.
        :return: str
        """
        
        _min, _max = sorted((int(min_length), int(max_lenght)))
        return "".join(choice(ascii_letters) for _ in range(randint(_min, _max)))

    def __wait(self, timeout=MSG_GEN_TIMEOUT):
        """
        Интервал генерирования сообщений.

        :param float timeout: сек
        """
        time.sleep(timeout)

    # 
    # __Public methods
    
    def publish(self):
        """
        Публикация сообщений в очередь в эксклюзивном режиме.
        При аварийном завершениии приложения, после таймаута, блокировка снова будет
        доступна для другого приложения.
        """

        # Trying to acuire lock. 
        # If lock has been acquired - return.
        if not self.__lock_acquire():
            self.logger.info(f"CID={self.cid}: lock was aquired from another app.")
            return
        Continue = True
        while Continue:
            self.__wait()
            msg = self.__generate_random_msg()
            self.__push_msg(msg)
            self.logger.info(f"CID={self.cid}: was published msg: {msg}")
            # Update lock
            Continue = self.__lock_acquire(overwrite=False)
