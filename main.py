# coding: utf-8
"""
Модуль Redis-приложения.
"""
from argparse import ArgumentParser
import logging

from api import RedisApi
from consumer import RedisConsumer
from producer import RedisProducer
from settings import REDIS_SERVER_CONF, QUEUE_CID


class RedisApp(object):
    """
    Класс приложения, который публикует сообщения в очередь/обрабатывает их.
    """
    __RedisApiCls = RedisApi
    __ConsumerCls = RedisConsumer
    __ProducerCls = RedisProducer

    def __init__(self, host, port, db):
        """
        Конструктор класса.

        :param host: адрес хоста, где запущен redis-сервер.
        :param port: адрес порта входящих соединений.
        :param db: имя базы данных.
        """
        self.__logger = None
        self.__cid = None
        self.__api = self.__RedisApiCls(host, port, db)
        self.__producer = self.__ProducerCls(self.__api, self.cid, logger=self.logger)
        self.__consumer = self.__ConsumerCls(self.__api, self.cid, logger=self.logger)

    @property
    def logger(self):
        """
        Логгер событий.
        """
        if self.__logger is None:
            logging.basicConfig(level=logging.INFO)
            self.__logger = logging
        return self.__logger

    @property
    def cid(self):
        """
        Уникальный идентификатор клиента (Client ID)
        """
        if self.__cid is None:
            self.__cid = self.__api.increment_value(QUEUE_CID)
        return self.__cid

    def run(self):
        """
        Метод запука приложения.
        Вначале приложение пытается захватить блокировку для
        монопольной генерации сообщений.
        Если блокировка уже захвачена - занимаемся обработкой.
        """
        self.__producer.publish()
        while True:
            self.__consumer.handle()
            self.__producer.publish()

    def get_errors(self):
        """
        Получение сообщений с ошибкой из очереди и вывод их в консоль.
        """
        self.__consumer.get_errors()


class CustomParser(ArgumentParser):
    """
    Класс пользовательского обработчика аргументов командной строки.
    """

    ERROR_CMD = "getErrors"
    HELP_USAGE = "Usage:\n$ python main.py # for run application." \
                 "\n$ python main.py getErrors # to get msg errors"

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.add_argument("cmd", nargs='?', default=None)


def main():
    """
    Главный метод запуска приложения.
    """

    app = RedisApp(**REDIS_SERVER_CONF)

    # создание парсера аргументов командной строки.
    parser = CustomParser()
    args = parser.parse_args()

    # обработка ключей командной строки
    if args.cmd is None:
        app.run()
    elif args.cmd == parser.ERROR_CMD:
        app.get_errors()
    else:
        print(parser.HELP_USAGE)


if __name__ == "__main__":
    main()
