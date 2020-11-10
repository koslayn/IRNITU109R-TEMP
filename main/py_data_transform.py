# -*- coding: utf-8 -*-
"""
    TODO Вероятно правильнее было бы делать разные коллекции и передавать данные как по pipline:
    * Получили ответы от ВК - всё записли как есть в коллекцию_1
    * Взяли всю коллекцию_1 - отобрали нужные поля и записали в коллекцию_2
    * Взяли то что есть в коллекции_2 - сделали модификации добавили поле даты и записали в коллекцию_3
    * ... и т.д.
"""
from pymongo import MongoClient
# Получаем настройки
from main.py_settings import *
# Обработка времени в документах
from main.py_datetime import *
# Обработка слов
from main.py_words import *

class Transfomer:
    def __init__(self, OWNER_ID, HOW_MANY_POSTS_TO_GET):
        self.OWNER_ID = OWNER_ID
        self.HOW_MANY_POSTS_TO_GET = HOW_MANY_POSTS_TO_GET
    # Подключение к MongoDB
        try:
            client = MongoClient(f"mongodb://{MC['user']}:{MC['password']}@{MC['host']}:{MC['port']}/{MC['authSource']}")
        except NameError:
            client = MongoClient('mongodb://mongo-server:27017')

        # Выбираем базу с которой будем работать и создаём коллекции в БД
        db = client[f'vk{self.OWNER_ID}']
        self.coll_posts = db.posts
        self.coll_comments = db.comments
        self.coll_profiles = db.profiles

    def transform_data(self):
        # Преобразование UNIX time в объекты datetime 
        # Документы с постами из сообщества
        MSG_date_posts = add_datetime_to_documents(collection=self.coll_posts)
        # Документы с комментариями к постам в сообществе
        MSG_date_comments = add_datetime_to_documents(collection=self.coll_comments)

        # Разбиваем посты на отдельные слова и tags
        # Коллекция постов
        MSG_tags_words_posts = add_tags_words_to_documents(collection=self.coll_posts)
        # Коллекция комментариев
        MSG_tags_words_comments = add_tags_words_to_documents(collection=self.coll_comments)

        # Нормализуем слова и выбираем: существительные, глаголы, прилогательные
        # Обрабатываем коллекцию с постами
        MSG_norm_posts = text_norm(collection=self.coll_posts)
        # Обрабатываем коллекцию с комментариями
        MSG_norm_comments = text_norm(collection=self.coll_comments)

        print(MSG_date_posts, MSG_date_comments, MSG_tags_words_posts, MSG_tags_words_comments, MSG_norm_posts, MSG_norm_comments)
        result = {'date_posts': MSG_date_posts,
                  'date_comments': MSG_date_comments,
                  'tags_words_posts': MSG_tags_words_posts, 
                  'tags_words_comments': MSG_tags_words_comments,
                  'norm_posts': MSG_norm_posts, 
                  'norm_comments': MSG_norm_comments
                }
        return result


if __name__ == "__main__":
    # Для отладки
    # python -m main.py_data_transform
    print(HOW_MANY_POSTS_TO_GET)
    print(VK_TOKEN)
    print(OWNER_ID)
    print(GET_NEW_DATA)
    print(TRANSFORM_DATA)
    print('*****************************')
    worker = Transfomer(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)
    worker.transform_data()