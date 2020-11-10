# -*- coding: utf-8 -*-
import requests
from pymongo import MongoClient

# Настройки
from main.py_settings import *
# Системыне (проверки)
from main.py_system import *
# Функции работы с VKAPI
from main.py_vkapi import *
# Функции для разбора ответа от API-VK
from main.py_parse import *
# Функции записи данных в БД
from main.py_mongodb import *

# Получаем данные

class UpLoader:
    def __init__(self, OWNER_ID, HOW_MANY_POSTS_TO_GET):
        self.OWNER_ID = OWNER_ID
        self.HOW_MANY_POSTS_TO_GET = HOW_MANY_POSTS_TO_GET
        # Подключение к MongoDB
        try:
            client = MongoClient(f"mongodb://{MC['user']}:{MC['password']}@{MC['host']}:{MC['port']}/{MC['authSource']}")
        except NameError:
            client = MongoClient('mongodb://mongo-server:27017')

        # Выбираем базу с которой будем работать и создаём коллекции в БД
        db = client[f'vk{self.OWNER_ID}'] # ❗ вписал SELF
        self.coll_posts = db.posts
        self.coll_comments = db.comments
        self.coll_profiles = db.profiles

    def get_posts_data(self):
        # Запись постов в коллекцию (дописывает недостающие посты)
        for offset in range(0, self.HOW_MANY_POSTS_TO_GET, 100):    
            raw_posts = get_posts(count=100, offset=offset, OWNER_ID=self.OWNER_ID, VK_TOKEN=VK_TOKEN)
            parsed_posts = parse_json_vk_posts(raw_posts)
            written_posts, MSG = write_posts_to_collection(parsed_posts, self.coll_posts)
            if written_posts is False:
                return MSG
            return MSG

    def get_comments_data(self):
    # Запись комментариев в коллекцию
        mongo_cursor_posts = self.coll_posts.find({'comments':{'$gt':0}})
        posts_ids = [i['id'] for i in mongo_cursor_posts]
        
        comments_counter = 0
        all_comments = []
        for post_id in posts_ids:
            raw_comments = get_comments(post_id=post_id, OWNER_ID=self.OWNER_ID, VK_TOKEN=VK_TOKEN)
            parsed_comments = parse_json_vk_comments(raw_comments)
            all_comments.extend(parsed_comments)
            comments_counter += 1
            
            if comments_counter % 100 == 0:
                print(comments_counter)
            
        written_comments, MSG = write_comments_to_collection(comments=all_comments, collection=self.coll_comments)
        print(MSG)
        return MSG

    def get_profiles_data(self):
    # Запись профайлов в коллекцию
        # TODO Нет проверки на дубликаты при записи в БД, нет проверки на "открытие\появление" новых доступных полей профайла
        mongo_cursor_profiles = self.coll_comments.find({}, {'from_id': True, '_id': False})
        profiles_ids = set([i['from_id'] for i in mongo_cursor_profiles])
        profiles_ids = list(profiles_ids)

        profiles_for_collection = []

        while len(profiles_ids) != 0:
            ids_100 = profiles_ids[0:100]
            del profiles_ids[0:100]
            part = get_profiles(ids=ids_100, VK_TOKEN=VK_TOKEN)
            profiles_for_collection.extend(part)
        
        written_profiles, MSG = write_profiles_to_collection(profiles=profiles_for_collection, collection=self.coll_profiles)
        print(MSG)
        return MSG

if __name__ == "__main__":
    # Для отладки
    # python -m main.py_data_get
    print(HOW_MANY_POSTS_TO_GET)
    print(VK_TOKEN)
    print(OWNER_ID)
    print(GET_NEW_DATA)
    print(TRANSFORM_DATA)
    test = UpLoader(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)
    test.get_posts_data()
    test.get_comments_data()
    test.get_profiles_data()
