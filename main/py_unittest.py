# -*- coding: utf-8 -*-
from py_settings import *
from py_datetime import *
from py_system import *
from py_vkapi import *
from py_parse import *
from py_words import *
from py_categories import *
from py_mongodb import *

from pymongo import MongoClient


###############################################
### -------------Тестирвоание-------------- ###
###############################################
import unittest
# Модуль для юниттестирвоания
# Более простой вариант использовать конструкцию assert
# assert my_func([1, 2]) == ['one', 'two'], 'что-то не так'

# Наследуемся от базового класса
class ExtractorTest(unittest.TestCase):
# Тесты должны начинаться с test_ они будут тестирующие
# Методы, которые названы по другому, не будет отнесены к тестам

# setUp - метод, который вызывается перед каждым тестом - параметры
    def setUp(self):
        MC = {'host': 'localhost', 'port': 27017, 'user': None, 'password': None, 'authSource': 'vk'}
        self.client = MongoClient(MC['host'], MC['port'])
        self.db = self.client.vk
        self.coll_posts = self.db.posts
        self.coll_comments = self.db.comments
        self.posts_ids = self.coll_posts.insert_many([{'id': 4}, {'id': 5}])

# tearDown - метод очистка после выполнения теста, подчищаем за тестами
    def tearDown(self):
        self.prof_del_1 = self.coll_posts.delete_many({'id': {"$in": [4, 5]}})
        self.prof_del_2 = self.coll_posts.delete_many({'id': {"$in": [1, 2, 3]}})
        self.prof_del_2 = self.coll_comments.delete_many({'id': {"$in": [1, 2, 3]}})
        # print('----------ОТРАБОТАЛ ОЧИСТКА------------------')

# Тесты - Очистка постов от повторов
    # @unittest.skip
    def test_duplicate_cleaner_new_data(self):
        result = duplicate_cleaner(data=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_posts)
        self.assertEqual(result, [{'id': 1}, {'id': 2}, {'id': 3}])

    # @unittest.skip
    def test_duplicate_cleaner_no_new_data(self):
        result = duplicate_cleaner(data=[{'id': 4}, {'id': 5}], collection=self.coll_posts)
        self.assertEqual(result, False)

    # @unittest.skip
    def test_duplicate_cleaner_empty_data(self):
        result = duplicate_cleaner(data=[], collection=self.coll_posts)
        self.assertEqual(result, False)

# Тесты запись постов в коллекцию - их очистка от повторов выше
    # @unittest.skip
    def test_write_posts_to_collection_new_data(self):
        result = write_posts_to_collection(posts=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_posts)
        search = self.coll_posts.find({'id': {'$in': [1, 2, 3]}}, {'_id': False})
        search_list = [i for i in search]
        self.assertEqual(result, True)
        self.assertEqual(search_list, [{'id': 1}, {'id': 2}, {'id': 3}])

    # @unittest.skip
    def test_write_posts_to_collection_empty_data(self):
        result = write_posts_to_collection(posts=[], collection=self.coll_posts)
        self.assertEqual(result, False)

# Тест записи в коллекцию - комментариев
    def test_write_comments_to_collection_new_data(self):

        result = write_comments_to_collection(comments=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_comments)
        search = self.coll_comments.find({'id': {'$in': [1, 2, 3]}}, {'_id': False})
        search_list = [i for i in search]
        self.assertEqual(result, True)
        self.assertEqual(search_list, [{'id': 1}, {'id': 2}, {'id': 3}])

### Если запускаем скрипт из консоли, то переменной присваивается значение и выполняется комманда
### Если импортируем библиотеку, то нет присвоения и не запускается команда
if __name__ == '__main__':
    unittest.main()