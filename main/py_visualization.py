# -*- coding: utf-8 -*-
from main.py_settings import *
from pymongo import MongoClient

import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.io.json import json_normalize
import time
# from pprint import pprint as print

class Vision:
    def __init__(self, OWNER_ID, HOW_MANY_POSTS_TO_GET):
        self.OWNER_ID = OWNER_ID
        self.HOW_MANY_POSTS_TO_GET = HOW_MANY_POSTS_TO_GET
        self.img_name = []
        # Подключение к MongoDB
        try:
            client = MongoClient(f"mongodb://{MC['user']}:{MC['password']}@{MC['host']}:{MC['port']}/{MC['authSource']}")
        except NameError:
            client = MongoClient('mongodb://mongo-server:27017')

        # Выбираем базу с которой будем работать и создаём коллекции в БД
        self.db = client[f'vk{self.OWNER_ID}'] # ❗ вписал SELF
        self.coll_posts = self.db.posts
        self.coll_comments = self.db.comments
        self.coll_profiles = self.db.profiles

        

    def bar_categories(self, sql_r, param, file_name, table=None):
        """
        Функция для визуализации данных по категориям в виде bar-chart
        input: DataFrame - ...,
        * parm - словарь с ключами соответсующими параметрам .catplot,
        * table=None - выводить ли таблицу значений для графика (по умолчанию не выводить),
        """

        g = sns.catplot(
            x      = param.setdefault('x', None), 
            y      = param.setdefault('y', None), 
            hue    = param.setdefault('hue', None),
            orient = param.setdefault('orient', None),
            kind   = param.setdefault('kind', 'strip'),
            col    = param.setdefault('col', None),
            data   = sql_r,
            height=5,
            aspect = 3,)
        
        # Прямой доступ к Figure, Axes библиотеки matplotlib - объект axes - если несколько полотен 'for ax in g.axes.flat:' или можно указывать индексы
        # Если одно полотно то доступ можно делать по атрибуту .ax без индекса, полотна хранятся в 2D массиве.
        # Объект который позволяети рисовать примитивы на полотне - например прямоугольник - https://matplotlib.org/api/_as_gen/matplotlib.patches.Rectangle.html
        # Цикл позволяеет запрашивать данные по всем объектам нанесённым на полотно
        # Перебираем все прямоугольники и получаем их координаты и размеры и использует их для нанесения значений на полотно
        for coor in g.ax.patches:
            # Ширина - прямоугольника (по оси X) - от 0 до конца
            coor_w = round(coor.get_width(), 2)
            # Высота - прямоугольника (по оси Y) - от 0 до конца
            coor_h = round(coor.get_height(), 2)
            # Расположение прямоугольника на оси Y - нижняя координата прямоугольника (ось Y)
            coor_y = round(coor.get_y(), 2)
            # Расположение прямоугольника на оси X - левая координата прямоугольника (ось X)
            coor_x = round(coor.get_x(), 2)
            
            # Отражаем на полотне - значения
            if param['orient']:
            # Для горизонтального случая + проверка на наличие отражаемого значения (для избегания предупреждения)
                if not np.isnan(coor_w):
                    g.ax.text(x=coor_w, y=coor_y + coor_h/2 + 0.05, s=str(coor_w), fontsize=14)
            else:
            # Для вертикального случая  + проверка на наличие отражаемого значения (для избегания предупреждения)
                if not np.isnan(coor_h):
                    g.ax.text(x=coor_x + coor_w/2 - 0.1, y=coor_h, s=str(coor_h), fontsize=14)

        # Чтобы подписи осей помещались - вертикальный
        if param['orient'] is None:
            g.set_xticklabels(rotation=90)

        # Показать таблицу со значениями, по умолчанию отключено
        if table:
            display(sql_r)
        epoch_time = int(time.time())
        g.savefig(f'./main/static/{self.db.name}-{file_name}-{epoch_time}.png')
        self.img_name.append(f'{self.db.name}-{file_name}-{epoch_time}.png')

    def collection_stats(self):
        posts_total = self.coll_posts.count_documents(filter={})
        comments_total = self.coll_comments.count_documents(filter={})
        profiles_total = self.coll_profiles.count_documents(filter={})
        MSG = f'Сейчас в базе данных {self.db.name}:\n«Постов» в коллекции: {posts_total}.\n«Комментариев» в коллекции: {comments_total}.\n«Профайлов» в коллекции: {profiles_total}.'
        return MSG

    def posts_by_mounths(self):
        pipline = [{'$group':
                    {'_id':
                        { 'Year': {'$year': '$datetime'},
                            'Month': {'$month': '$datetime'} },
                            'count': {'$sum': 1} }  },
                    {'$sort': {'_id.Year': 1, '_id.Month': 1}   }]

        mongo_cursor = self.coll_posts.aggregate(pipline)

        datapoint = list(mongo_cursor)
        df = json_normalize(datapoint)  
        df['year-month'] = df['_id.Year'].astype(str) +'-'+ df['_id.Month'].astype(str)

        self.bar_categories(df, {'x':'year-month', 'y': 'count', 'kind': 'bar',}, 'posts_by_mounths')

    def posts_by_daytime(self):
        pipline = [{'$group': 
                {'_id': 
                  {'Hour': {'$hour': '$datetime'}},
                  'count': {'$sum': 1}  }  },
            {'$sort': {'_id.Hour': 1}}  ]

        mongo_cursor = self.coll_posts.aggregate(pipline)
        df = json_normalize(mongo_cursor)

        self.bar_categories(df, {'x':'_id.Hour', 'y': 'count', 'kind': 'bar',}, 'posts_by_daytime')
    
    def posts_avg_likes(self):
        pipline = [{'$group': 
                {"_id":
                  {"Year" : {'$year': "$datetime"},
                   "Month": {'$month': "$datetime"} },
                   "mean_likes" : {'$avg': "$likes"} }  },
            {'$sort': {"_id.Year": 1, "_id.Month": 1}} ]

        mc = self.coll_posts.aggregate(pipline)
        df = json_normalize(mc)
        df['year-month'] = df['_id.Year'].astype(str) +'-'+ df['_id.Month'].astype(str)
        self.bar_categories(df, {'x':'year-month', 'y': 'mean_likes', 'kind': 'bar',}, 'posts_avg_likes')

    def posts_avg_views(self):
        pipline = [{'$group':
               {"_id":
                  {"Year" : {'$year': "$datetime"},
                   "Month": {'$month': "$datetime"} },
                   "mean_views" : {'$avg': "$views"} }  },
            {'$sort': {"_id.Year": 1, "_id.Month": 1}} ]

        mc = self.coll_posts.aggregate(pipline)
        df = json_normalize(mc)
        df['year-month'] = df['_id.Year'].astype(str) +'-'+ df['_id.Month'].astype(str)
        self.bar_categories(df, {'x':'year-month', 'y': 'mean_views', 'kind': 'bar',}, 'posts_avg_views')
    
    def posts_avg_weighed_views(self):
        # TODO - просмотры на один пост (среднее + деление)
        pipline = [{'$group':
                    {"_id":
                        {"Year" : {'$year': "$datetime"},
                        "Month": {'$month': "$datetime"} },
                        "count_posts": {'$sum': 1},
                        "mean_views" : {'$avg': "$views"} }  },
                    {'$project': {"name": 1, "mean_views_to_one_post": {'$divide':["$mean_views", "$count_posts"]}}},
                    {'$sort': {"_id.Year": 1, "_id.Month": 1}} ]

        mc = self.coll_posts.aggregate(pipline)
        df = json_normalize(mc)
        df['year-month'] = df['_id.Year'].astype(str) +'-'+ df['_id.Month'].astype(str)
        self.bar_categories(df, {'x':'year-month', 'y': 'mean_views_to_one_post', 'kind': 'bar',}, 'posts_avg_weighed_views')

    def post_avg_char_length(self):
        pipline = [{'$group': {"_id": {"Year" : {'$year': "$datetime"},
                            "Month": {'$month': "$datetime"} },
                            "mean_str" : {'$avg': {'$strLenCP': "$text"}} }  },
            {'$sort': {"_id.Year": 1, "_id.Month": 1}} ]

        mc = self.coll_posts.aggregate(pipline)
        df = json_normalize(mc)
        df['year-month'] = df['_id.Year'].astype(str) +'-'+ df['_id.Month'].astype(str)
        self.bar_categories(df, {'x':'year-month', 'y': 'mean_str', 'kind': 'bar',}, 'post_avg_char_length')
    
    def profiles_by_country(self):
        pipline = [{"$match": {"country.title": {'$ne': None}}},
            {'$group': 
                {"_id": "$country.title",
                            "count": {'$sum': 1} } },
                {'$project': 
                    {"country":"$_id", "count": 1, "_id":0}},
                {'$sort': {'count': -1}}  ]

        mc = self.coll_profiles.aggregate(pipline)

        df = pd.DataFrame(list(mc)).sort_values('count', ascending=False)[0:11]
        self.bar_categories(df, {'x':'country', 'y': 'count', 'kind': 'bar',}, 'profiles_by_country')

    def profiles_by_city(self):
        pipline = [{"$match": {"city.title": {'$ne': None}}},
            {'$group': 
                {"_id": "$city.title",
                            "count": {'$sum': 1} } },
                {'$project': 
                    {"city":"$_id", "count": 1, "_id":0}},
                {'$sort': {'count': -1}} ]

        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('count', ascending=False)[0:12]
        self.bar_categories(df, {'x':'city', 'y': 'count', 'kind': 'bar',}, 'profiles_by_city')
    
    def profiles_by_age(self):
        pipline = ([ {'$match': {"bdate": {'$exists': True}}},
             {'$project': {"bdate": 1, "bdate_len": {'$strLenCP': "$bdate"}} },
             {'$match': {"bdate_len": {'$gte': 8}}},
             {'$project': {"bdate": 1, "year": {'$split': ["$bdate", "."]}} },
             {'$unwind': "$year"},
             {'$addFields': {"int_year": {'$toInt': '$year'} }},
             {'$match': {"int_year": {'$gt': 1800}}},
             {'$addFields': {"age": {'$subtract': [2020, '$int_year']} }},
             {'$group': {"_id": "$age", "count": {'$sum': 1}}},
             {'$sort': {'count': -1}}
          ])

        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('_id', ascending=True)
        self.bar_categories(df, {'x':'_id', 'y': 'count', 'kind': 'bar',}, 'profiles_by_age')
    
    def comments_by_time(self):
        pipline = [{'$group':
                 {'_id': 
                    {'Hour': {'$hour': '$datetime'}},
                     'count': {'$sum': 1}} },
            {'$sort': {'_id.Hour': 1}} ]

        mc = self.coll_comments.aggregate(pipline)
        df = json_normalize(mc)

        self.bar_categories(df, {'x':'_id.Hour', 'y': 'count', 'kind': 'bar',}, 'comments_by_time')
    
    def profiles_by_emp(self):
        pipline = [{'$match': {'occupation': {'$exists': True}}},
           {'$group': 
               {'_id': '$occupation.type',
                'count': {'$sum': 1}}}
           ]


        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('_id', ascending=True)
        self.bar_categories(df, {'x':'_id', 'y': 'count', 'kind': 'bar',}, 'profiles_by_emp')
    
    def profiles_by_smoking(self):
        pipline = [{'$match': {'personal.smoking': {'$exists': True, '$ne':0}}},
           {'$group': {'_id': '$personal.smoking',
                       'count': {'$sum': 1}}},
            {'$project': {'type': '$_id', 'count': 1}},
            {"$addFields":
             {'type_text':
               {'$switch':
                 {'branches':
                  [ {'case': {'$eq': ['$type', 1]}, 'then': 'Резко негативное'},
                    {'case': {'$eq': ['$type', 2]}, 'then': 'Негативное'},
                    {'case': {'$eq': ['$type', 3]}, 'then': 'Компромиссное'},
                    {'case': {'$eq': ['$type', 4]}, 'then': 'Нейтральное'},
                    {'case': {'$eq': ['$type', 5]}, 'then': 'Положительное'}
                     ],
                    'default': 'ОШИБКА'
                  }}}},
            {'$sort': {'_id': 1}}]

        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('_id', ascending=True)
        self.bar_categories(df, {'x':'type_text', 'y': 'count', 'kind': 'bar',}, 'profiles_by_smoking')
    
    def profiles_by_drinking(self):
        pipline = [{'$match': {'personal.alcohol': {'$exists': True, '$ne':0}}},
           {'$group': {'_id': '$personal.alcohol',
                       'count': {'$sum': 1}}},
            {'$project': {'type': '$_id', 'count': 1}},
            {"$addFields":
             {'type_text':
               {'$switch':
                 {'branches':
                  [ {'case': {'$eq': ['$type', 1]}, 'then': 'Резко негативное'},
                    {'case': {'$eq': ['$type', 2]}, 'then': 'Негативное'},
                    {'case': {'$eq': ['$type', 3]}, 'then': 'Компромиссное'},
                    {'case': {'$eq': ['$type', 4]}, 'then': 'Нейтральное'},
                    {'case': {'$eq': ['$type', 5]}, 'then': 'Положительное'}
                     ],
                    'default': 'ОШИБКА'
                  }}}},
            {'$sort': {'_id': 1}}]

        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('_id', ascending=True)
        self.bar_categories(df, {'x':'type_text', 'y': 'count', 'kind': 'bar',}, 'profiles_by_drinking')

    def profiles_by_religion(self):
        pipline =[{'$match': {'personal.religion': {'$exists': True, '$ne':0}}},
            {'$group': {'_id': '$personal.religion',
                        'count': {'$sum': 1}}},
            {'$sort': {'count':1}}
            ]

        mc = self.coll_profiles.aggregate(pipline)
        df = pd.DataFrame(list(mc)).sort_values('count', ascending=False)
        df10 = df.copy()[:13]
        df20 = df.copy()[14:]
        self.bar_categories(df10, {'x':'_id', 'y': 'count', 'kind': 'bar',}, 'profiles_by_religion')

    def img_gen(self):
        self.posts_by_mounths()
        self.posts_by_daytime()
        self.posts_avg_likes()
        self.posts_avg_views()
        self.posts_avg_weighed_views()
        self.post_avg_char_length()
        self.profiles_by_country()
        self.profiles_by_city()
        self.profiles_by_age()
        self.comments_by_time()
        self.profiles_by_emp()
        self.profiles_by_smoking()
        self.profiles_by_drinking()
        self.profiles_by_religion()

        return self.img_name

if __name__ == "__main__":
    # Для отладки
    # python -m main.py_visualization
    test = Vision(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)
    a = test.collection_stats()
    files_name = test.img_gen()
    print(a)
    print(files_name)