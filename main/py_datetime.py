# -*- coding: utf-8 -*-
from datetime import datetime
from pymongo import UpdateOne

# Работа со временем в документах MongoDB
def unixtime_to_datetime(collection):
    """
    Преобразование unixtime в datetime
      * input: MongoDB collection with data fields
      * return: [(_id, datetatetime), (...)]
    """
    # Получаем все документы в которых нет поля со временем
    unix_time_field = [(d['_id'], d['date']) for d in collection.find({'datetime': None})]
    new_field = []
    for _id, utime in unix_time_field:
        new_field.append((_id, datetime.fromtimestamp(utime)))
    return new_field

def add_datetime_to_documents(collection):
    """
    Function add datetime object to mongoDB's documents
      * input: MongoDB collection
      * return: None
      * log: console - result of modification
    """
    operations = []
    
    for _id, date in unixtime_to_datetime(collection):
        operations.append(UpdateOne({'_id':_id}, {"$set": {'datetime': date}}))
    if operations:
        result = collection.bulk_write(operations)
        MSG = f'Внесены изменения в {len(operations)} документ из коллекции {collection.name} в базе данных {collection.database.name}. Преобразовано время\n'
        return MSG
    else:
        MSG = f'Нет данных для изменений. Всё время в коллекци {collection.name} в базе данных {collection.database.name} представлено datetime.\n'
        return MSG