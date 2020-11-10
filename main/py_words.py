# -*- coding: utf-8 -*-
import re
import pymorphy2
from pymongo import UpdateOne

from main.py_categories import *

# Преобразование текса в документах MongoDB
def split_post(text):
    """
    Split text to words and separate tags if they exists
      * input: text: str
      * return: (tags, words)
    """
    # Регулярка - выделение тегов
    re_tag = r'#\w{3,}\b'
    # Регулярка - разделение на слова (предлоги и др. отсекать будем при помощи pymorphy2).
    re_words = r'[а-яА-ЯёЁa-zA-Z]{1,}'
    # Выделяем таги из текста
    matched_tags = re.findall(re_tag, text)
    # Чистим текст от tag через замену
    if matched_tags:
        for tag in matched_tags:
            # f-string + raw-string
            text = re.sub(fr'{tag}', '', text)
    
    matched_words = re.findall(re_words, text)

    return matched_tags, matched_words

def get_and_prepare_post(collection):
    """
    Преобразование тексата поста и(или) комментария
      * input: коллекция документов, которую нужно обработать
      * return: list [('_id', 'tags', 'words'), ...] - поля для добавления к документу по его _id 
    """
    texts = [(d['_id'], d['text']) for d in collection.find({'$or': [{'words': None},{'tags': None}]})]
    new_fields = []

    for _id, text in texts:
        tags, words = split_post(text)

        new_fields.append((_id, tags, words))
    
    return new_fields

def add_tags_words_to_documents(collection):
    """
    Функция добавления полей в документы (слова и таги (если есть))
      * input: коллекция для обработки
      * log: пишет сообщения в консоль о результатах изменения
      * return: None
    """
    operation_tags = []
    operation_words = []

    data_for_coll = get_and_prepare_post(collection)

    if data_for_coll:
        for _id, tags, words in data_for_coll:
            operation_tags.append(UpdateOne({'_id':_id}, {"$set": {'tags': tags}}))
            operation_words.append(UpdateOne({'_id':_id}, {"$set": {'words': words}}))

        if operation_tags:
            result_tags = collection.bulk_write(operation_tags)
            MSG_tags = f'---------------TAG---------------\nВнесены изменения в {len(operation_tags)} документ из коллекции {collection.name} в базе данных {collection.database.name}.\n'
        else:
            MSG_tags = f'Нет изменений в TAG в коллекции {collection.name} в базе данных {collection.database.name}.\n'
        
        if operation_words:
            result_words = collection.bulk_write(operation_words)
            MSG_words = f'------------WORDS-----------------\nВнесены изменения в {len(operation_words)} документ из коллекции {collection.name} в базе данных {collection.database.name}.\n'
        else:
            MSG_words = f'Нет изменений в TAG в коллекции {collection.name} в базе данных {collection.database.name}.\n'
        
        MSG = MSG_tags + MSG_words
        return MSG
    else:
        return f'Все данные в коллекции {collection.name}, базы данных {collection.database.name} уже подготовлены.\n'


# Преобразование слов в документах MongoDB (нормализация и выборка отдельных частей речи)
def text_norm(collection):
    """
    Функция преобразования слов: приводит слова к нормальной форме, разбивает слова на три группы: существительные, глаголы, прилогательные
      * input: коллекция для обработки
      * log: пишет в консоль "Всё успешно", статистику не считает и не выводит
      * return: None
    """
    operation = []

    morph = pymorphy2.MorphAnalyzer()

    _id_words = [(d['_id'], d['words']) for d in collection.find({'$or': [{'norm_NOUN': None}, {'norm_VERB': None}, {'norm_ADJF': None}]})] 

    for _id, words in _id_words:
        norm_NOUN = []
        norm_VERB = []
        norm_ADJF = []
        
        for word in words:
            p = morph.parse(word.lower())[0]

            if 'NOUN' in p.tag:
                
                normal_form = p.normal_form
                norm_NOUN.append(normal_form)
                continue

            if 'VERB' in p.tag:
                normal_form = p.normal_form
                norm_VERB.append(normal_form)

            elif 'INFN' in p.tag:
                normal_form = p.normal_form
                norm_VERB.append(normal_form)
                continue

            if 'ADJF' in p.tag:
                normal_form = p.normal_form
                norm_ADJF.append(normal_form)

            elif 'ADJS' in p.tag:
                normal_form = p.normal_form
                norm_ADJF.append(normal_form)
                continue
        
        operation.append(UpdateOne({'_id': _id}, {"$set": {'norm_NOUN': norm_NOUN, 'norm_VERB': norm_VERB, 'norm_ADJF': norm_ADJF}}))
    
    if operation:
        result = collection.bulk_write(operation)
        MSG = f'Обработано {len(_id_words)} документов.\n'
        return MSG
    else:
        MSG = f'Все документы в коллекции {collection.name}, базы данных {collection.database.name} разбиты на отдельные нормализованные слова, никакие изменения не произвели.\n'
        return MSG
    return '**************'

# ❗ В разработке
# Категорирование постов и комментариев

def category_adder(mongo_cursor):
    """
    Define category of the post
      * input: mongo_cursor
      * return (_id, {Програмирование, Кейс, ...})
    """
    category = categories_words_collection_dic.keys()
    category_tags = set()
    for tag in mongo_cursor['tags']:
        for name in category:
            if tag in categories_words_collection_dic[name]:
                category_tags.add(name)

    for word in mongo_cursor['norm_NOUN']:
        for name in category:
            if word in categories_words_collection_dic[name]:
                category_tags.add(name)              

    return mongo_cursor['_id'], list(category_tags)

def category_comment_adder(mongo_cursor):
    """
    TODO - дописать доки
    """
    type_of_comment = set()
    for word in mongo_cursor['norm_ADJF']:
        if word in pos_neg_comment_dic['Позитивный']:
            type_of_comment.add('Позитивный')
        
        if word in pos_neg_comment_dic['Негативный']:
            type_of_comment.add('Негативный')

    if type_of_comment:
        return mongo_cursor['_id'], list(type_of_comment)
    
    return False
