# -*- coding: utf-8 -*-

# Функции для записи документов в MongoDB
def duplicate_cleaner(data, collection):
    """
    Функция определения наличия документа в коллекции по id и очистки постов для добавления
      * input: list - со словарями
      * return: [{'id': 123, 'id': 111, ...}, {}, ...]
    """
    # Какие посты в базе данных
    
    mongo_cursor = collection.find()
    id_in_collection = [i['id'] for i in mongo_cursor]
    # Какие новые посты выгрузили - id
    id_for_collection = [data_id['id'] for data_id in data]
    # ID постов/Комментариев, которые ещё не в БД
    id_to_add = set(id_for_collection) - set(id_in_collection)
    # Формируем список новых постов
    cleaned_data = [data_id for data_id in data if data_id['id'] in id_to_add]

    if cleaned_data:
        print(f'Новых документов для добавления: {len(cleaned_data)}')
        return cleaned_data
    else:
        print('Новых документов для добавления нет')
        return False

def write_posts_to_collection(posts, collection):
    """
    Функция записи новых постов в коллекцию
      * input: посты [{'id': 123, 'id': 111, ...}, {}, ...], Коллекция для записи
      * return: True - для продолжении записи, False для прерываения цикла в т.ч. предупреждение
    """
    posts_cleaned = duplicate_cleaner(data=posts, collection=collection)

    if posts_cleaned is False:
        posts_total = collection.count_documents(filter={})
        MSG = f'Новые посты для записи в коллекцию отсутствуют.\nСейчас «Постов» в коллекции: {posts_total}'
        # print(MSG)
        return (False, MSG)

    if posts_cleaned:
        posts_ids = collection.insert_many(posts_cleaned)
        posts_total = collection.count_documents(filter={})
        MSG = f'Записано постов: {len(posts_ids.inserted_ids)}.\nСейчас «Постов» в коллекции: {posts_total}'
        return (True, MSG)

def write_comments_to_collection(comments, collection):
    """
    Функция для записи комментариев в коллекцию
      * input:
      * return:
    """
    comments_cleaned = duplicate_cleaner(data=comments, collection=collection)

    if comments_cleaned is False:
        comments_total = collection.count_documents(filter={})
        MSG = f'Новые комментарии для записи в коллекцию отсутствуют.\nСейчас «Комментариев» в коллекции: {comments_total}'
        return (False, MSG)

    if comments_cleaned:
        comments_ids = collection.insert_many(comments_cleaned)
        comments_total = collection.count_documents(filter={})
        MSG = f'Записано комментариев: {len(comments_ids.inserted_ids)}.\nСейчас «Комментариев» в коллекции: {comments_total}'
        return (True, MSG)

def write_profiles_to_collection(profiles, collection):
    """
    Функция для записи профайлов в коллекцию, не пишет дубликаты при повторных запросах на добавление
      * input: список профайлов [{}, {}, ...]
      * return: None делает операции с БД
    """
    def cleaner(prof):
        if prof['id'] in in_collection:
            return None
        else:
            return prof

    ids_response = [i['id'] for i in profiles]

    profiles_in_coll = collection.find({}, {'_id': False, 'id': True})
    profiles_in_coll = [i['id'] for i in profiles_in_coll]

    in_collection = []
    for i in ids_response:
        if i in profiles_in_coll:
            in_collection.append(i)
    
    MSG = f'Профайлов получено для обработки: {len(ids_response)}.\nКоличество дубликатов профайлов: {len(in_collection)}\n'

    profiles_cleaned = map(cleaner, profiles)
    profiles_cleaned = [x for x in profiles_cleaned if x is not None]
    
    if profiles_cleaned:
        profiles_ids = collection.insert_many(profiles)
        profiles_total = collection.count_documents(filter={})
        MSG = MSG + f'Записано профайлов: {len(profiles_ids.inserted_ids)}.\n Сейчас «Профайлов» в коллекции: {profiles_total}'
        return (None, MSG)
    else:
        MSG = MSG + 'Новых профайлов нет, ничего не записываем'
        return (None, MSG)