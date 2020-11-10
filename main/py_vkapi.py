# -*- coding: utf-8 -*-
import requests
import time
# Системыне (проверки)
from main.py_system import *

# Функции для работы с VK-API
def get_posts(count=1, offset=0, OWNER_ID=-66669811, VK_TOKEN=None):
    """
    Получаем посты из сообщества VK:
        * count: количество запрашиваемых постов (не более 100 за раз),
        * offset:смещение относительно последнего доступного поста,
        * OWNER_ID - id сообщества,
        * VK_TOKEN - ключ от приложения в ВК
        * return: JSON
    """
    if param_cheker_owner_id(OWNER_ID) and param_cheker_vk_token(VK_TOKEN):

        if VK_TOKEN is None:
            VK_TOKEN = ask_credentials()

        url_post = f'https://api.vk.com/method/wall.get?access_token={VK_TOKEN}'
        response = requests.get(url_post, params={'owner_id':OWNER_ID,'count':count, 'offset':offset, 'v':5.92})
        response = response.json()

        return response

def get_comments(post_id=50679, OWNER_ID=-66669811, VK_TOKEN=None):
    """
    Получаем комментарии к посту по его ID в сообществе OWNER_ID
      * post_id: номер поста
      * return: JSON
    """
    if param_cheker_owner_id(OWNER_ID) and param_cheker_vk_token(VK_TOKEN) and param_cheker_post_id(post_id):
        if VK_TOKEN is None:
            VK_TOKEN = ask_credentials()
    
        url_post = f'https://api.vk.com/method/wall.getComments?access_token={str(VK_TOKEN)}'
        response = requests.get(url_post, params={'post_id':post_id, 'owner_id': OWNER_ID,'count':100, 'offset':0, 'extended':1, 'need_likes':1, 'v':5.92})
        response = response.json()
        
        # Заплатка, есть ограничение на запросы нужно смотреть документацию к API и запрашивать по другому скорее всего
        time.sleep(0.4)

        return response

def get_profiles(ids, VK_TOKEN=None):
    """
    Получаем расширенную информацию с профилей VK
      * ids: список id профилей, не более 100 за раз
      * return: сообщение об успешном выполнении
    """
    if param_cheker_vk_token(VK_TOKEN):
        if VK_TOKEN is None:
            VK_TOKEN = ask_credentials()

    # Превращаем лист в строку для запроса в ВК (до 100 профайлов)
    ids_str = ', '.join([str(i) for i in ids])
    url_post = f'https://api.vk.com/method/users.get?access_token={str(VK_TOKEN)}'
    response = requests.get(url_post, params={'user_ids':ids_str, 'fields': 'about, bdate, country, city, education, interests, occupation, relation, movies, personal, relation', 'v':5.92})
    response = response.json()

    profiles_to_collection = []
    for id in response['response']:
        # Списковая сборка словаря
        my_dict = {i: j for (i, j) in id.items()}
        profiles_to_collection.append(my_dict)
    
    return profiles_to_collection