# -*- coding: utf-8 -*-
import warnings

# Функции для разбора ответа от API-VK
def parse_json_vk_posts(data):
    """
    Преобразование JSON документа в JSON с меньшим количеством полей
      * data: JSON-пост из VK - response от VK-API
      * return: [{id: 1, date: 2, ...}, {}, ... {}]
    """
    root = data['response']['items']

    all_posts = []

    for p in root:
        post = {
            'id': p['id'],
            'from_id': p['from_id']*(-1),
            'date': p['date'],
            'text': p['text'],
            'views': p['views']['count'],
            'likes': p['likes']['count'],
            'reposts': p['reposts']['count'],
            'comments': p['comments']['count']
        }
        all_posts.append(post)
    return all_posts

def parse_json_vk_comments(data):
    """
    Разбирает комментарии на элементы: комментарий
      * data: response VK-API - комментарии и данные о профайле
      * return: comments = [JSON, ]
    """
    
    all_comments = []
    if data.get('error'):
        # Заплатка (падает если ошибку возвращает)
        print('error')
        print(data)
        pass
    else:
        root_comments = data['response']['items']
        for comm in root_comments:
            if 'deleted' in comm:
                continue
            else:
                comment = {'id': comm.get('id'),
                    'post_id': comm.get('post_id'),
                    'from_id': comm.get('from_id'),
                    'date': comm.get('date'),
                    'text': comm.get('text'),
                    'likes': comm.get('likes').get('count')}
                all_comments.append(comment)
    return all_comments

def parse_json_vk_profiles(data):
    """
    Не используемая функция - использовалась, для получения общих сведений о профайле из комментария
    Разбирает комментарии на элементы: профайл коментатора
      * data: response VK-API - комментарии и данные о профайле
      * return: profiles = [JSON, ]
    """
    warnings.warn('Функция использовалась для получения общей инофрмации о профайле вмемсте с получением комментария, не используется в коде')
    exc_prof = 0
    all_profiles = []

    root_profile = data['response']['profiles']
    for p in root_profile:

        profile = {
            'id': p['id'],
            'first_name': p.get('first_name'),
            'last_name': p.get('last_name'),
            'is_closed': p.get('is_closed'),
            'sex': p.get('sex'),
            'screen_name': p.get('screen_name')
        }
        all_profiles.append(profile)

    return all_profiles

