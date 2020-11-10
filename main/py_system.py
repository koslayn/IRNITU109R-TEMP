# -*- coding: utf-8 -*-
import os
import warnings
from main.py_settings import *

# Ключ доступа в отдельном файле
def ask_credentials():
    """
    Функция для получения ключа доступа для работы с VK-API из файла '__credentials.txt'
    return: возвращает ключ доступа str
    """
    # Для задания папки, где лежит модуль рабочей
    os.chdir(os.path.dirname(__file__))
    try:
        with open('./__credentials.txt', 'r') as C:
            VK_TOKEN = C.readline()
        return(VK_TOKEN)
    except:
        warnings.warn(("\nОтсутствует файл '__credentials.txt' c VK token."
                       "\nТокен можно указать в переменной «VK_TOKEN» для отладки и тестирования."
                       "\nТакже можно работать и без ключа, в этом случае будет происходить работа с предзагруженными данными"
                     ))
        return None


# Функции для проверки параметров функций
def param_cheker_owner_id(OWNER_ID=None):
    if OWNER_ID is None:
        warnings.warn('Не указано сообщество в VK в параметре <OWNER_ID>, данные не будут обновлены')
        return False
    else:
        return True

def param_cheker_vk_token(VK_TOKEN=None):
    if VK_TOKEN is None and __ask_credentials() is None:
        warnings.warn('Отсутствует ключ, для обновления данных в параметре <VK_TOKEN> и в файле <__credentials.txt>, данные не будут обновлены')
        return False
    else:
        return True

def param_cheker_post_id(post_id=None):
    if post_id is None:
        warnings.warn('Нет номера поста для запроса комментариев')
        return False
    else:
        return True

if __name__ == "__main__":
    ask_credentials()
    param_cheker_owner_id()
    param_cheker_vk_token()
    param_cheker_post_id()

