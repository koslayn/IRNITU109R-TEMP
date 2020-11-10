# Версия-0.2
# Базовый образ https://hub.docker.com/_/python/
# `docker build -t vk-mongo-parser-image:v0.1 .`
FROM python:3

# Доустанвока
RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "nano"]

# Определяем рабочую директорию (доступна юзеру)
WORKDIR /usr/src/app
# Копируем список для виртуального окружения
COPY requirements.txt ./
# Устанавливаем нужные библиотеки
RUN pip install --no-cache-dir -r requirements.txt
# Копируем разработанный проект
COPY . .
# Запуск проекта 'python manage.py runserver' - скрипт в рабочей директории создаётся
EXPOSE 8000
#CMD ["python", "manage.py", "runserver"]
CMD python manage.py runserver 0.0.0.0:8000