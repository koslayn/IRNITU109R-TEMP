from django.urls import path 
from . import views 

urlpatterns = [
    path('', views.home, name='home'),
    path('start/', views.start, name='start'),
    path('upload/', views.upload, name='upload'),
    path('transform/', views.transform, name='transform'),
    path('visualization/', views.visualization, name='visualization'),
    path('index/', views.index, name='index'),
    ]