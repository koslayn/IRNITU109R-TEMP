from django.shortcuts import render
from django.http import HttpResponse 

import main.py_settings as ps
from main.py_data_get import UpLoader
from main.py_data_transform import Transfomer
from main.py_visualization import Vision

# class Worker:
#      def __init__(self):
#         # Настройки по умолчанию
#         self.VK_TOKEN = ps.VK_TOKEN
#         self.OWNER_ID = ps.OWNER_ID
#         self.HOW_MANY_POSTS_TO_GET = ps.HOW_MANY_POSTS_TO_GET


# Create your views here.
def index(request):
    return HttpResponse('First view of application.')

# Стартовая страница
def home(request):
    return render(request, 'home.html')

# Настройка параметров


def start(request):
    if request.method == 'POST':
        if request.POST.get('OWNER_ID') == '':
            job = UpLoader(OWNER_ID=ps.OWNER_ID, HOW_MANY_POSTS_TO_GET=ps.HOW_MANY_POSTS_TO_GET)
            print(job.OWNER_ID)
        else:
            OWNER_ID = int(request.POST.get('OWNER_ID'))
            HOW_MANY_POSTS_TO_GET = int(request.POST.get('HOW_MANY_POSTS_TO_GET'))
            job = UpLoader(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)
            print(job.OWNER_ID)
            print('**********************')
        
        MSG_posts = job.get_posts_data()
        MSG_comments = job.get_comments_data()
        MSG_profiles = job.get_profiles_data()
        print('&&&&&&&&&&&&&&&&&&&', job.OWNER_ID)

        context = {'posts': MSG_posts, 'comments': MSG_comments, 'profiles': MSG_profiles, 'ID': job.OWNER_ID, 'COUNT': job.HOW_MANY_POSTS_TO_GET}

        return render(request, 'upload.html', context)
    return render(request, 'start.html',)

def upload(request):
    MSG_posts = 'Используем предзагруженные данные'
    MSG_comments = 'Используем предзагруженные данные'
    MSG_profiles = 'Используем предзагруженные данные'
    context = {'posts': MSG_posts, 'comments': MSG_comments, 'profiles': MSG_profiles}
    return render(request, 'upload.html', context)

def transform(request):
    # When a URL is like domain/search/?ID=haha&COUNT=100, you would use request.GET.get('q', '').
    OWNER_ID = int(request.GET.get('ID', '-95095088'))
    HOW_MANY_POSTS_TO_GET = int(request.GET.get('COUNT', '100'))

    transform_worker = Transfomer(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)
    MSG_dict = transform_worker.transform_data()

    context = {'posts': MSG_dict['date_posts'],
               'comments': MSG_dict['date_comments'],
               'split_posts': MSG_dict['tags_words_posts'],
               'split_comments': MSG_dict['tags_words_comments'],
               'norm_posts': MSG_dict['norm_posts'],
               'norm_comments': MSG_dict['norm_comments'],
               'ID': OWNER_ID,
               'COUNT': HOW_MANY_POSTS_TO_GET
                }
    
    return render(request, 'transform.html', context)

def visualization(request):
    OWNER_ID = int(request.GET.get('ID', '-95095088'))
    HOW_MANY_POSTS_TO_GET = int(request.GET.get('COUNT', '100'))
    visualization_worker = Vision(OWNER_ID=OWNER_ID, HOW_MANY_POSTS_TO_GET=HOW_MANY_POSTS_TO_GET)

    files_name = visualization_worker.img_gen()
    MSG = visualization_worker.collection_stats()
    print(MSG)
    context = {'posts_by_mounths': files_name[0],
               'posts_by_daytime': files_name[1],
               'posts_avg_likes': files_name[2],
               'posts_avg_views': files_name[3],
               'posts_avg_weighed_views': files_name[4],
               'post_avg_char_length': files_name[5],
               'profiles_by_country': files_name[6],
               'profiles_by_city': files_name[7],
               'profiles_by_age': files_name[8],
               'comments_by_time': files_name[9],
               'profiles_by_emp': files_name[10],
               'profiles_by_smoking': files_name[11], 
               'profiles_by_drinking': files_name[12],
               'profiles_by_religion': files_name[13], 
               'MSG': MSG,
               'ID': OWNER_ID,
               'COUNT': HOW_MANY_POSTS_TO_GET
               }
    
    return render(request, 'visualization.html', context)
    

if __name__ == "__main__":
    # python -m main.views
    print(ps.VK_TOKEN)
    print(ps.HOW_MANY_POSTS_TO_GET)
    print(ps.OWNER_ID)

