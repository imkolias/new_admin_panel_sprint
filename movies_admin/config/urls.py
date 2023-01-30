from django.contrib import admin
from django.urls import path, include
from dotenv import load_dotenv
import os

load_dotenv()


urlpatterns = [
    path('admin/', admin.site.urls),

]

DEBUG_MODE = os.environ.get('DEBUG_MODE')
if DEBUG_MODE == 'True':
    urlpatterns += [path('__debug__/', include('debug_toolbar.urls')), ]
