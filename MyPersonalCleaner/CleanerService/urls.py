from django.urls import path
from .views import homepage_view,home_view,cleanup,configurations
urlpatterns = [
    path('', home_view, name='home'),
    path('cleanup', cleanup,name='cleanup'),
    path('configurations', configurations, name='configurations'),

]


