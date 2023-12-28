from django.urls import path
from . import views


urlpatterns = [
    path("login/", views.user_login, name="login"),
    path("logout/", views.user_logout, name="logout"),
    path("home/", views.home, name="home"),
    path('create_order/', views.create_order, name='create_order'),
    path('container_pickup/', views.schedule_pickup, name='schedule_pickup'),
    path('palletize/', views.palletize, name='palletization'),
    path('palletize/<str:pk>/', views.packling_list, name='palletize'),
]