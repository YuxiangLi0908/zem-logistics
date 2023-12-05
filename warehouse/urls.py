from django.urls import path
from . import views


urlpatterns = [
    path("login/", views.user_login, name="login"),
    path("logout/", views.user_logout, name="logout"),
    path("home/", views.home, name="home"),
    path('create_order/', views.create_order, name='create_order_step_1'),
]