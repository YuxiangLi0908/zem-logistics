from django.urls import path
# from . import views
from warehouse.views.user_login import *
from warehouse.views.order_creation import OrderCreation
from warehouse.views.retrieval_schedule import ScheduleRetrieval
from warehouse.views.palletization import Palletization

urlpatterns = [
    path("", home, name="home"),
    path("login/", user_login, name="login"),
    path("logout/", user_logout, name="logout"),
    # path("home/", views.home, name="home"),
    path('create_order/', OrderCreation.as_view(), name='create_order'),
    path('container_pickup/', ScheduleRetrieval.as_view(), name='schedule_pickup'),
    path('palletize/', Palletization.as_view(), name='palletization'),
    # path('palletize/<str:pk>/', views.packling_list, name='palletize'),
    # path('schedule_shipment/', views.ScheduleShipment.as_view(), name='schedule_shipment'),
    # path('schedule_shipment/<str:warehouse>/<str:destination>/', views.ScheduleShipment.as_view(), name='schedule_shipment'),
    # path('outbound/', views.Outbound.as_view(), name='outbound'),
]