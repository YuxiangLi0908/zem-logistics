from django.urls import path
# from . import views
from warehouse.views.user_login import *
from warehouse.views.order_creation import OrderCreation
from warehouse.views.retrieval_schedule import ScheduleRetrieval
from warehouse.views.palletization import Palletization
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path("", home, name="home"),
    path("login/", user_login, name="login"),
    path("logout/", user_logout, name="logout"),
    # path("home/", views.home, name="home"),
    path('create_order/', OrderCreation.as_view(), name='create_order'),
    path('container_pickup/', ScheduleRetrieval.as_view(), name='schedule_pickup'),
    path('palletize/', Palletization.as_view(), name='palletization'),
    path('palletize/<str:pk>/', Palletization.as_view(), name='palletize'),
    # path('schedule_shipment/', views.ScheduleShipment.as_view(), name='schedule_shipment'),
    # path('schedule_shipment/<str:warehouse>/<str:destination>/', views.ScheduleShipment.as_view(), name='schedule_shipment'),
    # path('outbound/', views.Outbound.as_view(), name='outbound'),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)