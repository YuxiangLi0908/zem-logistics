from django.urls import path
from django.conf import settings
from django.conf.urls.static import static

from warehouse.views.user_login import *
from warehouse.views.order_creation import OrderCreation
from warehouse.views.retrieval_schedule import ScheduleRetrieval
from warehouse.views.palletization import Palletization
from warehouse.views.shipment_schedule import ScheduleShipment
from warehouse.views.shipment_dispatch import ShipmentDispatch
from warehouse.views.export_file import ExportFile
from warehouse.views.customer_management import CustomerManagement
from warehouse.views.utils import *

urlpatterns = [
    path("", home, name="home"),
    path("login/", user_login, name="login"),
    path("logout/", user_logout, name="logout"),
    # path("home/", views.home, name="home"),
    path('create_order/', OrderCreation.as_view(), name='create_order'),
    path('container_pickup/', ScheduleRetrieval.as_view(), name='schedule_pickup'),
    path('palletize/', Palletization.as_view(), name='palletization'),
    path('palletize/<str:pk>/', Palletization.as_view(), name='palletize'),
    path('schedule_shipment/', ScheduleShipment.as_view(), name='schedule_shipment'),
    # path('schedule_shipment/<str:warehouse>/<str:destination>/', views.ScheduleShipment.as_view(), name='schedule_shipment'),
    path('outbound/', ShipmentDispatch.as_view(), name='outbound'),
    path('generate_pdf/', ExportFile.as_view(), name='generate_pdf'),
    path('new_customer/', CustomerManagement.as_view(), name='new_customer'),
    path('order_list/', OrderManagement.as_view(), name='order_list'),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)