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
from warehouse.views.order_management import *
from warehouse.views.bol import BOL
from warehouse.views.po import PO
from warehouse.views.pod import POD
from warehouse.views.stuff_user_ability import StuffPower
from warehouse.views.quote_management import QuoteManagement
from warehouse.views.accounting import Accounting
from warehouse.views.shipment_status import ShipmentStatus

urlpatterns = [
    path("", home, name="home"),
    path("login/", user_login, name="login"),
    path("logout/", user_logout, name="logout"),
    path('create_order/', OrderCreation.as_view(), name='create_order'),
    path('container_pickup/', ScheduleRetrieval.as_view(), name='schedule_pickup'),
    path('palletize/', Palletization.as_view(), name='palletization'),
    path('palletize/<str:pk>/', Palletization.as_view(), name='palletize'),
    path('schedule_shipment/', ScheduleShipment.as_view(), name='schedule_shipment'),
    path('outbound/', ShipmentDispatch.as_view(), name='outbound'),
    path('generate_pdf/', ExportFile.as_view(), name='generate_pdf'),
    path('customer_management/', CustomerManagement.as_view(), name='customer_management'),
    path('customer_management/<str:name>/', CustomerManagement.as_view(), name='customer_management'),
    path('order_list/', OrderManagement.as_view(), name='order_list'),
    path('bol/', BOL.as_view(), name='bol'),
    path('po/', PO.as_view(), name='po'),
    path('pod/', POD.as_view(), name='pod'),
    path('quote/', QuoteManagement.as_view(), name='quote_management'),
    path('stuff_user/', StuffPower.as_view(), name='stuff_user'),
    path('accounting/', Accounting.as_view(), name='accounting'),
    path('shipment_status/', ShipmentStatus.as_view(), name='shipment_status'),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)