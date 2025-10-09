from django.conf import settings
from django.conf.urls.static import static
from django.urls import path

from warehouse.views.accounting import Accounting
from warehouse.views.bol import BOL
from warehouse.views.container_tracking import ContainerTracking
from warehouse.views.customer_management import CustomerManagement
from warehouse.views.data_query.db_query import DBConn
from warehouse.views.export_file import ExportFile
from warehouse.views.heartbeat import get_heartbeat
from warehouse.views.order_creation import OrderCreationLegacy
from warehouse.views.palletization import Palletization as LegacyPalletization
from warehouse.views.po import PO
from warehouse.views.pod import POD
from warehouse.views.home import Home
from warehouse.views.post_port.post_nsop import PostNsop
from warehouse.views.post_port.posport_dash import PostportDash
from warehouse.views.post_port.shipment.fleet_management import FleetManagement
from warehouse.views.post_port.shipment.shipping_management import ShippingManagement
from warehouse.views.post_port.timeout_warning import TimeoutWarning
from warehouse.views.post_port.warehouse.inventory import Inventory
from warehouse.views.post_port.warehouse.palletization import Palletization
from warehouse.views.post_port.warehouse.transfer_pallet import TransferPallet
from warehouse.views.post_port.warehouse.warehouse_operations import WarehouseOperations
from warehouse.views.pre_port.order_creation import OrderCreation
from warehouse.views.pre_port.pickup_containers_status import ContainerPickupStatus
from warehouse.views.pre_port.pre_port_dash import PrePortDash
from warehouse.views.pre_port.terminal_dispatch import TerminalDispatch
from warehouse.views.pre_port.tracking import PrePortTracking
from warehouse.views.quote_management import QuoteManagement
from warehouse.views.retrieval_schedule import ScheduleRetrieval
from warehouse.views.shipment_dispatch import ShipmentDispatch
from warehouse.views.shipment_schedule import ScheduleShipment
from warehouse.views.shipment_status import ShipmentStatus
from warehouse.views.statistics.order_quantity import OrderQuantity
from warehouse.views.stuff_user_ability import StuffPower
from warehouse.views.terminal49_webhook import T49Webhook
from warehouse.views.test.async_view import AsyncView
from warehouse.views.user_login import *

urlpatterns = [
    path("", Home.as_view(), name="home"),
    path("login/", user_login, name="login"),
    path("logout/", user_logout, name="logout"),
    path("health/", get_heartbeat, name="health_check"),
    path(
        "create_order_legacy/",
        OrderCreationLegacy.as_view(),
        name="create_order_legacy",
    ),
    path("create_order/", OrderCreation.as_view(), name="create_order"),
    path("pre_port_tracking/", PrePortTracking.as_view(), name="pre_port_tracking"),
    path("terminal_dispatch/", TerminalDispatch.as_view(), name="terminal_dispatch"),
    path(
        "contaier_pickup_status/",
        ContainerPickupStatus.as_view(),
        name="contaier_pickup_status",
    ),
    path(
        "contaier_pre_port_summary_dash/",
        PrePortDash.as_view(),
        name="contaier_pre_port_summary_dash",
    ),
    path("container_pickup/", ScheduleRetrieval.as_view(), name="schedule_pickup"),
    path("palletize/", Palletization.as_view(), name="palletization"),
    path("warehouse_operations/", WarehouseOperations.as_view(), name="warehouse_operations"),
    path("palletize/<str:pk>/", Palletization.as_view(), name="palletize_container"),
    path("inventory/", Inventory.as_view(), name="inventory"),
    path("schedule_shipment/", ShippingManagement.as_view(), name="schedule_shipment"),
    path(
        "shipment_management/", ShippingManagement.as_view(), name="shipment_management"
    ),
    path("fleet_management/", FleetManagement.as_view(), name="fleet_management"),
    path("outbound/", FleetManagement.as_view(), name="outbound"),
    path("pod/", FleetManagement.as_view(), name="pod"),
    path("post_port_summary/", PostportDash.as_view(), name="post_port_summary"),
    path("timeout_warning/", TimeoutWarning.as_view(), name="timeout_warning"),
    path("generate_pdf/", ExportFile.as_view(), name="generate_pdf"),
    path(
        "customer_management/", CustomerManagement.as_view(), name="customer_management"
    ),
    path(
        "customer_management/<str:name>/",
        CustomerManagement.as_view(),
        name="customer_management",
    ),
    path("bol/", BOL.as_view(), name="bol"),
    path("po/", PO.as_view(), name="po"),
    path("post_nsop/", PostNsop.as_view(), name="post_nsop"),
    path("quote/", QuoteManagement.as_view(), name="quote_management"),
    path("stuff_user/", StuffPower.as_view(), name="stuff_user"),
    path("accounting/", Accounting.as_view(), name="accounting"),
    path("shipment_status/", ShipmentStatus.as_view(), name="shipment_status"),
    path("container_tracking/", ContainerTracking.as_view(), name="container_tracking"),
    path("t49webhook/", T49Webhook.as_view(), name="t49webhook"),
    path("async_view", AsyncView.as_view(), name="async_view"),
    path("dbconn", DBConn.as_view(), name="dbconn"),
    path("order_statistics", OrderQuantity.as_view(), name="order_statistics"),
    # legacy views, to be removed in the future
    path(
        "palletize_legacy/", LegacyPalletization.as_view(), name="palletization_legacy"
    ),
    path(
        "palletization_abnormal/",
        LegacyPalletization.as_view(),
        name="palletization_abnormal",
    ),
    path(
        "palletize_legacy/<str:pk>/",
        LegacyPalletization.as_view(),
        name="palletize_legacy",
    ),
    path(
        "schedule_shipment_legacy/",
        ScheduleShipment.as_view(),
        name="schedule_shipment_legacy",
    ),
    path("outbound_legacy/", ShipmentDispatch.as_view(), name="outbound_legacy"),
    path("pod_legacy/", POD.as_view(), name="pod_legacy"),
    path("inventory/", Inventory.as_view(), name="inventory"),
    path("transferpallet/", TransferPallet.as_view(), name="transferpallet"),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
