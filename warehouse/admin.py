from django.contrib import admin
from simple_history.admin import SimpleHistoryAdmin

from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.fleet import Fleet
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice import Invoice, InvoiceItem, InvoiceStatement
from warehouse.models.invoice_details import (
    InvoiceDelivery,
    InvoicePreport,
    InvoiceWarehouse,
)
from warehouse.models.offload import Offload
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.quote import Quote
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.shipment_status import ShipmentStatus
from warehouse.models.terminal49_webhook_raw import T49Raw
from warehouse.models.transfer_location import TransferLocation
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse

# Register your models here.
admin.site.register(Clearance)
admin.site.register(Container, SimpleHistoryAdmin)
admin.site.register(Customer, SimpleHistoryAdmin)
admin.site.register(Offload, SimpleHistoryAdmin)
admin.site.register(AbnormalOffloadStatus)
admin.site.register(Order, SimpleHistoryAdmin)
admin.site.register(PackingList, SimpleHistoryAdmin)
admin.site.register(Retrieval, SimpleHistoryAdmin)
admin.site.register(Shipment, SimpleHistoryAdmin)
admin.site.register(ZemWarehouse)
admin.site.register(Pallet, SimpleHistoryAdmin)
admin.site.register(Quote)
admin.site.register(Invoice)
admin.site.register(InvoiceItem)
admin.site.register(InvoiceStatement)
admin.site.register(T49Raw)
admin.site.register(Vessel)
admin.site.register(Fleet, SimpleHistoryAdmin)
admin.site.register(FleetShipmentPallet)
admin.site.register(ShipmentStatus)
admin.site.register(PoCheckEtaSeven)
admin.site.register(InvoicePreport)
admin.site.register(InvoiceWarehouse)
admin.site.register(InvoiceDelivery)
admin.site.register(TransferLocation)
