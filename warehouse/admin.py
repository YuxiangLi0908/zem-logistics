from django.contrib import admin
from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.pallet import Pallet
from warehouse.models.quote import Quote
from warehouse.models.invoice import Invoice, InvoiceItem, InvoiceStatement
from warehouse.models.terminal49_webhook_raw import T49Raw
from warehouse.models.vessel import Vessel
from warehouse.models.fleet import Fleet
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.shipment_status import ShipmentStatus
# Register your models here.

admin.site.register(Clearance)
admin.site.register(Container)
admin.site.register(Customer)
admin.site.register(Offload)
admin.site.register(AbnormalOffloadStatus)
admin.site.register(Order)
admin.site.register(PackingList)
admin.site.register(Retrieval)
admin.site.register(Shipment)
admin.site.register(ZemWarehouse)
admin.site.register(Pallet)
admin.site.register(Quote)
admin.site.register(Invoice)
admin.site.register(InvoiceItem)
admin.site.register(InvoiceStatement)
admin.site.register(T49Raw)
admin.site.register(Vessel)
admin.site.register(Fleet)
admin.site.register(FleetShipmentPallet)
admin.site.register(ShipmentStatus)