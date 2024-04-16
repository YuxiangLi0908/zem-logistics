from django.contrib import admin
from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.pallet import Pallet
from warehouse.models.quote import Quote
# Register your models here.

admin.site.register(Clearance)
admin.site.register(Container)
admin.site.register(Customer)
admin.site.register(Offload)
admin.site.register(Order)
admin.site.register(PackingList)
admin.site.register(Retrieval)
admin.site.register(Shipment)
admin.site.register(ZemWarehouse)
admin.site.register(Pallet)
admin.site.register(Quote)