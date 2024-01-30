from django.contrib import admin
from .models.clearance import Clearance
from .models.container import Container
from .models.customer import Customer
from .models.offload import Offload
from .models.order import Order
from .models.packing_list import PackingList
from .models.retrieval import Retrieval
from .models.shipment import Shipment
from .models.warehouse import ZemWarehouse
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