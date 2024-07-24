from django.db import models
from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.offload import Offload
from warehouse.models.shipment import Shipment
from warehouse.models.invoice import Invoice


class Order(models.Model):
    order_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.SET_NULL)
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE, related_name="order")
    warehouse = models.ForeignKey(ZemWarehouse, null=True, on_delete=models.SET_NULL, related_name="order")
    created_at = models.DateTimeField()
    eta = models.DateField(null=True, blank=True)
    order_type = models.CharField(max_length=255, null=True)
    clearance_id = models.ForeignKey(Clearance, null=True, blank=True, on_delete=models.SET_NULL)
    retrieval_id = models.ForeignKey(Retrieval, null=True, blank=True, on_delete=models.SET_NULL)
    offload_id = models.ForeignKey(Offload, null=True, blank=True, on_delete=models.SET_NULL)
    shipment_id = models.ForeignKey(Shipment, null=True, blank=True, on_delete=models.SET_NULL, related_name="order")
    do_sent = models.BooleanField(default=False, blank=True)
    invoice_id = models.ForeignKey(Invoice, null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        indexes = [
            models.Index(fields=['order_id']),
            models.Index(fields=['eta']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self) -> str:
        return self.customer_name.zem_name + " - " + self.container_number.container_number