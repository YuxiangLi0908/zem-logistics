from django.db import models
from .customer import Customer
from .container import Container
from .warehouse import ZemWarehouse
from .clearance import Clearance
from .retrieval import Retrieval
from .offload import Offload

class Order(models.Model):
    order_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.CASCADE)
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    warehouse = models.ForeignKey(ZemWarehouse, null=True, on_delete=models.CASCADE)
    created_at = models.DateTimeField()
    eta = models.DateField(null=True)
    order_type = models.CharField(max_length=255, null=True)
    clearance_id = models.ForeignKey(Clearance, null=True, on_delete=models.SET_NULL)
    retrieval_id = models.ForeignKey(Retrieval, null=True, on_delete=models.SET_NULL)
    offload_id = models.ForeignKey(Offload, null=True, on_delete=models.SET_NULL)

    def __str__(self) -> str:
        return self.customer_name.zem_name + " - " + self.container_number.container_number