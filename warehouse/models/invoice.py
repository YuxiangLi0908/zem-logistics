from django.db import models
from warehouse.models.customer import Customer
from warehouse.models.container import Container

class Invoice(models.Model):
    invoice_number = models.CharField(max_length=200, null=True, blank=True)
    invoice_date = models.DateTimeField(null=True, blank=True)
    invoice_link = models.CharField(max_length=2000, null=True, blank=True)
    customer = models.ForeignKey(Customer, null=True, blank=True, on_delete=models.SET_NULL)
    container_number = models.ForeignKey(Container, null=True, blank=True, on_delete=models.SET_NULL)
    total_amount = models.FloatField(null=True, blank=True)

    def __str__(self) -> str:
        return self.customer.zem_name + " - " + self.container_number.container_number + " - " + self.invoice_number