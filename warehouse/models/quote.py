from django.db import models

from warehouse.models.customer import Customer
from warehouse.models.warehouse import ZemWarehouse


class Quote(models.Model):
    parent_id = models.CharField(max_length=255, null=True)
    quote_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.CASCADE)
    warehouse = models.ForeignKey(ZemWarehouse, null=True, on_delete=models.CASCADE)
    platform = models.CharField(max_length=255, null=True, blank=True)
    load_type = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateField(null=True, blank=True)
    pickup_date = models.DateField(null=True, blank=True)
    zipcode = models.CharField(max_length=20, null=True, blank=True)
    address = models.CharField(max_length=500, null=True, blank=True)
    note = models.CharField(max_length=1000, null=True, blank=True)
    is_lift_gate = models.CharField(max_length=10, null=True, blank=True)
    is_oversize = models.CharField(max_length=10, null=True, blank=True)
    cost = models.FloatField(null=True, blank=True)
    price = models.FloatField(null=True, blank=True)
    comment = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.customer_name}-{self.warehouse}-{self.zipcode}-{self.quote_id}"
