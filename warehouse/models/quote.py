from django.db import models

from warehouse.models.customer import Customer
from warehouse.models.warehouse import ZemWarehouse

class Quote(models.Model):
    quote_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.CASCADE)
    warehouse = models.ForeignKey(ZemWarehouse, null=True, on_delete=models.CASCADE)
    platform = models.CharField(max_length=255, null=True, blank=True)
    load_type = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateField(null=True)
    pickup_date = models.DateField(null=True)
    zipcode = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=500, null=True, blank=True)
    note = models.CharField(max_length=500, null=True, blank=True)
    length = models.FloatField(null=True, blank=True)
    width = models.FloatField(null=True, blank=True)
    height = models.FloatField(null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    n_pallet = models.IntegerField(null=True, blank=True)
    pcs = models.IntegerField(null=True, blank=True)
    is_lift_gate = models.BooleanField(default=False)
    cost = models.FloatField(null=True, blank=True)
    price = models.FloatField(null=True, blank=True)
