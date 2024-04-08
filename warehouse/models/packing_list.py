from django.db import models
from .container import Container
from .shipment import Shipment

class PackingList(models.Model):
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    product_name = models.CharField(max_length=255, null=True, blank=True)
    delivery_method = models.CharField(max_length=255)
    shipping_mark = models.CharField(max_length=400, null=True, blank=True)
    fba_id = models.CharField(max_length=400, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    zipcode = models.CharField(max_length=20, null=True, blank=True)
    ref_id = models.CharField(max_length=400, null=True, blank=True)
    pcs = models.IntegerField(null=True)
    unit_weight_lbs = models.FloatField(null=True, blank=True)
    total_weight_lbs = models.FloatField(null=True,)
    cbm = models.FloatField(null=True)
    n_pallet = models.IntegerField(null=True, blank=True)
    shipment_batch_number = models.ForeignKey(Shipment, null=True, blank=True, on_delete=models.SET_NULL)
    note = models.CharField(null=True, blank=True, max_length=2000)

    def __str__(self):
        return f"{self.container_number}-{self.destination}-{self.shipping_mark if self.shipping_mark else 'no_mt'}-{self.fba_id if self.fba_id else 'no_fba'}"