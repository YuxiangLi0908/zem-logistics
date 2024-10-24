from django.db import models
from .container import Container
from .packing_list import PackingList
from .shipment import Shipment


class Pallet(models.Model):
    packing_list = models.ForeignKey(PackingList, null=True, blank=True, on_delete=models.CASCADE)
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    shipment_batch_number = models.ForeignKey(Shipment, null=True, blank=True, on_delete=models.SET_NULL, related_name='pallet')
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    zipcode = models.CharField(max_length=20, null=True, blank=True)
    delivery_method = models.CharField(max_length=255, null=True, blank=True)
    pallet_id = models.CharField(max_length=255, null=True, blank=True)
    shipping_mark = models.CharField(max_length=1000, null=True, blank=True)
    fba_id = models.CharField(max_length=1000, null=True, blank=True)
    ref_id = models.CharField(max_length=1000, null=True, blank=True)
    pcs = models.IntegerField(null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)
    note = models.CharField(null=True, blank=True, max_length=2000)

    def __str__(self):
        return f"{self.container_number}-{self.destination}-{self.delivery_method}"
