from django.db import models
from .container import Container
from .packing_list import PackingList
from .shipment import Shipment


class Pallet(models.Model):
    packing_list = models.ForeignKey(PackingList, null=True, on_delete=models.CASCADE)
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    destination = models.CharField(max_length=255, null=True, blank=True)
    delivery_method = models.CharField(max_length=255, null=True, blank=True)
    pallet_id = models.CharField(max_length=255, null=True, blank=True)
    pcs = models.IntegerField(null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)
    shipment_number = models.ForeignKey(Shipment, null=True, blank=True, on_delete=models.SET_NULL, related_name='pallet')

    def __str__(self):
        return f"{self.container_number}-{self.destination}-{self.delivery_method}"
