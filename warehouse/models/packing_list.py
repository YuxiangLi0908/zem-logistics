from django.db import models
from .container import Container
from .shipment import Shipment

class PackingList(models.Model):
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    product_name = models.CharField(max_length=255, null=True)
    delivery_method = models.CharField(max_length=255)
    shipping_mark = models.CharField(max_length=255, null=True)
    fba_id = models.CharField(max_length=255, null=True)
    destination = models.CharField(max_length=255)
    address = models.CharField(max_length=255, null=True)
    zipcode = models.CharField(max_length=20, null=True)
    ref_id = models.CharField(max_length=255, null=True)
    pcs = models.IntegerField(null=True)
    unit_weight_lbs = models.FloatField(null=True)
    total_weight_lbs = models.FloatField(null=True)
    cbm = models.FloatField(null=True)
    n_pallet = models.IntegerField(null=True)
    shipment_batch_number = models.ForeignKey(Shipment, null=True, on_delete=models.SET_NULL)

    def __str__(self):
        return f"{self.container_number} - {self.destination}"