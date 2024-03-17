from django.db import models
from .container import Container
from .packing_list import PackingList

class Pallet(models.Model):
    packing_list = models.ForeignKey(PackingList, null=True, on_delete=models.CASCADE)
    pallet_id = models.CharField(max_length=255, null=True, blank=True)
    pcs = models.IntegerField(null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)

    def __str__(self):
        return f"{self.packing_list}-{self.pallet_id}"
