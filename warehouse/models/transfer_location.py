from django.db import models

class TransferLocation(models.Model):
    shipping_warehouse = models.CharField(max_length=255, null=True, blank=True)
    receiving_warehouse = models.CharField(max_length=2000, null=True, blank=True)
    shipping_time = models.DateTimeField(null=True, blank=True)
    ETA = models.DateTimeField(null=True, blank=True)
    arrival_time = models.DateTimeField(null=True, blank=True)
    batch_number = models.CharField(max_length=2000, null=True, blank=True)
    container_number = models.CharField(max_length=2000, null=True, blank=True)
    total_pallet = models.IntegerField(null=True, blank=True)
    total_pcs = models.IntegerField(null=True, blank=True)
    total_cbm = models.FloatField(null=True, blank=True)
    total_weight = models.FloatField(null=True, blank=True)


    def __str__(self):
        return f"{self.batch_number}"