from django.db import models
from datetime import datetime, timedelta
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment

class Shipment(models.Model):
    shipment_batch_number = models.ForeignKey(Shipment, on_delete=models.SET_NULL)
    pallet_id = models.ForeignKey(Pallet, on_delete=models.SET_NULL)
    shipment_batch_number_str = models.CharField(max_length=255, null=True, blank=True)
    pallet_id_str = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(null=True, blank=True)

    def __str__(self) -> str:
        return self.shipment_batch_number
