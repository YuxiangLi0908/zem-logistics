from django.db import models
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet

class FleetShipmentPallet(models.Model):
    fleet_number = models.ForeignKey(Fleet, null=True, blank=True, on_delete=models.SET_NULL)
    shipment_batch_number = models.ForeignKey(Shipment, null=True, blank=True, on_delete=models.SET_NULL)
    pallet_id = models.ForeignKey(Pallet, null=True, blank=True, on_delete=models.SET_NULL)
    fleet_number_str = models.CharField(max_length=255, null=True, blank=True)
    shipment_batch_number_str = models.CharField(max_length=255, null=True, blank=True)
    pallet_id_str = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(null=True, blank=True)

    def __str__(self) -> str:
        return self.shipment_batch_number
