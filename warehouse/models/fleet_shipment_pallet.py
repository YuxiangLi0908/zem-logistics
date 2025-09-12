from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.transfer_location import TransferLocation

from .container import Container


class FleetShipmentPallet(models.Model):
    fleet_number = models.ForeignKey(
        Fleet, null=True, blank=True, on_delete=models.SET_NULL
    )
    pickup_number = models.CharField(max_length=255, null=True, blank=True)
    shipment_batch_number = models.ForeignKey(
        Shipment, null=True, blank=True, on_delete=models.SET_NULL
    )
    transfer_number = models.ForeignKey(
        TransferLocation, null=True, blank=True, on_delete=models.SET_NULL
    )
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    PO_ID = models.CharField(max_length=20, null=True, blank=True)
    total_pallet = models.FloatField(null=True, default=0)
    expense = models.FloatField(null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.PO_ID
