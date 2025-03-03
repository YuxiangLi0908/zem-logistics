from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.shipment import Shipment


class ShipmentStatus(models.Model):
    shipment_batch_number = models.ForeignKey(
        Shipment, null=True, blank=True, on_delete=models.SET_NULL
    )
    shipment_batch_number_str = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(null=True, blank=True)
    created_by = models.CharField(max_length=255, null=True, blank=True)
    status_code = models.IntegerField(
        null=True, blank=True
    )  # need a code to status mapping
    status_name = models.CharField(max_length=255, null=True, blank=True)
    related_batch_number = models.CharField(max_length=255, null=True, blank=True)
    related_third_party = models.CharField(max_length=255, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.shipment_batch_number
