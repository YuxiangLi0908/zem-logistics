from django.conf import settings
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
        Shipment, null=True, blank=True, on_delete=models.SET_NULL, related_name="fleetshipmentpallets"
    )
    transfer_number = models.ForeignKey(
        TransferLocation, null=True, blank=True, on_delete=models.SET_NULL
    )
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    PO_ID = models.CharField(max_length=20, null=True, blank=True)
    total_pallet = models.FloatField(null=True, default=0)
    expense = models.FloatField(null=True, blank=True)
    is_recorded = models.BooleanField(default=False, verbose_name="是否已记录")
    cost_input_time = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name="成本录入时间",
        help_text="录入/更新该批次成本的时间"
    )
    operator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name="操作人员",
        help_text="录入/更新成本的操作人员"
    )
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.PO_ID
