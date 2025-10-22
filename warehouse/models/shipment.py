from datetime import datetime, timedelta

from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.fleet import Fleet


class Shipment(models.Model):
    shipment_batch_number = models.CharField(max_length=255, null=True, blank=True)
    master_batch_number = models.CharField(max_length=255, null=True, blank=True)
    batch = models.IntegerField(null=True, default=0)
    shipment_cargo_id = models.CharField(max_length=255, null=True, blank=True)
    appointment_id = models.CharField(max_length=255, null=True, blank=True)
    origin = models.CharField(max_length=255, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    carrier = models.CharField(max_length=255, null=True, blank=True)
    third_party_address = models.CharField(max_length=500, null=True, blank=True)
    is_shipment_schduled = models.BooleanField(default=False, blank=True)
    shipment_schduled_at = models.DateTimeField(null=True, blank=True)
    shipment_appointment = models.DateTimeField(null=True, blank=True)
    shipment_appointment_tz = models.CharField(max_length=20, null=True, blank=True)
    shipment_appointment_utc = models.DateTimeField(null=True, blank=True)
    is_shipped = models.BooleanField(default=False, null=True, blank=True)
    shipped_at = models.DateTimeField(null=True, blank=True)
    shipped_at_utc = models.DateTimeField(null=True, blank=True)
    is_full_out = models.BooleanField(default=False, null=True, blank=True)
    is_arrived = models.BooleanField(default=False, null=True, blank=True)
    arrived_at = models.DateTimeField(null=True, blank=True)
    arrived_at_utc = models.DateTimeField(null=True, blank=True)
    load_type = models.CharField(max_length=255, null=True, blank=True)
    shipment_account = models.CharField(max_length=255, null=True, blank=True)
    shipment_type = models.CharField(max_length=255, null=True, blank=True)
    total_weight = models.FloatField(null=True, default=0)
    total_cbm = models.FloatField(null=True, default=0)
    total_pallet = models.FloatField(null=True, default=0)
    total_pcs = models.FloatField(null=True, default=0)
    shipped_weight = models.FloatField(null=True, default=0, blank=True)
    shipped_cbm = models.FloatField(null=True, default=0, blank=True)
    shipped_pallet = models.FloatField(null=True, default=0)
    shipped_pcs = models.FloatField(null=True, default=0, blank=True)
    note = models.CharField(max_length=1000, null=True, blank=True)
    pod_link = models.CharField(max_length=2000, null=True, blank=True)
    pod_uploaded_at = models.DateTimeField(null=True, blank=True)
    pallet_dumpped = models.FloatField(null=True, blank=True, default=0)
    fleet_number = models.ForeignKey(
        Fleet, null=True, blank=True, on_delete=models.SET_NULL, related_name="shipment"
    )
    abnormal_palletization = models.BooleanField(default=False, null=True, blank=True)
    po_expired = models.BooleanField(default=False, null=True, blank=True)
    in_use = models.BooleanField(default=True, null=True, blank=True)
    is_canceled = models.BooleanField(default=False, null=True, blank=True)
    cancelation_reason = models.CharField(max_length=2000, null=True, blank=True)
    priority = models.CharField(max_length=10, null=True, blank=True)
    status = models.CharField(max_length=20, null=True, blank=True)
    status_description = models.CharField(max_length=1000, null=True, blank=True)
    previous_fleets = models.CharField(max_length=1000, null=True, blank=True)
    ARM_BOL = models.CharField(max_length=255, null=True, blank=True)
    ARM_PRO = models.CharField(max_length=255, null=True, blank=True)
    express_number = models.CharField(max_length=255, null=True, blank=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["shipment_batch_number"]),
            models.Index(fields=["appointment_id"]),
        ]

    def __str__(self) -> str:
        if self.shipment_batch_number:
            return self.shipment_batch_number
        else:
            return self.appointment_id

    @property
    def shipping_status(self) -> str:
        today = datetime.now().date()
        if self.shipment_appointment.date() <= today:
            return "past_due"
        elif self.shipment_appointment.date() <= today + timedelta(days=7):
            return "need_attention"
        else:
            return "on_time"

    @property
    def appointment_status(self) -> str:
        today = datetime.now().date()
        if self.shipment_appointment.date() <= today + timedelta(days=2):
            return "past_due"
        elif self.shipment_appointment.date() <= today + timedelta(days=5):
            return "need_attention"
        else:
            return "on_time"
