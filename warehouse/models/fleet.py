from datetime import datetime, timedelta

from django.db import models


class Fleet(models.Model):
    fleet_number = models.CharField(max_length=255, null=True)
    fleet_zem_serial = models.CharField(max_length=255, null=True, blank=True)
    amf_id = models.CharField(max_length=255, null=True, blank=True)
    fleet_type = models.CharField(max_length=255, null=True, blank=True)
    origin = models.CharField(max_length=255, null=True, blank=True)
    carrier = models.CharField(max_length=100, null=True, blank=True)
    third_party_address = models.CharField(max_length=500, null=True, blank=True)
    license_plate = models.CharField(max_length=100, null=True, blank=True)
    motor_carrier_number = models.CharField(max_length=100, null=True, blank=True)
    dot_number = models.CharField(max_length=100, null=True, blank=True)
    appointment_datetime = models.DateTimeField(null=True, blank=True)
    appointment_datetime_tz = models.CharField(max_length=20, null=True, blank=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    departured_at = models.DateTimeField(null=True, blank=True)
    arrived_at = models.DateTimeField(null=True, blank=True)
    total_weight = models.FloatField(null=True)
    total_cbm = models.FloatField(null=True)
    total_pallet = models.FloatField(null=True)
    total_pcs = models.FloatField(null=True)
    note = models.CharField(max_length=1000, null=True, blank=True)
    shipped_weight = models.FloatField(null=True, default=0, blank=True)
    shipped_cbm = models.FloatField(null=True, default=0, blank=True)
    shipped_pallet = models.FloatField(null=True, default=0)
    shipped_pcs = models.FloatField(null=True, default=0, blank=True)
    cost_price = models.FloatField(null=True, default=0, blank=True)
    multipule_destination = models.BooleanField(default=False, null=True, blank=True)
    pod_link = models.CharField(max_length=2000, null=True, blank=True)
    pod_uploaded_at = models.DateTimeField(null=True, blank=True)
    is_canceled = models.BooleanField(default=False, null=True, blank=True)
    cancelation_reason = models.CharField(max_length=2000, null=True, blank=True)
    status = models.CharField(max_length=20, null=True, blank=True)
    status_description = models.CharField(max_length=1000, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["fleet_number"]),
            models.Index(fields=["fleet_zem_serial"]),
        ]

    def __str__(self) -> str:
        return self.fleet_number

    @property
    def departure_status(self) -> str:
        today = datetime.now().date()
        if self.appointment_datetime.date() <= today:
            return "past_due"
        elif self.appointment_datetime.date() <= today + timedelta(days=1):
            return "need_attention"
        else:
            return "on_time"

    @property
    def arrival_status(self) -> str:
        today = datetime.now().date()
        if self.departured_at.date() <= today:
            return "past_due"
        elif self.departured_at <= today + timedelta(days=1):
            return "need_attention"
        else:
            return "on_time"
