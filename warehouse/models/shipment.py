from django.db import models
from datetime import datetime, timedelta

from warehouse.models.fleet import Fleet


class Shipment(models.Model):
    shipment_batch_number = models.CharField(max_length=255, null=True)
    appointment_id = models.CharField(max_length=255, null=True, blank=True)
    origin = models.CharField(max_length=255, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    carrier = models.CharField(max_length=255, null=True, blank=True)
    third_party_address = models.CharField(max_length=500, null=True, blank=True)
    is_shipment_schduled = models.BooleanField(default=False, blank=True)
    shipment_schduled_at = models.DateTimeField(null=True, blank=True)
    shipment_appointment = models.DateTimeField(null=True, blank=True)
    is_shipped = models.BooleanField(default=False, blank=True)
    shipped_at = models.DateTimeField(null=True, blank=True)
    is_full_out = models.BooleanField(default=False, blank=True)
    is_arrived = models.BooleanField(default=False, blank=True)
    arrived_at = models.DateTimeField(null=True, blank=True)
    load_type = models.CharField(max_length=255, null=True, blank=True)
    total_weight = models.FloatField(null=True)
    total_cbm = models.FloatField(null=True)
    total_pallet = models.FloatField(null=True)
    total_pcs = models.FloatField(null=True)
    # shipped_weight = models.FloatField(null=True)
    # shipped_cbm = models.FloatField(null=True)
    # shipped_pallet = models.FloatField(null=True)
    # shipped_pcs = models.FloatField(null=True)
    note = models.CharField(max_length=1000, null=True, blank=True)
    pod_link = models.CharField(max_length=2000, null=True, blank=True)
    pallet_dumpped = models.FloatField(null=True, blank=True, default=0)
    fleet_number = models.ForeignKey(Fleet, null=True, blank=True, on_delete=models.SET_NULL, related_name='shipment')

    def __str__(self) -> str:
        return self.shipment_batch_number
    
    @property
    def shipping_status(self) -> str:
        today = datetime.now().date()
        if self.shipment_appointment.date() <= today:
            return "past_due"
        elif self.shipment_appointment.date() <= today + timedelta(days=7):
            return "need_attention"
        else:
            return "on_time"