from django.db import models
from datetime import datetime, timedelta

class Fleet(models.Model):
    fleet_number = models.CharField(max_length=255, null=True)
    origin = models.CharField(max_length=255, null=True, blank=True)
    carrier = models.CharField(max_length=100, null=True, blank=True)
    third_party_address = models.CharField(max_length=500, null=True, blank=True)
    appointment_date = models.DateField(null=True, blank=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    departured_at = models.DateTimeField(null=True, blank=True)
    arrived_at = models.DateTimeField(null=True, blank=True)
    total_weight = models.FloatField(null=True)
    total_cbm = models.FloatField(null=True)
    total_pallet = models.FloatField(null=True)
    total_pcs = models.FloatField(null=True)
    note = models.CharField(max_length=1000, null=True, blank=True)
    multipule_destination = models.BooleanField(default=False, blank=True)
    pod_link = models.CharField(max_length=2000, null=True, blank=True)

    def __str__(self) -> str:
        return self.fleet_number
    
    @property
    def departure_status(self) -> str:
        today = datetime.now().date()
        if self.appointment_date <= today:
            return "past_due"
        elif self.appointment_date <= today + timedelta(days=1):
            return "need_attention"
        else:
            return "on_time"