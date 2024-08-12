from django.db import models
from datetime import datetime, timedelta


class Offload(models.Model):
    offload_id = models.CharField(max_length=255, null=True)
    offload_required = models.BooleanField(default=True)
    offload_at = models.DateTimeField(null=True, blank=True)
    total_pallet = models.IntegerField(null=True, blank=True)
    devanning_company = models.CharField(max_length=100, null=True, blank=True)
    devanning_fee = models.FloatField(null=True, blank=True)
    devanning_fee_paid_at = models.DateField(null=True, blank=True)
    is_devanning_fee_paid = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self) -> str:
        return self.offload_id
    

    @property
    def offload_status(self) -> str:
        today = datetime.now().date()
        if today > self.offload_at + timedelta(days=1):
            return "past_due"
        else:
            return "on_time"