from django.db import models
from datetime import datetime, timedelta
from warehouse.models.container import Container
from warehouse.models.offload import Offload
from warehouse.models.pallet import Pallet

class AbnormalOffloadStatus(models.Model):
    offload = models.ForeignKey(Offload, null=True, on_delete=models.CASCADE, related_name="offload_status")
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE, related_name="offload_status")
    created_at = models.DateTimeField(null=True, blank=True)
    resolved_at = models.DateTimeField(null=True, blank=True)
    is_resolved = models.BooleanField(default=False)
    confirmed_by_warehouse = models.BooleanField(default=False)
    destination = models.CharField(max_length=255, null=True, blank=True)
    delivery_method = models.CharField(max_length=255, null=True, blank=True)
    pcs_reported = models.IntegerField(null=True, blank=True)
    pcs_actual = models.IntegerField(null=True, blank=True)
    abnormal_reason = models.CharField(max_length=255, null=True, blank=True)
    note = models.CharField(max_length=1000, null=True, blank=True)

    def __str__(self) -> str:
        return self.container_number.container_number + " - " + self.destination + " - " + str(self.is_resolved)
    
    @property
    def abnormal_status(self) -> str:
        today = datetime.now().date()
        if self.created_at.date() <= today + timedelta(days=-2):
            return "past_due"
        else:
            return "on_time"