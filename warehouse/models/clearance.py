from django.db import models
from simple_history.models import HistoricalRecords


class Clearance(models.Model):
    clearance_id = models.CharField(max_length=255, null=True)
    is_clearance_required = models.BooleanField(default=True)
    clear_by_zem = models.BooleanField(default=True)
    clearance_agent = models.CharField(max_length=255, null=True, blank=True)
    cleared_at = models.DateTimeField(null=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.clearance_id
