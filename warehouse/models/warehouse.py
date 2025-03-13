from django.db import models
from simple_history.models import HistoricalRecords


class ZemWarehouse(models.Model):
    name = models.CharField(max_length=200)
    address = models.CharField(max_length=200, null=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.name
