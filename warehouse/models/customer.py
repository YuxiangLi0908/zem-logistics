from django.db import models
from simple_history.models import HistoricalRecords


class Customer(models.Model):
    zem_name = models.CharField(max_length=200)
    full_name = models.CharField(max_length=200, null=True)
    accounting_name = models.CharField(max_length=200, null=True)
    zem_code = models.CharField(max_length=20, null=True, blank=True)
    email = models.CharField(max_length=100, null=True, blank=True)
    phone = models.CharField(max_length=30, null=True, blank=True)
    note = models.CharField(max_length=500, null=True, blank=True)
    address = models.CharField(max_length=500, null=True, blank=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["zem_name"]),
        ]

    def __str__(self) -> str:
        return f"{self.zem_name}" if self.zem_name else f"{self.full_name}"
