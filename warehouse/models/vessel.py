from datetime import datetime, timedelta

from django.db import models
from simple_history.models import HistoricalRecords


class Vessel(models.Model):
    vessel_id = models.CharField(max_length=255, null=True)
    master_bill_of_lading = models.CharField(max_length=255, null=True, blank=True)
    origin_port = models.CharField(max_length=255, null=True, blank=True)
    destination_port = models.CharField(max_length=255, null=True, blank=True)
    shipping_line = models.CharField(max_length=255, null=True, blank=True)
    #2025-11-4起，船名 船次 合并到了船名vessel字段
    vessel = models.CharField(max_length=100, blank=True, null=True)
    voyage = models.CharField(max_length=100, blank=True, null=True)
    vessel_etd = models.DateTimeField(null=True, blank=True)
    vessel_eta = models.DateTimeField(null=True, blank=True)
    #Terminal 49返回数据
    origin_port_t49 = models.CharField(max_length=255, null=True, blank=True)
    destination_port_t49 = models.CharField(max_length=255, null=True, blank=True)
    vessel_t49 = models.CharField(max_length=100, blank=True, null=True)
    vessel_etd_t49 = models.DateTimeField(null=True, blank=True)
    vessel_eta_t49 = models.DateTimeField(null=True, blank=True)
    vessel_imo = models.CharField(max_length=100, blank=True, null=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["vessel_id"]),
            models.Index(fields=["master_bill_of_lading"]),
            models.Index(fields=["vessel"]),
            models.Index(fields=["voyage"]),
        ]

    def __str__(self) -> str:
        return self.vessel_id

    @property
    def eta_status(self) -> str:
        today = datetime.now().date()
        if self.vessel_eta.date() <= today:
            return "past_due"
        elif self.vessel_eta.date() <= today + timedelta(weeks=1):
            return "within_one_week"
        else:
            return "on_time"
