from datetime import datetime, timedelta

from django.db import models
from simple_history.models import HistoricalRecords


class Offload(models.Model):
    offload_id = models.CharField(max_length=255, null=True)
    offload_required = models.BooleanField(default=True)
    offload_at = models.DateTimeField(null=True, blank=True)
    total_pallet = models.IntegerField(null=True, blank=True)
    devanning_company = models.CharField(max_length=100, null=True, blank=True)
    devanning_fee = models.FloatField(null=True, blank=True)
    devanning_fee_paid_at = models.DateField(null=True, blank=True)
    is_devanning_fee_paid = models.CharField(max_length=100, null=True, blank=True)
    history = HistoricalRecords()
    warehouse_unpacked_time = models.DateTimeField(null=True, blank=True, verbose_name="仓库确认拆柜完成时间")
    warehouse_unpacking_time = models.DateTimeField(null=True, blank=True, verbose_name="首次下载拆柜单变拆柜中时间")
    def __str__(self) -> str:
        return self.offload_id

    @property
    def offload_status(self) -> str:
        today = datetime.now().date()
        if today > self.offload_at + timedelta(days=1):
            return "past_due"
        else:
            return "on_time"

    @property
    def shipment_status(self) -> str:
        today = datetime.now().date()
        if today > self.offload_at + timedelta(days=7):
            return "past_due"
        else:
            return "on_time"
