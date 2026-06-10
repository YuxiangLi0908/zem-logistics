from django.db import models
from simple_history.models import HistoricalRecords


class ContainerPickupCarrier(models.Model):
    """
    港前提柜供应商表
    """
    name = models.CharField("供应商名称", max_length=100, unique=True)
    is_active = models.BooleanField("是否启用", default=True)

    history = HistoricalRecords()

    class Meta:
        db_table = "container_pickup_carrier"
        verbose_name = "港前提柜供应商"
        verbose_name_plural = "港前提柜供应商"

    def __str__(self):
        return self.name