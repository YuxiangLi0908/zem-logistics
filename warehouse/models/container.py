from django.db import models
from simple_history.models import HistoricalRecords


class Container(models.Model):
    DELIVERY_TYPE_CHOICES = [("mixed", "混合"), ("public", "公仓"), ("other", "其他")]
    container_number = models.CharField(max_length=255, null=True)
    container_type = models.CharField(max_length=255, null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)
    is_special_container = models.BooleanField(default=False, null=True, blank=True)
    delivery_type = models.CharField(
        max_length=20, choices=DELIVERY_TYPE_CHOICES, default="mixed"
    )
    note = models.CharField(max_length=100, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self):
        return self.container_number
