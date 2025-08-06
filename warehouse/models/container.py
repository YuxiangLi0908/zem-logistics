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
    # 因为组合柜存在整柜都按转运的方式计算，需要一个标识区分，因为order_type是转运的，所以
    account_order_type = models.CharField(max_length=255, null=True, default="转运组合")
    non_combina_reason = models.CharField(max_length=255, null=True)
    note = models.CharField(max_length=100, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self):
        return self.container_number
