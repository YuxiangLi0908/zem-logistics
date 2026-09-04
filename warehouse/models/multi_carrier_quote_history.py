from django.conf import settings
from django.db import models


class MultiCarrierQuoteHistory(models.Model):
    origin_warehouse = models.CharField(max_length=100, db_index=True, verbose_name="发货仓库")
    destination_warehouse = models.CharField(max_length=100, db_index=True, verbose_name="收货仓点")
    pickup_date = models.DateField(db_index=True, verbose_name="取件日期")
    quote_type = models.CharField(max_length=20, db_index=True, verbose_name="运输类型")
    ftl_car_type = models.CharField(max_length=100, blank=True, default="", verbose_name="FTL车型")
    freight_class = models.CharField(max_length=20, blank=True, default="", verbose_name="Freight Class")
    declared_value = models.DecimalField(max_digits=12, decimal_places=2, verbose_name="申报价值")
    pallet_items = models.JSONField(default=list, verbose_name="板子明细")
    maersk_quotes = models.JSONField(default=dict, blank=True, verbose_name="马士基报价")
    kakas_quotes = models.JSONField(default=dict, blank=True, verbose_name="卡卡省报价")
    kakas_request_payload = models.JSONField(default=dict, blank=True, verbose_name="卡卡省询价请求体")
    abf_quotes = models.JSONField(default=dict, blank=True, verbose_name="ABF报价")
    operator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="multi_carrier_quote_histories",
        verbose_name="询价人",
    )
    created_at = models.DateTimeField(auto_now_add=True, db_index=True, verbose_name="询价时间")

    class Meta:
        db_table = "warehouse_multi_carrier_quote_history"
        ordering = ["id"]
        verbose_name = "三方询价历史"
        verbose_name_plural = "三方询价历史"

    def __str__(self):
        return f"#{self.id} {self.origin_warehouse} -> {self.destination_warehouse}"
