from django.db import models
from django.conf import settings
from simple_history.models import HistoricalRecords


class SystemParameter(models.Model):
    category = models.CharField(max_length=100, verbose_name="参数分类")
    key = models.CharField(max_length=200, verbose_name="参数键")
    value = models.CharField(max_length=200, verbose_name="参数值")
    sort_order = models.IntegerField(default=0, verbose_name="排序顺序")
    is_active = models.BooleanField(default=True, verbose_name="是否启用")
    created_by = models.CharField(max_length=150, null=True, blank=True, verbose_name="登记人")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="更新时间")
    history = HistoricalRecords()

    class Meta:
        db_table = "warehouse_system_parameter"
        verbose_name = "系统参数"
        verbose_name_plural = "系统参数"
        ordering = ["category", "sort_order", "id"]
        unique_together = [["category", "key"]]
        indexes = [
            models.Index(fields=["category"]),
        ]

    def __str__(self):
        return f"[{self.category}] {self.key}: {self.value}"
