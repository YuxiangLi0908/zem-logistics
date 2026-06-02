from django.db import models
from django.conf import settings
from simple_history.models import HistoricalRecords
import json


class SystemParameter(models.Model):
    category = models.CharField(max_length=100, verbose_name="参数分类")
    key = models.CharField(max_length=200, verbose_name="参数键")
    value = models.TextField(verbose_name="参数值")
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
        unique_together = [["category", "key", "value"]]
        indexes = [
            models.Index(fields=["category"]),
        ]

    def __str__(self):
        return f"[{self.category}] {self.key}: {self.value}"

    @staticmethod
    def get_active_by_category(category_name):
        """
        获取某个分类下的所有启用参数，返回 {key: value} 字典
        """
        params = SystemParameter.objects.filter(category=category_name, is_active=True).order_by("sort_order", "id")
        return {param.key: param.value for param in params}

    @staticmethod
    def get_active_list_by_category(category_name):
        """
        获取某个分类下的所有启用参数，返回 [value] 列表
        """
        params = SystemParameter.objects.filter(category=category_name, is_active=True).order_by("sort_order", "id")
        return [param.value for param in params]

    @staticmethod
    def get_warehouse_destinations():
        """
        获取州仓点映射，返回 {州代码: [仓点列表]} 字典
        每个仓点是一条独立记录，key 为州代码，value 为仓点名称
        """
        params = SystemParameter.objects.filter(category="州仓点", is_active=True).order_by("sort_order", "id")
        result = {}
        for param in params:
            if param.key not in result:
                result[param.key] = []
            result[param.key].append(param.value)
        return result

    @staticmethod
    def get_fba_locations():
        params = SystemParameter.objects.filter(category="FBA仓点", is_active=True).order_by("sort_order", "id")
        result = {}
        for param in params:
            try:
                result[param.key] = json.loads(param.value)
            except (json.JSONDecodeError, TypeError):
                pass
        return result
