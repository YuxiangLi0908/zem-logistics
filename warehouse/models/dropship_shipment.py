from decimal import Decimal

from django.core.validators import DecimalValidator, MinValueValidator
from django.db import models
from simple_history.models import HistoricalRecords

class DropshipShipment(models.Model):
    """出库批次表 - 对应您的预约批次概念"""
    shipment_batch_number = models.CharField(max_length=255, unique=True, verbose_name="预约批次号")
    warehouse = models.ForeignKey('ZemWarehouse', on_delete=models.SET_NULL, null=True, verbose_name="出库仓库")
    
    # 时间
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="创建时间")
    pickup_time = models.DateField(null=True, blank=True, verbose_name="预计提货日期")
    shipped_at = models.DateTimeField(null=True, blank=True, verbose_name="实际出库日期")
    arrived_at = models.DateTimeField(null=True, blank=True, verbose_name="送达时间")
    
    # 出库信息
    total_pcs = models.PositiveIntegerField(default=0, verbose_name="总出库件数")
    
    # POD文件
    pod_link = models.CharField(max_length=2000, null=True, blank=True)
    pod_uploaded_at = models.DateTimeField(null=True, blank=True, verbose_name="POD上传时间")
    
    # 收货地址
    shipping_address = models.TextField(verbose_name="收货地址", blank=True, null=True)
    contact_person = models.CharField(max_length=100, verbose_name="联系人", blank=True, null=True)
    contact_phone = models.CharField(max_length=20, verbose_name="联系电话", blank=True, null=True)
    
    # 备注
    note = models.TextField(null=True, blank=True, verbose_name="备注")
    operator = models.CharField(max_length=100, verbose_name="操作人")
    
    class Meta:
        indexes = [
            models.Index(fields=['shipment_batch_number']),
        ]