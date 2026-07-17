



from decimal import Decimal

from django.core.validators import DecimalValidator, MinValueValidator
from django.db import models
from simple_history.models import HistoricalRecords

from .dropship_shipment_detail import DropshipShipmentDetail

from .dropship_cargo import DropshipCargo

class DropshipInventory(models.Model):
    """库存流水表 - 记录所有库存变动"""
    TRANSACTION_TYPES = (
        ('unpack', '拆柜入库'),
        ('return', '退货入库'),
        ('transfer', '调拨入库'),
        ('pick', '预约出库'),
        ('adjust', '库存调整'),
    )
    
    cargo = models.ForeignKey('DropshipCargo', on_delete=models.CASCADE, verbose_name="货物")
    transaction_type = models.CharField(max_length=20, choices=TRANSACTION_TYPES, verbose_name="操作类型")
    
    # 件数变动（正数表示入库，负数表示出库）
    pcs_change = models.IntegerField(verbose_name="件数变动（正=增加，负=减少）")
    
    # 操作后的库存快照（便于追踪历史状态）
    after_pcs = models.PositiveIntegerField(verbose_name="操作后件数", null=True, blank=True)
    
    # 关联批次
    shipment_detail = models.ForeignKey(
        'DropshipShipmentDetail', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        verbose_name="关联出库明细"
    )
    # return_detail = models.ForeignKey(
    #     'DropshipReturnDetail', 
    #     on_delete=models.SET_NULL, 
    #     null=True, 
    #     blank=True,
    #     verbose_name="关联退货明细"
    # )
       
    # 操作信息
    transaction_date = models.DateTimeField(auto_now_add=True, verbose_name="操作时间")
    operator = models.CharField(max_length=100, null=True, blank=True, verbose_name="操作人")
    note = models.TextField(null=True, blank=True, verbose_name="备注")
    # 对于预约出库来说，为真表示确认出库了；对于建单来说，为真表示已经拆柜入库了
    is_verify = models.BooleanField(default=False, verbose_name="是否核验")
    verify_pcs = models.PositiveIntegerField(verbose_name="核验后件数", null=True, blank=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['cargo', 'transaction_type']),
            models.Index(fields=['transaction_date']),
            models.Index(fields=['cargo', 'transaction_date']),
        ]