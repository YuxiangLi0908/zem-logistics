
from decimal import Decimal

from django.core.validators import DecimalValidator, MinValueValidator
from django.db import models
from simple_history.models import HistoricalRecords

from .dropship_shipment import DropshipShipment


class DropshipCargo (models.Model):
    """货物主表 - 所有操作围绕此表 - 以唛头为最小管理单位"""
    PICKUP_TYPE_CHOICES = (
        ('pickup', '自提'),
        ('self_ship', '自发'),
    )
    STATUS_CHOICES = (
        ('not_in_stock', '未入库'),
        ('in_stock', '在库'),
        ('all_out', '全部出库'),
    )
    # 基本信息
    shipping_mark = models.CharField(max_length=255, db_index=True, verbose_name="唛头")
    model = models.CharField(max_length=255, null=True, blank=True, verbose_name="型号")
    product_name = models.CharField(max_length=255, null=True, blank=True, verbose_name="产品名称")
    
    # 关联信息
    container = models.ForeignKey('Container', on_delete=models.SET_NULL, null=True, verbose_name="柜子")
    order = models.ForeignKey('Order', on_delete=models.SET_NULL, null=True, verbose_name="关联订单")
    warehouse = models.ForeignKey('ZemWarehouse', on_delete=models.SET_NULL, null=True, verbose_name="所在仓库")
    
    # 库存件数（当前可用库存）
    pcs = models.PositiveIntegerField(default=0, verbose_name="当前库存件数")
    pallets = models.PositiveIntegerField(default=0, verbose_name="当前板数")
    unit_weight_lbs = models.FloatField(null=True, blank=True)
    total_weight_lbs = models.FloatField(null=True)
    total_weight_kg = models.FloatField(null=True)
    cbm = models.FloatField(null=True)
    long = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="长",
        validators=[
            MinValueValidator(Decimal("0.01")),
            DecimalValidator(max_digits=10, decimal_places=2),
        ],
    )
    width = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="宽",
        validators=[
            MinValueValidator(Decimal("0.01")),
            DecimalValidator(max_digits=10, decimal_places=2),
        ],
    )
    height = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="高",
        validators=[
            MinValueValidator(Decimal("0.01")),
            DecimalValidator(max_digits=10, decimal_places=2),
        ],
    )
    PO_ID = models.CharField(max_length=200, null=True, blank=True)
    
    # 已出库件数（累计）
    shipped_quantity = models.PositiveIntegerField(default=0, verbose_name="累计出库件数")
    
    # 已退货件数（累计）
    returned_quantity = models.PositiveIntegerField(default=0, verbose_name="累计退货件数")
    
    # 提货类型
    delivery_type = models.CharField(
        max_length=20,
        default='一件代发',
        verbose_name="提货类型"
    )

    delivery_method = models.CharField(
        max_length=20,
        choices=PICKUP_TYPE_CHOICES,
        default='pickup',
        verbose_name="派送方式"
    )
    # 地址（提货地址，仅自发的有值）
    address = models.TextField(null=True, blank=True, verbose_name="提货地址")
    # 状态
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='not_in_stock', verbose_name="货物状态")
    
    # 备注
    note = models.TextField(null=True, blank=True, verbose_name="备注")
    class Meta:
        unique_together = [['shipping_mark', 'order']]  # 同一订单下唛头唯一
        indexes = [
            models.Index(fields=['shipping_mark']),
            models.Index(fields=['order', 'shipping_mark']),
            models.Index(fields=['status']),
            models.Index(fields=['container']),
        ]
    def __str__(self):
        return f"{self.shipping_mark} - {self.product_name} - {self.model} - {self.pcs}"

    history = HistoricalRecords()