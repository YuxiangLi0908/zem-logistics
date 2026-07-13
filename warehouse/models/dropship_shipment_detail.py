from decimal import Decimal

from django.core.validators import DecimalValidator, MinValueValidator
from django.db import models

from .dropship_shipment import DropshipShipment

from .dropship_cargo import DropshipCargo

class DropshipShipmentDetail(models.Model):
    """出库明细表"""
    shipment = models.ForeignKey(
        'DropshipShipment', 
        on_delete=models.CASCADE, 
        related_name='details',
        verbose_name="预约批次"
    )
    cargo = models.ForeignKey(
        'DropshipCargo', 
        on_delete=models.CASCADE, 
        verbose_name="货物"
    )
    
    pcs = models.PositiveIntegerField(verbose_name="出库件数（件）")
    pallets = models.PositiveIntegerField(default=0, verbose_name="板数")
    
    note = models.TextField(null=True, blank=True, verbose_name="备注")
    
    class Meta:
        unique_together = [['shipment', 'cargo']]
        indexes = [
            models.Index(fields=['shipment']),
            models.Index(fields=['cargo']),
        ]