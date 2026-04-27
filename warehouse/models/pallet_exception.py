from django.db import models
from simple_history.models import HistoricalRecords

from .pallet import Pallet


class PalletException(models.Model):
    EXCEPTION_TYPE_CHOICES = [
        ('unpack', '拆柜异常'),
        ('appointment', '拿约异常'),
    ]

    pallet = models.ForeignKey(
        Pallet,
        on_delete=models.CASCADE,
        related_name='exceptions',
        verbose_name='关联Pallet'
    )
    exception_type = models.CharField(
        max_length=50,
        choices=EXCEPTION_TYPE_CHOICES,
        verbose_name='异常类型'
    )
    exception_reason = models.CharField(
        max_length=500,
        verbose_name='异常原因'
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name='创建时间'
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name='更新时间'
    )
    history = HistoricalRecords()

    class Meta:
        verbose_name = 'Pallet异常'
        verbose_name_plural = 'Pallet异常'
        indexes = [
            models.Index(fields=['pallet']),
            models.Index(fields=['exception_type']),
        ]

    def __str__(self):
        return f"{self.pallet.id} - {self.get_exception_type_display()}"
