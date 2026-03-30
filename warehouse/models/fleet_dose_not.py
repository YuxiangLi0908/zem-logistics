from django.db import models
from django.conf import settings
from django.utils import timezone


class FleetDoseNot(models.Model):
    """
    ftl上传车次不在的表
    """
    # 提货号
    pickup_number = models.CharField(
        max_length=300,
        verbose_name="提货号",
        null=True,
        blank=True,
    )

    # 出库批次号
    shipment_batch_number = models.CharField(
        max_length=300,
        verbose_name="出库批次号",
        null=True,
        blank=True,
    )

    # 预约ID
    appointment_id = models.CharField(
        max_length=300,
        verbose_name="预约ID",
        blank=True, null=True,
    )

    # 操作人（关联系统用户）
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        verbose_name="操作人",
        related_name="fleet_dose_not"
    )

    # 创建时间（自动记录）
    created_at = models.DateTimeField(
        default=timezone.now,
        verbose_name="创建时间"
    )

    class Meta:
        verbose_name = "ftl上传车次不在的表"
        verbose_name_plural = "ftl上传车次不在的表"
        ordering = ["-created_at"]  # 最新创建的排在前面

    def __str__(self):
        return f"{self.pickup_number} - {self.shipment_batch_number}"