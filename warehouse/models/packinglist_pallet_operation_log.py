from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone


class PackingListPalletOperationLog(models.Model):
    TARGET_TYPE_CHOICES = [
        ("packing_list", "PackingList"),
        ("pallet", "Pallet"),
    ]

    ACTION_TYPE_CHOICES = [
        ("create", "新增"),
        ("update", "修改"),
        ("delete", "删除"),
        ("bind", "绑定"),
        ("unbind", "解绑"),
        ("schedule", "排约"),
        ("transfer", "转仓/转干线"),
        ("merge", "合板"),
        ("split", "拆分"),
        ("status", "状态变更"),
        ("export", "导出"),
        ("other", "其他"),
    ]

    target_type = models.CharField(
        max_length=20,
        choices=TARGET_TYPE_CHOICES,
        db_index=True,
        verbose_name="对象类型",
    )
    target_id = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="对象ID",
    )
    target_display = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="对象显示信息",
    )
    container_number = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="柜号",
    )
    po_id = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="PO_ID",
    )
    fba_id = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="FBA",
    )
    ref_id = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="REF",
    )
    shipping_mark = models.CharField(
        max_length=1000,
        null=True,
        blank=True,
        verbose_name="唛头",
    )
    destination = models.CharField(
        max_length=1000,
        null=True,
        blank=True,
        verbose_name="仓点",
    )
    warehouse = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="操作仓库/区域",
    )
    operation_location = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="操作位置",
    )
    operation_name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="操作名称",
    )
    action_type = models.CharField(
        max_length=20,
        choices=ACTION_TYPE_CHOICES,
        default="other",
        db_index=True,
        verbose_name="操作类型",
    )
    action_detail = models.TextField(
        null=True,
        blank=True,
        verbose_name="操作内容",
    )
    operator = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="packinglist_pallet_operation_logs",
        verbose_name="操作人",
    )
    operator_username = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        db_index=True,
        verbose_name="操作人用户名",
    )
    request_path = models.CharField(
        max_length=500,
        null=True,
        blank=True,
        verbose_name="请求路径",
    )
    operation_time_utc = models.DateTimeField(
        auto_now_add=True,
        db_index=True,
        verbose_name="操作时间(UTC)",
    )
    operation_time_beijing = models.DateTimeField(
        default=timezone.now,
        db_index=True,
        verbose_name="国内操作时间",
    )
    metadata = models.JSONField(default=dict, blank=True, verbose_name="额外信息")

    class Meta:
        db_table = "warehouse_packinglist_pallet_operation_log"
        ordering = ["-operation_time_utc"]
        indexes = [
            models.Index(fields=["target_type", "target_id"]),
            models.Index(fields=["operator_username"]),
            models.Index(fields=["operation_time_beijing"]),
            models.Index(fields=["action_type"]),
            models.Index(fields=["operation_name"]),
            models.Index(fields=["container_number"]),
            models.Index(fields=["po_id"]),
            models.Index(fields=["warehouse"]),
        ]
        verbose_name = "板子/packinglist修改记录"
        verbose_name_plural = "板子/packinglist修改记录"

    def __str__(self):
        return f"{self.operator_username or '-'} - {self.get_action_type_display()} - {self.target_display or self.target_id or '-'}"
