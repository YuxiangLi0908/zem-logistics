from django.db import models
from django.contrib.auth.models import User
from simple_history.models import HistoricalRecords

class ShipmentBindingLog(models.Model):
    # 操作类型
    OPERATION_TYPE_CHOICES = [
        ('bind', '绑定约'),
        ('unbind', '解绑约'),
    ]
    
    # PO类型
    PO_TYPE_CHOICES = [
        ('pallet', 'Pallet'),
        ('packing_list', 'PackingList'),
    ]
    
    # 约类型（主约还是实际约）
    SHIPMENT_TYPE_CHOICES = [
        ('master', '主约'),
        ('actual', '实际约'),
        ('all', '俩约')
    ]
    
    # 基础字段
    operation_type = models.CharField(
        max_length=20,
        choices=OPERATION_TYPE_CHOICES,
        verbose_name='操作类型'
    )
    
    shipment_type = models.CharField(
        max_length=20,
        choices=SHIPMENT_TYPE_CHOICES,
        null=True,
        blank=True,
        verbose_name='约类型'
    )
    
    po_type = models.CharField(
        max_length=20,
        choices=PO_TYPE_CHOICES,
        verbose_name='PO类型'
    )
    
    po_id = models.IntegerField(verbose_name='PO ID')
    po_display = models.CharField(max_length=500, null=True, blank=True, verbose_name='PO显示信息')
    
    # Shipment信息
    shipment_batch_number = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Shipment批次号'
    )
    
    # 仓库类型
    delivery_type = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='仓库类型'
    )
    
    # 额外信息
    container_number = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='柜号'
    )
    destination = models.CharField(
        max_length=4000,
        null=True,
        blank=True,
        verbose_name='仓点'
    )
    warehouse = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name='仓库'
    )
    
    # 操作人信息
    operator = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='shipment_binding_operations',
        verbose_name='操作人'
    )
    operator_username = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        verbose_name='操作人用户名'
    )
    
    # 操作按钮信息（可自定义）
    operation_button = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        verbose_name='操作按钮名称'
    )
    
    # 时间信息
    operation_time_utc = models.DateTimeField(auto_now_add=True, verbose_name='操作时间(UTC)')
    operation_time_beijing = models.DateTimeField(verbose_name='操作时间(北京时间)')
    
    # 备注信息
    note = models.TextField(null=True, blank=True, verbose_name='备注')
    
    history = HistoricalRecords()
    
    class Meta:
        db_table = 'warehouse_shipment_binding_log'
        ordering = ['-operation_time_utc']
        indexes = [
            models.Index(fields=['po_type', 'po_id']),
            models.Index(fields=['operation_time_utc']),
            models.Index(fields=['operator']),
            models.Index(fields=['delivery_type']),
            models.Index(fields=['container_number']),
        ]
    
    def __str__(self):
        return f"{self.operator_username} - {self.get_operation_type_display()} - {self.po_display}"