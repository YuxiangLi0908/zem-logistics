from django.db import models
from django.db.models import JSONField
from simple_history.models import HistoricalRecords

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.invoice import InvoiceStatement

class Invoicev2(models.Model):
    invoice_number = models.CharField(max_length=200, null=True, blank=True)
    invoice_date = models.DateField(null=True, blank=True)
    created_at = models.DateField(null=True, blank=True)
    invoice_link = models.CharField(max_length=2000, null=True, blank=True)
    customer = models.ForeignKey(
        Customer, null=True, blank=True, on_delete=models.SET_NULL
    )
    container_number = models.ForeignKey(Container, null=True, blank=True, on_delete=models.SET_NULL, related_name="invoicesv2")

    receivable_total_amount = models.FloatField(null=True, blank=True)
    receivable_preport_amount = models.FloatField(null=True, blank=True)
    receivable_wh_public_amount = models.FloatField(null=True, blank=True)
    receivable_wh_other_amount = models.FloatField(null=True, blank=True)
    receivable_delivery_public_amount = models.FloatField(null=True, blank=True)
    receivable_delivery_other_amount = models.FloatField(null=True, blank=True)
    receivable_direct_amount = models.FloatField(null=True, blank=True)
    receivable_is_locked = models.BooleanField(default=False)  # 财务确认后锁定
    is_invoice_delivered = models.BooleanField(default=False) # 是否通知客户
    received_amount = models.FloatField(null=True, blank=True) # 客户支付的金额
    remain_offset = models.FloatField(null=True, blank=True) # 待核销金额

    statement_id = models.ForeignKey(
        InvoiceStatement, null=True, blank=True, on_delete=models.SET_NULL
    )
    payable_total_amount = models.FloatField(null=True, blank=True)
    payable_preport_amount = models.FloatField(null=True, blank=True)
    payable_warehouse_amount = models.FloatField(null=True, blank=True)
    payable_delivery_amount = models.FloatField(null=True, blank=True)
 
    history = HistoricalRecords()

    def __str__(self) -> str:
        try:
            return (
                self.container_number.container_number
                + " - "
                + self.invoice_number
            )
        except:
            return "None" + " - " + self.invoice_number


class InvoiceStatusv2(models.Model):
    INVOICE_TYPE_CHOICES = [("receivable", "应收账单"), ("payable", "应付账单")]
    container_number = models.ForeignKey(
        Container, on_delete=models.CASCADE, related_name="invoice_statusesv2"
    )
    invoice = models.ForeignKey(Invoicev2, on_delete=models.CASCADE) #绑定的账单
    invoice_type = models.CharField(max_length=20, choices=INVOICE_TYPE_CHOICES) #应收还是应付
    preport_status = models.CharField(
        max_length=20,
        default="unstarted",  # 未开始
        choices=[
            ("unstarted", "未录入"),
            ("in_progress", "录入中"), 
            ("pending_review", "待组长审核"),
            ("completed", "已完成"),
            ("rejected", "已驳回"),
        ]
    )
    
    warehouse_public_status = models.CharField(
        max_length=20,
        default="unstarted",
        choices=[
            ("unstarted", "未录入"),
            ("in_progress", "录入中"),
            ("completed", "已完成"),
            ("rejected", "已驳回"),
        ]
    )
    
    warehouse_other_status = models.CharField(
        max_length=20,
        default="unstarted", 
        choices=[
            ("unstarted", "未录入"),
            ("in_progress", "录入中"),
            ("completed", "已完成"),
            ("rejected", "已驳回"),
        ]
    )
    
    delivery_public_status = models.CharField(
        max_length=20,
        default="unstarted",
        choices=[
            ("unstarted", "未录入"),
            ("in_progress", "录入中"),
            ("completed", "已完成"),
            ("rejected", "已驳回"),
        ]
    )
    
    delivery_other_status = models.CharField(
        max_length=20,
        default="unstarted",
        choices=[
            ("unstarted", "未录入"),
            ("in_progress", "录入中"),
            ("completed", "已完成"),
            ("rejected", "已驳回"),
        ]
    )
    
    # 财务审核状态（原来的待确认和已完成）
    finance_status = models.CharField(
        max_length=20,
        default="unstarted",
        choices=[
            ("unstarted", "未开始"),
            ("tobeconfirmed", "待确认"),
            ("completed", "已完成"),
        ]
    )    
    preport_reason = models.TextField(blank=True)
    warehouse_public_reason = models.TextField(blank=True)
    warehouse_self_reason = models.TextField(blank=True)
    delivery_public_reason = models.TextField(blank=True)
    delivery_other_reason = models.TextField(blank=True)
    payable_status = models.JSONField(
        default={
            "pickup": "pending",  # 提拆状态
            "warehouse": "pending",  # 仓库状态
            "delivery": "pending",  # 派送状态
        }
    )
    payable_date = models.DateTimeField(null=True, blank=True, verbose_name="应付待审核通过时间")
    history = HistoricalRecords()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["invoice", "invoice_type"],
                name="unique_invoice_status",
            )
        ]

    def __str__(self) -> str:
        try:
            return self.container_number.container_number + " - " + self.invoice_type
        except:
            return (
                self.container_number.container_number
                + " - "
                + "None"
                + " - "
                + self.invoice_type
            )


class InvoiceItemv2(models.Model):
    INVOICE_TYPE_CHOICES = [("receivable", "应收账单"), ("payable", "应付账单")]
    ITEM_CATEGORY_CHOICES = [
        ("preport", "港前"),
        ("warehouse_public", "公仓库内"),
        ("warehouse_other", "私仓库内"),
        ("delivery_public", "公仓派送"),
        ("delivery_other", "私仓派送"),
    ]
    container_number = models.ForeignKey(
        Container, on_delete=models.CASCADE, related_name="invoice_itemv2"
    )
    invoice_number = models.ForeignKey(Invoicev2, on_delete=models.CASCADE)   
    invoice_type = models.CharField(max_length=20, choices=INVOICE_TYPE_CHOICES) #应收还是应付
    item_category = models.CharField(max_length=30, choices=ITEM_CATEGORY_CHOICES) #账单类型
    
    cbm = models.FloatField(null=True, blank=True) #体积
    weight = models.FloatField(null=True, blank=True) #重量
    description = models.CharField(max_length=1000, null=True, blank=True) #费用名称
    qty = models.FloatField(null=True, blank=True) #数量
    rate = models.FloatField(null=True, blank=True) #单价
    amount = models.FloatField(null=True, blank=True) #总价
    PO_ID = models.CharField(max_length=20, null=True, blank=True)
    delivery_type = models.CharField(max_length=50, null=True, blank=True) #亚马逊、组合柜什么的
    warehouse_code = models.CharField(max_length=200, null=True, blank=True) #目的地
    surcharges = models.FloatField(null=True, blank=True) #附加费
    note = models.CharField(max_length=2000, null=True, blank=True) #备注
    registered_user = models.CharField(max_length=2000, null=True, blank=True) #记录谁录的费用
    history = HistoricalRecords()

    def __str__(self) -> str:
        return f"{self.container_number} - {self.invoice_number} - {self.item_category} - {self.description}"
