from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from django.db.models import JSONField


class InvoiceStatement(models.Model):
    invoice_statement_id = models.CharField(max_length=200, null=True, blank=True)
    statement_amount = models.FloatField(null=True, blank=True)
    statement_date = models.DateField(null=True, blank=True)
    due_date = models.DateField(null=True, blank=True)
    invoice_terms = models.CharField(max_length=200, null=True, blank=True)
    customer = models.ForeignKey(
        Customer, null=True, blank=True, on_delete=models.SET_NULL
    )
    statement_link = models.CharField(max_length=2000, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.customer.zem_name + " - " + self.invoice_statement_id


class Invoice(models.Model):
    invoice_number = models.CharField(max_length=200, null=True, blank=True)
    invoice_date = models.DateField(null=True, blank=True)
    invoice_link = models.CharField(max_length=2000, null=True, blank=True)
    customer = models.ForeignKey(
        Customer, null=True, blank=True, on_delete=models.SET_NULL
    )
    container_number = models.ForeignKey(
        Container, null=True, blank=True, on_delete=models.SET_NULL
    )
    receivable_total_amount = models.FloatField(null=True, blank=True)
    receivable_preport_amount = models.FloatField(null=True, blank=True)
    receivable_warehouse_amount = models.FloatField(null=True, blank=True)
    receivable_delivery_amount = models.FloatField(null=True, blank=True)
    receivable_direct_amount = models.FloatField(null=True, blank=True)

    payable_total_amount = models.FloatField(null=True, blank=True)
    payable_basic = models.FloatField(null=True, blank=True)  #提柜费
    payable_chassis = models.FloatField(null=True, blank=True) #车架费
    payable_overweight = models.FloatField(null=True, blank=True) #超重费
    payable_palletization = models.FloatField(null=True, blank=True) #拆柜费
    payable_surcharge = JSONField(default=dict, null=True, blank=True)

    statement_id = models.ForeignKey(
        InvoiceStatement, null=True, blank=True, on_delete=models.SET_NULL
    )
    received_amount = models.FloatField(null=True, blank=True)
    #是否通知客户
    is_invoice_delivered = models.BooleanField(default=False)
    #待核销金额
    remain_offset= models.FloatField(null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        try:
            return (
                self.customer.zem_name
                + " - "
                + self.container_number.container_number
                + " - "
                + self.invoice_number
            )
        except:
            return self.customer.zem_name + " - " + "None" + " - " + self.invoice_number


class InvoiceStatus(models.Model):
    INVOICE_TYPE_CHOICES = [("receivable", "应收账单"), ("payable", "应付账单")]
    STAGE_CHOICES = [
        ("unstarted", "未录入"),
        ("preport", "港前"),
        ("warehouse", "仓库"),
        ("delivery", "派送"),
        ("tobeconfirmed", "待确认"),
        ("confirmed", "已完成"),
    ]

    container_number = models.ForeignKey(
        Container, on_delete=models.CASCADE, related_name="invoice_statuses"
    )
    invoice_type = models.CharField(max_length=20, choices=INVOICE_TYPE_CHOICES)
    stage = models.CharField(max_length=20, choices=STAGE_CHOICES, default="unstarted")
    stage_public = models.CharField(
        max_length=20,
        default="pending",
        choices=[
            ("pending", "仓库待处理"),
            ("warehouse_completed", "仓库已完成"),
            ("delivery_completed", "派送已完成"),
            ("warehouse_rejected", "仓库已驳回"),
            ("delivery_rejected", "派送已驳回"),
        ],
    )
    stage_other = models.CharField(
        max_length=20,
        default="pending",
        choices=[
            ("pending", "待处理"),
            ("warehouse_completed", "仓库已完成"),
            ("delivery_completed", "派送已完成"),
            ("warehouse_rejected", "仓库已驳回"),
            ("delivery_rejected", "派送已驳回"),
        ],
    )
    is_rejected = models.BooleanField(default=False)
    reject_reason = models.TextField(blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["container_number", "invoice_type"],
                name="unique_container_invoice",
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


class InvoiceItem(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    description = models.CharField(max_length=1000, null=True, blank=True)
    warehouse_code = models.CharField(max_length=200, null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    weight = models.FloatField(null=True, blank=True)
    qty = models.FloatField(null=True, blank=True)
    rate = models.FloatField(null=True, blank=True)
    amount = models.FloatField(null=True, blank=True)
    note = models.CharField(max_length=2000, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return (
            self.invoice_number.__str__()
            + " - "
            + str(self.description)
            + " - "
            + str(self.warehouse_code)
        )
