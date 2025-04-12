from django.db import models
from django.db.models import JSONField
from simple_history.models import HistoricalRecords

from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.invoice import Invoice, InvoiceStatus
from warehouse.models.offload import Offload
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse


class Order(models.Model):
    order_id = models.CharField(max_length=255, null=True)
    customer_name = models.ForeignKey(Customer, null=True, on_delete=models.SET_NULL)
    container_number = models.ForeignKey(
        Container, null=True, on_delete=models.CASCADE, related_name="order"
    )
    warehouse = models.ForeignKey(
        ZemWarehouse,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="order",
    )
    created_at = models.DateTimeField()
    eta = models.DateField(null=True, blank=True)
    order_type = models.CharField(max_length=255, null=True)
    vessel_id = models.ForeignKey(
        Vessel, null=True, blank=True, on_delete=models.SET_NULL
    )
    clearance_id = models.ForeignKey(
        Clearance, null=True, blank=True, on_delete=models.SET_NULL
    )
    retrieval_id = models.ForeignKey(
        Retrieval, null=True, blank=True, on_delete=models.SET_NULL
    )
    offload_id = models.ForeignKey(
        Offload, null=True, blank=True, on_delete=models.SET_NULL
    )
    shipment_id = models.ForeignKey(
        Shipment, null=True, blank=True, on_delete=models.SET_NULL, related_name="order"
    )
    customer_do_link = models.CharField(max_length=2000, null=True, blank=True)
    do_sent = models.BooleanField(default=False, blank=True)
    invoice_id = models.ForeignKey(
        Invoice, null=True, blank=True, on_delete=models.SET_NULL, related_name="order"
    )
    add_to_t49 = models.BooleanField(default=False)
    packing_list_updloaded = models.BooleanField(default=False)
    cancel_notification = models.BooleanField(default=False)
    cancel_time = models.DateField(null=True, blank=True)
    # 标记当前账单状态
    invoice_status = models.CharField(max_length=255, null=True)
    # 标记当前状态是否被上一步驳回
    invoice_reject = models.BooleanField(default=False)
    invoice_reject_reason = models.CharField(max_length=255, null=True, blank=True)
    receivable_status = models.ForeignKey(
        InvoiceStatus,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="receivable_orders",
        limit_choices_to={"invoice_type": "receivable"},  # 限制只能关联应收状态
    )

    payable_status = models.ForeignKey(
        InvoiceStatus,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="payable_orders",
        limit_choices_to={"invoice_type": "payable"},  # 限制只能关联应付状态
    )
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["order_id"]),
            models.Index(fields=["eta"]),
            models.Index(fields=["created_at"]),
        ]

    def __str__(self) -> str:
        if self.customer_name:
            return (
                self.customer_name.zem_name
                + " - "
                + self.container_number.container_number
            )
        else:
            return self.container_number.container_number
