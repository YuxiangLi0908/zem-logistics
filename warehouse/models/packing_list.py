from decimal import Decimal

from django.core.validators import DecimalValidator, MinValueValidator
from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.quote import Quote

from .container import Container
from .shipment import Shipment


class PackingList(models.Model):
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    product_name = models.CharField(max_length=255, null=True, blank=True)
    delivery_method = models.CharField(max_length=255, null=True, blank=True)
    delivery_type = models.CharField(max_length=255, null=True, blank=True)
    shipping_mark = models.CharField(max_length=2000, null=True, blank=True)
    fba_id = models.CharField(max_length=4000, null=True, blank=True)
    destination = models.CharField(max_length=4000, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    zipcode = models.CharField(max_length=200, null=True, blank=True)
    contact_name = models.CharField(max_length=255, null=True, blank=True)
    contact_method = models.CharField(max_length=400, null=True, blank=True)
    ref_id = models.CharField(max_length=2000, null=True, blank=True)
    pcs = models.IntegerField(null=True)
    delivery_window_start = models.DateField(null=True, blank=True)
    delivery_window_end = models.DateField(null=True, blank=True)
    unit_weight_lbs = models.FloatField(null=True, blank=True)
    total_weight_lbs = models.FloatField(null=True)
    total_weight_kg = models.FloatField(null=True)
    cbm = models.FloatField(null=True)
    n_pallet = models.IntegerField(
        null=True, blank=True, validators=[MinValueValidator(1)]
    )
    shipment_batch_number = models.ForeignKey(
        Shipment,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="packinglist",
    )
    master_shipment_batch_number = models.ForeignKey(
        Shipment,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="packinglist_master",
    )
    note = models.CharField(null=True, blank=True, max_length=6000)
    office_note = models.CharField(null=True, blank=True, max_length=6000, verbose_name="客服备注")
    note_sp = models.CharField(max_length=2000, null=True, blank=True)
    express_number = models.CharField(null=True, blank=True, verbose_name="快递单号")
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
    quote_id = models.ForeignKey(
        Quote, null=True, blank=True, on_delete=models.SET_NULL
    )
    PO_ID = models.CharField(max_length=200, null=True, blank=True)
    ltl_verify = models.BooleanField(default=False, verbose_name="ltl组是否核实")
    ltl_supplier = models.CharField(max_length=100, null=True, blank=True, verbose_name="ltl供应商")
    carrier_company = models.CharField(max_length=200, null=True, blank=True, verbose_name="ltl承运公司")
    ltl_bol_num = models.CharField(max_length=200, null=True, blank=True, verbose_name="ltl-bol")
    ltl_pro_num = models.CharField(max_length=200, null=True, blank=True, verbose_name="ltl-pro")
    PickupAddr = models.CharField(max_length=200, null=True, blank=True, verbose_name="客提详细地址")
    est_pickup_time = models.DateField(null=True, blank=True, verbose_name="自提预计提货时间")
    ltl_follow_status = models.CharField(max_length=400, null=True, blank=True, verbose_name="ltl跟进状态")
    ltl_release_command = models.CharField(max_length=400, null=True, blank=True, verbose_name="ltl未放行时客户指令")
    ltl_contact_method = models.CharField(max_length=400, null=True, blank=True, verbose_name="ltl预约送货联系方式")
    ltl_correlation_id = models.CharField(max_length=400, null=True, blank=True, verbose_name="ltl一提多卸关联关系")
    shipment_note = models.CharField(max_length=1000, null=True, blank=True, verbose_name="公仓排约备注")
    ltl_address = models.CharField(max_length=1000, null=True, blank=True, verbose_name="ltl询价的地址")
    ltl_city = models.CharField(max_length=100, null=True, blank=True, verbose_name="ltl询价的城市")
    ltl_state = models.CharField(max_length=100, null=True, blank=True, verbose_name="ltl询价的州")
    ltl_zipcode = models.CharField(max_length=100, null=True, blank=True, verbose_name="ltl询价的邮编")
    ltl_address_type = models.CharField(max_length=100, null=True, blank=True, verbose_name="ltl询价地址类型")
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["PO_ID"]),
        ]

    def __str__(self):
        return f"{self.container_number}-{self.destination}-{self.shipping_mark if self.shipping_mark else 'no_mt'}-{self.fba_id if self.fba_id else 'no_fba'}"
