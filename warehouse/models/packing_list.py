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
    shipping_mark = models.CharField(max_length=400, null=True, blank=True)
    fba_id = models.CharField(max_length=400, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=2000, null=True, blank=True)
    zipcode = models.CharField(max_length=20, null=True, blank=True)
    contact_name = models.CharField(max_length=255, null=True, blank=True)
    contact_method = models.CharField(max_length=400, null=True, blank=True)
    ref_id = models.CharField(max_length=400, null=True, blank=True)
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
    note = models.CharField(null=True, blank=True, max_length=2000)
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
    PO_ID = models.CharField(max_length=20, null=True, blank=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["PO_ID"]),
        ]

    def __str__(self):
        return f"{self.container_number}-{self.destination}-{self.shipping_mark if self.shipping_mark else 'no_mt'}-{self.fba_id if self.fba_id else 'no_fba'}"
