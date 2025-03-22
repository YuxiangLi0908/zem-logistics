from django.db import models
from django.db.models import JSONField
from simple_history.models import HistoricalRecords

from warehouse.models.invoice import Invoice


class InvoicePreport(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    pickup = models.FloatField(null=True, blank=True, verbose_name="提拆/打托缠膜")
    chassis = models.FloatField(null=True, blank=True, verbose_name="托架费")
    chassis_split = models.FloatField(null=True, blank=True, verbose_name="托架提取费")
    prepull = models.FloatField(null=True, blank=True, verbose_name="预提费")
    yard_storage = models.FloatField(null=True, blank=True, verbose_name="货柜放置费")
    handling_fee = models.FloatField(null=True, blank=True, verbose_name="操作处理费")
    pier_pass = models.FloatField(null=True, blank=True, verbose_name="码头")
    congestion_fee = models.FloatField(null=True, blank=True, verbose_name="港口拥堵费")
    hanging_crane = models.FloatField(null=True, blank=True, verbose_name="吊柜费")
    dry_run = models.FloatField(null=True, blank=True, verbose_name="空跑费")
    exam_fee = models.FloatField(null=True, blank=True, verbose_name="查验费")
    hazmat = models.FloatField(null=True, blank=True, verbose_name="危险品")
    over_weight = models.FloatField(null=True, blank=True, verbose_name="超重费")
    urgent_fee = models.FloatField(null=True, blank=True, verbose_name="加急费")
    other_serive = models.FloatField(null=True, blank=True, verbose_name="其他服务")
    demurrage = models.FloatField(null=True, blank=True, verbose_name="港内滞期费")
    per_diem = models.FloatField(null=True, blank=True, verbose_name="港外滞期费")
    second_pickup = models.FloatField(null=True, blank=True, verbose_name="二次提货")
    amount = models.FloatField(null=True, blank=True)
    surcharges = JSONField(default=dict)
    surcharge_notes = JSONField(default=dict)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return str(self.invoice_number)


class InvoiceWarehouse(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    sorting = models.FloatField(null=True, blank=True, verbose_name="分拣费")
    intercept = models.FloatField(null=True, blank=True, verbose_name="拦截费")
    po_activation = models.FloatField(
        null=True, blank=True, verbose_name="亚马逊PO激活"
    )
    self_pickup = models.FloatField(null=True, blank=True, verbose_name="客户自提")
    re_pallet = models.FloatField(null=True, blank=True, verbose_name="重新打板")
    handling = models.FloatField(null=True, blank=True, verbose_name="")
    counting = models.FloatField(null=True, blank=True, verbose_name="货品清点费")
    warehouse_rent = models.FloatField(null=True, blank=True, verbose_name="仓租")
    specified_labeling = models.FloatField(
        null=True, blank=True, verbose_name="指定贴标"
    )
    inner_outer_box = models.FloatField(null=True, blank=True, verbose_name="内外箱")
    inner_outer_box_label = models.FloatField(null=True, blank=True, verbose_name="")
    pallet_label = models.FloatField(null=True, blank=True, verbose_name="托盘标签")
    open_close_box = models.FloatField(null=True, blank=True, verbose_name="开封箱")
    destroy = models.FloatField(null=True, blank=True, verbose_name="销毁")
    take_photo = models.FloatField(null=True, blank=True, verbose_name="拍照")
    take_video = models.FloatField(null=True, blank=True, verbose_name="拍视频")
    repeated_operation_fee = models.FloatField(
        null=True, blank=True, verbose_name="重复操作费"
    )
    amount = models.FloatField(null=True, blank=True)
    surcharges = JSONField(default=dict)
    surcharge_notes = JSONField(default=dict)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return str(self.invoice_number)


class InvoiceDelivery(models.Model):
    invoice_delivery = models.CharField(max_length=200, null=True, blank=True)
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    type = models.CharField(max_length=200, null=True, blank=True)
    destination = models.CharField(max_length=200, null=True, blank=True)
    zipcode = models.CharField(max_length=200, null=True, blank=True)
    total_pallet = models.FloatField(null=True, blank=True)
    total_cbm = models.FloatField(null=True, blank=True)
    total_weight_lbs = models.FloatField(null=True, blank=True)
    total_cost = models.FloatField(null=True, blank=True)
    surcharges = JSONField(default=dict)
    surcharge_notes = JSONField(default=dict)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return str(self.invoice_number) + " - " + str(self.destination)
