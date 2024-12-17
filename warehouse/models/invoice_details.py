

from django.db import models
from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.invoice import Invoice
    
class InvoicePreport(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    pickup = models.FloatField(null=True, blank=True)
    chassis = models.FloatField(null=True, blank=True)
    chassis_split = models.FloatField(null=True, blank=True)
    prepull = models.FloatField(null=True, blank=True)
    yard_storage = models.FloatField(null=True, blank=True)
    handling_fee = models.FloatField(null=True, blank=True)
    pier_pass = models.FloatField(null=True, blank=True)
    congestion_fee = models.FloatField(null=True, blank=True)
    hanging_crane = models.FloatField(null=True, blank=True)
    dry_run = models.FloatField(null=True, blank=True)
    exam_fee = models.FloatField(null=True, blank=True)
    hazmat = models.FloatField(null=True, blank=True)
    over_weight = models.FloatField(null=True, blank=True)
    urgent_fee = models.FloatField(null=True, blank=True)
    other_serive = models.FloatField(null=True, blank=True)
    demurrage = models.FloatField(null=True, blank=True)
    per_diem = models.FloatField(null=True, blank=True)
    second_pickup = models.FloatField(null=True, blank=True)
    amount = models.FloatField(null=True, blank=True)

    def __str__(self) -> str:
        return str(self.invoice_number) 

class InvoiceWarehouse(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    sorting = models.FloatField(null=True, blank=True)
    intercept = models.FloatField(null=True, blank=True)
    po_activation = models.FloatField(null=True, blank=True)
    self_pickup = models.FloatField(null=True, blank=True)
    re_pallet = models.FloatField(null=True, blank=True)
    handling = models.FloatField(null=True, blank=True)
    counting = models.FloatField(null=True, blank=True)
    warehouse_rent = models.FloatField(null=True, blank=True)
    specified_labeling = models.FloatField(null=True, blank=True)
    inner_outer_box = models.FloatField(null=True, blank=True)
    inner_outer_box_label = models.FloatField(null=True, blank=True)
    pallet_label = models.FloatField(null=True, blank=True)
    open_close_box = models.FloatField(null=True, blank=True)
    destroy = models.FloatField(null=True, blank=True)
    take_photo = models.FloatField(null=True, blank=True)
    take_video = models.FloatField(null=True, blank=True)
    repeated_operation_fee = models.FloatField(null=True, blank=True)
    amount = models.FloatField(null=True, blank=True)

    def __str__(self) -> str:
        return str(self.invoice_number)

class InvoiceDelivery(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    
    amount = models.FloatField(null=True, blank=True)

    def __str__(self) -> str:
        return str(self.invoice_number)
