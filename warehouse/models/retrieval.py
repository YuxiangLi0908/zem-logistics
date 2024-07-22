from django.db import models

class Retrieval(models.Model):
    retrieval_id = models.CharField(max_length=255, null=True)
    shipping_order_number = models.CharField(max_length=255, null=True, blank=True)
    master_bill_of_lading = models.CharField(max_length=255, null=True, blank=True)
    retrive_by_zem = models.BooleanField(default=True, blank=True)
    retrieval_carrier = models.CharField(max_length=100, null=True, blank=True)
    origin_port = models.CharField(max_length=255, null=True, blank=True)
    destination_port = models.CharField(max_length=255, null=True, blank=True)
    retrieval_location = models.CharField(max_length=255, null=True, blank=True)
    shipping_line = models.CharField(max_length=255, null=True, blank=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    target_retrieval_timestamp = models.DateTimeField(null=True, blank=True)
    actual_retrieval_timestamp = models.DateTimeField(null=True, blank=True)
    trucking_fee = models.FloatField(null=True, blank=True)
    chassis_fee = models.FloatField(null=True, blank=True)
    is_trucking_fee_paid = models.BooleanField(default=False, blank=True)
    is_chassis_fee_paid = models.BooleanField(default=False, blank=True)
    trucking_fee_paid_at = models.FloatField(null=True, blank=True)
    chassis_fee_paid_at = models.FloatField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['retrieval_id']),
            models.Index(fields=['target_retrieval_timestamp']),
        ]

    def __str__(self) -> str:
        return self.retrieval_id