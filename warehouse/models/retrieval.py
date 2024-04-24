from django.db import models

class Retrieval(models.Model):
    retrieval_id = models.CharField(max_length=255, null=True)
    shipping_order_number = models.CharField(max_length=255, null=True, blank=True)
    retrive_by_zem = models.BooleanField(default=True, blank=True)
    origin_port = models.CharField(max_length=255, null=True, blank=True)
    destination_port = models.CharField(max_length=255, null=True, blank=True)
    retrieval_location = models.CharField(max_length=255, null=True, blank=True)
    shipping_line = models.CharField(max_length=255, null=True, blank=True)
    scheduled_at = models.DateTimeField(null=True, blank=True)
    target_retrieval_timestamp = models.DateTimeField(null=True, blank=True)
    actual_retrieval_timestamp = models.DateTimeField(null=True, blank=True)

    def __str__(self) -> str:
        return self.retrieval_id