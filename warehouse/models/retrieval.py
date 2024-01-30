from django.db import models

class Retrieval(models.Model):
    retrieval_id = models.CharField(max_length=255, null=True)
    shipping_order_number = models.CharField(max_length=255, null=True)
    retrive_by_zem = models.BooleanField(default=True)
    origin = models.CharField(max_length=255, null=True)
    destination = models.CharField(max_length=255, null=True)
    retrieval_location = models.CharField(max_length=255, null=True)
    shipping_line = models.CharField(max_length=255, null=True)
    scheduled_at = models.DateTimeField(null=True)
    target_retrieval_timestamp = models.DateTimeField(null=True)
    actual_retrieval_timestamp = models.DateTimeField(null=True)

    def __str__(self) -> str:
        return self.retrieval_id