from django.db import models

class Shipment(models.Model):
    # double check the def of batch
    # e.g. goods sent to the same destination by a same carrier with multiple trucks
    shipment_batch_number = models.CharField(max_length=255, null=True)
    origin = models.CharField(max_length=255, null=True)
    destination = models.CharField(max_length=255, null=True)
    address = models.CharField(max_length=255, null=True)
    carrier = models.CharField(max_length=255, null=True)
    is_shipment_schduled = models.BooleanField(default=False)
    shipment_schduled_at = models.DateTimeField(null=True)
    shipment_appointment = models.DateField(null=True)
    is_shipped = models.BooleanField(default=False)
    shipped_at = models.DateTimeField(null=True)
    is_arrived = models.BooleanField(default=False)
    arrived_at = models.DateTimeField(null=True)

    def __str__(self) -> str:
        return self.shipment_batch_number