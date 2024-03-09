from django.db import models

class Shipment(models.Model):
    # double check the def of batch
    # e.g. goods sent to the same destination by a same carrier with multiple trucks
    shipment_batch_number = models.CharField(max_length=255, null=True)
    appointment_id = models.CharField(max_length=255, null=True, blank=True)
    origin = models.CharField(max_length=255, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    address = models.CharField(max_length=255, null=True, blank=True)
    carrier = models.CharField(max_length=255, null=True, blank=True)
    is_shipment_schduled = models.BooleanField(default=False, blank=True)
    shipment_schduled_at = models.DateTimeField(null=True, blank=True)
    shipment_appointment = models.DateField(null=True, blank=True)
    is_shipped = models.BooleanField(default=False, blank=True)
    shipped_at = models.DateTimeField(null=True, blank=True)
    is_arrived = models.BooleanField(default=False, blank=True)
    arrived_at = models.DateTimeField(null=True, blank=True)

    def __str__(self) -> str:
        return self.shipment_batch_number