from django.db import models

class Vessel(models.Model):
    vessel_id = models.CharField(max_length=255, null=True)
    master_bill_of_lading = models.CharField(max_length=255, null=True, blank=True)
    origin_port = models.CharField(max_length=255, null=True, blank=True)
    destination_port = models.CharField(max_length=255, null=True, blank=True)
    shipping_line = models.CharField(max_length=255, null=True, blank=True)
    vessel = models.CharField(max_length=100, blank=True, null=True)
    voyage = models.CharField(max_length=100, blank=True, null=True)
    vessel_etd = models.DateField(null=True, blank=True)
    vessel_eta = models.DateField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['vessel_id']),
            models.Index(fields=['master_bill_of_lading']),
            models.Index(fields=['vessel']),
            models.Index(fields=['voyage']),
        ]

    def __str__(self) -> str:
        return self.vessel_id