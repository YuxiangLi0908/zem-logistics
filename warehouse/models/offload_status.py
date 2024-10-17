from django.db import models
from warehouse.models.container import Container
from warehouse.models.offload import Offload


class AbnormalOffloadStatus(models.Model):
    offload = models.ForeignKey(Offload, null=True, on_delete=models.CASCADE, related_name="offload_status")
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE, related_name="offload_status")
    is_resolved = models.BooleanField(default=False)
    destination = models.CharField(max_length=255, null=True, blank=True)
    pcs_reported = models.IntegerField(null=True, blank=True)
    pcs_actual = models.IntegerField(null=True, blank=True)
    note = models.CharField(max_length=1000, null=True)

    def __str__(self) -> str:
        return self.container_number + " - " + self.destination + "-" + self.is_resolved
