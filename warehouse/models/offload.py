from django.db import models

class Offload(models.Model):
    offload_id = models.CharField(max_length=255, null=True)
    offload_required = models.BooleanField(default=True)
    offload_at = models.DateTimeField(null=True, blank=True)
    total_pallet = models.IntegerField(null=True, blank=True)

    def __str__(self) -> str:
        return self.offload_id