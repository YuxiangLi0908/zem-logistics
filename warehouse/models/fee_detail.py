from django.db import models
from django.db.models import JSONField

from warehouse.models.quotation_master import QuotationMaster


class FeeDetail(models.Model):
    fee_detail_id = models.CharField(max_length=200, null=True)
    quotation_id = models.ForeignKey(
        QuotationMaster,
        on_delete=models.CASCADE,
        related_name="fee_details",
        db_index=True,
    )
    fee_type = models.CharField(max_length=255, null=True)
    warehouse = models.CharField(max_length=20, null=True, blank=True)
    details = JSONField(default=dict)
    niche_warehouse = models.CharField(max_length=2000, null=True)

    def __str__(self) -> str:
        return f"{self.fee_type} ({self.quotation_id})"
