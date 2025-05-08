from django.db import models


# 报价表版本管理
class QuotationMaster(models.Model):
    quotation_id = models.CharField(max_length=200, null=True)
    upload_date = models.DateField(null=True, blank=True)
    version = models.CharField(max_length=2000, null=True, blank=True)
    active = models.BooleanField(default=True)
    filename = models.CharField(max_length=2000, null=True, blank=True)
    is_user_exclusive = models.BooleanField(
        default=False,
        verbose_name="用户专属",
    )
    exclusive_user = models.CharField(max_length=2000, null=True, blank=True,verbose_name="专属用户",)
    effective_date = models.DateField(
        null=True,
        blank=True,
        verbose_name="生效日期",
    )
    def __str__(self) -> str:
        return str(self.effective_date)+'-'+self.version
