from django.db import models


# 报价表版本管理
class MaerskPriceRate(models.Model):
    rate_id = models.CharField(max_length=200, null=True)
    is_user_exclusive = models.BooleanField(
        default=False,
        verbose_name="用户专属",
    )
    exclusive_user = models.CharField(
        max_length=2000,
        null=True,
        blank=True,
        verbose_name="专属用户",
    )
    effective_date = models.DateField(
        null=True,
        blank=True,
        verbose_name="生效日期",
    )
    increase_percentage = models.FloatField(null=False, verbose_name="涨幅百分比")

    def __str__(self) -> str:
        return str(self.effective_date) + "-" + str(self.exclusive_user) + "-" + self.rate_id
