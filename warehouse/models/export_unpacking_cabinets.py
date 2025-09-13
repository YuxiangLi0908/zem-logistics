from django.db import models

from warehouse.models.container import Container


class ExportUnpackingCabinets(models.Model):
    container_number = models.ForeignKey(
        Container, null=True, on_delete=models.CASCADE, related_name="order"
    )
    download_num = models.IntegerField(null=True, blank=True, verbose_name="导出拆柜单次数")
    download_date = models.DateTimeField(null=True, blank=True, verbose_name="导出拆柜单时间")