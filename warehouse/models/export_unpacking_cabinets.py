from django.db import models

from warehouse.models.container import Container


class ExportUnpackingCabinets(models.Model):
    """
    记录柜号，第几次导出拆柜单，导出拆柜单时间表
    """
    export_unpacking_id = models.CharField(max_length=255, null=True)
    container_number = models.ForeignKey(
        Container,
        null=True,
        on_delete=models.SET_NULL,
        related_name="export_unpacking_cabinets"
    )
    download_num = models.IntegerField(null=True, blank=True, verbose_name="导出拆柜单次数")
    download_date = models.DateTimeField(null=True, blank=True, verbose_name="导出拆柜单时间")