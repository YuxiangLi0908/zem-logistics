from django.db import models

    
#报价表版本管理
class QuotationMaster(models.Model):
    quotation_id = models.CharField(max_length=200, null=True)
    upload_date = models.DateField(null=True, blank=True)
    version = models.CharField(max_length=2000, null=True, blank=True)
    active = models.BooleanField(default=True)
    # preport_fee = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # warehouse_fee = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # nj_local= models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # nj_walmart = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # nj_combina = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # sav_local = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # sav_walmart = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # sav_combina = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # la_local = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # la_walmart = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    # la_combina = models.ForeignKey('FeeDetail', null=True, blank=True, on_delete=models.SET_NULL)
    def __str__(self) -> str:
        return self.version


