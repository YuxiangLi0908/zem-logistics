
from django.db import models
from .packing_list import PackingList
from .container import Container
from datetime import datetime, timedelta


class PoCheckEtaSeven(models.Model):
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    vessel_eta = models.DateField(null=True, blank=True)   #用于到港前一周页面进行到港时间的排序，规定紧急程度
    packing_list = models.ForeignKey(PackingList, null=True, blank=True, on_delete=models.SET_NULL)
    shipping_mark = models.CharField(max_length=400, null=True, blank=True)
    fba_id = models.CharField(max_length=400, null=True, blank=True)
    destination = models.CharField(max_length=255, null=True, blank=True)
    ref_id = models.CharField(max_length=400, null=True, blank=True)
    #pl有效无效，默认是false，如果未查验的话是根据checktime去确定
    status = models.BooleanField(default=False)   
    #最后一次查验时间，由于到港前一周的和提柜前一天的PO都要查验，所以两个时间分开存
    last_eta_checktime = models.DateTimeField(null=True, blank=True)  
    last_retrieval_checktime = models.DateTimeField(null=True, blank=True)
    #货物时间状态, true表示处于到港前一周, false表示提柜前一天，用于确定在待查验的哪个页面，所以PO查验总表数量=到港前一周+提柜前一天
    time_status = models.BooleanField(default=False)
    is_notified = models.BooleanField(default=False)#是否通知客户
    is_active = models.BooleanField(default=False)#是否激活
    handling_method = models.CharField(max_length=255, null=True, blank=True) #失效后的操作指令

    def __str__(self):
        return f"{self.packing_list}"
    
    @property
    def eta_status(self) -> str:
        today = datetime.now().date()
        if self.vessel_eta <= today + timedelta(days=1):
            return "past_due"
        else:
            return "on_time"


