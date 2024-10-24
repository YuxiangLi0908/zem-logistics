
from django.db import models
from .packing_list import PackingList
from .container import Container


class PoCheckEtaSeven(models.Model):
    container_number = models.ForeignKey(Container, null=True, on_delete=models.CASCADE)
    status = models.BooleanField(default=False)
    last_checktime = models.DateTimeField(null=True, blank=True)
    is_notified = models.BooleanField(default=False)
    is_active = models.BooleanField(default=False)
    handling_method = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"{self.container_number}"


