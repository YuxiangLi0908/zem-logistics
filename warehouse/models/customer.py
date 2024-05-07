from django.db import models


class Customer(models.Model):
    zem_name = models.CharField(max_length=200)
    full_name = models.CharField(max_length=200, null=True)
    zem_code = models.CharField(max_length=20, null=True, blank=True)
    email = models.CharField(max_length=100, null=True, blank=True)
    phone = models.CharField(max_length=30, null=True, blank=True)
    note = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.zem_name} - {self.full_name}"