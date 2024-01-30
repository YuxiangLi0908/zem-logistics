from django.db import models

class Customer(models.Model):
    zem_name = models.CharField(max_length=200)
    full_name = models.CharField(max_length=200, null=True)

    def __str__(self) -> str:
        return self.zem_name