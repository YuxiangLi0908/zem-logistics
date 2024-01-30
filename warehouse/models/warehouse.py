from django.db import models
    
class ZemWarehouse(models.Model):
    name = models.CharField(max_length=200)
    address = models.CharField(max_length=200, null=True)

    def __str__(self) -> str:
        return self.name