from django.db import models

class Container(models.Model):
    container_number = models.CharField(max_length=255, null=True)
    container_type = models.CharField(max_length=255)
    weight_lbs = models.FloatField(null=True)

    def __str__(self):
        return self.container_number