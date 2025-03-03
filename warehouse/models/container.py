from django.db import models


class Container(models.Model):
    container_number = models.CharField(max_length=255, null=True)
    container_type = models.CharField(max_length=255, null=True, blank=True)
    weight_lbs = models.FloatField(null=True, blank=True)
    is_special_container = models.BooleanField(default=False, null=True, blank=True)
    note = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self):
        return self.container_number
