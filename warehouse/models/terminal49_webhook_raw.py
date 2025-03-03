from django.db import models


class T49Raw(models.Model):
    received_at = models.DateTimeField(null=True, blank=True)
    ip_address = models.CharField(max_length=100, null=True, blank=True)
    header = models.JSONField(null=True, blank=True)
    body = models.JSONField(null=True, blank=True)
    payload = models.JSONField(null=True, blank=True)
