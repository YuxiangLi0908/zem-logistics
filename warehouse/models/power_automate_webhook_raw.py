from django.db import models


class PowerAutomateWebhookRaw(models.Model):
    received_at = models.DateTimeField(null=True, blank=True)
    ip_address = models.CharField(max_length=100, null=True, blank=True)
    header = models.JSONField(null=True, blank=True)
    body = models.JSONField(null=True, blank=True)
    payload = models.JSONField(null=True, blank=True)

    class Meta:
        verbose_name = "Power Automate Webhook Raw"
        verbose_name_plural = "Power Automate Webhook Raw"

    def __str__(self):
        return f"Power Automate Webhook - {self.received_at}"
