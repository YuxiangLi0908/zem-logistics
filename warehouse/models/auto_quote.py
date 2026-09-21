import uuid

from django.conf import settings
from django.db import models


GROUPS = [(name, name) for name in ("LA", "SAV", "NJ")]


class AutoQuoteAddress(models.Model):
    group = models.CharField(max_length=3, choices=GROUPS, db_index=True)
    city = models.CharField(max_length=120)
    state = models.CharField(max_length=2)
    zipcode = models.CharField(max_length=10)
    address = models.CharField(max_length=500)
    distance_miles = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    fingerprint = models.CharField(max_length=64)
    import_id = models.UUIDField(default=uuid.uuid4)
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["group", "fingerprint"], name="auto_quote_address_unique")]
        ordering = ["id"]

    def snapshot(self):
        return {"id": self.pk, "group": self.group, "city": self.city, "state": self.state,
                "zipcode": self.zipcode, "address": self.address,
                "distance_miles": str(self.distance_miles) if self.distance_miles is not None else ""}


class AutoQuoteProfile(models.Model):
    fingerprint = models.CharField(max_length=64, unique=True)
    configuration = models.JSONField(default=dict)
    origin_label = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)

    @property
    def code(self):
        return f"AQ{self.pk:06d}"


class AutoQuoteBatch(models.Model):
    group = models.CharField(max_length=3, choices=GROUPS)
    parameters = models.JSONField(default=dict)
    profile = models.ForeignKey(AutoQuoteProfile, null=True, blank=True, on_delete=models.PROTECT)
    operator = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL)
    # Stable browser submission ID prevents duplicate batches after a network retry.
    submission_id = models.UUIDField(unique=True)
    status = models.CharField(max_length=20, default="queued", db_index=True)
    stop_requested = models.BooleanField(default=False)
    parent = models.ForeignKey("self", null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-id"]


class AutoQuoteItem(models.Model):
    batch = models.ForeignKey(AutoQuoteBatch, related_name="items", on_delete=models.CASCADE)
    address_snapshot = models.JSONField(default=dict)
    status = models.CharField(max_length=20, default="pending", db_index=True)
    result = models.JSONField(default=dict, blank=True)
    request_payload = models.JSONField(default=dict, blank=True)
    quote_uuid = models.CharField(max_length=200, blank=True, default="")
    error = models.TextField(blank=True, default="")
    lease_token = models.UUIDField(null=True)
    lease_until = models.DateTimeField(null=True, db_index=True)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)
    attempts = models.PositiveIntegerField(default=0)
    analysis_version = models.PositiveSmallIntegerField(default=0, db_index=True)

    class Meta:
        ordering = ["id"]


class AutoQuoteWorkerState(models.Model):
    """Singleton lock serializes claims and enforces a global in-flight limit."""
    heartbeat_at = models.DateTimeField(null=True)


class AutoQuotePrice(models.Model):
    item = models.ForeignKey(AutoQuoteItem, related_name="prices", on_delete=models.CASCADE)
    row_number = models.PositiveIntegerField()
    platform = models.CharField(max_length=20)
    carrier = models.CharField(max_length=200)
    carrier_code = models.CharField(max_length=100, blank=True)
    service = models.CharField(max_length=200, blank=True)
    service_code = models.CharField(max_length=100, blank=True)
    series_key = models.CharField(max_length=64, db_index=True)
    currency = models.CharField(max_length=3)
    currency_assumed = models.BooleanField(default=False)
    price = models.DecimalField(max_digits=16, decimal_places=2)
    comparable = models.BooleanField(default=True)
    platform_complete = models.BooleanField(default=False)
    raw = models.JSONField(default=dict)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["item", "row_number"], name="auto_quote_price_row_unique")]
