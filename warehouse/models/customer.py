from django.contrib.auth.hashers import check_password, make_password
from django.db import models
from simple_history.models import HistoricalRecords


class Customer(models.Model):
    zem_name = models.CharField(max_length=200)
    full_name = models.CharField(max_length=200, null=True)
    accounting_name = models.CharField(max_length=200, null=True)
    zem_code = models.CharField(max_length=20, null=True, blank=True)
    email = models.CharField(max_length=100, null=True, blank=True)
    phone = models.CharField(max_length=30, null=True, blank=True)
    note = models.CharField(max_length=500, null=True, blank=True)
    address = models.CharField(max_length=500, null=True, blank=True)
    username = models.CharField(max_length=150, unique=True, null=True, blank=True)
    password = models.CharField(max_length=255, null=True, blank=True)
    balance = models.FloatField(null=True, blank=True,default=0.0)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["zem_name"]),
        ]

    def set_password(self, raw_password: str) -> None:
        """Hashes and stores the password."""
        self.password = make_password(raw_password)

    def check_password(self, raw_password: str) -> bool:
        """Checks if the given password matches the stored hash."""
        return check_password(raw_password, self.password)

    def __str__(self) -> str:
        return f"{self.zem_name}" if self.zem_name else f"{self.full_name}"
