from django.contrib.auth.hashers import check_password, make_password
from django.contrib.auth.models import User
from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.customer import Customer


class Transaction(models.Model):
    TRANSACTION_TYPES = (
        ("recharge", "充值"),
        ("write_off", "核销"),
    )
    customer = models.ForeignKey(
        Customer, on_delete=models.CASCADE, related_name="transactions"
    )
    amount = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True, verbose_name="交易金额"
    )
    transaction_type = models.CharField(max_length=20, choices=TRANSACTION_TYPES)
    note = models.CharField(max_length=500, null=True, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(null=True, blank=True)
    image_link = models.CharField(max_length=2000, null=True, blank=True)
    history = HistoricalRecords()

    class Meta:
        indexes = [
            models.Index(fields=["customer"]),
        ]

    def __str__(self) -> str:
        return (
            str(self.customer)
            + " - "
            + self.transaction_type
            + " - "
            + str(self.created_at)
        )
