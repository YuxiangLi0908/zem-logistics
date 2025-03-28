from django.db import models
from simple_history.models import HistoricalRecords

from warehouse.models.container import Container
from warehouse.models.customer import Customer


class InvoiceStatement(models.Model):
    invoice_statement_id = models.CharField(max_length=200, null=True, blank=True)
    statement_amount = models.FloatField(null=True, blank=True)
    statement_date = models.DateField(null=True, blank=True)
    due_date = models.DateField(null=True, blank=True)
    invoice_terms = models.CharField(max_length=200, null=True, blank=True)
    customer = models.ForeignKey(
        Customer, null=True, blank=True, on_delete=models.SET_NULL
    )
    statement_link = models.CharField(max_length=2000, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return self.customer.zem_name + " - " + self.invoice_statement_id


class Invoice(models.Model):
    invoice_number = models.CharField(max_length=200, null=True, blank=True)
    invoice_date = models.DateField(null=True, blank=True)
    invoice_link = models.CharField(max_length=2000, null=True, blank=True)
    customer = models.ForeignKey(
        Customer, null=True, blank=True, on_delete=models.SET_NULL
    )
    container_number = models.ForeignKey(
        Container, null=True, blank=True, on_delete=models.SET_NULL
    )
    total_amount = models.FloatField(null=True, blank=True)
    preport_amount = models.FloatField(null=True, blank=True)
    warehouse_amount = models.FloatField(null=True, blank=True)
    delivery_amount = models.FloatField(null=True, blank=True)
    direct_amount = models.FloatField(null=True, blank=True)
    statement_id = models.ForeignKey(
        InvoiceStatement, null=True, blank=True, on_delete=models.SET_NULL
    )
    received_amount = models.FloatField(null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        try:
            return (
                self.customer.zem_name
                + " - "
                + self.container_number.container_number
                + " - "
                + self.invoice_number
            )
        except:
            return self.customer.zem_name + " - " + "None" + " - " + self.invoice_number


class InvoiceItem(models.Model):
    invoice_number = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    description = models.CharField(max_length=1000, null=True, blank=True)
    warehouse_code = models.CharField(max_length=200, null=True, blank=True)
    cbm = models.FloatField(null=True, blank=True)
    weight = models.FloatField(null=True, blank=True)
    qty = models.FloatField(null=True, blank=True)
    rate = models.FloatField(null=True, blank=True)
    amount = models.FloatField(null=True, blank=True)
    note = models.CharField(max_length=2000, null=True, blank=True)
    history = HistoricalRecords()

    def __str__(self) -> str:
        return (
            self.invoice_number.__str__()
            + " - "
            + str(self.description)
            + " - "
            + str(self.warehouse_code)
        )
