# Generated by Django 5.1.1 on 2024-12-17 07:15

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0129_invoicedelivery"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="pallet",
            name="price_quote",
        ),
        migrations.AddField(
            model_name="pallet",
            name="invoice_delivery",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                to="warehouse.invoicedelivery",
            ),
        ),
    ]
