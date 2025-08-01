# Generated by Django 4.2.7 on 2025-04-22 02:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0190_alter_historicalinvoicedelivery_total_pallet_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="historicalinvoicedelivery",
            name="surcharge_notes",
        ),
        migrations.RemoveField(
            model_name="historicalinvoicedelivery",
            name="surcharges",
        ),
        migrations.RemoveField(
            model_name="invoicedelivery",
            name="surcharge_notes",
        ),
        migrations.RemoveField(
            model_name="invoicedelivery",
            name="surcharges",
        ),
        migrations.AddField(
            model_name="historicalinvoicedelivery",
            name="note",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
        migrations.AddField(
            model_name="invoicedelivery",
            name="note",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
    ]
