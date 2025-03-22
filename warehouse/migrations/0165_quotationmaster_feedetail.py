# Generated by Django 4.2.7 on 2025-03-05 01:50

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0164_invoicedelivery_surcharge_notes_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="QuotationMaster",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("quotation_id", models.CharField(max_length=200, null=True)),
                ("upload_date", models.DateField(blank=True, null=True)),
                ("version", models.CharField(blank=True, max_length=2000, null=True)),
            ],
        ),
        migrations.CreateModel(
            name="FeeDetail",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("fee_detail_id", models.CharField(max_length=200, null=True)),
                ("fee_type", models.CharField(max_length=255, null=True)),
                ("warehouse", models.CharField(blank=True, max_length=20, null=True)),
                ("details", models.JSONField(default=dict)),
                (
                    "quotation_id",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="fee_details",
                        to="warehouse.quotationmaster",
                    ),
                ),
            ],
        ),
    ]
