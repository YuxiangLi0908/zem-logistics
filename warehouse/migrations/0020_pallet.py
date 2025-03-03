# Generated by Django 4.2.7 on 2024-03-15 21:15

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0019_shipment_load_type_shipment_total_cbm_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="Pallet",
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
                ("pallet_id", models.CharField(max_length=255)),
                ("pcs", models.IntegerField(null=True)),
                ("cbm", models.FloatField(null=True)),
                (
                    "packing_list",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        to="warehouse.packinglist",
                    ),
                ),
            ],
        ),
    ]
