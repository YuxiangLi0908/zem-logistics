# Generated by Django 4.2.7 on 2024-12-16 20:44

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0126_shipment_shipment_account"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="shipment",
            name="ARM_BOL",
        ),
        migrations.RemoveField(
            model_name="shipment",
            name="ARM_PRO",
        ),
        migrations.RemoveField(
            model_name="shipment",
            name="shipment_account",
        ),
        migrations.AddField(
            model_name="invoice",
            name="delivery_amount",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="invoice",
            name="direct_amount",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="invoice",
            name="preport_amount",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="invoice",
            name="warehouse_amount",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="order",
            name="invoice_reject",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="order",
            name="invoice_reject_reason",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="order",
            name="invoice_status",
            field=models.CharField(max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="delivery_type",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="price_quote",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name="InvoiceWarehouse",
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
                ("sorting", models.FloatField(blank=True, null=True)),
                ("intercept", models.FloatField(blank=True, null=True)),
                ("po_activation", models.FloatField(blank=True, null=True)),
                ("self_pickup", models.FloatField(blank=True, null=True)),
                ("re_pallet", models.FloatField(blank=True, null=True)),
                ("handling", models.FloatField(blank=True, null=True)),
                ("counting", models.FloatField(blank=True, null=True)),
                ("warehouse_rent", models.FloatField(blank=True, null=True)),
                ("specified_labeling", models.FloatField(blank=True, null=True)),
                ("inner_outer_box", models.FloatField(blank=True, null=True)),
                ("inner_outer_box_label", models.FloatField(blank=True, null=True)),
                ("pallet_label", models.FloatField(blank=True, null=True)),
                ("open_close_box", models.FloatField(blank=True, null=True)),
                ("destroy", models.FloatField(blank=True, null=True)),
                ("take_photo", models.FloatField(blank=True, null=True)),
                ("take_video", models.FloatField(blank=True, null=True)),
                ("repeated_operation_fee", models.FloatField(blank=True, null=True)),
                ("amount", models.FloatField(blank=True, null=True)),
                (
                    "invoice_number",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="warehouse.invoice",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="InvoicePreport",
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
                ("pickup", models.FloatField(blank=True, null=True)),
                ("chassis", models.FloatField(blank=True, null=True)),
                ("chassis_split", models.FloatField(blank=True, null=True)),
                ("prepull", models.FloatField(blank=True, null=True)),
                ("yard_storage", models.FloatField(blank=True, null=True)),
                ("handling_fee", models.FloatField(blank=True, null=True)),
                ("pier_pass", models.FloatField(blank=True, null=True)),
                ("congestion_fee", models.FloatField(blank=True, null=True)),
                ("hanging_crane", models.FloatField(blank=True, null=True)),
                ("dry_run", models.FloatField(blank=True, null=True)),
                ("exam_fee", models.FloatField(blank=True, null=True)),
                ("hazmat", models.FloatField(blank=True, null=True)),
                ("over_weight", models.FloatField(blank=True, null=True)),
                ("urgent_fee", models.FloatField(blank=True, null=True)),
                ("other_serive", models.FloatField(blank=True, null=True)),
                ("demurrage", models.FloatField(blank=True, null=True)),
                ("per_diem", models.FloatField(blank=True, null=True)),
                ("second_pickup", models.FloatField(blank=True, null=True)),
                ("amount", models.FloatField(blank=True, null=True)),
                (
                    "invoice_number",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="warehouse.invoice",
                    ),
                ),
            ],
        ),
    ]
