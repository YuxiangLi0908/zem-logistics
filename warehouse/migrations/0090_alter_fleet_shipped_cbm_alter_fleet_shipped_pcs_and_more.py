# Generated by Django 4.2.7 on 2024-10-09 16:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0089_pallet_shipment_number"),
    ]

    operations = [
        migrations.AlterField(
            model_name="fleet",
            name="shipped_cbm",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
        migrations.AlterField(
            model_name="fleet",
            name="shipped_pcs",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
        migrations.AlterField(
            model_name="fleet",
            name="shipped_weight",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
        migrations.AlterField(
            model_name="shipment",
            name="shipped_cbm",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
        migrations.AlterField(
            model_name="shipment",
            name="shipped_pcs",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
        migrations.AlterField(
            model_name="shipment",
            name="shipped_weight",
            field=models.FloatField(blank=True, default=0, null=True),
        ),
    ]