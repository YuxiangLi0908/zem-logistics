# Generated by Django 4.2.7 on 2024-10-24 17:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0100_rename_shipment_number_pallet_shipment_batch_number"),
    ]

    operations = [
        migrations.AddField(
            model_name="pallet",
            name="address",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="zipcode",
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
    ]