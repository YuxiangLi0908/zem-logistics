# Generated by Django 4.2.7 on 2024-03-09 06:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0018_shipment_appointment_id_alter_container_weight_lbs"),
    ]

    operations = [
        migrations.AddField(
            model_name="shipment",
            name="load_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="total_cbm",
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="total_pallet",
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="total_pcs",
            field=models.FloatField(null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="total_weight",
            field=models.FloatField(null=True),
        ),
    ]
