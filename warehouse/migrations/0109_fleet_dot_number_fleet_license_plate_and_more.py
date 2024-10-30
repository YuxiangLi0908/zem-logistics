# Generated by Django 4.2.7 on 2024-10-28 03:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0108_alter_shipment_is_canceled"),
    ]

    operations = [
        migrations.AddField(
            model_name="fleet",
            name="dot_number",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="fleet",
            name="license_plate",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name="fleet",
            name="motor_carrier_number",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
