# Generated by Django 4.2.7 on 2024-10-29 03:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0113_alter_fleet_is_canceled_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="shipment",
            name="shipment_batch_number",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
