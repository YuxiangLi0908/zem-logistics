# Generated by Django 4.2.7 on 2024-10-26 18:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0107_shipment_is_canceled"),
    ]

    operations = [
        migrations.AlterField(
            model_name="shipment",
            name="is_canceled",
            field=models.BooleanField(blank=True, default=False),
        ),
    ]
