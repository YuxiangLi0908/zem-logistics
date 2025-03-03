# Generated by Django 4.2.7 on 2023-12-28 20:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="packinglist",
            name="is_shipped",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="packinglist",
            name="shipment_schduled_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name="packinglist",
            name="shipped_at",
            field=models.DateTimeField(null=True),
        ),
    ]
