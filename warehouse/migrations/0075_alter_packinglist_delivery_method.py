# Generated by Django 4.2.7 on 2024-07-31 19:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0074_order_vessel_id"),
    ]

    operations = [
        migrations.AlterField(
            model_name="packinglist",
            name="delivery_method",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
