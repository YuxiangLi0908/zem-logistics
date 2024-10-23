# Generated by Django 4.2.7 on 2024-10-23 20:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0098_abnormaloffloadstatus_confirmed_by_warehouse"),
    ]

    operations = [
        migrations.AddField(
            model_name="pallet",
            name="fba_id",
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="ref_id",
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="shipping_mark",
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
    ]
