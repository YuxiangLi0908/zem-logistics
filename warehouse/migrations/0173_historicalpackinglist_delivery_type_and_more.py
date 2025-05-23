# Generated by Django 4.2.7 on 2025-04-08 03:43

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0172_historicalinvoicewarehouse_split_delivery_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="historicalpackinglist",
            name="delivery_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="historicalpallet",
            name="delivery_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="packinglist",
            name="delivery_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="pallet",
            name="delivery_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
