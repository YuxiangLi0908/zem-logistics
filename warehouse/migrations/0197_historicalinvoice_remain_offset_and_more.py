# Generated by Django 4.2.7 on 2025-05-23 02:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0196_remove_historicaltransaction_image_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="historicalinvoice",
            name="remain_offset",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="invoice",
            name="remain_offset",
            field=models.FloatField(blank=True, null=True),
        ),
    ]
