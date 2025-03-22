# Generated by Django 4.2.7 on 2025-03-12 08:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0160_alter_historicalpallet_fba_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="historicalshipment",
            name="express_number",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="shipment",
            name="express_number",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
