# Generated by Django 5.1.1 on 2025-01-26 02:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0152_transferlocation_pallet_transfer_batch_number"),
    ]

    operations = [
        migrations.AddField(
            model_name="transferlocation",
            name="container_number",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
    ]
