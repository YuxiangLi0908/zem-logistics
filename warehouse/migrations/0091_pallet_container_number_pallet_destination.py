# Generated by Django 5.1.1 on 2024-10-17 08:56

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0090_alter_fleet_shipped_cbm_alter_fleet_shipped_pcs_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="pallet",
            name="container_number",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="warehouse.container",
            ),
        ),
        migrations.AddField(
            model_name="pallet",
            name="destination",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
