# Generated by Django 4.2.7 on 2024-10-31 02:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0117_pallet_location_alter_pallet_fba_id_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="fleet",
            name="fleet_type",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
