# Generated by Django 4.2.7 on 2024-03-16 04:20

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0023_pallet_weight_lbs_alter_pallet_cbm"),
    ]

    operations = [
        migrations.AlterField(
            model_name="offload",
            name="offload_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="offload",
            name="total_pallet",
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
