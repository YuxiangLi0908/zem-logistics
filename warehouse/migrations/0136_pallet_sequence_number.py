# Generated by Django 5.1.1 on 2024-12-19 07:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0135_pallet_height_pallet_length_pallet_width"),
    ]

    operations = [
        migrations.AddField(
            model_name="pallet",
            name="sequence_number",
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
