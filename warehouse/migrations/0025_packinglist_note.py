# Generated by Django 4.2.7 on 2024-03-21 15:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0024_alter_offload_offload_at_alter_offload_total_pallet'),
    ]

    operations = [
        migrations.AddField(
            model_name='packinglist',
            name='note',
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
    ]
