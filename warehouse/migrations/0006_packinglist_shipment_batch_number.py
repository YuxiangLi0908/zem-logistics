# Generated by Django 4.2.7 on 2023-12-29 03:06

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0005_rename_shipment_appointmend_packinglist_shipment_appointment'),
    ]

    operations = [
        migrations.AddField(
            model_name='packinglist',
            name='shipment_batch_number',
            field=models.CharField(max_length=255, null=True),
        ),
    ]