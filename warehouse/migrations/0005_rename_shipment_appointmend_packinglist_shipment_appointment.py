# Generated by Django 4.2.7 on 2023-12-29 02:45

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0004_packinglist_shipment_appointmend'),
    ]

    operations = [
        migrations.RenameField(
            model_name='packinglist',
            old_name='shipment_appointmend',
            new_name='shipment_appointment',
        ),
    ]
