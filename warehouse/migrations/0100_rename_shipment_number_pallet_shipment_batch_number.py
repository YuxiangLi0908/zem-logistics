# Generated by Django 4.2.7 on 2024-10-24 17:05

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0099_pallet_fba_id_pallet_ref_id_pallet_shipping_mark"),
    ]

    operations = [
        migrations.RenameField(
            model_name="pallet",
            old_name="shipment_number",
            new_name="shipment_batch_number",
        ),
    ]
