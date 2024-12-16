# Generated by Django 5.1.1 on 2024-12-12 01:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0133_pallet_price_quote'),
    ]

    operations = [
        migrations.RenameField(
            model_name='invoice',
            old_name='total_amount',
            new_name='amazon_amount',
        ),
        migrations.AddField(
            model_name='invoice',
            name='combine_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='local_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='preport_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='warehouse_amount',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
