# Generated by Django 4.2.7 on 2025-04-09 08:24

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0177_historicalorder_payable_status_and_more'),
    ]

    operations = [
        migrations.RenameField(
            model_name='historicalinvoice',
            old_name='delivery_amount',
            new_name='payable_delivery_amount',
        ),
        migrations.RenameField(
            model_name='historicalinvoice',
            old_name='direct_amount',
            new_name='payable_direct_amount',
        ),
        migrations.RenameField(
            model_name='historicalinvoice',
            old_name='preport_amount',
            new_name='payable_preport_amount',
        ),
        migrations.RenameField(
            model_name='historicalinvoice',
            old_name='total_amount',
            new_name='payable_total_amount',
        ),
        migrations.RenameField(
            model_name='historicalinvoice',
            old_name='warehouse_amount',
            new_name='payable_warehouse_amount',
        ),
        migrations.RenameField(
            model_name='invoice',
            old_name='delivery_amount',
            new_name='payable_delivery_amount',
        ),
        migrations.RenameField(
            model_name='invoice',
            old_name='direct_amount',
            new_name='payable_direct_amount',
        ),
        migrations.RenameField(
            model_name='invoice',
            old_name='preport_amount',
            new_name='payable_preport_amount',
        ),
        migrations.RenameField(
            model_name='invoice',
            old_name='total_amount',
            new_name='payable_total_amount',
        ),
        migrations.RenameField(
            model_name='invoice',
            old_name='warehouse_amount',
            new_name='payable_warehouse_amount',
        ),
        migrations.RemoveField(
            model_name='historicalinvoice',
            name='delivery_type',
        ),
        migrations.RemoveField(
            model_name='invoice',
            name='delivery_type',
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='receivable_delivery_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='receivable_direct_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='receivable_preport_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='receivable_total_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='receivable_warehouse_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='receivable_delivery_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='receivable_direct_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='receivable_preport_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='receivable_total_amount',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='invoice',
            name='receivable_warehouse_amount',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
