# Generated by Django 4.2.7 on 2025-04-09 03:46

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0175_remove_historicalorder_new_invoice_status_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='historicalorder',
            name='sub_status',
        ),
        migrations.RemoveField(
            model_name='order',
            name='sub_status',
        ),
        migrations.AddField(
            model_name='historicalinvoice',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='historicalinvoicedelivery',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='historicalinvoicepreport',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='historicalinvoicewarehouse',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='invoice',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='invoicedelivery',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='invoicepreport',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.AddField(
            model_name='invoicewarehouse',
            name='delivery_type',
            field=models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], default='receivable', max_length=20),
        ),
        migrations.CreateModel(
            name='InvoiceStatus',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('invoice_type', models.CharField(choices=[('receivable', '应收账单'), ('payable', '应付账单')], max_length=20)),
                ('stage', models.CharField(choices=[('preport', '港前'), ('warehouse', '仓库'), ('delivery', '派送'), ('tobeconfirmed', '待确认'), ('confirmed', '已完成')], max_length=20)),
                ('stage_public', models.CharField(choices=[('pending', '待处理'), ('completed', '已完成'), ('rejected', '已驳回')], default='pending', max_length=20)),
                ('stage_other', models.CharField(choices=[('pending', '待处理'), ('completed', '已完成'), ('rejected', '已驳回')], default='pending', max_length=20)),
                ('is_rejected', models.BooleanField(default=False)),
                ('reject_reason', models.TextField(blank=True)),
                ('container_number', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='invoice_statuses', to='warehouse.container')),
            ],
        ),
        migrations.AddConstraint(
            model_name='invoicestatus',
            constraint=models.UniqueConstraint(fields=('container_number', 'invoice_type'), name='unique_container_invoice'),
        ),
    ]
