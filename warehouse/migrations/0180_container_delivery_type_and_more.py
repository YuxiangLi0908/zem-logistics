# Generated by Django 4.2.7 on 2025-04-10 03:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0179_rename_delivery_type_historicalinvoicepreport_invoice_type_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='container',
            name='delivery_type',
            field=models.CharField(choices=[('mixed', '混合'), ('public', '公仓'), ('other', '其他')], default='mixed', max_length=20),
        ),
        migrations.AddField(
            model_name='historicalcontainer',
            name='delivery_type',
            field=models.CharField(choices=[('mixed', '混合'), ('public', '公仓'), ('other', '其他')], default='mixed', max_length=20),
        ),
        migrations.AlterField(
            model_name='invoicestatus',
            name='stage',
            field=models.CharField(choices=[('unstarted', '未录入'), ('preport', '港前'), ('warehouse', '仓库'), ('delivery', '派送'), ('tobeconfirmed', '待确认'), ('confirmed', '已完成')], default='unstarted', max_length=20),
        ),
    ]
