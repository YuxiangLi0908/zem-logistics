# Generated by Django 4.2.7 on 2024-04-08 03:47

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0030_alter_packinglist_destination'),
    ]

    operations = [
        migrations.AlterField(
            model_name='packinglist',
            name='fba_id',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
        migrations.AlterField(
            model_name='packinglist',
            name='ref_id',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
        migrations.AlterField(
            model_name='packinglist',
            name='shipping_mark',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
    ]
