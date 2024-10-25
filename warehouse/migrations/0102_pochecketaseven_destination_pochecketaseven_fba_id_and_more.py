# Generated by Django 5.1.1 on 2024-10-25 03:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0101_pochecketaseven_packing_list'),
    ]

    operations = [
        migrations.AddField(
            model_name='pochecketaseven',
            name='destination',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='pochecketaseven',
            name='fba_id',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
        migrations.AddField(
            model_name='pochecketaseven',
            name='ref_id',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
        migrations.AddField(
            model_name='pochecketaseven',
            name='shipping_mark',
            field=models.CharField(blank=True, max_length=400, null=True),
        ),
    ]