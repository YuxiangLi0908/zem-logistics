# Generated by Django 4.2.7 on 2024-02-15 04:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0015_alter_retrieval_retrieval_id'),
    ]

    operations = [
        migrations.AlterField(
            model_name='retrieval',
            name='actual_retrieval_timestamp',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='destination',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='origin',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='retrieval_location',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='retrive_by_zem',
            field=models.BooleanField(blank=True, default=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='scheduled_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='shipping_line',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='shipping_order_number',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='retrieval',
            name='target_retrieval_timestamp',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]