# Generated by Django 4.2.7 on 2024-02-15 04:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0014_alter_container_container_type_alter_order_eta'),
    ]

    operations = [
        migrations.AlterField(
            model_name='retrieval',
            name='retrieval_id',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]