# Generated by Django 5.1.1 on 2024-10-26 03:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0103_alter_pochecketaseven_packing_list'),
    ]

    operations = [
        migrations.AddField(
            model_name='pochecketaseven',
            name='time_status',
            field=models.BooleanField(default=False),
        ),
    ]