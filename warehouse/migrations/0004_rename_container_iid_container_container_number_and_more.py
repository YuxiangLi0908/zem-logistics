# Generated by Django 4.2.7 on 2023-12-15 04:47

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0003_zemwarehouse_address'),
    ]

    operations = [
        migrations.RenameField(
            model_name='container',
            old_name='container_iid',
            new_name='container_number',
        ),
        migrations.RenameField(
            model_name='packinglist',
            old_name='container_iid',
            new_name='container_number',
        ),
    ]
