# Generated by Django 5.1.1 on 2024-12-03 07:17

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0129_invoicepreport'),
    ]

    operations = [
        migrations.RenameField(
            model_name='invoicepreport',
            old_name='Yard_storage',
            new_name='yard_storage',
        ),
        migrations.RemoveField(
            model_name='invoicepreport',
            name='container_number',
        ),
    ]
