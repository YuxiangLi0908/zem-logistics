# Generated by Django 4.2.7 on 2025-04-16 02:10

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0187_quotationmaster_effective_date_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='feedetail',
            name='niche_warehouse',
            field=models.CharField(max_length=2000, null=True),
        ),
    ]
