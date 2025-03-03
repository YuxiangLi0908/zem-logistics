# Generated by Django 4.2.7 on 2024-04-17 07:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0036_remove_quote_cbm_remove_quote_height_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="quote",
            name="is_lift_gate",
            field=models.CharField(blank=True, max_length=10, null=True),
        ),
        migrations.AlterField(
            model_name="quote",
            name="is_oversize",
            field=models.CharField(blank=True, max_length=10, null=True),
        ),
        migrations.AlterField(
            model_name="quote",
            name="note",
            field=models.CharField(blank=True, max_length=1000, null=True),
        ),
    ]
