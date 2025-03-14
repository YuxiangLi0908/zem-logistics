# Generated by Django 4.2.7 on 2024-04-06 13:36

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("warehouse", "0027_alter_packinglist_address"),
    ]

    operations = [
        migrations.AlterField(
            model_name="packinglist",
            name="address",
            field=models.CharField(blank=True, max_length=2000, null=True),
        ),
        migrations.AlterField(
            model_name="packinglist",
            name="n_pallet",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="packinglist",
            name="product_name",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name="packinglist",
            name="unit_weight_lbs",
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="packinglist",
            name="zipcode",
            field=models.CharField(blank=True, max_length=20, null=True),
        ),
    ]
