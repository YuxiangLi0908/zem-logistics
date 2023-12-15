# Generated by Django 4.2.7 on 2023-11-27 04:10

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0002_container_packinglist_remove_order_departure_port_and_more'),
    ]

    operations = [
        # migrations.AddField(
        #     model_name='container',
        #     name='id',
        #     field=models.BigAutoField(default=0, primary_key=True, serialize=False),
        # ),
        migrations.AddField(
            model_name='packinglist',
            name='container_id',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='warehouse.container'),
        ),
        migrations.AlterField(
            model_name='container',
            name='container_id',
            field=models.CharField(max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='packinglist',
            name='id',
            field=models.BigAutoField(primary_key=True, serialize=False),
        ),
        migrations.DeleteModel(
            name='Order',
        ),
        migrations.DeleteModel(
            name='Warehouse',
        ),
    ]
