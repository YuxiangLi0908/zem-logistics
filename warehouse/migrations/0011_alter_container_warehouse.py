# Generated by Django 4.2.7 on 2023-12-13 18:49

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0010_zemwarehouse_container_order_type'),
    ]

    operations = [
        migrations.AlterField(
            model_name='container',
            name='warehouse',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='warehouse.zemwarehouse'),
        ),
    ]
