# Generated by Django 4.2.7 on 2025-07-23 07:09

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import simple_history.models


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('warehouse', '0209_remove_historicalpallet_expense_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='HistoricalPalletDestroyed',
            fields=[
                ('id', models.BigIntegerField(auto_created=True, blank=True, db_index=True, verbose_name='ID')),
                ('destination', models.CharField(blank=True, max_length=255, null=True)),
                ('address', models.CharField(blank=True, max_length=2000, null=True)),
                ('zipcode', models.CharField(blank=True, max_length=20, null=True)),
                ('delivery_method', models.CharField(blank=True, max_length=255, null=True)),
                ('delivery_type', models.CharField(blank=True, max_length=255, null=True)),
                ('palletDes_id', models.CharField(blank=True, max_length=255, null=True)),
                ('PO_ID', models.CharField(blank=True, max_length=20, null=True)),
                ('shipping_mark', models.CharField(blank=True, max_length=4000, null=True)),
                ('fba_id', models.CharField(blank=True, max_length=4000, null=True)),
                ('ref_id', models.CharField(blank=True, max_length=4000, null=True)),
                ('pcs', models.IntegerField(blank=True, null=True)),
                ('sequence_number', models.CharField(blank=True, max_length=2000, null=True)),
                ('length', models.FloatField(blank=True, null=True)),
                ('width', models.FloatField(blank=True, null=True)),
                ('height', models.FloatField(blank=True, null=True)),
                ('cbm', models.FloatField(blank=True, null=True)),
                ('weight_lbs', models.FloatField(blank=True, null=True)),
                ('abnormal_palletization', models.BooleanField(blank=True, default=False, null=True)),
                ('po_expired', models.BooleanField(blank=True, default=False, null=True)),
                ('note', models.CharField(blank=True, max_length=2000, null=True)),
                ('priority', models.CharField(blank=True, max_length=20, null=True)),
                ('location', models.CharField(blank=True, max_length=100, null=True)),
                ('contact_name', models.CharField(blank=True, max_length=255, null=True)),
                ('history_id', models.AutoField(primary_key=True, serialize=False)),
                ('history_date', models.DateTimeField(db_index=True)),
                ('history_change_reason', models.CharField(max_length=100, null=True)),
                ('history_type', models.CharField(choices=[('+', 'Created'), ('~', 'Changed'), ('-', 'Deleted')], max_length=1)),
                ('container_number', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='warehouse.container')),
                ('history_user', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='+', to=settings.AUTH_USER_MODEL)),
                ('packing_list', models.ForeignKey(blank=True, db_constraint=False, null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='warehouse.packinglist')),
            ],
            options={
                'verbose_name': 'historical pallet destroyed',
                'verbose_name_plural': 'historical pallet destroyeds',
                'ordering': ('-history_date', '-history_id'),
                'get_latest_by': ('history_date', 'history_id'),
            },
            bases=(simple_history.models.HistoricalChanges, models.Model),
        ),
        migrations.CreateModel(
            name='PalletDestroyed',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('destination', models.CharField(blank=True, max_length=255, null=True)),
                ('address', models.CharField(blank=True, max_length=2000, null=True)),
                ('zipcode', models.CharField(blank=True, max_length=20, null=True)),
                ('delivery_method', models.CharField(blank=True, max_length=255, null=True)),
                ('delivery_type', models.CharField(blank=True, max_length=255, null=True)),
                ('palletDes_id', models.CharField(blank=True, max_length=255, null=True)),
                ('PO_ID', models.CharField(blank=True, max_length=20, null=True)),
                ('shipping_mark', models.CharField(blank=True, max_length=4000, null=True)),
                ('fba_id', models.CharField(blank=True, max_length=4000, null=True)),
                ('ref_id', models.CharField(blank=True, max_length=4000, null=True)),
                ('pcs', models.IntegerField(blank=True, null=True)),
                ('sequence_number', models.CharField(blank=True, max_length=2000, null=True)),
                ('length', models.FloatField(blank=True, null=True)),
                ('width', models.FloatField(blank=True, null=True)),
                ('height', models.FloatField(blank=True, null=True)),
                ('cbm', models.FloatField(blank=True, null=True)),
                ('weight_lbs', models.FloatField(blank=True, null=True)),
                ('abnormal_palletization', models.BooleanField(blank=True, default=False, null=True)),
                ('po_expired', models.BooleanField(blank=True, default=False, null=True)),
                ('note', models.CharField(blank=True, max_length=2000, null=True)),
                ('priority', models.CharField(blank=True, max_length=20, null=True)),
                ('location', models.CharField(blank=True, max_length=100, null=True)),
                ('contact_name', models.CharField(blank=True, max_length=255, null=True)),
                ('container_number', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='warehouse.container')),
                ('packing_list', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='warehouse.packinglist')),
            ],
            options={
                'indexes': [models.Index(fields=['PO_ID'], name='warehouse_p_PO_ID_9ea539_idx')],
            },
        ),
    ]
