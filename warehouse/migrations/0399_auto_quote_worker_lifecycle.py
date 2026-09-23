from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0398_backfill_auto_quote_analysis")]
    operations = [
        migrations.AddField(model_name="autoquoteworkerstate", name="run_token", field=models.UUIDField(null=True)),
        migrations.AddField(model_name="autoquoteworkerstate", name="lease_until", field=models.DateTimeField(null=True)),
        migrations.AddField(model_name="autoquoteworkerstate", name="last_activity_at", field=models.DateTimeField(null=True)),
        migrations.AddField(model_name="autoquoteworkerstate", name="last_error", field=models.CharField(max_length=300, blank=True, default="")),
    ]
