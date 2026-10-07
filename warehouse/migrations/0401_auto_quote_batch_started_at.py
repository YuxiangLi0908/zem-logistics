from django.db import migrations, models
from django.db.models import Min


def backfill_started_at(apps, schema_editor):
    Batch = apps.get_model("warehouse", "AutoQuoteBatch")
    Item = apps.get_model("warehouse", "AutoQuoteItem")
    alias = schema_editor.connection.alias
    starts = Item.objects.using(alias).filter(started_at__isnull=False).values(
        "batch_id"
    ).annotate(first_started_at=Min("started_at"))
    for row in starts.iterator():
        Batch.objects.using(alias).filter(pk=row["batch_id"]).update(
            started_at=row["first_started_at"]
        )


class Migration(migrations.Migration):
    dependencies = [("warehouse", "0400_add_sav_31349_warehouse_address")]
    operations = [
        migrations.AddField(
            model_name="autoquotebatch", name="started_at",
            field=models.DateTimeField(null=True, blank=True),
        ),
        migrations.RunPython(backfill_started_at, migrations.RunPython.noop),
    ]
