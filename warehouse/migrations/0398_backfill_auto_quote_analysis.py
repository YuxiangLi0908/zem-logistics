from decimal import InvalidOperation

from django.db import migrations, transaction

from warehouse.utils.quote_identity_v1 import digest, normalize_prices, profile_configuration


def backfill(apps, schema_editor):
    alias = schema_editor.connection.alias
    Batch = apps.get_model("warehouse", "AutoQuoteBatch")
    Profile = apps.get_model("warehouse", "AutoQuoteProfile")
    Item = apps.get_model("warehouse", "AutoQuoteItem")
    Price = apps.get_model("warehouse", "AutoQuotePrice")
    skipped = 0
    for batch in Batch.objects.using(alias).filter(profile__isnull=True).iterator(chunk_size=100):
        try:
            configuration = profile_configuration(batch.parameters)
        except (ValueError, TypeError, KeyError, InvalidOperation):
            skipped += 1
            continue
        profile, _ = Profile.objects.using(alias).get_or_create(
            fingerprint=digest(configuration), defaults={"configuration": configuration,
                                                        "origin_label": str(batch.parameters.get("originWarehouse") or "")[:200]})
        Batch.objects.using(alias).filter(pk=batch.pk, profile__isnull=True).update(profile_id=profile.pk)
    candidates = Item.objects.using(alias).filter(analysis_version=0, finished_at__isnull=False,
                                                batch__profile__isnull=False).exclude(status="cancelled")
    for candidate in candidates.iterator(chunk_size=100):
        with transaction.atomic(using=alias):
            item = Item.objects.using(alias).select_for_update().get(pk=candidate.pk)
            if item.analysis_version:
                continue
            Price.objects.using(alias).filter(item_id=item.pk).delete()
            Price.objects.using(alias).bulk_create([Price(item_id=item.pk, **row) for row in normalize_prices(item.result)], batch_size=500)
            Item.objects.using(alias).filter(pk=item.pk).update(analysis_version=1)
    if skipped:
        print(f"Auto quote analysis: {skipped} batches have incomplete parameters; original results were preserved.")


class Migration(migrations.Migration):
    atomic = False
    dependencies = [("warehouse", "0397_auto_quote_price_analysis")]
    operations = [migrations.RunPython(backfill, migrations.RunPython.noop)]
