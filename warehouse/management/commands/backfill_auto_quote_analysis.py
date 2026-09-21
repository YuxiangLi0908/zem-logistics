from decimal import InvalidOperation

from django.core.management.base import BaseCommand

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem
from warehouse.utils.quote_analysis_storage import get_profile, index_item


class Command(BaseCommand):
    help = "Idempotently index historical automatic quotes; never submits external quote requests."

    def handle(self, *args, **options):
        skipped = 0
        for batch in AutoQuoteBatch.objects.filter(profile__isnull=True).iterator(chunk_size=100):
            try:
                profile = get_profile(batch.parameters)
            except (ValueError, TypeError, KeyError, InvalidOperation):
                skipped += 1
                self.stderr.write(f"Batch {batch.pk}: incomplete parameters; skipped")
                continue
            AutoQuoteBatch.objects.filter(pk=batch.pk, profile__isnull=True).update(profile=profile)
        count = 0
        for item_id in AutoQuoteItem.objects.filter(analysis_version=0, finished_at__isnull=False,
                                                    batch__profile__isnull=False).exclude(status="cancelled").values_list("pk", flat=True).iterator(chunk_size=100):
            index_item(item_id)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Indexed {count} items; skipped {skipped} incomplete batches."))
