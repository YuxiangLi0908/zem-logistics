from decimal import InvalidOperation

from django.core.management.base import BaseCommand

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem
from warehouse.utils.quote_analysis_storage import get_profile, index_item, profile_matches_group


class Command(BaseCommand):
    help = "Idempotently index historical automatic quotes; never submits external quote requests."

    def handle(self, *args, **options):
        skipped = 0
        for batch in AutoQuoteBatch.objects.select_related("profile").iterator(chunk_size=100):
            if profile_matches_group(batch.profile, batch.group):
                continue
            try:
                profile = get_profile(batch.parameters, batch.group)
            except (ValueError, TypeError, KeyError, InvalidOperation):
                skipped += 1
                self.stderr.write(f"Batch {batch.pk}: incomplete parameters; skipped")
                continue
            AutoQuoteBatch.objects.filter(pk=batch.pk).update(profile=profile)
        count = 0
        for item_id in AutoQuoteItem.objects.filter(analysis_version=0, finished_at__isnull=False,
                                                    batch__profile__isnull=False).exclude(status="cancelled").values_list("pk", flat=True).iterator(chunk_size=100):
            index_item(item_id)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Indexed {count} items; skipped {skipped} incomplete batches."))
