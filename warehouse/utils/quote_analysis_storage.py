from django.db import transaction

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem, AutoQuotePrice, AutoQuoteProfile
from warehouse.utils.quote_identity_v1 import digest, normalize_prices, profile_configuration

ANALYSIS_VERSION = 1


def get_profile(parameters):
    configuration = profile_configuration(parameters)
    profile, _ = AutoQuoteProfile.objects.get_or_create(
        fingerprint=digest(configuration), defaults={"configuration": configuration,
                                                    "origin_label": str(parameters.get("originWarehouse") or "")[:200]})
    return profile


@transaction.atomic
def index_item(item_id):
    item = AutoQuoteItem.objects.select_for_update().select_related("batch").get(pk=item_id)
    if item.analysis_version == ANALYSIS_VERSION or not item.finished_at:
        return
    if not item.batch.profile_id:
        profile = get_profile(item.batch.parameters)
        AutoQuoteBatch.objects.filter(pk=item.batch_id, profile__isnull=True).update(profile=profile)
    AutoQuotePrice.objects.filter(item=item).delete()
    AutoQuotePrice.objects.bulk_create([AutoQuotePrice(item=item, **row) for row in normalize_prices(item.result)])
    item.analysis_version = ANALYSIS_VERSION
    item.save(update_fields=["analysis_version"])
