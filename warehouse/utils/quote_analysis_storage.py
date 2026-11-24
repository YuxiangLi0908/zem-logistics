from django.db import transaction

from warehouse.models.auto_quote import AutoQuoteBatch, AutoQuoteItem, AutoQuotePrice, AutoQuoteProfile
from warehouse.utils.quote_identity_v1 import digest, normalize_prices
from warehouse.utils.quote_identity_v2 import profile_configuration

ANALYSIS_VERSION = 1


def get_profile(parameters, address_group):
    configuration = profile_configuration(parameters, address_group)
    profile, _ = AutoQuoteProfile.objects.get_or_create(
        fingerprint=digest(configuration), defaults={"configuration": configuration,
                                                    "origin_label": str(parameters.get("originWarehouse") or "")[:200]})
    return profile


def profile_matches_group(profile, address_group):
    return bool(profile and profile.configuration.get("version") == 2 and
                profile.configuration.get("address_group") == address_group)


@transaction.atomic
def index_item(item_id):
    item = AutoQuoteItem.objects.select_for_update(of=("self",)).select_related("batch__profile").get(pk=item_id)
    if not item.finished_at:
        return
    if not profile_matches_group(item.batch.profile, item.batch.group):
        profile = get_profile(item.batch.parameters, item.batch.group)
        AutoQuoteBatch.objects.filter(pk=item.batch_id).update(profile=profile)
    if item.analysis_version == ANALYSIS_VERSION:
        return
    AutoQuotePrice.objects.filter(item=item).delete()
    AutoQuotePrice.objects.bulk_create([AutoQuotePrice(item=item, **row) for row in normalize_prices(item.result)])
    item.analysis_version = ANALYSIS_VERSION
    item.save(update_fields=["analysis_version"])
