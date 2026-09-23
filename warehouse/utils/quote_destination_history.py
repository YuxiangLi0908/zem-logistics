"""Destination history uses immutable snapshots and the caller's batch permissions."""
from datetime import date

from django.core.paginator import Paginator
from django.db.models import F, Q, Window
from django.db.models.functions import RowNumber
from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteItem, AutoQuotePrice
from warehouse.utils.quote_identity_v1 import address_key


def operator_name(batch):
    return (batch.operator.get_full_name() or batch.operator.get_username()) if batch.operator else "原用户已删除 / 未记录"


def lead_days(item):
    if not item.started_at:
        return "unknown"
    try:
        return str((date.fromisoformat(item.batch.parameters.get("pickupDate", "")) - timezone.localdate(item.started_at)).days)
    except (ValueError, TypeError):
        return "unknown"


def destination_history(batches, params):
    query = str(params.get("q", "")).strip()
    if not query or len(query) > 250:
        raise ValueError("请输入收货地址、城市或邮编（最多250字）")
    items = AutoQuoteItem.objects.filter(batch__in=batches)
    fields = ("city", "state", "zipcode", "address")
    for token in query.split():
        match = Q()
        for field in fields:
            match |= Q(**{f"address_snapshot__{field}__icontains": token})
        items = items.filter(match)
    for key, lookup in (("start", "gte"), ("end", "lte")):
        if params.get(key):
            try:
                value = date.fromisoformat(params[key])
            except ValueError:
                raise ValueError("日期格式应为 YYYY-MM-DD")
            items = items.filter(**{f"batch__created_at__date__{lookup}": value})
    if params.get("start") and params.get("end") and params["start"] > params["end"]:
        raise ValueError("开始日期不能晚于结束日期")

    # Read only distinct address components, not thousands of large quote JSONs.
    snapshots = list(items.order_by().values_list(*[f"address_snapshot__{f}" for f in fields]).distinct()[:501])
    if len(snapshots) > 500:
        raise ValueError("匹配地址超过500个，请输入更具体的街道或邮编")
    choices, variants = {}, {}
    for values in snapshots:
        address = dict(zip(fields, values))
        key = address_key(address)
        choices[key] = {"key": key, "label": " · ".join(str(address.get(f) or "") for f in ("address", "city", "state", "zipcode"))}
        variants.setdefault(key, []).append(address)
    selected = params.get("address", "")
    if selected and selected not in choices:
        raise ValueError("所选地址不在当前搜索结果中，请重新搜索")
    if len(choices) == 1:
        selected = next(iter(choices))
    if selected:
        match = Q()
        for address in variants[selected]:
            match |= Q(**{f"address_snapshot__{f}": address[f] for f in fields})
        items = items.filter(match)

    page = Paginator(items.select_related("batch__operator").order_by("-batch__created_at", "-id"), 30).get_page(params.get("page"))
    records = [{"id": item.pk, "batch_id": item.batch_id, "operator": operator_name(item.batch),
                "origin": item.batch.parameters.get("originWarehouse", ""),
                "profile_code": f"AQ{item.batch.profile_id:06d}" if item.batch.profile_id else "待归档",
                "address": item.address_snapshot, "status": item.status, "started_at": item.started_at,
                "created_at": item.batch.created_at, "finished_at": item.finished_at,
                "result": item.result, "error": item.error} for item in page]
    response = {"rows": records, "total": page.paginator.count, "page": page.number, "pages": page.paginator.num_pages,
                "addresses": sorted(choices.values(), key=lambda v: v["label"]), "address": selected,
                "chart": [], "profiles": [], "leads": [], "currencies": [], "chart_limited": False}
    if not selected:
        response["chart_message"] = "请选择一个具体收货地址后查看走势" if choices else "没有匹配的历史记录"
        return response

    profiles = list(items.order_by("-batch__profile_id").values_list("batch__profile_id", flat=True).distinct())
    profiles = [pk for pk in profiles if pk]
    response["profiles"] = [{"id": pk, "code": f"AQ{pk:06d}"} for pk in profiles]
    profile = int(params.get("profile") or (profiles[0] if profiles else 0))
    response["profile"] = profile
    if profile not in profiles:
        response["chart_message"] = "此地址没有该比较编号的记录；请选择可用编号"
        return response
    chart_items = items.filter(batch__profile_id=profile)
    # Bound chart size explicitly; the paginated history still contains ALL matches.
    samples = list(chart_items.select_related("batch").defer("result", "request_payload").order_by("-started_at", "-id")[:1001])
    response["chart_limited"] = len(samples) > 1000
    samples = samples[:1000]
    leads = sorted({lead_days(item) for item in samples}, key=lambda v: (v == "unknown", int(v) if v != "unknown" else 0))
    lead = str(params.get("lead") or (lead_days(samples[0]) if samples else "unknown"))
    response.update(leads=leads, lead=lead)
    samples = [item for item in samples if lead_days(item) == lead]
    prices = AutoQuotePrice.objects.filter(item_id__in=[item.pk for item in samples])
    platform = params.get("platform", "")
    if platform:
        if platform not in ("maersk", "kakas", "abf"):
            raise ValueError("请选择有效平台")
        prices = prices.filter(platform=platform)
    currencies = sorted(set(prices.values_list("currency", flat=True)))
    currency = params.get("currency") or ("USD" if "USD" in currencies else next(iter(currencies), "USD"))
    response.update(currencies=currencies, currency=currency, platform=platform)
    ranked = prices.filter(currency=currency).annotate(rank=Window(
        expression=RowNumber(), partition_by=[F("item_id")], order_by=[F("price").asc(), F("id").asc()]
    )).filter(rank__lte=10).order_by("item_id", "rank").values(
        "item_id", "rank", "price", "platform", "carrier", "service", "currency_assumed", "platform_complete")
    by_item = {}
    for price in ranked:
        by_item.setdefault(price.pop("item_id"), []).append(price)
    response["chart"] = [{"id": item.pk, "batch_id": item.batch_id,
                          "time": item.started_at or item.batch.created_at, "status": item.status,
                          "prices": by_item.get(item.pk, [])}
                         for item in sorted(samples, key=lambda i: (i.started_at or i.batch.created_at, i.pk))]
    response["chart_message"] = "按每次报价价格排名连线；同一条线的承运商可能变化。未返回价格留空，部分报价仅代表已返回结果。"
    return response
