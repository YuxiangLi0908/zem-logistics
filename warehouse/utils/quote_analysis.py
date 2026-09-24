"""Comparable daily quote series and descriptive market statistics (no forecasting)."""
import math
import statistics
from collections import defaultdict
from datetime import date, timedelta

from django.utils import timezone

from warehouse.models.auto_quote import AutoQuoteItem, AutoQuotePrice, AutoQuoteProfile
from warehouse.utils.quote_identity_v1 import address_key


def pct(current, baseline):
    return (current - baseline) / baseline * 100 if baseline is not None and baseline > 0 and current is not None else None


def rounded(value):
    return round(value, 4) if value is not None and math.isfinite(value) else None


def series_statistics(points):
    values = [point["price"] for point in points if point["price"] is not None]
    valid = [point for point in points if point["price"] is not None]
    latest = points[-1]["price"] if points else None
    previous = next((point for point in reversed(points[:-1]) if point["price"] is not None), None)
    mean = statistics.mean(values) if values else None
    cv = statistics.pstdev(values) / mean * 100 if len(values) >= 3 and mean and mean > 0 else None
    daily_changes = [pct(b["price"], a["price"]) for a, b in zip(points, points[1:])
                     if a["price"] is not None and b["price"] is not None]
    daily_changes = [value for value in daily_changes if value is not None]
    return {"samples": len(values), "expected": len(points), "coverage_pct": rounded(len(values) / len(points) * 100) if points else None,
            "latest": latest, "previous": previous["price"] if previous else None,
            "previous_date": previous["date"] if previous else None,
            "latest_date": points[-1]["date"] if points else None,
            "latest_change": rounded(latest - previous["price"]) if latest is not None and previous else None,
            "latest_change_pct": rounded(pct(latest, previous["price"])) if previous else None,
            "first": values[0] if values else None, "last_available": values[-1] if values else None,
            "last_available_date": valid[-1]["date"] if valid else None,
            "period_change_pct": rounded(pct(values[-1], values[0])) if len(values) >= 2 else None,
            "minimum": min(values) if values else None, "maximum": max(values) if values else None,
            "mean": rounded(mean), "volatility_pct": rounded(cv),
            "range_pct": rounded(pct(max(values), min(values))) if values else None,
            "max_adjacent_move_pct": rounded(max(map(abs, daily_changes))) if daily_changes else None}


def analysis_options(batches):
    latest_pickup = batches.exclude(parameters__pickupDate=None).order_by("-parameters__pickupDate").values_list("parameters__pickupDate", flat=True).first()
    ids = batches.exclude(profile=None).values_list("profile_id", flat=True).distinct()
    profiles = list(AutoQuoteProfile.objects.filter(pk__in=ids).order_by("-id"))
    return {"profiles": [{"id": p.pk, "code": p.code, "origin": p.origin_label,
                           "configuration": p.configuration} for p in profiles],
            "unindexed": AutoQuoteItem.objects.filter(batch__in=batches, finished_at__isnull=False,
                                                       analysis_version=0).exclude(status="cancelled").count(),
            "latest_pickup_date": latest_pickup,
            "timezone": timezone.get_current_timezone_name()}


def build_analysis(batches, params, *, export=False):
    try:
        profile_id = int(params.get("profile", ""))
        start = date.fromisoformat(params.get("start") or (timezone.localdate() - timedelta(days=29)).isoformat())
        end = date.fromisoformat(params.get("end") or timezone.localdate().isoformat())
        page = max(1, int(params.get("page") or 1))
    except (ValueError, TypeError):
        raise ValueError("请选择比较编号并填写有效的日期范围")
    if start > end or (end - start).days > 365:
        raise ValueError("日期范围须在366天以内，开始日期不能晚于结束日期")
    batches = batches.filter(profile_id=profile_id)
    profile = AutoQuoteProfile.objects.filter(pk=profile_id, pk__in=batches.values("profile_id")).first()
    if not profile:
        raise ValueError("没有权限查看该编号，或编号不存在")
    if params.get("group"):
        if params["group"] not in ("LA", "SAV", "NJ"):
            raise ValueError("地址组不正确")
        batches = batches.filter(group=params["group"])
    items = AutoQuoteItem.objects.filter(batch__in=batches, finished_at__isnull=False, batch__parameters__pickupDate__gte=start.isoformat(),
                                         batch__parameters__pickupDate__lte=end.isoformat()).exclude(status__in=("cancelled", "pending", "running"))
    if items.count() > 20000:
        raise ValueError("范围内超过20000条地址询价记录，请缩小日期范围或选择地址组")
    records, addresses, leads = [], {}, defaultdict(int)
    unindexed = 0
    for row in items.values("id", "batch_id", "address_snapshot", "started_at", "finished_at", "analysis_version", "batch__parameters__pickupDate").iterator():
        address = row["address_snapshot"]
        key = address_key(address)
        addresses[key] = {"id": key, "zipcode": str(address.get("zipcode", "")), "label": f'{address.get("city", "")}, {address.get("state", "")} {address.get("zipcode", "")} · {address.get("address", "")}'}
        try:
            day = date.fromisoformat(row["batch__parameters__pickupDate"])
        except (KeyError, ValueError, TypeError):
            continue
        lead = (day - timezone.localdate(row["started_at"])).days if row["started_at"] else None
        leads[lead] += 1
        records.append({"id": row["id"], "batch_id": row["batch_id"], "route": key, "day": day.isoformat(),
                        "started_at": row["started_at"] or row["finished_at"], "lead": lead, "indexed": row["analysis_version"] == 1})
        unindexed += int(row["analysis_version"] != 1)
    available_leads = sorted(value for value in leads if value is not None)
    lead = "all"  # Compare by pickup date regardless of when the quote was requested.
    route_filter = params.get("address", "")
    zipcode = str(params.get("zipcode", "")).strip().casefold()
    matching_routes = {key for key, address in addresses.items() if not zipcode or zipcode in address["zipcode"].casefold()}
    if not route_filter and zipcode and len(matching_routes) == 1:
        route_filter = next(iter(matching_routes))
    platform_filter = params.get("platform", "")
    if platform_filter not in ("", "maersk", "kakas", "abf"):
        raise ValueError("报价平台不正确")
    currency = (params.get("currency") or "USD").upper()
    if len(currency) != 3 or not currency.isalpha():
        raise ValueError("币种须为三位字母")
    # One address is sampled once per pickup date: last completed attempt wins, including failures.
    # Thus a retry does not give that day more weight, and a failure never becomes a zero price.
    daily = {}
    for row in records:
        if row["route"] not in matching_routes:
            continue
        if route_filter and row["route"] != route_filter:
            continue
        key = (row["route"], row["day"])
        if key not in daily or (row["started_at"], row["id"]) > (daily[key]["started_at"], daily[key]["id"]):
            daily[key] = row
    by_item = {row["id"]: row for row in daily.values()}
    route_days = defaultdict(list)
    for (route, day), row in sorted(daily.items()):
        route_days[route].append((day, row))
    series = {}
    diagnostics = {"invalid_identity": 0, "partial_excluded": 0, "assumed_currency": 0, "duplicates_collapsed": 0,
                   "unindexed": unindexed, "unindexed_daily_samples": sum(not row["indexed"] for row in daily.values())}
    prices = AutoQuotePrice.objects.filter(item_id__in=by_item, currency=currency)
    if platform_filter:
        prices = prices.filter(platform=platform_filter)
    selected_series = params.get("carrier", "")
    include_partial = params.get("include_partial") == "1"
    for price in prices.values("item_id", "platform", "carrier", "carrier_code", "service", "service_code", "series_key",
                               "price", "comparable", "platform_complete", "currency_assumed").iterator():
        if not price["comparable"]:
            diagnostics["invalid_identity"] += 1
            continue
        row = by_item[price["item_id"]]
        key = (row["route"], price["series_key"])
        if key not in series:
            series[key] = {"route": row["route"], "address": addresses[row["route"]]["label"],
                           "key": price["series_key"], "platform": price["platform"], "carrier": price["carrier"],
                           "carrier_code": price["carrier_code"], "service": price["service"] or "未标注服务",
                           "service_code": price["service_code"], "observations": {}}
        if not include_partial and not price["platform_complete"]:
            diagnostics["partial_excluded"] += 1
            continue
        diagnostics["assumed_currency"] += int(price["currency_assumed"])
        target = series[key]["observations"]
        if row["day"] in target:
            diagnostics["duplicates_collapsed"] += 1
        amount = float(price["price"])
        if row["day"] not in target or amount < target[row["day"]]["price"]:
            target[row["day"]] = {"price": amount, "partial": not price["platform_complete"]}
    choices = {}
    for entry in series.values():
        choices[entry["key"]] = {"id": entry["key"], "label": f'{entry["platform"]} / {entry["carrier"]} / {entry["service"]}'}
    if selected_series:
        series = {key: value for key, value in series.items() if value["key"] == selected_series}
    carrier_query = str(params.get("carrier_query", "")).strip().casefold()
    if carrier_query:
        series = {key: value for key, value in series.items() if carrier_query in (value["carrier"] + " " + value["carrier_code"] + " " + value["service"] + " " + value["service_code"]).casefold()}
    entries = []
    for entry in series.values():
        points = [{"date": day, "price": entry["observations"].get(day, {}).get("price"),
                   "partial": entry["observations"].get(day, {}).get("partial", False),
                   "item_id": row["id"], "batch_id": row["batch_id"], "queried_at": row["started_at"].isoformat()}
                  for day, row in route_days[entry["route"]]]
        entry.pop("observations")
        entry["points"] = points
        entry.update(series_statistics(points))
        entries.append(entry)
    entries.sort(key=lambda entry: (entry["volatility_pct"] is None, -(entry["volatility_pct"] or 0), entry["address"], entry["key"]))
    ranking_groups = defaultdict(list)
    for entry in entries:
        if entry["volatility_pct"] is not None:
            ranking_groups[entry["key"]].append(entry)
    # Compare carrier/service products on an identical route set, not different geographic mixes.
    candidate_routes = set.intersection(*(set(e["route"] for e in values) for values in ranking_groups.values())) if ranking_groups else set()
    matched_statistics = {}
    for route in candidate_routes:
        route_entries = [next(entry for entry in values if entry["route"] == route) for values in ranking_groups.values()]
        common_days = set.intersection(*(set(p["date"] for p in entry["points"] if p["price"] is not None) for entry in route_entries))
        if len(common_days) < 3:
            continue
        matched = []
        for entry in route_entries:
            stats = series_statistics([point for point in entry["points"] if point["date"] in common_days])
            if stats["volatility_pct"] is None:
                break
            matched.append({**entry, **stats, "coverage_pct": entry["coverage_pct"]})
        if len(matched) == len(route_entries):
            matched_statistics[route] = {entry["key"]: entry for entry in matched}
    common_routes = set(matched_statistics)
    rankings = []
    for key, values in ranking_groups.items():
        matched = [matched_statistics[route][key] for route in common_routes]
        chosen = matched or values
        example = values[0]
        rankings.append({"key": key, "platform": example["platform"], "carrier": example["carrier"], "service": example["service"],
                         "routes": len(chosen), "available_routes": len(values),
                         "volatility_pct": rounded(statistics.mean(entry["volatility_pct"] for entry in chosen)),
                         "coverage_pct": rounded(statistics.mean(entry["coverage_pct"] for entry in chosen)),
                         "min_samples": min(entry["samples"] for entry in chosen), "comparable_routes": bool(common_routes)})
    rankings.sort(key=lambda row: (-row["volatility_pct"], row["key"]))
    daily_minima = []
    for route, days in route_days.items():
        route_entries = [entry for entry in entries if entry["route"] == route]
        previous_winners = None
        for index, (day, row) in enumerate(days):
            available = [(entry, entry["points"][index]["price"]) for entry in route_entries if entry["points"][index]["price"] is not None]
            minimum = min((value for _, value in available), default=None)
            winners = [entry for entry, value in available if value == minimum]
            winner_keys = {entry["key"] for entry in winners}
            daily_minima.append({"route": route, "address": addresses[route]["label"], "date": day, "price": minimum,
                                 "winners": [f'{entry["platform"]}/{entry["carrier"]}/{entry["service"]}' for entry in winners],
                                 "winner_changed": bool(winner_keys and previous_winners is not None and winner_keys != previous_winners),
                                 "offers": len(available)})
            previous_winners = winner_keys if winner_keys else None
    dates = sorted({row["day"] for row in daily.values()})
    # Fixed panel: each included route+carrier+service must have a positive price on EVERY displayed day.
    balanced = [entry for entry in entries if len(entry["points"]) == len(dates) and
                all(p["price"] is not None and p["price"] > 0 for p in entry["points"])] if len(dates) >= 2 else []
    market_index = [{"date": day, "value": rounded(statistics.mean(entry["points"][i]["price"] / entry["points"][0]["price"] * 100 for entry in balanced))}
                    for i, day in enumerate(dates)] if balanced else []
    chart_entries = sorted(entries, key=lambda entry: (-entry["samples"], entry["key"], entry["route"]))[:8] if route_filter else []
    requested_page = page
    pages = max(1, math.ceil(len(entries) / 50))
    page = min(page, pages)
    latest_moves = [e for e in entries if e["latest_change_pct"] is not None]
    enough = len(rankings) >= 2 and bool(common_routes)
    def brief(entry):
        return {key: value for key, value in entry.items() if key != "points"}
    report = {"profile": {"id": profile.pk, "code": profile.code, "origin": profile.origin_label, "configuration": profile.configuration},
            "selected_address": route_filter, "filters": {"start": start.isoformat(), "end": end.isoformat(), "lead": lead, "currency": currency,
                        "timezone": timezone.get_current_timezone_name(), "include_partial": include_partial},
            "addresses": sorted(addresses.values(), key=lambda row: row["label"]), "lead_days": available_leads,
            "carriers": sorted(choices.values(), key=lambda row: row["label"]), "diagnostics": diagnostics,
            "summary": {"daily_samples": len(daily), "routes": len(route_days), "series": len(entries),
                        "ranked_products": len(rankings), "common_routes": len(common_routes),
                        "most_volatile": rankings[0] if enough else None, "most_stable": rankings[-1] if enough else None,
                        "most_volatile_ties": sum(r["volatility_pct"] == rankings[0]["volatility_pct"] for r in rankings) if enough else 0,
                        "most_stable_ties": sum(r["volatility_pct"] == rankings[-1]["volatility_pct"] for r in rankings) if enough else 0,
                        "largest_increase": brief(max(latest_moves, key=lambda e: e["latest_change_pct"])) if any(e["latest_change_pct"] > 0 for e in latest_moves) else None,
                        "largest_decrease": brief(min(latest_moves, key=lambda e: e["latest_change_pct"])) if any(e["latest_change_pct"] < 0 for e in latest_moves) else None,
                        "balanced_series": len(balanced), "winner_changes": sum(row["winner_changed"] for row in daily_minima)},
            "rankings": rankings, "rows": [brief(entry) for entry in entries[(page-1)*50:page*50]],
            "page": page, "pages": pages, "total": len(entries), "chart": chart_entries,
            "chart_truncated": bool(route_filter and len(entries) > 8), "market_index": market_index,
            "minimum_history": daily_minima if route_filter else [],
            "methodology": "按取件日期、地址取最后发起且已结束的询价；同平台承运商服务的多条有效报价取最低。CV=总体标准差/均价×100%，至少3个报价日。排名采用共有线路的至少3个共同报价日，线路等权；缺失不补零。"}
    if export:
        report["rows"] = [brief(entry) for entry in entries]
    elif params.get("group_by") == "address":
        # Group before pagination so an address and all its services stay together.
        grouped = defaultdict(list)
        minima_by_route = defaultdict(list)
        for entry in entries:
            grouped[entry["route"]].append(brief(entry))
        for minimum in daily_minima:
            minima_by_route[minimum["route"]].append(minimum)
        routes = []
        for route, minima in minima_by_route.items():
            services = sorted(grouped[route], key=lambda r: (r["latest"] is None, r["latest"] or 0, r["key"]))
            routes.append({"route": route, "address": addresses[route]["label"],
                           **series_statistics(minima), "winners": minima[-1]["winners"], "services": services})
        routes.sort(key=lambda r: (r["address"], r["route"]))
        pages = max(1, math.ceil(len(routes) / 20))
        page = min(requested_page, pages)
        report.update(route_rows=routes[(page-1)*20:page*20], rows=[], page=page, pages=pages, total=len(routes))
    return report
