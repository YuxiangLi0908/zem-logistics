"""Versioned, pure normalization rules. Keep v1 stable for historical profile IDs."""
import hashlib
import json
from decimal import Decimal, InvalidOperation


def text(value):
    return " ".join(str(value or "").strip().split())


def number(value):
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("货物参数必须是有效数字")
    return format(value.normalize(), "f")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def profile_configuration(parameters):
    origin = {key: text(parameters.get("origin" + key)).casefold()
              for key in ("DetailAddress", "City", "State", "PostCode")}
    if not origin["City"] or not origin["State"] or not origin["PostCode"]:
        raise ValueError("缺少发货地址，无法建立比较编号")
    cargo = []
    for item in parameters["items"]:
        cargo.append({**{key: number(item[key]) for key in ("length", "width", "height", "weight", "pieces", "palletCount")},
                      "description": text(item.get("description") or "Pallet")})
    if not cargo:
        raise ValueError("缺少货物明细")
    return {"version": 1, "origin": origin, "items": sorted(cargo, key=lambda item: json.dumps(item, sort_keys=True)),
            "declaredValue": number(parameters["declaredValue"]), "quoteType": int(parameters.get("quoteType") or 1),
            "carType": int(parameters.get("carType") or 1) if int(parameters.get("quoteType") or 1) == 2 else None,
            "commodityUnit": int(parameters.get("commodityUnit") or 11), "palletType": int(parameters.get("palletType") or 1),
            "originType": int(parameters.get("originType") or 1), "destinationType": int(parameters.get("destinationType") or 1),
            "needLiftgate": bool(parameters.get("needLiftgate"))}


def address_key(address):
    return digest([text(address.get(key)).casefold() for key in ("city", "state", "zipcode", "address")])


def quote_rows(value):
    if isinstance(value, dict):
        for key in ("rates", "quotes"):
            if isinstance(value.get(key), list):
                return value[key]
        for child in value.values():
            found = quote_rows(child)
            if found:
                return found
    return []


def normalize_prices(result):
    output = []
    carriers = result.get("results", {}) if isinstance(result, dict) else {}
    if not isinstance(carriers, dict):
        return output
    for platform in ("maersk", "kakas", "abf"):
        state = carriers.get(platform) or {}
        if not isinstance(state, dict):
            continue
        for row in quote_rows(state):
            if not isinstance(row, dict):
                continue
            value = next((row[key] for key in ("TotalQuote", "totalPrice", "price") if row.get(key) is not None), None)
            try:
                price = Decimal(str(value))
                if not price.is_finite() or price < 0 or price >= Decimal("100000000000000"):
                    continue
                price = price.quantize(Decimal("0.01"))
                if price >= Decimal("100000000000000"):
                    continue
            except (InvalidOperation, TypeError, ValueError):
                continue
            code = text(row.get("carrierCode") or row.get("SCAC") or row.get("scac"))
            name = text(row.get("carrierName") or row.get("CarrierName") or code)
            if not name and platform in ("maersk", "abf"):
                name = "Maersk" if platform == "maersk" else "ABF"
            service_code = text(row.get("serviceCode") or row.get("Service"))
            service = text(row.get("serviceName") or row.get("DisplayService") or row.get("rateType") or service_code)
            currency_raw = row.get("currency") or row.get("currencyCode") or row.get("Currency")
            currency = text(currency_raw or "USD").upper()
            if currency in ("$", "US$"):
                currency = "USD"
            valid_currency = len(currency) == 3 and currency.isalpha()
            # Codes are authoritative. Missing codes use exact normalized names, never fuzzy matching.
            identity = [platform, ("code:" + code if code else "name:" + name).casefold(),
                        ("code:" + service_code if service_code else "name:" + service).casefold(),
                        currency, row.get("guaranteed"), row.get("rateType")]
            output.append(dict(row_number=len(output), platform=platform, carrier=(name or "未知承运商")[:200],
                               carrier_code=code[:100], service=service[:200], service_code=service_code[:100],
                               series_key=digest(identity), currency=currency if valid_currency else "UNK",
                               currency_assumed=not bool(currency_raw), price=price,
                               comparable=bool(name) and valid_currency,
                               platform_complete=state.get("status") == "success" and not state.get("warning") and not state.get("error"), raw=row))
    return output
