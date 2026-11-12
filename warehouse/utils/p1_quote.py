"""Priority1 v2 LTL rating adapter. Credentials stay in the HTTP header only."""
import asyncio
import json
import math
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

import aiohttp


API_BASES = ("https://api.priority1.com", "https://dev-api.priority1.com")


def prepare_p1_quote(form, classes):
    if int(form.get("quoteType") or 1) != 1:
        raise ValueError("P1 当前接入的是 LTL 询价接口，不支持 FTL 整车询价")
    items = form.get("items")
    if not isinstance(items, list) or not items or len(classes) != len(items):
        raise ValueError("P1 货物明细不能为空，且每行必须有计算后的货物等级")
    payload = {"pickupDate": datetime.strptime(form["pickupDate"], "%Y-%m-%d").isoformat(),
               "items": [], "accessorialServices": []}
    for prefix in ("origin", "destination"):
        for source, target in (("City", "City"), ("State", "StateAbbreviation"), ("PostCode", "ZipCode")):
            value = str(form.get(prefix + source) or "").strip()
            if not value:
                raise ValueError(f"P1 缺少地址字段：{prefix + source}")
            payload[prefix + target] = value
        payload[prefix + "CountryCode"] = "US"
        if int(form.get(prefix + "Type") or 1) == 2:
            payload["accessorialServices"].append({"code": "RESPU" if prefix == "origin" else "RESDEL"})
    if form.get("needLiftgate"):
        # The shared option applies to both pickup and delivery, as in Kakas/ABF.
        payload["accessorialServices"].extend([{"code": "LGPU"}, {"code": "LGDEL"}])
    packaging = {1: "Pallet", 2: "Crate"}.get(int(form.get("palletType") or 1))
    if not packaging:
        raise ValueError("P1 不支持当前托盘类型")
    for index, (item, freight_class) in enumerate(zip(items, classes), 1):
        def positive(field, integer=False):
            try:
                number = float(item.get(field, 1 if field == "pieces" else None))
                if not math.isfinite(number) or number <= 0 or (integer and not number.is_integer()):
                    raise ValueError
                return math.ceil(number)
            except (ValueError, TypeError, OverflowError) as exc:
                raise ValueError(f"P1 第 {index} 行 {field} 必须为正{'整数' if integer else '数'}") from exc

        count = positive("palletCount", integer=True)
        payload["items"].append({
            "freightClass": str(freight_class), "packagingType": packaging,
            "units": count, "pieces": positive("pieces", integer=True),
            "totalWeight": positive("weight") * count,
            "length": positive("length"), "width": positive("width"), "height": positive("height"),
            "isStackable": False, "isHazardous": False, "isUsed": False, "isMachinery": False,
            "description": str(item.get("description") or "Pallet"),
        })
    return payload


def message_text(value):
    """Keep P1's field names and carrier error messages, including validation errors."""
    if isinstance(value, list):
        return "；".join(filter(None, (message_text(item) for item in value)))
    if isinstance(value, dict):
        parts = []
        for key in ("text", "message", "title", "detail", "errorMessages", "messages", "errors"):
            if value.get(key):
                detail = value[key]
                if key == "errors" and isinstance(detail, dict):
                    parts.extend(f"{field}: {message_text(errors)}" for field, errors in detail.items())
                else:
                    parts.append(message_text(detail))
        return "；".join(dict.fromkeys(filter(None, parts)))
    return str(value) if value is not None else ""


def parse_p1_response(data):
    if not isinstance(data, dict) or not any(key in data for key in ("rateQuotes", "invalidRateQuotes")):
        raise ValueError("P1 返回格式错误：" + (message_text(data) or "缺少 rateQuotes 报价列表"))
    rates = data.get("rateQuotes") or []
    invalid = data.get("invalidRateQuotes") or []
    if not isinstance(rates, list) or not isinstance(invalid, list):
        raise ValueError("P1 返回格式错误：rateQuotes / invalidRateQuotes 必须为列表")
    warnings = [message_text(data.get("messages"))]
    for item in invalid:
        if isinstance(item, dict):
            name = item.get("carrierName") or item.get("carrierCode") or "未知承运商"
            warnings.append(f"{name}：{message_text(item.get('errorMessages')) or '未返回可用报价'}")
    quotes = []
    for index, rate in enumerate(rates, 1):
        if not isinstance(rate, dict):
            warnings.append(f"第 {index} 条报价格式错误")
            continue
        name = rate.get("carrierName") or rate.get("carrierCode") or "P1 承运商"
        detail = rate.get("rateQuoteDetail")
        try:
            amount = Decimal(str(detail.get("total"))) if isinstance(detail, dict) else Decimal("NaN")
            if not amount.is_finite() or amount < 0:
                raise InvalidOperation
        except InvalidOperation:
            warnings.append(f"{name}：rateQuoteDetail.total 金额缺失或无效")
            continue
        quotes.append({
            "carrierName": name, "carrierCode": rate.get("carrierCode"),
            "serviceCode": rate.get("serviceLevel"),
            "serviceName": rate.get("serviceLevelDescription") or rate.get("serviceLevel"),
            "quoteId": rate.get("id"), "carrierQuoteNumber": rate.get("carrierQuoteNumber"),
            "TotalQuote": str(amount), "currency": "USD", "transitDays": rate.get("transitDays"),
            "deliveryDate": rate.get("deliveryDate"), "expirationDate": rate.get("expirationDate"),
            "itemizedCharges": detail.get("charges") or [], "message": rate.get("message"),
        })
    warning = "；".join(filter(None, warnings))
    if not quotes:
        raise ValueError("P1 未返回可用报价：" + (warning or "当前路线及货物条件没有可用承运商"))
    quotes.sort(key=lambda item: Decimal(item["TotalQuote"]))
    result = {"status": "success", "data": {"quotes": quotes, "quoteRequestId": data.get("id"), "raw": data}}
    if warning:
        result["warning"] = warning
    return result


async def request_p1_quote(session, form, classes):
    if int(form.get("quoteType") or 1) != 1:
        return {"status": "skipped", "error": "P1 当前仅接入 LTL 询价，本次为 FTL"}, {}
    payload = {}
    key = os.environ.get("P1_API_KEY", "").strip()
    try:
        payload = prepare_p1_quote(form, classes)
        if not key:
            raise ValueError("服务器未配置 P1_API_KEY")
        base = (os.environ.get("P1_API_BASE_URL") or API_BASES[0]).strip().rstrip("/")
        if base not in API_BASES:
            raise ValueError("P1_API_BASE_URL 只能使用 https://api.priority1.com 或 https://dev-api.priority1.com")
        async with session.post(base + "/v2/ltl/quotes/rates", json=payload,
                                headers={"X-API-KEY": key, "Accept": "application/json"},
                                allow_redirects=False) as response:
            raw = (await response.text()).replace(key, "[REDACTED]")
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                if response.status == 200:
                    raise ValueError("P1 HTTP 200 返回内容不是有效 JSON")
                data = None
            if response.status != 200:
                hint = {401: "密钥无效或未授权", 403: "账号无询价权限", 429: "请求过于频繁，请稍后重试"}.get(response.status, "询价请求失败")
                detail = message_text(data) or (raw[:1000] if not raw.lstrip().startswith("<") else "")
                raise ValueError(f"P1 HTTP {response.status}：{hint}" + (f"；{detail}" if detail else ""))
        return parse_p1_response(data), payload
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return {"status": "error", "error": "P1 接口连接失败或超时，请稍后重试"}, payload
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        message = str(exc)
        if key:
            message = message.replace(key, "[REDACTED]")
        return {"status": "error", "error": message}, payload
