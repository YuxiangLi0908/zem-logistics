"""Shared payload conversion and gateway calls for manual and queued quotes."""
import asyncio
import math
import os
from datetime import datetime

import aiohttp

from warehouse.utils.kakas_polling import wait_for_kakas_quotes


def prepare_quote(form):
    if not isinstance(form, dict):
        raise ValueError("询价参数必须是对象")
    required = (
        "originWarehouse", "destinationWarehouse", "pickupDate",
        "originCity", "originState", "originPostCode",
        "destinationCity", "destinationState", "destinationPostCode",
        "declaredValue", "items",
    )
    missing = [name for name in required if form.get(name) in (None, "", [])]
    if missing:
        raise ValueError("缺少必填项: " + ", ".join(missing))

    items = form["items"]
    if not isinstance(items, list) or not items:
        raise ValueError("至少需要一条货物明细")

    quote_type = int(form.get("quoteType") or 1)
    if quote_type == 2 and not form.get("carType"):
        raise ValueError("FTL 询价必须选择车型")

    pickup_date = datetime.strptime(form["pickupDate"], "%Y-%m-%d")
    maersk_items = []
    kakas_items = []
    freight_classes = []
    commodity_unit = int(form.get("commodityUnit") or 11)

    def calculate_freight_class(length, width, height, weight):
        density = weight / ((length * width * height) / 1728)
        density_classes = (
            (1, "400"), (2, "300"), (4, "250"), (6, "175"),
            (8, "125"), (10, "100"), (12, "92.5"), (15, "85"),
            (22.5, "70"), (30, "65"), (35, "60"), (50, "55"),
        )
        return next((code for maximum_density, code in density_classes if density <= maximum_density), "50")

    for item in items:
        if not isinstance(item, dict):
            raise ValueError("货物明细格式错误")
        pieces = max(1, int(item.get("pieces") or 1))
        pallet_count = max(1, int(item.get("palletCount") or 1))
        length = max(1, math.ceil(float(item.get("length") or 0)))
        width = max(1, math.ceil(float(item.get("width") or 0)))
        height = max(1, math.ceil(float(item.get("height") or 0)))
        weight = max(1, math.ceil(float(item.get("weight") or 0)))
        freight_class = calculate_freight_class(length, width, height, weight)
        freight_classes.append(freight_class)
        description = str(item.get("description") or "Pallet")
        maersk_items.append({
            "description": description, "pieces": pallet_count,
            "length": length, "width": width, "height": height,
            "weight": weight,
        })
        kakas_items.append({
            "describe": description,
            # PALLETS 的货物数量就是板数；其他单位按每板件数累计。
            "commodityNum": pallet_count if commodity_unit == 11 else pieces * pallet_count,
            "commodityUnit": commodity_unit,
            "consignNum": pallet_count,
            "palletType": int(form.get("palletType") or 1),
            "length": length, "width": width, "height": height,
            # 卡卡省的尺寸是单板尺寸，重量是该合并行所有板的总重量。
            "weight": weight * pallet_count,
            "declaredValue": max(1, math.ceil(float(form["declaredValue"]))),
            "freightClass": freight_class,
        })

    need_liftgate = bool(form.get("needLiftgate"))

    def kakas_address(prefix):
        return {
            "type": int(form.get(prefix + "Type") or 1),
            "detailAddress": form.get(prefix + "DetailAddress") or "",
            "city": form[prefix + "City"],
            "state": form[prefix + "State"],
            "postCode": form[prefix + "PostCode"],
            "country": "US",
            "serveIds": [2] if need_liftgate else [],
        }

    kakas_payload = {
        "quoteType": quote_type,
        "pickupDate": form["pickupDate"], "iu": 0,
        "originalMsg": kakas_address("origin"),
        "destinationMsg": kakas_address("destination"),
        "commodityList": kakas_items,
    }
    if kakas_payload["quoteType"] == 2:
        kakas_payload["carType"] = int(form.get("carType") or 1)

    gateway_payload = {
        "carrier": "all",
        "carrierPayloads": {
            "maersk": {
                "shipDate": pickup_date.strftime("%m/%d/%Y"),
                "origin_zip": form["originPostCode"],
                "dest_zip": form["destinationPostCode"],
                "lineItems": maersk_items,
                "liftgate": "true" if need_liftgate else "false",
            },
            "kakas": kakas_payload,
        },
    }
    return gateway_payload, freight_classes


def gateway_config():
    base = os.environ.get("MAERSK_GATEWAY_URL", "https://zem-maersk-gateway.kindmoss-a5050a64.eastus.azurecontainerapps.io").rstrip("/")
    key = os.environ.get("MAERSK_GATEWAY_API_KEY") or os.environ.get("MAERSK_API_KEY")
    if not key:
        raise ValueError("未配置网关 API Key")
    return base + "/rating", {"Content-Type": "application/json", "x-api-key": key}


def find_uuid(value):
    if isinstance(value, dict):
        return value.get("uuid") or find_uuid(value.get("data"))
    return value.strip() if isinstance(value, str) and value.strip() else None


async def execute_quote(form, *, checkpoint=None, resume=None, before_request=None, automatic=False):
    payload, classes = prepare_quote(form)
    url, headers = gateway_config()
    result = (resume or {}).get("result") or None
    quote_uuid = (resume or {}).get("quote_uuid") or ""
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=50, connect=10)) as session:
        if result is None:
            for attempt in range(3):
                try:
                    if before_request:
                        await before_request("submit")
                    async with session.post(url, json=payload, headers=headers) as response:
                        if response.status == 429 or response.status >= 500:
                            if attempt < 2:
                                await asyncio.sleep(2 ** (attempt + 1))
                                continue
                        if response.status != 200:
                            raise ValueError(f"询价网关返回 HTTP {response.status}")
                        result = await response.json(content_type=None)
                        if not isinstance(result, dict) or not isinstance(result.get("results"), dict):
                            raise ValueError("询价网关返回了无法识别的数据")
                        break
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError):
                    if attempt == 2:
                        raise ValueError("询价网关连接超时，已重试3次")
                    await asyncio.sleep(attempt + 1)
            kakas = result.get("results", {}).get("kakas", {})
            quote_uuid = find_uuid(kakas.get("data")) if kakas.get("status") == "success" else ""
        result.setdefault("freightClasses", classes)
        kakas = result.setdefault("results", {}).setdefault("kakas", {})
        if checkpoint:
            await checkpoint(result, quote_uuid or "", payload)

        async def updated(_):
            if checkpoint:
                await checkpoint(result, quote_uuid or "", payload)

        if quote_uuid:
            kakas.pop("warning", None)
            await wait_for_kakas_quotes(
                session, url, headers, quote_uuid, kakas,
                max_seconds=90 if automatic else None,
                poll_interval=3 if automatic else 1,
                before_request=before_request, on_update=updated if checkpoint else None,
            )
        if checkpoint:
            await checkpoint(result, quote_uuid or "", payload)
    return result, payload
