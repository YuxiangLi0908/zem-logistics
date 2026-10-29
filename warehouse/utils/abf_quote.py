"""ABF Rate Quote XML adapter; no booking or pickup requests."""
import asyncio
import math
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation

import aiohttp

ENDPOINT = "https://www.abfs.com/xml/aquotexml.asp"


def prepare_abf_quote(form, classes):
    if int(form.get("quoteType") or 1) != 1:
        raise ValueError("ABF Rate Quote 仅支持 LTL，不能用于 FTL 整车报价")
    items = form["items"]
    if not 1 <= len(items) <= 15:
        raise ValueError("ABF 报价要求 1 至 15 行货物明细，不会自动截断")
    if len(classes) != len(items):
        raise ValueError("ABF 货物行数与货物等级数量不一致")
    day = datetime.strptime(form["pickupDate"], "%Y-%m-%d")
    params = {"DL": "2", "ShipAff": "Y", "ShipCountry": "US", "ConsCountry": "US",
              "ShipMonth": str(day.month), "ShipDay": str(day.day), "ShipYear": str(day.year),
              "FrtLWHType": "IN", "Acc_PALLET": "Y"}
    for source, target in (("origin", "Ship"), ("destination", "Cons")):
        for suffix, field in (("City", "City"), ("State", "State"), ("PostCode", "Zip"), ("DetailAddress", "Addr")):
            params[target + field] = str(form.get(source + suffix) or "").strip()
        if int(form.get(source + "Type") or 1) == 2:
            params["Acc_RPU" if source == "origin" else "Acc_RDEL"] = "Y"
    if form.get("needLiftgate"):
        # Existing shared option applies to both pickup and delivery in Kakas.
        params.update(Acc_GRD_PU="Y", Acc_GRD_DEL="Y")
    for index, (item, freight_class) in enumerate(zip(items, classes), 1):
        dimensions = [float(item.get(key) or 0) for key in ("length", "width", "height", "weight")]
        count = float(item.get("palletCount") or 1)
        if not all(math.isfinite(value) and value > 0 for value in dimensions + [count]) or not count.is_integer():
            raise ValueError(f"ABF 第 {index} 行尺寸、单板重量必须为正数，板数必须为正整数")
        length, width, height, weight = map(math.ceil, dimensions)
        total_weight = weight * int(count)
        if count > 9999 or total_weight > 999999:
            raise ValueError(f"ABF 第 {index} 行板数超过4位或总重量超过6位")
        params.update({f"FrtLng{index}": str(length), f"FrtWdth{index}": str(width),
                       f"FrtHght{index}": str(height), f"UnitNo{index}": str(int(count)),
                       f"UnitType{index}": "CRT" if int(form.get("palletType") or 1) == 2 else "PLT",
                       f"Wgt{index}": str(total_weight), f"Class{index}": str(freight_class),
                       f"Desc{index}": str(item.get("description") or "Pallet")})
    return params


def parse_abf_response(raw):
    if "<!DOCTYPE" in raw.upper() or "<!ENTITY" in raw.upper():
        raise ValueError("ABF 返回了不支持的 XML 声明")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("ABF 返回的内容不是有效 XML") from exc
    if root.tag != "ABF":
        raise ValueError(f"ABF 返回了未知 XML 根节点：{root.tag}")
    errors = [f"{node.findtext('ERRORCODE', '?')}: {node.findtext('ERRORMESSAGE', '未知错误')}"
              for node in root.findall("ERROR")]
    if root.findtext("NUMERRORS") != "0" or errors:
        raise ValueError("ABF 询价失败：" + ("；".join(errors) or "NUMERRORS 缺失或不为0"))
    quote_id = root.findtext("QUOTEID")
    try:
        amount = Decimal(root.findtext("CHARGE") or "")
        if not amount.is_finite() or amount < 0:
            raise InvalidOperation
    except InvalidOperation as exc:
        raise ValueError("ABF CHARGE 金额缺失或无效") from exc
    if not quote_id:
        raise ValueError("ABF 未返回 QUOTEID 报价编号")
    return {"quotes": [{"carrierName": "ABF Freight", "carrierCode": "ABFS",
                         "serviceCode": "LTL", "serviceName": "Standard LTL", "currency": "USD",
                         "TotalQuote": str(amount), "quoteId": quote_id,
                         "transitDays": root.findtext("ADVERTISEDTRANSIT"),
                         "dueDate": root.findtext("ADVERTISEDDUEDATE"),
                         "expirationDate": root.findtext("EXPIRATIONDATE"),
                         "itemizedCharges": [dict(node.attrib) for node in root.findall("ITEMIZEDCHARGES/ITEM")]}],
            "notes": [{"type": node.get("TYPE"), "text": node.text} for node in root.findall("NOTES/NOTE")],
            "rawXml": raw}


async def request_abf_quote(session, form, classes, before_request=None):
    """Keep credential-bearing POST data out of saved payloads and exceptions."""
    if int(form.get("quoteType") or 1) != 1:
        return {"status": "skipped", "error": "ABF Rate Quote 仅支持 LTL，本次为 FTL"}, {}
    payload = {}
    try:
        payload = prepare_abf_quote(form, classes)
        key = os.environ.get("ABF_API_KEY", "").strip()
        if not key:
            raise ValueError("服务器未配置 ABF_API_KEY")
        # 已确认默认业务：ZEM 为发货方并承担运费。
        payer = (os.environ.get("ABF_PAYING_PARTY") or "shipper").strip().lower()
        if payer not in ("shipper", "consignee", "third_party"):
            raise ValueError("请配置 ABF_PAYING_PARTY：shipper、consignee 或 third_party（付款方）")
        payload[{"shipper": "ShipPay", "consignee": "ConsPay", "third_party": "TPBPay"}[payer]] = "Y"
        for env, field in (("ABF_SHIPPER_ACCOUNT", "ShipAcct"), ("ABF_CONSIGNEE_ACCOUNT", "ConsAcct"),
                           ("ABF_THIRD_PARTY_ACCOUNT", "TPBAcct")):
            if os.environ.get(env):
                payload[field] = os.environ[env].strip()
        if payer == "third_party" and not payload.get("TPBAcct"):
            raise ValueError("第三方付款需要配置 ABF_THIRD_PARTY_ACCOUNT")
        if before_request:
            await before_request("abf")
        secret_payload = dict(payload, ID=key)
        if os.environ.get("ABF_APP_ID"):
            secret_payload["APP_ID"] = os.environ["ABF_APP_ID"]
        async with session.post(ENDPOINT, data=secret_payload, allow_redirects=False) as response:
            if response.status != 200:
                raise ValueError(f"ABF 报价接口返回 HTTP {response.status}")
            raw = await response.text()
        # Redact before parsing so echoed credentials cannot enter notes or errors.
        raw = raw.replace(key, "[REDACTED]")
        if os.environ.get("ABF_APP_ID"):
            raw = raw.replace(os.environ["ABF_APP_ID"], "[REDACTED]")
        data = parse_abf_response(raw)
        return {"status": "success", "data": data}, payload
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return {"status": "error", "error": "ABF 接口连接失败或超时，请稍后重试"}, payload
    except (ValueError, TypeError, OverflowError) as exc:
        message = str(exc)
        for name in ("ABF_API_KEY", "ABF_APP_ID"):
            if os.environ.get(name):
                message = message.replace(os.environ[name], "[REDACTED]")
        return {"status": "error", "error": message}, payload
