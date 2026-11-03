"""ArcBest Bill of Lading and Pickup Request API adapter."""
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from django.core.exceptions import ValidationError
from django.core.validators import validate_email

from warehouse.utils.abf_quote import prepare_abf_quote

ENDPOINT = "https://www.abfs.com/xml/bolpkupxml.asp"


class ABFRejected(ValueError):
    """API explicitly rejected the request; safe to correct and resubmit."""


def booking_mode():
    mode = os.environ.get("ABF_BOOKING_MODE", "test").strip().lower()
    if mode not in ("test", "live"):
        raise ValueError("ABF_BOOKING_MODE 必须为 test 或 live")
    return mode


def prepare_booking(source, quote_id, fields, history_id):
    if not isinstance(fields, dict):
        raise ValueError("下单信息必须是对象")
    if not re.fullmatch(r"[A-Za-z0-9-]{1,10}", str(quote_id)):
        raise ValueError("ABF QuoteID 必须为1至10个有效字符，请重新询价")
    rated = prepare_abf_quote(source["form"], source["classes"])
    if not source["form"].get("originDetailAddress") or not source["form"].get("destinationDetailAddress"):
        raise ValueError("原询价缺少发货或收货详细地址，请补全地址后重新询价")
    if os.environ.get("ABF_PAYING_PARTY", "shipper").strip().lower() not in ("", "shipper"):
        raise ValueError("当前下单仅支持已确认的发货方付款，请核实服务器 ABF_PAYING_PARTY 配置")
    params = {"DL": "2", "RequesterType": "1", "PayTerms": "P", "QuoteID": quote_id,
              "PickupDate": datetime.strptime(source["form"]["pickupDate"], "%Y-%m-%d").strftime("%m/%d/%Y"),
              "FileFormat": "A", "ProAutoAssign": "Y", "LWHType": "IN"}
    for prefix in ("Ship", "Cons"):
        for suffix in ("City", "State", "Zip", "Country"):
            params[prefix + suffix] = rated[prefix + suffix]
        params[prefix + "Address"] = rated[prefix + "Addr"]
    required = {"RequesterName": "申请人姓名", "RequesterPhone": "申请人电话", "RequesterEmail": "申请人邮箱",
                "ShipContact": "发货联系人", "ShipName": "发货公司", "ShipPhone": "发货电话",
                "ConsContact": "收货联系人", "ConsName": "收货公司", "ConsPhone": "收货电话"}
    for field, label in required.items():
        value = str(fields.get(field) or "").strip()
        if not value:
            raise ValueError(f"请填写{label}")
        if len(value) > 200:
            raise ValueError(f"{label}过长")
        if field.endswith("Phone"):
            value = re.sub(r"[\s()+.-]", "", value)
            if value.startswith("1") and len(value) == 11:
                value = value[1:]
            if not re.fullmatch(r"[0-9]{10}", value):
                raise ValueError(f"{label}必须为10位美国电话号码，分机请勿填写在号码中")
        params[field] = value
    for field in ("RequesterEmail", "ShipEmail", "ConsEmail"):
        value = str(fields.get(field) or "").strip()
        if value:
            try:
                validate_email(value)
            except ValidationError as exc:
                raise ValueError(f"{field} 邮箱格式不正确") from exc
            params[field] = value
    for field, label in (("AT", "货好时间"), ("OT", "仓库开门时间"), ("CT", "仓库关门时间")):
        value = str(fields.get(field) or "").strip()
        if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):(?:00|30)", value):
            raise ValueError(f"{label}请按半小时填写，例如09:00或09:30（发货仓当地时间）")
        params[field] = value
    if not params["OT"] <= params["AT"] < params["CT"]:
        raise ValueError("时间必须满足：开门时间 ≤ 货好时间 < 关门时间")
    params["Bol"] = str(fields.get("Bol") or f"ZEMABF{history_id}").strip()
    if not re.fullmatch(r"[A-Za-z0-9-]{1,20}", params["Bol"]):
        raise ValueError("BOL编号请使用1至20个字母、数字或连字符")
    params["Instructions"] = str(fields.get("Instructions") or "").strip()
    if len(params["Instructions"]) > 370:
        raise ValueError("装运备注最多370个字符")
    for index in range(1, len(source["form"]["items"]) + 1):
        for quote_name, booking_name in (("UnitNo", "HN"), ("UnitType", "HT"), ("Wgt", "WT"), ("Class", "CL"),
                                         ("FrtLng", "FrtLng"), ("FrtWdth", "FrtWdth"), ("FrtHght", "FrtHght"), ("Desc", "Desc")):
            params[f"{booking_name}{index}"] = rated[f"{quote_name}{index}"]
        if int(params[f"FrtLng{index}"]) > 999 or int(params[f"FrtWdth{index}"]) > 99 or int(params[f"FrtHght{index}"]) > 99:
            raise ValueError(f"第{index}行尺寸超出 ABF 下单范围（长≤999、宽高≤99英寸）")
    for field in ("Acc_GRD_PU", "Acc_GRD_DEL", "Acc_RPU", "Acc_RDEL"):
        if field in rated:
            params[field] = rated[field]
    mode = booking_mode()
    if mode == "test":
        params["Test"] = "Y"
    return params, mode


def document_url(value):
    if not value:
        return ""
    url = urljoin(ENDPOINT, value.strip())
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or parsed.hostname not in ("abfs.com", "www.abfs.com", "arcb.com", "www.arcb.com") or parsed.username or parsed.password:
        return ""
    return parsed._replace(scheme="https").geturl()


def parse_booking_response(raw, mode):
    if "<!DOCTYPE" in raw.upper() or "<!ENTITY" in raw.upper():
        raise ValueError("ABF 返回了不支持的 XML 声明")
    root = ET.fromstring(raw)
    if root.tag != "ABF":
        raise ValueError("ABF 下单接口返回了未知内容")
    errors = [f"{node.findtext('ERRORCODE', '?')}: {node.findtext('ERRORMESSAGE', '未知错误')}" for node in root.findall("ERROR")]
    if errors or (root.findtext("NUMERRORS") is not None and root.findtext("NUMERRORS") != "0"):
        raise ABFRejected("ABF 拒绝下单：" + ("；".join(errors) or "NUMERRORS 不为0"))
    if root.findtext("NUMERRORS") is None:
        raise ValueError("ABF 返回缺少 NUMERRORS，无法确认是否成功")
    confirmation = root.findtext("CONFIRMATION") or ""
    if not confirmation or (mode == "live" and confirmation == "WWW0000000"):
        raise ValueError("未收到有效的正式提货确认号，不能确认下单成功")
    return {"confirmation": confirmation, "pro_number": root.findtext("PRONUMBER") or "",
            "bol_number": root.findtext("BOLNUMBER") or "",
            "document": document_url(root.findtext("DOCUMENT")),
            "label_document": document_url(root.findtext("LABELDOCUMENT")),
            "pro_label_document": document_url(root.findtext("PROLABELDOCUMENT")), "raw_xml": raw}


def submit_booking(params, mode):
    key = os.environ.get("ABF_API_KEY", "").strip()
    if not key:
        raise ValueError("服务器未配置 ABF_API_KEY")
    data = dict(params, ID=key)
    if os.environ.get("ABF_APP_ID"):
        data["APP_ID"] = os.environ["ABF_APP_ID"]
    # No automatic retry: ABF might have accepted a request whose response was lost.
    response = requests.post(ENDPOINT, data=data, timeout=(10, 60), allow_redirects=False)
    if response.status_code != 200:
        raise ValueError(f"ABF 返回 HTTP {response.status_code}，请核实是否已受理")
    raw = response.text
    for secret in (key, os.environ.get("ABF_APP_ID")):
        if secret:
            raw = raw.replace(secret, "[REDACTED]")
    return parse_booking_response(raw, mode)
