"""Read-only ArcBest Shipment Tracking and Document Retrieval XML APIs."""
import os
import re
import xml.etree.ElementTree as ET

import requests

from warehouse.utils.abf_booking import document_url

TRACKING_ENDPOINT = "https://www.abfs.com/xml/tracexml.asp"
DOCUMENT_ENDPOINT = "https://www.abfs.com/xml/docretxml.asp"
REFERENCE_TYPES = {"A": "PRO号", "B": "BOL编号", "P": "PO编号", "C": "客户参考号", "K": "提货确认号"}
DOCUMENT_TYPES = {
    "ALL": "全部可用文件", "BOL": "提单", "DR": "送货回单 / POD", "INV": "账单",
    "PSL": "装箱单", "WSL": "重量复核证书", "WRP": "重量复核照片", "WINS": "重量复核检查",
    "CUS": "海关文件", "CBL": "更正提单", "CON": "合同", "RECO": "改派授权",
    "Str": "签收与清点记录", "EQAD": "到达核实", "BOND": "美国海关保税文件", "RDR": "拒收回单",
    "FFWD": "货代送货单", "DIM": "尺寸证书", "AFSS": "特殊服务授权", "CUST": "客户交运文件",
    "PODI": "签收图片", "RETL": "零售文件",
}


class ABFQueryError(ValueError):
    def __init__(self, operation, errors):
        self.errors = errors
        super().__init__(f"ABF {operation}失败：" + "；".join(f"{item['code']}: {item['message']}" for item in errors))


def reference_params(ref_type, ref_num, documents=False):
    ref_type = str(ref_type or "").strip().upper()
    ref_num = str(ref_num or "").strip()
    allowed = set(REFERENCE_TYPES) - ({"K"} if documents else set())
    if ref_type not in allowed:
        raise ValueError("文件获取不支持提货确认号，请先查询物流获取 PRO号" if ref_type == "K" else "请选择有效的编号类型")
    if not ref_num or len(ref_num) > 100 or re.search(r"[\x00-\x1f\x7f]", ref_num):
        raise ValueError("请填写有效的查询编号（1至100个字符）")
    if ref_num == "WWW0000000":
        raise ValueError("WWW0000000 是测试提货确认号，不能查询真实运输，请使用真实 PRO、BOL 或提货确认号")
    return {"DL": "2", "RefNum": ref_num, "RefType": ref_type}


def document_params(ref_type, ref_num, doc_type="ALL", image_type="L"):
    params = reference_params(ref_type, ref_num, documents=True)
    doc_type = str(doc_type or "ALL").strip().upper()
    doc_type = "Str" if doc_type == "STR" else doc_type
    image_type = str(image_type or "L").strip().upper()
    if doc_type not in DOCUMENT_TYPES:
        raise ValueError("不支持的文件类型")
    if image_type not in ("L", "H", "T"):
        raise ValueError("图片格式必须为 L（低清JPEG）、H（高清JPEG）或 T（TIFF）")
    if doc_type == "INV" and params["RefType"] != "A":
        raise ValueError("ABF 账单查询必须使用 PRO号（RefType=A）")
    return dict(params, DocType=doc_type, ImageType=image_type)


def checked_xml(raw, operation):
    if "<!DOCTYPE" in raw.upper() or "<!ENTITY" in raw.upper():
        raise ValueError("ABF 返回了不支持的 XML 声明")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("ABF 返回的内容不是有效 XML") from exc
    if root.tag != "ABF":
        raise ValueError("ABF 返回了未知的 XML 根节点")
    errors = []
    # Both APIs document ERRORS with direct ERRORCODE / ERRORMESSAGE children.
    # Handle repeated pairs and ERROR wrappers without pairing unrelated codes.
    for group in root.iter():
        codes = group.findall("ERRORCODE")
        messages = group.findall("ERRORMESSAGE")
        for index in range(max(len(codes), len(messages))):
            errors.append({"code": (codes[index].text or "?").strip() if index < len(codes) else "?",
                           "message": (messages[index].text or "未知错误").strip() if index < len(messages) else "未知错误"})
    count = (root.findtext("NUMERRORS") or "").strip()
    if errors:
        raise ABFQueryError(operation, errors)
    if count != "0":
        raise ValueError("ABF 返回缺少有效 NUMERRORS=0，无法确认查询成功")
    return root


def parse_tracking(raw):
    root = checked_xml(raw, "物流查询")
    if root.find("SHIPMENTS") is None:
        raise ValueError("ABF 物流响应缺少 SHIPMENTS 节点")
    shipments = []
    fields = {
        "PRONUMBER": "pro_number", "REASSIGNMENT": "reassigned", "PICKUP": "pickup_date", "PICKUPTIME": "pickup_time",
        "PICKUPDELAYCODE": "pickup_delay_code", "DELIVERYDATE": "delivery_date", "DELIVERYTIME": "delivery_time",
        "DUEDATE": "due_date", "EXPECTEDDELIVERYDATE": "expected_delivery_date", "APPOINTMENT": "appointment",
        "APPTDATE": "appointment_date", "APPTTIME": "appointment_time", "NOTIFYNAME": "notified_contact",
        "WEIGHT": "weight", "PIECES": "pieces", "SHORTSTATUS2": "status_code", "LONGSTATUS": "status",
        "OSDEXCEPTION": "osd_exception", "EXCEPTIONDESCRIPTION": "exception_description",
        "LATESTTTECODE": "transit_exception_code", "LATESTTTEDESCRIPTION": "transit_exception_description",
        "DELIVSIGFIRSTNAME": "signature_first_name", "DELIVSIGLASTNAME": "signature_last_name",
        "SHIPPERNAME": "shipper_name", "SHIPPERCITY": "shipper_city", "SHIPPERSTATE": "shipper_state",
        "CONSIGNEEname": "consignee_name", "CONSIGNEECITY": "consignee_city", "CONSIGNEESTATE": "consignee_state",
    }
    for node in root.findall("SHIPMENTS/SHIPMENT"):
        item = {value: (node.findtext(key) or "").strip() for key, value in fields.items()}
        # Document spells CONSIGNEEname with mixed case; also accept the uppercase variant.
        item["consignee_name"] = item["consignee_name"] or (node.findtext("CONSIGNEENAME") or "").strip()
        item["bol_numbers"] = [(entry.text or "").strip() for entry in node.findall("BOLNUMBERS/BOLNUMBER")]
        item["po_numbers"] = [(entry.text or "").strip() for entry in node.findall("PONUMBERS/PONUMBER")]
        item["carrier_pros"] = [{"carrier": child.findtext("CARRIER") or "", "pro": child.findtext("PRO") or ""}
                               for child in node.findall("CARRIERPROS/CARRIERPRO")]
        shipments.append(item)
    return {"shipments": shipments}


def parse_documents(raw):
    root = checked_xml(raw, "文件获取")
    if root.find("DOCUMENTS") is None:
        raise ValueError("ABF 文件响应缺少 DOCUMENTS 节点")
    documents = []
    for node in root.findall("DOCUMENTS/DOCUMENT"):
        kind = (node.findtext("TYPE") or "").strip()
        pages = []
        for index, page in enumerate(node.findall("PAGES/PAGE"), 1):
            value = (page.text or "").strip()
            url = document_url(value)
            if not url:
                raise ValueError(f"ABF {kind} 文件第{index}页没有返回受支持的文件链接")
            pages.append({"number": index, "url": url})
        documents.append({"type": kind, "label": DOCUMENT_TYPES.get("Str" if kind.upper() == "STR" else kind.upper(), kind), "pages": pages})
    return {"documents": documents}


def query_xml(endpoint, params):
    key = os.environ.get("ABF_API_KEY", "").strip()
    if not key:
        raise ValueError("服务器未配置 ABF_API_KEY")
    try:
        # POST keeps the key out of URLs. No booking-mode or Test parameter is used.
        response = requests.post(endpoint, data=dict(params, ID=key), timeout=(10, 40), allow_redirects=False)
    except requests.Timeout as exc:
        raise ConnectionError("ABF 查询超时，请稍后重试") from exc
    except requests.RequestException as exc:
        raise ConnectionError("ABF 查询连接失败，请稍后重试") from exc
    if response.status_code != 200:
        raise ConnectionError(f"ABF 查询接口返回 HTTP {response.status_code}")
    return response.text.replace(key, "[REDACTED]")


def track_shipment(ref_type, ref_num):
    params = reference_params(ref_type, ref_num)
    return parse_tracking(query_xml(TRACKING_ENDPOINT, params))


def retrieve_documents(ref_type, ref_num, doc_type="ALL", image_type="L"):
    params = document_params(ref_type, ref_num, doc_type, image_type)
    return parse_documents(query_xml(DOCUMENT_ENDPOINT, params))
