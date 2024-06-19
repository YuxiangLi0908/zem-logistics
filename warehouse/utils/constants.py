import os
import yaml
from pathlib import Path


ORDER_TYPES = {
    "TD": "转运",
    "DD": "直送",
}

ORDER_TYPE_OPTIONS = [
    ("", ""), ("转运", "转运"), ("直送", "直送"),
]

SHIPPING_LINE_OPTIONS = [
    ("", ""), ("N/A", "N/A"), ("COSCO", "COSCO"), ("EMC", "EMC"), ("MSC", "MSC"),
    ("OOCL", "OOCL"), ("ZIM", "ZIM"),
]

CLEARANCE_OPTIONS = [
    ("", ""), ('代理清关', '代理清关'), ('自理清关', '自理清关'), ('N/A', 'N/A'),
]

RETRIEVAL_OPTIONS = [
    ("", ""), ('代理卡车', '代理卡车'), ('自理卡车', '自理卡车'),
]

CONTAINER_TYPE_OPTIONS = [
    ("", ""), ('45HQ/GP', '45HQ/GP'), ('40HQ/GP', '40HQ/GP'),
    ('20GP', '20GP'), ('53HQ', '53HQ'),
]

PORT_OPTIONS = [
    ("", ""), ("N/A", "N/A"), ("LOS ANGELES", "LOS ANGELES"), ("NEW YORK, NY", "NEW YORK, NY"),
    ("NINGBO", "NINGBO"), ("QINGDAO", "QINGDAO"), ("SAVANNAH, GA", "SAVANNAH, GA"),
    ("TAMPA", "TAMPA"), ("XIAMEN", "XIAMEN"), ("YANTIAN", "YANTIAN"), ("SHANGHAI", "SHANGHAI"),
    ("SHENZHEN", "SHENZHEN"), 
]

DELIVERY_METHOD_OPTIONS = [
    ("", ""),
    ("卡车派送", "卡车派送"),
    ("客户自提", "客户自提"),
    ("暂扣留仓(HOLD)", "暂扣留仓(HOLD)"),
    ("整柜直送", "整柜直送"),
    ("UPS", "UPS"),
    ("FEDEX", "FEDEX"),
    ("DHL", "DHL"),
    ("DPD", "DPD"),
    ("TNT", "TNT"),
]

WAREHOUSE_OPTIONS = [
    ("", ""), ("美东新泽西仓", "美东新泽西仓"), ("美东南萨瓦纳仓", "美东南萨瓦纳仓"),
    ("N/A(直送)", "N/A(直送)")
]

CARRIER_OPTIONS = [
    ("UPS", "UPS"), ("FedEx", "FedEx"), ("ZEM", "ZEM"), ("客户自提", "客户自提")
]

LOAD_TYPE_OPTIONS = [
    ("卡板", "卡板"), ("地板", "地板"),
]

QUOTE_PLATFORM_OPTIONS = [
    ("ARM", "ARM"), ("Uber Freight", "Uber Freight"), ("Unishipper", "Unishipper"),
    ("Coyote", "Coyote"), ("报价表", "报价表"), ("整柜直送", "整柜直送"),
]

PACKING_LIST_TEMP_COL_MAPPING = {
    "唛头": "shipping_mark",
    "品名": "product_name",
    "箱数": "pcs",
    "单箱重kg": "unit_weight_kg",
    "总箱重kg": "total_weight_kg",
    "单箱磅(lb)": "unit_weight_lbs",
    "总箱磅(lb)": "total_weight_lbs",
    "总cbm": "cbm",
    "仓库代码": "destination",
    "邮编": "zipcode",
    "收件人": "contact_name",
    "联系方式": "contact_method",
    "仓库/私人 地址": "address",
    "FBA号": "fba_id",
    "Amazon Reference ID": "ref_id",
    "派送方式": "delivery_method",
    "备注": "note",
}

file_path = Path(__file__).parent.resolve().joinpath("fba_fulfillment_center.yaml")
with open(file_path, "r") as f:
    amazon_fba_locations = yaml.safe_load(f)

SP_USER = os.environ.get("MS_SP_USER")
SP_PASS = os.environ.get("MS_SP_PASS")
SP_URL = os.environ.get("MS_SP_URL")
SP_SITE = os.environ.get("MS_SP_SITE")
SP_DOC_LIB = os.environ.get("MS_SP_DOC_LIB")
SYSTEM_FOLDER = "system_archive"
APP_ENV = "prd" if os.environ.get("APP_ENV", "staging") == "production" else "stg"