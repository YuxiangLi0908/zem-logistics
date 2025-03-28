import os
from pathlib import Path

import pandas as pd
import yaml

ORDER_TYPES = {
    "TD": "转运",
    "DD": "直送",
}

ORDER_TYPE_OPTIONS = [
    ("", ""),
    ("转运", "转运"),
    ("直送", "直送"),
]

SHIPPING_LINE_OPTIONS = [
    ("", ""),
    ("CMA CGM", "CMA CGM"),
    ("COSCO", "COSCO"),
    ("Evergreen", "Evergreen"),
    ("Hapag-Lloyd", "Hapag-Lloyd"),
    ("Hyundai", "Hyundai"),
    ("MSC", "MSC"),
    ("Maersk", "Maersk"),
    ("ONE", "ONE"),
    ("OOCL", "OOCL"),
    ("Wan Hai Lines", "Wan Hai Lines"),
    ("Yangming", "Yangming"),
    ("Zim Line", "Zim Line"),
    ("HEDE", "HEDE"),
    ("SML", "SML"),
    ("SeaLead", "SeaLead"),
    ("Matson", "Matson"),
    ("TSL", "TSL"),
]

CONTAINER_PICKUP_CARRIER = [
    ("", ""),
    ("东海岸", "东海岸"),
    ("kars", "kars"),
    ("大方广", "大方广"),
    ("Eric", "Eric"),
    ("客户自提", "客户自提"),
]

CLEARANCE_OPTIONS = [
    ("", ""),
    ("代理清关", "代理清关"),
    ("自理清关", "自理清关"),
    ("N/A", "N/A"),
]

RETRIEVAL_OPTIONS = [
    ("", ""),
    ("代理卡车", "代理卡车"),
    ("自理卡车", "自理卡车"),
]

CONTAINER_TYPE_OPTIONS = [
    ("", ""),
    ("45HQ/GP", "45HQ/GP"),
    ("40HQ/GP", "40HQ/GP"),
    ("20GP", "20GP"),
    ("53HQ", "53HQ"),
]

PORT_OPTIONS = [
    ("", ""),
    ("N/A", "N/A"),
    ("LOS ANGELES", "LOS ANGELES"),
    ("NEW YORK, NY", "NEW YORK, NY"),
    ("NINGBO", "NINGBO"),
    ("QINGDAO", "QINGDAO"),
    ("SAVANNAH, GA", "SAVANNAH, GA"),
    ("TAMPA", "TAMPA"),
    ("XIAMEN", "XIAMEN"),
    ("YANTIAN", "YANTIAN"),
    ("SHANGHAI", "SHANGHAI"),
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

DELIVERY_METHOD_CODE = {
    "卡车派送": "T",
    "整柜直送": "D",
    "UPS": "UP",
    "FEDEX": "FD",
    "DHL": "DH",
    "DPD": "DP",
    "TNT": "TN",
}

WAREHOUSE_OPTIONS = [
    ("", ""),
    ("NJ-07001", "NJ-07001"),
    ("NJ-08817", "NJ-08817"),
    ("SAV-31326", "SAV-31326"),
    ("LA-91761", "LA-91761"),
    ("N/A(直送)", "N/A(直送)"),
    ("Empty", "Empty"),
]

CARRIER_OPTIONS = [
    ("UPS", "UPS"),
    ("FedEx", "FedEx"),
    ("ZEM", "ZEM"),
    ("客户自提", "客户自提"),
]

LOAD_TYPE_OPTIONS = [
    ("卡板", "卡板"),
    ("地板", "地板"),
]

QUOTE_PLATFORM_OPTIONS = [
    ("无", "无"),
    ("ARM", "ARM"),
    ("Uber Freight", "Uber Freight"),
    ("Unishipper", "Unishipper"),
    ("Coyote", "Coyote"),
    ("报价表", "报价表"),
    ("整柜直送", "整柜直送"),
]

PICKUP_FEE = {
    ("LA", "20GP"): 1350.00,
    ("LA", "40HQ/GP"): 1450.00,
    ("LA", "45HQ/GP"): 1550.00,
    ("NJ", "20GP"): 1350.00,
    ("NJ", "40HQ/GP"): 1450.00,
    ("NJ", "45HQ/GP"): 1550.00,
    ("SAV", "20"): 1400.00,
    ("SAV", "40HQ/GP"): 1500.00,
    ("SAV", "45HQ/GP"): 1550.00,
    ("ST LOUIS", "20GP"): 1400.00,
    ("ST LOUIS", "40HQ/GP"): 1500.00,
    ("ST LOUIS", "45HQ/GP"): 1550.00,
}
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

MODEL_CHOICES = {
    'packinglist': {
        'model': 'HistoricalPackingList',
        'name': 'packinglist',
        'search_field': 'container_number',
        'warehouse':'warehouse_packinglist',
        'station_field':['shipping_mark','fba_id','ref_id']
    },
    '板子': {
        'model': 'HistoricalPallet',
        'name': 'pallet',
        'search_field': 'shipment_batch_number',
        'warehouse':'warehouse_pallet',
        'station_field':['destination','fba_id','ref_id']
    },
    'invoice': {
        'model': 'warehouse.Invoice',
        'name': '发票',
        'search_field': 'invoice_number'
    },
    
}
file_path = Path(__file__).parent.resolve().joinpath("fba_fulfillment_center.yaml")
with open(file_path, "r",encoding='utf8') as f:
    amazon_fba_locations = yaml.safe_load(f)

SP_USER = os.environ.get("MS_SP_USER")
SP_PASS = os.environ.get("MS_SP_PASS")
SP_URL = os.environ.get("MS_SP_URL")
SP_SITE = os.environ.get("MS_SP_SITE")
SP_DOC_LIB = os.environ.get("MS_SP_DOC_LIB")
SYSTEM_FOLDER = "system_archive"
APP_ENV = "prd" if os.environ.get("APP_ENV", "staging") == "production" else "stg"

ACCT_ACH_ROUTING_NUMBER = os.environ.get("ACCT_ACH_ROUTING_NUMBER")
ACCT_BANK_NAME = os.environ.get("ACCT_BANK_NAME")
ACCT_BENEFICIARY_ACCOUNT = os.environ.get("ACCT_BENEFICIARY_ACCOUNT")
ACCT_BENEFICIARY_ADDRESS = os.environ.get("ACCT_BENEFICIARY_ADDRESS")
ACCT_BENEFICIARY_NAME = os.environ.get("ACCT_BENEFICIARY_NAME")
ACCT_SWIFT_CODE = os.environ.get("ACCT_SWIFT_CODE")

file_path = (
    Path(__file__)
    .parent.resolve()
    .joinpath("data/20240826_additional_containers/data.xlsx")
)
ADDITIONAL_CONTAINER = pd.read_excel(file_path)["container_number"].to_list()
