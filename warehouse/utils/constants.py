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
SHIPMENT_TABLE_MAPPING = {
    "预约批次号":"shipment_batch_number",
    "ISA":"appointment_id",
    "发货仓库":"origin",
    "目的地":"destination",
    "地址":"address",
    "供应商":"carrier",
    "预约时间":"shipment_appointment",
    "出库":"is_shipped",
    "出库时间":"shipped_at",
    "到达":"is_arrived",
    "到达时间":"arrived_at",
    "装车类型":"load_type",
    "预约账户":"shipment_account",
    "预约类型":"shipment_type",
    "预约总重量":"total_weight",
    "预约总体积":"total_cbm",
    "预约总板数":"total_pallet",
    "预约总件数":"total_pcs",
    "发货总重量":"shipped_weight",
    "发货总体积":"shipped_cbm",
    "发货总板数":"shipped_pallet",
    "发货总件数":"shipped_pcs",
    "备注":"note",
    "pod链接":"pod_link",
    "pod上传时间":"pod_uploaded_at",
    "落板数":"pallet_dumpped",
    "车次":"fleet_number",
    "异常打板":"abnormal_palletization",
    "使用":"in_use",
    "取消":"is_canceled",
    "取消原因":"cancelation_reason",
    "状态":"status",
    "BOL号":"ARM_BOL",
    "PRO号":"ARM_PRO",
    "快递单号":"express_number"
}

PALLET_TABLE_MAPPING={
    "柜号id":"container_number",
    "预约批次id":"shipment_batch_number",
    "改仓批次":"transfer_batch_number",
    "目的地":"destination",
    "邮编":"zipcode",
    "派送方式":"delivery_method",
    "唛头":"shipping_mark",
    "件数":"pcs",
    "长":"length",
    "宽":"width",
    "高":"height",
    "异常拆柜":"abnormal_palletization",
    "所在仓":"location",
    "联系人":"contact_name"
}

INVOICE_PREPORT_TABLE_MAPPING={
    "提拆/打托缠膜":"pickup",
    "托架费":"chassis",
    "托架提取费":"chassis_split",
    "预提费":"prepull",
    "货柜放置费":"yard_storage",
    "操作处理费":"handling_fee",
    "码头":"pier_pass",
    "港口拥堵费":"congestion_fee",
    "吊柜费":"hanging_crane",
    "空跑费":"dry_run",
    "查验费":"exam_fee",
    "异常拆柜":"abnormal_palletization",
    "所在仓":"location",
    "联系人":"contact_name"
}
    # hazmat = models.FloatField(null=True, blank=True, verbose_name="危险品")
    # over_weight = models.FloatField(null=True, blank=True, verbose_name="超重费")
    # urgent_fee = models.FloatField(null=True, blank=True, verbose_name="加急费")
    # other_serive = models.FloatField(null=True, blank=True, verbose_name="其他服务")
    # demurrage = models.FloatField(null=True, blank=True, verbose_name="港内滞期费")
    # per_diem = models.FloatField(null=True, blank=True, verbose_name="港外滞期费")
    # second_pickup = models.FloatField(null=True, blank=True, verbose_name="二次提货")
    # amount = models.FloatField(null=True, blank=True)
    # other_fees = JSONField(default=dict)
    # surcharges = JSONField(default=dict)
    # surcharge_notes = JSONField(default=dict)

MODEL_CHOICES = {
    'packinglist': {
        'model': 'HistoricalPackingList',
        'name': 'packinglist',
        'search_field': 'container_number',
        'warehouse':'warehouse_packinglist',
        'station_field':['destination','shipping_mark','fba_id','ref_id'],
        "mapping":PACKING_LIST_TEMP_COL_MAPPING,
    },
    'pallet': {
        'model': 'HistoricalPallet',
        'name': '板子信息',
        'search_field': 'container_number',
        'warehouse':'warehouse_pallet',
        'station_field':['destination','delivery_method'],
        "mapping":PALLET_TABLE_MAPPING,
    },
    'shipment': {
        'model': 'HistoricalShipment',
        'name': '预约信息',
        'search_field': 'shipment_batch_number',
        'warehouse':'warehouse_shipment',
        'station_field':['appointment_id'],
        "mapping":SHIPMENT_TABLE_MAPPING
    },
    'invoicepreport': {
        'model': 'HistoricalInvoicePreport',
        'name': '提拆柜账单',
        'search_field': 'invoice_number',
        'warehouse':'warehouse_invoicepreport',
        'station_field':[],
        "mapping":INVOICE_PREPORT_TABLE_MAPPING,
        'indirect_search': {  # 添加间接查询配置
            'field': 'invoice_number',  # 本表的外键字段
            'related_model': 'Invoice',  # 关联模型
            'target_field': 'container_number'  # 最终要查询的字段
        }
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
