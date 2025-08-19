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
    ("KMTC", "KMTC"),
]

CONTAINER_PICKUP_CARRIER = [
    ("", ""),
    ("东海岸", "东海岸"),
    ("kars", "kars"),
    ("大方广", "大方广"),
    ("Eric", "Eric"),
    ("客户自提", "客户自提"),
    ("GM", "GM"),
    ("客户自送", "客户自送"),
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
    ("TX-77503", "TX-77503"),
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
    "预约批次号": "shipment_batch_number",
    "ISA": "appointment_id",
    "发货仓库": "origin",
    "目的地": "destination",
    "地址": "address",
    "供应商": "carrier",
    "预约时间": "shipment_appointment",
    "出库": "is_shipped",
    "出库时间": "shipped_at",
    "到达": "is_arrived",
    "到达时间": "arrived_at",
    "装车类型": "load_type",
    "预约账户": "shipment_account",
    "预约类型": "shipment_type",
    "预约总重量": "total_weight",
    "预约总体积": "total_cbm",
    "预约总板数": "total_pallet",
    "预约总件数": "total_pcs",
    "发货总重量": "shipped_weight",
    "发货总体积": "shipped_cbm",
    "发货总板数": "shipped_pallet",
    "发货总件数": "shipped_pcs",
    "备注": "note",
    "pod链接": "pod_link",
    "pod上传时间": "pod_uploaded_at",
    "落板数": "pallet_dumpped",
    "车次": "fleet_number",
    "异常打板": "abnormal_palletization",
    "使用": "in_use",
    "取消": "is_canceled",
    "取消原因": "cancelation_reason",
    "状态": "status",
    "BOL号": "ARM_BOL",
    "PRO号": "ARM_PRO",
    "快递单号": "express_number",
}

PALLET_TABLE_MAPPING = {
    "柜号id": "container_number",
    "预约批次id": "shipment_batch_number",
    "改仓批次": "transfer_batch_number",
    "目的地": "destination",
    "邮编": "zipcode",
    "派送方式": "delivery_method",
    "唛头": "shipping_mark",
    "件数": "pcs",
    "长": "length",
    "宽": "width",
    "高": "height",
    "异常拆柜": "abnormal_palletization",
    "所在仓": "location",
    "联系人": "contact_name",
}

INVOICE_PREPORT_TABLE_MAPPING = {
    "提拆/打托缠膜": "pickup",
    "托架费": "chassis",
    "托架提取费": "chassis_split",
    "预提费": "prepull",
    "货柜放置费": "yard_storage",
    "操作处理费": "handling_fee",
    "码头": "pier_pass",
    "港口拥堵费": "congestion_fee",
    "吊柜费": "hanging_crane",
    "空跑费": "dry_run",
    "查验费": "exam_fee",
    "异常拆柜": "abnormal_palletization",
    "所在仓": "location",
    "联系人": "contact_name",
    "危险品": "hazmat",
    "超重费": "over_weight",
    "加急费": "urgent_fee",
    "其他服务": "other_serive",
    "港内滞期费": "demurrage",
    "港外滞期费": "per_diem",
    "二次提货": "second_pickup",
    "总计": "amount",
    "额外费用": "other_fees",
    "附加费": "surcharges",
    "附加费说明": "surcharge_notes",
}
INVOICE_WAREHOUSE_TABLE_MAPPING = {
    "账单类型": "invoice_type",
    "公仓/私仓": "delivery_type",
    "分拣费": "sorting",
    "拦截费": "intercept",
    "客户自提": "self_pickup",
    "拆柜交付快递": "split_delivery",
    "重新打板": "re_pallet",
    "货品清点费": "counting",
    "仓租": "warehouse_rent",
    "指定贴标": "specified_labeling",
    "内外箱": "inner_outer_box",
    "托盘标签": "pallet_label",
    "开封箱": "open_close_box",
    "销毁": "destroy",
    "拍照": "take_photo",
    "拍视频": "take_video",
    "重复操作费": "repeated_operation_fee",
    "总计": "amount",
    "港内滞期费": "rate",
    "港外滞期费": "qty",
    "额外费用": "other_fees",
    "附加费": "surcharges",
    "附加费说明": "surcharge_notes",
}
INVOICE_DELIVERY_TABLE_MAPPING = {
    "名称": "invoice_delivery",
    "账单类型": "invoice_type",
    "公仓/私仓": "delivery_type",
    "派送方式": "type",
    "亚马逊PO激活": "po_activation",
    "目的地": "destination",
    "邮编": "zipcode",
    "总板数": "total_pallet",
    "单价": "cost",
    "总cbm": "total_cbm",
    "总重": "total_weight_lbs",
    "总金额": "total_cost",
    "单价": "expense",
    "备注": "note",
}
RETRIEVAL_TEMP_COL_MAPPING = {
    "预计提柜时间": "target_retrieval_timestamp",
    "预计最早提柜时间": "target_retrieval_timestamp_lower",
    "实际提柜时间": "actual_retrieval_timestamp",
    "到仓": "arrive_at_destination",
    "到仓时间": "arrive_at",
    "还空": "empty_returned",
    "还空时间": "empty_returned_at",
    "目的地": "retrieval_destination_precise",
    "所属仓": "retrieval_destination_area",
    "备注": "note",
    "供应商": "retrieval_carrier",
    "原始港口": "origin_port",
    "目的港口": "destination_port",
}
INVOICE_ITEM_TABLE_MAPPING = {
    "名称": "description",
    "仓库": "warehouse_code",
}
INVOICE_TABLE_MAPPING = {
    "应收总金额": "receivable_total_amount",
    "应付总金额": "payable_total_amount",
    "通知客户": "is_invoice_delivered",
    "最新账单生成时间": "invoice_date",
    "账单链接": "invoice_link",
    "应收提拆费": "receivable_preport_amount",
    "应收仓库费": "receivable_warehouse_amount",
    "应收派送费": "receivable_delivery_amount",
    "应收直送费": "receivable_direct_amount",
    "应付提拆费": "payable_preport_amount",
    "应付仓库费": "payable_warehouse_amount",
    "应付派送费": "payable_delivery_amount",
    "应付直送费": "payable_direct_amount",
    "待核销金额": "remain_offset",
}
INVOICESTATUS_TABLE_MAPPING = {
    "账单类型": "invoice_type",
    "主状态": "stage",
    "公仓状态": "stage_public",
    "私仓状态": "stage_other",
    "拒绝": "is_rejected",
    "拒绝原因": "reject_reason",
}
ORDER_TABLE_MAPPING = {
    "柜型": "order_type",
}
MODEL_CHOICES = {
    # 直接根据container_number就能找的类型
    "packinglist": {
        "model": "HistoricalPackingList",
        "name": "packinglist",
        "search_field": "container_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_packinglist",
        "station_field": ["destination", "shipping_mark", "fba_id", "ref_id"],
        "mapping": PACKING_LIST_TEMP_COL_MAPPING,
        "transfer_table": None,
    },
    "pallet": {
        "model": "HistoricalPallet",
        "name": "板子信息",
        "search_field": "container_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_pallet",
        "station_field": ["destination", "delivery_method"],
        "mapping": PALLET_TABLE_MAPPING,
        "transfer_table": None,
    },
    "order": {
        "model": "HistoricalOrder",
        "name": "订单信息",
        "search_field": "container_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_pallet",
        "station_field": [],
        "mapping": ORDER_TABLE_MAPPING,
        "transfer_table": None,
    },
    "shipment": {
        "model": "HistoricalShipment",
        "name": "预约信息",
        "search_field": "shipment_batch_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_shipment",
        "station_field": ["appointment_id"],
        "mapping": SHIPMENT_TABLE_MAPPING,
        "transfer_table": None,
    },
    # 没有外键的类型，需要先根据原始表找到id，再匹配到对应历史表
    "retrieval": {
        "model": "Retrieval",
        "name": "调度信息",
        "search_field": "container_number",
        "search_process": "order__container_number__container_number",
        "has_foreignKey": False,
        "warehouse": "warehouse_packinglist",
        "station_field": [],
        "mapping": RETRIEVAL_TEMP_COL_MAPPING,
        "transfer_table": None,
    },
    # 有外键，但是需要另外一张表中转的
    "invoicepreport": {
        "model": "HistoricalInvoicePreport",
        "name": "提拆柜账单",
        "search_field": "invoice_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoicepreport",
        "station_field": [],
        "mapping": INVOICE_PREPORT_TABLE_MAPPING,
        "transfer_table": "Invoice",
    },
    "invoicewarehouse": {
        "model": "HistoricalInvoiceWarehouse",
        "name": "库内账单",
        "search_field": "invoice_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoicewarehouse",
        "station_field": [],
        "mapping": INVOICE_WAREHOUSE_TABLE_MAPPING,
        "transfer_table": "Invoice",
    },
    "invoicedelivery": {
        "model": "HistoricalInvoiceDelivery",
        "name": "派送账单",
        "search_field": "invoice_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoicedelivery",
        "station_field": [],
        "mapping": INVOICE_DELIVERY_TABLE_MAPPING,
        "transfer_table": "Invoice",
    },
    "invoiceitem": {
        "model": "HistoricalInvoiceItem",
        "name": "账单生成明细",
        "search_field": "invoice_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoiceitem",
        "station_field": [],
        "mapping": INVOICE_ITEM_TABLE_MAPPING,
        "transfer_table": "Invoice",
    },
    "invoice": {
        "model": "HistoricalInvoice",
        "name": "总账单",
        "search_field": "invoice_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoice",
        "station_field": [],
        "mapping": INVOICE_TABLE_MAPPING,
        "transfer_table": None,
    },
    "invoicestatus": {
        "model": "HistoricalInvoiceStatus",
        "name": "账单状态",
        "search_field": "container_number",
        "search_process": "container_number__container_number",
        "has_foreignKey": True,
        "warehouse": "warehouse_invoicstatus",
        "station_field": [],
        "mapping": INVOICESTATUS_TABLE_MAPPING,
        "transfer_table": None,
    },
}
file_path = Path(__file__).parent.resolve().joinpath("fba_fulfillment_center.yaml")
with open(file_path, "r", encoding='utf-8') as f:
    amazon_fba_locations = yaml.safe_load(f)

SP_USER = os.environ.get("MS_SP_USER")
SP_PASS = os.environ.get("MS_SP_PASS")
SP_URL = os.environ.get("MS_SP_URL")
SP_SITE = os.environ.get("MS_SP_SITE")
SP_DOC_LIB = os.environ.get("MS_SP_DOC_LIB")
SYSTEM_FOLDER = "system_archive"
# 未设置APP_ENV返回staging预发布环境，值为production返回prd生产环境，
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
