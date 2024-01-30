ORDER_TYPES = {
    "TD": "转运",
    "DD": "直送",
}

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

ORDER_TYPE_OPTIONS = [
    ("", ""), ('整柜海卡', '整柜海卡'), ('码头直送', '码头直送'),
    ('美国境内中转', '美国境内中转'), ('专线', '专线'), ('其它', '其它'),
]

CONTAINER_TYPE_OPTIONS = [
    ("", ""), ('45HQ/GP', '45HQ/GP'), ('40HQ/GP', '40HQ/GP'),
    ('20GP', '20GP'), ('53HQ', '53HQ'),
]

PORT_OPTIONS = [
    ("", ""), ("N/A", "N/A"), ("LOS ANGELES", "LOS ANGELES"), ("NEW YORK", "NEW YORK"), ("NINGBO", "NINGBO"),
    ("QINGDAO", "QINGDAO"), ("SAVANNAH", "SAVANNAH"), ("TAMPA", "TAMPA"), ("XIAMEN", "XIAMEN"),
    ("YANTIAN", "YANTIAN"),
]

DELIVERY_METHOD_OPTIONS = [
    ("", ""),
    ("卡车派送", "卡车派送"),
    ("客户自提", "客户自提"),
    ("暂扣留仓", "暂扣留仓"),
    ("整柜直送", "整柜直送"),
    ("UPS", "UPS"),
    ("FEDEX", "FEDEX"),
    ("DHL", "DHL"),
]

WAREHOUSE_OPTIONS = [
    ("", ""), ("美东新泽西仓", "美东新泽西仓"), ("美东南萨瓦纳仓", "美东南萨瓦纳仓"),
    ("N/A", "N/A")
]

CARRIER_OPTIONS = [
    ("UPS", "UPS"), ("FedEx", "FedEx"), ("ZEM", "ZEM"), ("客户自提", "客户自提")
]
