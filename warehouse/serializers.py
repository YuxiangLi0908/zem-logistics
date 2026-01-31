# warehouse/serializers.py
from rest_framework import serializers

# 港前数据序列化器（对应 _get_pre_port_data 返回的字典）
class PrePortDataSerializer(serializers.Serializer):
    container_number = serializers.CharField(help_text="货柜号")
    customer_name = serializers.CharField(help_text="客户名称", allow_null=True, required=False)
    warehouse_name = serializers.CharField(help_text="仓库名称", allow_null=True, required=False)
    order_created = serializers.DateTimeField(help_text="订单创建时间", format="%Y-%m-%d %H:%M:%S", allow_null=True, required=False)
    port_arrival = serializers.DateField(help_text="到港时间", format="%Y-%m-%d", allow_null=True, required=False)
    target_retrieval = serializers.DateTimeField(help_text="计划提柜时间", format="%Y-%m-%d %H:%M:%S", allow_null=True, required=False)
    actual_retrieval = serializers.DateTimeField(help_text="实际提柜时间", format="%Y-%m-%d %H:%M:%S", allow_null=True, required=False)
    warehouse_arrival = serializers.DateTimeField(help_text="到仓时间", format="%Y-%m-%d %H:%M:%S", allow_null=True, required=False)
    unloading_completed = serializers.DateTimeField(help_text="卸柜完成时间", format="%Y-%m-%d %H:%M:%S", allow_null=True, required=False)
    status = serializers.CharField(help_text="柜子状态（英文）")
    status_display = serializers.SerializerMethodField(help_text="柜子状态（中文）")

    # 把英文状态转中文（和你现有 _get_status_display 逻辑一致）
    def get_status_display(self, obj):
        status_map = {
            'completed': '已完成',
            'at_warehouse': '已到仓',
            'retrieved': '已提柜',
            'at_port': '已到港',
            'ordered': '已下单'
        }
        return status_map.get(obj['status'], obj['status'])

# 港后仓库数据序列化器（对应 _get_post_port_data 返回的 warehouse 字典）
class PostPortWarehouseSerializer(serializers.Serializer):
    container_number = serializers.CharField(help_text="货柜号")
    customer_name = serializers.CharField(help_text="客户名称", allow_null=True, required=False)
    warehouse_name = serializers.CharField(help_text="仓库名称", allow_null=True, required=False)
    destination = serializers.CharField(help_text="目的地", allow_null=True, required=False)
    address = serializers.CharField(help_text="地址", allow_null=True, required=False)
    delivery_method = serializers.CharField(help_text="配送方式", allow_null=True, required=False)
    pallet_count = serializers.IntegerField(help_text="板数", allow_null=True, required=False)
    pallet_type = serializers.CharField(help_text="板类型（ACT/EST）", allow_null=True, required=False)
    status = serializers.CharField(help_text="仓点状态（英文）")
    status_display = serializers.CharField(help_text="仓点状态（中文）")
    total_pcs = serializers.IntegerField(help_text="总件数", allow_null=True, required=False)
    total_cbm = serializers.FloatField(help_text="总体积", allow_null=True, required=False)
    total_weight_lbs = serializers.FloatField(help_text="总重量（磅）", allow_null=True, required=False)

# 状态汇总序列化器
class StatusSummarySerializer(serializers.Serializer):
    pending = serializers.IntegerField(help_text="待预约")
    scheduled = serializers.IntegerField(help_text="已预约")
    in_transit = serializers.IntegerField(help_text="运输中")
    delivered = serializers.IntegerField(help_text="已送达")
    completed = serializers.IntegerField(help_text="已完成")

# 最终返回的整体数据序列化器
class ContainerQueryResponseSerializer(serializers.Serializer):
    pre_port_data = PrePortDataSerializer(many=True, help_text="港前数据", allow_null=True, required=False)
    post_port_data = PostPortWarehouseSerializer(many=True, help_text="港后数据", allow_null=True, required=False)
    post_port_table = PostPortWarehouseSerializer(many=True, help_text="港后表格数据", allow_null=True, required=False)
    status_summary = StatusSummarySerializer(help_text="状态汇总", allow_null=True, required=False)
    query_params = serializers.DictField(help_text="查询参数", allow_null=True, required=False)