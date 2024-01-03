from django import forms
from .models import Customer, Container, PackingList, ZemWarehouse
from .utils.constants import (
    SHIPPING_LINE_OPTIONS,
    PICKUP_METHOD_OPTIONS,
    ORDER_TYPE_OPTIONS,
    CONTAINER_TYPE_OPTIONS,
    PORT_OPTIONS,
    DELIVERY_METHOD_OPTIONS,
    WAREHOUSE_OPTIONS,
)

class ContainerForm(forms.ModelForm):
    class Meta:
        model = Container
        fields = "__all__"
        exclude = [
            "port_arrived_at", "port_picked_at", "warehouse_arrived_at", "unpacked_at",
            "pickup_scheduled_at", "pickup_appointment", "palletized_at"
        ]
        widgets = {
            "created_at": forms.DateInput(attrs={'type':'date'}),
            "eta": forms.DateInput(attrs={'type':'date'}),
            "container_type": forms.Select(choices=CONTAINER_TYPE_OPTIONS),
            "order_type": forms.Select(choices=ORDER_TYPE_OPTIONS),
            "pickup_method": forms.Select(choices=PICKUP_METHOD_OPTIONS),
            "shipping_line": forms.Select(choices=SHIPPING_LINE_OPTIONS),
            "departure_port": forms.Select(choices=PORT_OPTIONS),
            "destination_port": forms.Select(choices=PORT_OPTIONS),
        }
        labels = {
            "order_type": "订单类型",
            "container_number": "柜号(#Ref)",
            "customer_name": "客户",
            "created_at": "建单日期",
            "eta": "ETA",
            "shipping_line": "船/空运公司",
            "container_type": "柜型",
            "departure_port": "起运港",
            "destination_port": "目的港",
            "warehouse": "入仓仓库",
            "pickup_method": "提柜方式",
            "pickup_id": "提单号",
        }

class PackingListForm(forms.ModelForm):
    class Meta:
        model = PackingList
        fields = "__all__"
        exclude = [
            "container_number", "n_pallet", "is_shipment_schduled", "shipment_schduled_at",
            "is_shipped", "shipped_at", "shipment_appointment", "shipment_batch_number"
        ]
        widgets = {
            "delivery_method": forms.Select(choices=DELIVERY_METHOD_OPTIONS),
        }
        labels = {
            "container_number": "货柜号",
            "product_name": "品名",
            "delivery_method": "派送方式",
            "shipping_mark": "唛头",
            "fba_id": "FBA号",
            "destination": "目的仓库",
            "address": "地址",
            "zipcode": "邮编",
            "ref_id": "refid",
            "pcs": "箱数",
            "unit_weight_kg": "单箱重量-kg",
            "total_weight_kg": "总重量-kg",
            "unit_weight_lbs": "单箱重量-lbs",
            "total_weight_lbs": "总重量-lbs",
            "cbm": "CBM",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        nullable_fields = [
            'product_name', 'shipping_mark', 'fba_id', 'address', 'zipcode', 
            'ref_id', 'unit_weight_kg', 'total_weight_kg', 'unit_weight_lbs',
            'total_weight_lbs'
        ]
        for field_name in nullable_fields:
            self.fields[field_name].required = False
        for field_name in self.Meta.labels.keys():
            if field_name not in self.Meta.exclude:
                self.fields[field_name].widget.attrs['style'] = 'width:180px; height:25px; font-size: 13px'

class UpdatePickupForm(forms.Form):
    pickup_at = forms.DateField(widget=forms.DateInput(attrs={'type': 'date'}))

class UpdatePalletizationForm(forms.ModelForm):
    class Meta:
        model = PackingList
        fields = [
            "id", "container_number", "shipping_mark", "fba_id", "ref_id", 
            "destination", "pcs", "cbm", "n_pallet"
        ]

class WarehouseSelectForm(forms.Form):
    warehouse = forms.CharField(widget=forms.Select(choices=WAREHOUSE_OPTIONS))

class ShipmentForm(forms.ModelForm):
    class Meta:
        model = PackingList
        fields = [
            "id", "container_number", "shipping_mark", "fba_id", "ref_id", 
            "destination", "pcs", "cbm", "n_pallet", "is_shipment_schduled",
            "shipment_appointment"
        ]
        widgets = {"shipment_appointment": forms.DateInput(attrs={'type':'date'})}

    def __init__(self, *args, **kwargs):
        super(ShipmentForm, self).__init__(*args, **kwargs)
        self.fields['shipment_appointment'].required = False
