from django import forms
from warehouse.models.order import Order
from warehouse.utils.constants import ORDER_TYPE_OPTIONS

class OrderForm(forms.ModelForm):
    class Meta:
        model = Order
        fields = "__all__"
        widgets = {
            "eta": forms.DateInput(attrs={'type':'date'}),
            "order_type": forms.Select(choices=ORDER_TYPE_OPTIONS),
        }
        labels = {
            "order_id": "订单号",
            "customer_name": "客户",
            "container_number": "货柜号(#Ref)",
            "warehouse": "仓库",
            "eta": "ETA",
            "order_type": "订单类型",
            "clearance_id": "清关ID",
            "retrieval_id": "提柜ID",
            "offload_id": "卸柜ID",
        }

    def __init__(self, *args, **kwargs):
        super(OrderForm, self).__init__(*args, **kwargs)
        self.fields['container_number'].required = False
        self.fields['clearance_id'].required = False
        self.fields['retrieval_id'].required = False
        self.fields['offload_id'].required = False
        self.fields['eta'].required = False