from django import forms
from .models import Customer, Container, PackingList

# class OrderForm(forms.ModelForm):
#     class Meta:
#         model = Order
#         fields = ["container_id", "eta"]
#         widgets = {
#             'eta': forms.DateInput(attrs={'type':'date'}),
#         }
#         labels = {
#             'eta': 'ETA'
#         }

# class PortForm(forms.ModelForm):
#     departure_port = forms.ModelChoiceField(
#         queryset=Port.objects.all(),
#         widget=forms.Select(attrs={'class': 'form-control'}),
#         required=False,
#         empty_label='--- Select Departure Port ---',
#     )
#     arrival_port = forms.ModelChoiceField(
#         queryset=Port.objects.all(),
#         widget=forms.Select(attrs={'class': 'form-control'}),
#         required=False,
#         empty_label='--- Select Arrival Port ---',
#     )

#     class Meta:
#         model = Port
#         fields = []

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.fields['departure_port'].widget = forms.Select(choices=self.get_port_choices())
#         self.fields['departure_port'].label = 'Departure Port'
#         self.fields['arrival_port'].widget = forms.Select(choices=self.get_port_choices())
#         self.fields['arrival_port'].label = 'Arrival Port'

#     def get_port_choices(self):
#         choices = [('',  '')]
#         choices += [(port.id, f"{port.code} - {port.name}") for port in Port.objects.all()]
#         return choices

# class WarehouseForm(forms.ModelForm):
#     class Meta:
#         model = Warehouse
#         fields = "__all__"

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.fields["name"].widget = forms.Select(choices=[(w.id, w.name) for w in Warehouse.objects.all()])
#         self.fields['name'].label = 'Warehouse'

# class CustomerForm(forms.ModelForm):
#     class Meta:
#         model = Customer
#         fields = "__all__"

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.fields["name"].widget = forms.Select(choices=[(c.id, c.name) for c in Customer.objects.all()])
#         self.fields['name'].label = 'Customer'


class ContainerForm(forms.ModelForm):
    class Meta:
        model = Container
        exclude = [
            "port_arrived_at", "port_picked_at", "warehouse_arrived_at", "unpacked_at"
        ]
        widgets = {
            "created_at": forms.DateInput(attrs={'type':'date'}),
            "eta": forms.DateInput(attrs={'type':'date'}),
            "container_type": forms.Select()
        }
        labels = {
            "container_id": "货柜号",
            "customer_name": "客户",
            "created_at": "建单日期",
            "eta": "ETA",
            "shipping_line": "船/空运公司",
            "container_type": "柜型",
            "departure_port": "起运港",
            "destination_port": "目的港",
            "warehouse": "入仓仓库",
        }


class PackingListForm(forms.ModelForm):
    class Meta:
        model = PackingList
        fields = "__all__"
        exclude = [
            "container_id"
        ]
        labels = {
            "container_id": "货柜号",
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