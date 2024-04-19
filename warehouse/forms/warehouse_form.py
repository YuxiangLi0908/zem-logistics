from django import forms
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import WAREHOUSE_OPTIONS


class ZemWarehouseForm(forms.ModelForm):
    class Meta:
        model = ZemWarehouse
        fields = "__all__"
        widgets = {
            "name": forms.Select(choices=WAREHOUSE_OPTIONS),
        }