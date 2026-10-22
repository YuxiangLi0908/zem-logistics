from django import forms

from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import WAREHOUSE_OPTIONS


class ZemWarehouseForm(forms.ModelForm):
    def __init__(self, *args, warehouse_options=None, **kwargs):
        super().__init__(*args, **kwargs)
        if warehouse_options is not None:
            self.fields["name"].widget.choices = warehouse_options

    class Meta:
        model = ZemWarehouse
        fields = "__all__"
        widgets = {
            "name": forms.Select(
                choices=WAREHOUSE_OPTIONS, attrs={"class": "form-select"}
            )
        }
