from django import forms
from warehouse.models.offload import Offload
from warehouse.utils.constants import (
    SHIPPING_LINE_OPTIONS,
    ORDER_TYPE_OPTIONS,
    CONTAINER_TYPE_OPTIONS,
    PORT_OPTIONS,
    DELIVERY_METHOD_OPTIONS,
    WAREHOUSE_OPTIONS,
    CARRIER_OPTIONS,
)

class OffloadForm(forms.ModelForm):
    class Meta:
        model = Offload
        fields = "__all__"