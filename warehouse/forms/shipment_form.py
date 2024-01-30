from django import forms
from warehouse.models.shipment import Shipment
from warehouse.utils.constants import (
    SHIPPING_LINE_OPTIONS,
    ORDER_TYPE_OPTIONS,
    CONTAINER_TYPE_OPTIONS,
    PORT_OPTIONS,
    DELIVERY_METHOD_OPTIONS,
    WAREHOUSE_OPTIONS,
    CARRIER_OPTIONS,
)

class ShipmentForm(forms.ModelForm):
    class Meta:
        model = Shipment
        fields = "__all__"