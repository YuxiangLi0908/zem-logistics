from django import forms
from warehouse.models.customer import Customer
from warehouse.utils.constants import (
    SHIPPING_LINE_OPTIONS,
    ORDER_TYPE_OPTIONS,
    CONTAINER_TYPE_OPTIONS,
    PORT_OPTIONS,
    DELIVERY_METHOD_OPTIONS,
    WAREHOUSE_OPTIONS,
    CARRIER_OPTIONS,
)

class CustomerForm(forms.ModelForm):
    class Meta:
        model = Customer
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super(CustomerForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False