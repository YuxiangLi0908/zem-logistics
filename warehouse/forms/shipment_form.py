from django import forms
from warehouse.models.shipment import Shipment
from warehouse.utils.constants import CARRIER_OPTIONS, LOAD_TYPE_OPTIONS

class ShipmentForm(forms.ModelForm):
    class Meta:
        model = Shipment
        fields = "__all__"
        widgets = {
                "shipment_appointment": forms.DateTimeInput(attrs={'type':'datetime-local'}),
                "carrier": forms.Select(choices=CARRIER_OPTIONS),
                "shipped_at": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
                "arrived_at": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
                "load_type": forms.Select(choices=LOAD_TYPE_OPTIONS)
            }

    def __init__(self, *args, **kwargs) -> None:
        super(ShipmentForm, self).__init__(*args, **kwargs)
        self.fields['shipment_appointment'].required = False
        self.fields['appointment_id'].required = False