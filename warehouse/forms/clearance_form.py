from django import forms
from warehouse.models.clearance import Clearance
from warehouse.utils.constants import CLEARANCE_OPTIONS


class ClearanceForm(forms.ModelForm):
    class Meta:
        model = Clearance
        fields = "__all__"
        labels = {
            "clearance_id": "清关ID",
        }
    
    def __init__(self, *args, **kwargs):
        super(ClearanceForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False

class ClearanceSelectForm(forms.Form):
    clearance_option = forms.CharField(widget=forms.Select(choices=CLEARANCE_OPTIONS))