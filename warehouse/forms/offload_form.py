from django import forms
from warehouse.models.offload import Offload


class OffloadForm(forms.ModelForm):
    class Meta:
        model = Offload
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super(OffloadForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
            if k in ["devanning_company", "devanning_fee"]:
                self.fields[k].required = True
            else:
                self.fields[k].required = False