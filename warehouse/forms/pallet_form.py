from django import forms
from warehouse.models.pallet import Pallet
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS

class PackingListForm(forms.ModelForm):
    class Meta:
        model = Pallet
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super(PackingListForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
