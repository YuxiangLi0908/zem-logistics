from django import forms
from warehouse.models.packing_list import PackingList
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS


class PackingListForm(forms.ModelForm):
    class Meta:
        model = PackingList
        fields = "__all__"
        widgets = {
            "delivery_method": forms.Select(choices=DELIVERY_METHOD_OPTIONS),
        }

    def __init__(self, *args, **kwargs):
        super(PackingListForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
