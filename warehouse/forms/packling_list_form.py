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
        self.fields['container_number'].required = False
        self.fields['n_pallet'].required = False
        self.fields['shipment_batch_number'].required = False
        self.fields['product_name'].required = False
        self.fields['shipping_mark'].required = False
        self.fields['fba_id'].required = False
        self.fields['address'].required = False
        self.fields['zipcode'].required = False
        self.fields['ref_id'].required = False
        self.fields['unit_weight_lbs'].required = False

        for field_name in self.fields.keys():
            self.fields[field_name].widget.attrs['style'] = 'width:180px; height:25px; font-size: 13px'