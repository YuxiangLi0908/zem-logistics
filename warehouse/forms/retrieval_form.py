from django import forms
from warehouse.models.retrieval import Retrieval
from warehouse.utils.constants import RETRIEVAL_OPTIONS, PORT_OPTIONS, SHIPPING_LINE_OPTIONS


class RetrievalForm(forms.ModelForm):
    class Meta:
        model = Retrieval
        fields = "__all__"
        widgets = {
            "origin_port": forms.Select(choices=PORT_OPTIONS),
            "destination_port": forms.Select(choices=PORT_OPTIONS),
            "shipping_line": forms.Select(choices=SHIPPING_LINE_OPTIONS),
            "target_retrieval_timestamp": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
            "actual_retrieval_timestamp": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        }
    
    def __init__(self, *args, **kwargs):
        super(RetrievalForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            if k in ["trucking_fee"]:
                self.fields[k].required = True
            else:
                self.fields[k].required = False
                 
class RetrievalSelectForm(forms.Form):
    retrieval_option = forms.CharField(widget=forms.Select(choices=RETRIEVAL_OPTIONS))