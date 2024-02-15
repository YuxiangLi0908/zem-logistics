from django import forms
from warehouse.models.retrieval import Retrieval
from warehouse.utils.constants import RETRIEVAL_OPTIONS, PORT_OPTIONS, SHIPPING_LINE_OPTIONS

class RetrievalForm(forms.ModelForm):
    class Meta:
        model = Retrieval
        fields = "__all__"
        widgets = {
            "origin": forms.Select(choices=PORT_OPTIONS),
            "destination": forms.Select(choices=PORT_OPTIONS),
            "shipping_line": forms.Select(choices=SHIPPING_LINE_OPTIONS),
            "target_retrieval_timestamp": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
            "actual_retrieval_timestamp": forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        }
    
    def __init__(self, *args, **kwargs):
        super(RetrievalForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
                 
class RetrievalSelectForm(forms.Form):
    retrieval_option = forms.CharField(widget=forms.Select(choices=RETRIEVAL_OPTIONS))