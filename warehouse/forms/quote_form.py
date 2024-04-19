from django import forms
from warehouse.models.quote import Quote
from warehouse.utils.constants import QUOTE_PLATFORM_OPTIONS


class QuoteForm(forms.ModelForm):
    class Meta:
        model = Quote
        fields = "__all__"
        widgets = {
            "created_at": forms.DateInput(attrs={'type':'date'}),
            "pickup_date": forms.DateInput(attrs={'type':'date', 'style':'width: 100%'}),
            "is_lift_gate": forms.Select(choices=[("no", "no"), ("yes", "yes")]),
            "is_oversize": forms.Select(choices=[("no", "no"), ("yes", "yes"),]),
            "load_type": forms.Select(choices=[("LTL", "LTL"), ("FTL", "FTL")]),
            "platform": forms.Select(choices=QUOTE_PLATFORM_OPTIONS),
            "note": forms.Textarea(attrs={'rows': '6'}),
        }

    def __init__(self, *args, **kwargs) -> None:
        super(QuoteForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
            self.fields[k].widget.attrs.update({
                'style':'width: 100%',
            })
