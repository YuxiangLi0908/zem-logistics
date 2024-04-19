from django import forms
from warehouse.models.offload import Offload


class OffloadForm(forms.ModelForm):
    class Meta:
        model = Offload
        fields = "__all__"