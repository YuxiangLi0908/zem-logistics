from django import forms
from warehouse.models.container import Container
from warehouse.utils.constants import CONTAINER_TYPE_OPTIONS

class ContainerForm(forms.ModelForm):
    class Meta:
        model = Container
        fields = "__all__"
        widgets = {
            "container_type": forms.Select(choices=CONTAINER_TYPE_OPTIONS),
        }
        labels = {
            "container_number": "货柜号(#Ref)",
            "container_type": "货柜类型",
        }