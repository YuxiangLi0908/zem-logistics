from django import forms

from warehouse.models.customer import Customer


class CustomerForm(forms.ModelForm):
    class Meta:
        model = Customer
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super(CustomerForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
