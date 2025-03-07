from django import forms

from warehouse.models.customer import Customer


class CustomerForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput(), required=True)

    class Meta:
        model = Customer
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super(CustomerForm, self).__init__(*args, **kwargs)
        for k in self.fields.keys():
            self.fields[k].required = False
        self.fields["username"].widget.attrs["disabled"] = True
