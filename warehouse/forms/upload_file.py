from django import forms


class UploadFileForm(forms.Form):
    def __init__(self, *args, **kwargs):
        required = kwargs.pop("required", False)
        super(UploadFileForm, self).__init__(*args, **kwargs)
        self.fields["file"].required = required

    file = forms.FileField()
