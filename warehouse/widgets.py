from django import forms
from django.forms.widgets import Select

class DropdownTextInput(forms.MultiWidget):
    def __init__(self, choices=None, attrs=None):
        widgets = [
            Select(choices=choices or []),
            forms.TextInput(attrs={'class': 'form-control'}),
        ]
        super().__init__(widgets, attrs)

    def decompress(self, value):
        if value:
            return [value, '']
        return [None, '']

    def format_output(self, rendered_widgets):
        return f'<div class="dropdown-text-input">{rendered_widgets[0]}<br>{rendered_widgets[1]}</div>'

    def value_from_datadict(self, data, files, name):
        return data[name][0] if data[name][0] != '' else None