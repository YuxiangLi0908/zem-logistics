from django import forms

from warehouse.models.warehouse import ZemWarehouse


def with_blank_warehouse_option(options):
    return [("", "")] + [
        (value, label) for value, label in options if value and str(value).strip()
    ]


def get_warehouse_options():
    """渲染下拉框时读取最新仓库，不在导入模块时查询数据库。"""
    return with_blank_warehouse_option(
        ZemWarehouse.objects.order_by("name").values_list("name", "name").distinct()
    )


class ZemWarehouseForm(forms.ModelForm):
    def __init__(self, *args, warehouse_options=None, **kwargs):
        super().__init__(*args, **kwargs)
        if warehouse_options is not None:
            self.fields["name"].widget.choices = with_blank_warehouse_option(warehouse_options)

    class Meta:
        model = ZemWarehouse
        fields = "__all__"
        widgets = {
            "name": forms.Select(
                choices=get_warehouse_options, attrs={"class": "form-select"}
            )
        }
