from io import BytesIO

from django.http import FileResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.utils.constants import PACKING_LIST_TEMP_COL_MAPPING


def export_order_packing_list(request):
    """Export persisted rows for one order, independently of browser edits/filters."""
    container_number = request.POST.get("container_number", "").strip()
    if not container_number:
        return HttpResponseBadRequest("请指定要导出的柜号")
    order = get_object_or_404(
        Order, container_number__container_number=container_number
    )
    columns = {
        **PACKING_LIST_TEMP_COL_MAPPING,
        "派送类别": "delivery_type",
        "板数": "n_pallet",
        "客服备注": "office_note",
    }
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Packing List"
    sheet.append(list(columns))
    for row in PackingList.objects.filter(
        container_number_id=order.container_number_id
    ).order_by("id").iterator():
        values = []
        for field in columns.values():
            if field == "unit_weight_kg":
                value = row.total_weight_kg / row.pcs if row.pcs and row.total_weight_kg is not None else None
            else:
                value = getattr(row, field)
            values.append(value)
        sheet.append(values)
        for cell in sheet[sheet.max_row]:
            # Uploaded identifiers and notes are text, never executable Excel formulas.
            if isinstance(cell.value, str):
                cell.data_type = "s"
                cell.number_format = "@"
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    for cell in sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="3275D9")
    for index, field in enumerate(columns.values(), 1):
        sheet.column_dimensions[get_column_letter(index)].width = (
            40 if field in {"address", "note", "office_note"} else 20
        )
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return FileResponse(
        output,
        as_attachment=True,
        filename=f"{container_number}_packing_list.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
