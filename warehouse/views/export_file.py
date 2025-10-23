import io
import json
import os
import zipfile
from datetime import datetime
from io import BytesIO
import openpyxl
from openpyxl.styles import Font, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from typing import Any
import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.postgres.aggregates import StringAgg
from django.contrib.staticfiles import finders
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db.models import (
    Case,
    CharField,
    Count,
    DateTimeField,
    F,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Concat
from django.forms import model_to_dict
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.template.loader import get_template
from django.utils.decorators import method_decorator
from django.views import View
from xhtml2pdf import pisa

from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.utils.constants import (
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE,
)


@method_decorator(login_required(login_url="login"), name="dispatch")
class ExportFile(View):
    template_main = {"DO": "export_file/do.html", "PL": "export_file/packing_list.html"}
    file_name = {"DO": "D/O", "PL": "拆柜单"}

    def get(self, request: HttpRequest) -> HttpResponse:
        name = request.GET.get("name")
        template_path = self.template_main[name]
        template = get_template(template_path)
        context = {"sample_data": "Hello, this is some sample data!"}
        html = template.render(context)

        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{self.file_name[name]}.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response


def export_bol(context: dict[str, Any]) -> HttpResponse:
    template_path = "export_file/bol_template.html"
    template = get_template(template_path)
    html = template.render(context)
    response = HttpResponse(content_type="application/pdf")
    response["Content-Disposition"] = (
        f'attachment; filename="BOL_{context["batch_number"]}.pdf"'
    )
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        raise ValueError(
            "Error during PDF generation: %s" % pisa_status.err,
            content_type="text/plain",
        )
    return response


async def export_palletization_list_v2(request: HttpRequest) -> HttpResponse:
    """
    (新)拆柜单导出
    """
    status = request.POST.get("status")
    warehouse = request.POST.get("warehouse").split("-")[0].upper()
    container_number = request.POST.get("container_number")
    warehouse_unpacking_time = request.POST.get("first_time_download")
    try:
        warehouse_unpacking_time = datetime.strptime(warehouse_unpacking_time, "%Y-%m-%d %H:%M:%S").strftime(
            "%d/%m /%Y")
    except (ValueError, TypeError):
        warehouse_unpacking_time = "未获取到时间"

    if status == "non_palletized":
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related("container_number", "pallet")
            .filter(container_number__container_number=container_number)
            .annotate(
                custom_delivery_method=Case(
                    When(
                        Q(delivery_method="暂扣留仓(HOLD)")
                        | Q(delivery_method="暂扣留仓"),
                        then=Concat(
                            "delivery_method", Value("-"), "fba_id", Value("-"), "id"
                        ),
                    ),
                    When(
                        Q(delivery_method="客户自提") | Q(destination="客户自提"),
                        then=Concat(
                            "delivery_method",
                            Value("-"),
                            "destination",
                            Value("-"),
                            "shipping_mark",
                        ),
                    ),
                    default=F("delivery_method"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
                str_shipping_mark=Cast("shipping_mark", CharField()),
            )
            .values(
                "container_number__container_number",
                "destination",
                "address",
                "zipcode",
                "contact_name",
                "custom_delivery_method",
                "note",
                "shipment_batch_number__shipment_batch_number",
                "PO_ID",
                "delivery_type",
            )
            .annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                shipping_marks=StringAgg(
                    "str_shipping_mark", delimiter=",", distinct=True
                ),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count("pallet__pallet_id", distinct=True),
                weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
            )
            .order_by("-cbm")
        )
    else:
        packing_list = []

    data = [i for i in packing_list]
    df = pd.DataFrame.from_records(data)
    slot_rules = {
        "SAV": {
            "destinations": {"BNA2", "BNA6", "CHA2", "CLT2", "CLT3", "GSO1", "HSV1", "IUS3",
                             "JAX3", "MCO2", "MGE3", "MEM1", "PBI3", "RDU2", "RDU4", "RYY2",
                             "SAV3", "TMB8", "TPA2", "TPA3", "TPA6", "WALMART-ATL1", "WALMART-ATL3",
                             "WALMART-MCO1", "XAV3", "XLX6", "XPB2"},
            "slot": "NQ1"
        },
        "NJ": {
            "destinations": {"ABE4", "ABE8", "ACY2", "ALB1", "AVP1", "BOS7", "BWI4", "CHO1",
                             "CMH2", "CMH3", "DEN4", "DET1", "DET2", "HGR6", "ILG1", "IUS1",
                             "LBE1", "MDT1", "MDT4", "ORF2", "PHL6", "PIT2", "RNN3", "SWF1",
                             "SWF2", "TEB3", "TEB4", "TEB6"},
            "slot": "NQ2"
        },
        "LA": {
            "rules": [
                {"destinations": {"ABQ2", "DCA6", "DEN8", "FAT2", "FTW1", "FWA4", "GEU2", "GEU3"}, "slot": "NR"},
                {"destinations": {"GEU5", "GYR2", "GYR3", "IAH3", "IND9", "IUSF", "IUSJ", "IUSP"}, "slot": "NS"},
                {"destinations": {"IUTI", "LAS1", "LAX9", "LFB1", "LGB6", "LGB8", "MCE1", "MDW2"}, "slot": "NT"},
                {"destinations": {"MIT2", "MQJ1", "ONT8", "POC1", "POC3", "QXY8", "RFD2", "RMN3"}, "slot": "NU"},
                {"destinations": {"SBD1", "SBD2", "SCK4", "SMF6", "TCY1", "TCY2", "TEB9", "VGT2", "WALMART-LAX2T",
                                  "XLX7"}, "slot": "NV"},
                {"destinations": {"AMA1", "DEN2", "DFW6", "FOE1", "FTW5", "GEG2", "HEA2", "ICT2", "IGQ2", "IUSL",
                                  "IUSQ", "IUST", "IUTE", "IUTH", "LAN2", "LFT1", "LIT2", "MCC1", "MDW6", "MKC4",
                                  "OAK3", "ORD2", "PDX7", "PHX5", "PHX7", "PPO4", "PSC2", "SCK1", "SLC2", "SNA4",
                                  "STL3", "STL4", "Walmart-DFW2n", "Walmart-DFW5s", "Walmart-DFW6s", "SMF3", "POC2",
                                  "MEM6", "SCK8", "SAT4", "HLI2", "IUSR", "IUSW"}, "slot": "NX"}
            ]
        }
    }
    df["slot"] = ""

    if warehouse == "SAV":
        mask = df["destination"].isin(slot_rules["SAV"]["destinations"])
        df.loc[mask, "slot"] = slot_rules["SAV"]["slot"]

    elif warehouse == "NJ":
        mask = df["destination"].isin(slot_rules["NJ"]["destinations"])
        df.loc[mask, "slot"] = slot_rules["NJ"]["slot"]

    elif warehouse == "LA":
        for rule in slot_rules["LA"]["rules"]:
            mask = df["destination"].isin(rule["destinations"])
            df.loc[mask, "slot"] = rule["slot"]
    if not df.empty:
        df = df.rename(
            {
                "container_number__container_number": "container_number",
                "custom_delivery_method": "delivery_method",
                "shipping_marks": "shipping_mark",
                "n_pallet": "pl"
            },
            axis=1,
        )
        df["delivery_method"] = df["delivery_method"].apply(
            lambda x: x.split("-")[0] if x and isinstance(x, str) else x
        )
        df["pcs"] = df["pcs"].astype(str)

        mask = (df["delivery_method"] == "卡车派送") & (df["delivery_type"] == "public")
        df.loc[mask, "shipping_mark"] = ""
        df.loc[mask, "pcs"] = ""
        df["pl"] = ""
        df = df[
            [
                "destination",
                "delivery_method",
                "shipping_mark",
                "pcs",
                "pl",
                "note",
                "slot",
            ]
        ]
    else:
        df = pd.DataFrame(columns=["destination", "delivery_method", "shipping_mark", "pcs", "pl", "note", "slot"])

    buffer = BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "拆柜单"

    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    header1_font = Font(size=15, bold=True)
    header2_font = Font(size=11, bold=True)
    data_font = Font(size=12, bold=True)
    center_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    left_alignment = Alignment(horizontal='left', vertical='center', wrap_text=True)
    UNIFIED_ROW_HEIGHT = 30

    ws.merge_cells('A1:C1')
    for col_idx in range(1, 4):
        cell = ws.cell(row=1, column=col_idx)
        cell.border = thin_border

    ws['A1'] = container_number or "未指定柜号"
    ws['A1'].font = header1_font
    ws['A1'].alignment = left_alignment
    ws.row_dimensions[1].height = UNIFIED_ROW_HEIGHT

    ws.merge_cells('D1:E1')
    for col_idx in range(4, 6):
        cell = ws.cell(row=1, column=col_idx)
        cell.border = thin_border

    ws['D1'] = warehouse_unpacking_time
    ws['D1'].font = header1_font
    ws['D1'].alignment = center_alignment

    ws.merge_cells('F1:G1')
    for col_idx in range(6, 8):
        cell = ws.cell(row=1, column=col_idx)
        cell.border = thin_border

    ws['F1'] = 'dock'
    ws['F1'].font = header1_font
    ws['F1'].border = thin_border
    ws['F1'].alignment = center_alignment

    column_names = ["destination", "delivery_method", "shipping_mark", "pcs", "pl", "note", "slot"]
    for col_idx, name in enumerate(column_names, 1):
        cell = ws.cell(row=2, column=col_idx)
        cell.value = name
        cell.font = header2_font
        cell.border = thin_border
        cell.alignment = center_alignment
    ws.row_dimensions[2].height = UNIFIED_ROW_HEIGHT

    for row_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False), 3):
        ws.row_dimensions[row_idx].height = UNIFIED_ROW_HEIGHT
        for col_idx, value in enumerate(row, 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.value = value if pd.notna(value) else ""
            cell.font = data_font
            cell.border = thin_border
            if col_idx == 6:
                cell.alignment = left_alignment
            else:
                cell.alignment = center_alignment

    total_columns = len(column_names)
    for col_idx in range(1, total_columns + 1):
        max_length = 0
        column_letter = get_column_letter(col_idx)
        for row_idx in range(1, ws.max_row + 1):
            cell = ws[f"{column_letter}{row_idx}"]
            if isinstance(cell, openpyxl.cell.cell.MergedCell):
                continue
            cell_value = str(cell.value) if cell.value is not None else ""
            if len(cell_value) > max_length:
                max_length = len(cell_value)

        if col_idx == 6:
            adjusted_width = max(10, min(max_length + 5, 30))
        else:
            adjusted_width = max(8, min(max_length + 2, 20))

        ws.column_dimensions[column_letter].width = adjusted_width

    wb.save(buffer)
    buffer.seek(0)

    response = HttpResponse(
        buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    filename = f"{container_number if container_number else '拆柜单'}.xlsx"
    response["Content-Disposition"] = f"attachment; filename={filename}"

    return response


async def export_palletization_list(request: HttpRequest) -> HttpResponse:
    status = request.POST.get("status")
    container_number = request.POST.get("container_number")
    if status == "non_palletized":
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related("container_number", "pallet")
            .filter(container_number__container_number=container_number)
            .annotate(
                custom_delivery_method=Case(
                    When(
                        Q(delivery_method="暂扣留仓(HOLD)")
                        | Q(delivery_method="暂扣留仓"),
                        then=Concat(
                            "delivery_method", Value("-"), "fba_id", Value("-"), "id"
                        ),
                    ),
                    When(
                        Q(delivery_method="客户自提") | Q(destination="客户自提"),
                        then=Concat(
                            "delivery_method",
                            Value("-"),
                            "destination",
                            Value("-"),
                            "shipping_mark",
                        ),
                    ),
                    default=F("delivery_method"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
                str_shipping_mark=Cast("shipping_mark", CharField()),
            )
            .values(
                "container_number__container_number",
                "destination",
                "address",
                "zipcode",
                "contact_name",
                "custom_delivery_method",
                "note",
                "shipment_batch_number__shipment_batch_number",
                "PO_ID",
                # "delivery_window_start",
                # "delivery_window_end",
            )
            .annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                shipping_marks=StringAgg(
                    "str_shipping_mark", delimiter=",", distinct=True
                ),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count("pallet__pallet_id", distinct=True),
                weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
            )
            .order_by("-cbm")
        )
    elif status == "palletized":
        packing_list = await sync_to_async(list)(
            Pallet.objects.select_related("container_number")
            .filter(container_number__container_number=container_number)
            .values(
                "container_number__container_number",
                "delivery_method",
                "destination",
                "fba_id",
                "ref_id",
                "shipping_mark",
                "note",
                "PO_ID",
                # "delivery_window_start",
                # "delivery_window_end",
            )
            .annotate(
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-cbm")
        )
        packing_list_complement = await sync_to_async(list)(
            PackingList.objects.select_related("container_number", "pallet")
            .filter(container_number__container_number=container_number)
            .annotate(
                custom_delivery_method=Case(
                    When(
                        Q(delivery_method="暂扣留仓(HOLD)")
                        | Q(delivery_method="暂扣留仓"),
                        then=Concat(
                            "delivery_method", Value("-"), "fba_id", Value("-"), "id"
                        ),
                    ),
                    When(
                        Q(delivery_method="客户自提") | Q(destination="客户自提"),
                        then=Concat(
                            "delivery_method",
                            Value("-"),
                            "destination",
                            Value("-"),
                            "shipping_mark",
                        ),
                    ),
                    default=F("delivery_method"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
                str_shipping_mark=Cast("shipping_mark", CharField()),
            )
            .values(
                "container_number__container_number",
                "destination",
                "address",
                "zipcode",
                "contact_name",
                "custom_delivery_method",
                "note",
                "shipment_batch_number__shipment_batch_number",
                "PO_ID",
                # "delivery_window_start",
                # "delivery_window_end",
            )
            .annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                shipping_marks=StringAgg(
                    "str_shipping_mark", delimiter=",", distinct=True
                ),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count("pallet__pallet_id", distinct=True),
                weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
            )
            .order_by("-cbm")
        )
        existing_po = {
            (plt["container_number__container_number"], plt["destination"])
            for plt in packing_list
        }
        for pl in packing_list_complement:
            po = (pl["container_number__container_number"], pl["destination"])
            if po not in existing_po:
                packing_list.append(
                    {
                        "container_number__container_number": pl[
                            "container_number__container_number"
                        ],
                        "delivery_method": pl["custom_delivery_method"],
                        "destination": pl["destination"],
                        "fba_id": pl["fba_ids"],
                        "ref_id": pl["ref_ids"],
                        "shipping_mark": pl["shipping_marks"],
                        "note": pl["note"],
                        "PO_ID": pl["PO_ID"],
                        "pcs": 0,
                        "cbm": 0,
                        "n_pallet": 0,
                        # "delivery_window_start": pl["delivery_window_start"],
                        # "delivery_window_end": pl["delivery_window_end"],
                    }
                )
    else:
        raise ValueError(f"Unknown container status: {status}\n{request.POST}")

    data = [i for i in packing_list]
    df = pd.DataFrame.from_records(data)
    df = df.rename(
        {
            "container_number__container_number": "container_number",
            "custom_delivery_method": "delivery_method",
            "fba_ids": "fba_id",
            "ref_ids": "ref_id",
            "shipping_marks": "shipping_mark",
        },
        axis=1,
    )
    df["delivery_method"] = df["delivery_method"].apply(lambda x: x.split("-")[0])
    df = df[
        [
            "container_number",
            "destination",
            "delivery_method",
            "fba_id",
            "ref_id",
            "shipping_mark",
            "pcs",
            "cbm",
            "n_pallet",
            "PO_ID",
            "note",
            # "delivery_window_start",
            # "delivery_window_end",
        ]
    ]
    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = f"attachment; filename={container_number}.xlsx"
    df.to_excel(excel_writer=response, index=False, columns=df.columns)
    return response


def export_po_check(request: HttpRequest) -> HttpResponse:
    pl_ids = request.POST.getlist("pl_ids")
    pls = [pl.split(",") for pl in pl_ids]
    selections = request.POST.getlist("is_selected")
    ids = [o for s, co in zip(selections, pls) for o in co if s == "on"]

    checkid_list = request.POST.getlist("ids")
    check_ids = [i.split(",") for i in checkid_list]
    check_id = [o for s, co in zip(selections, check_ids) for o in co if s == "on"]
    id_check_id_map = {
        int(id_val): check_val for id_val, check_val in zip(ids, check_id)
    }
    if ids:
        # 查找柜号下的pl
        packing_list = (
            PackingList.objects.select_related("container_number", "pallet")
            .filter(id__in=ids)
            .values(
                "id",
                "shipping_mark",
                "fba_id",
                "ref_id",
                "address",
                "zipcode",
                "container_number__container_number",
            )
            .annotate(
                total_pcs=Sum(
                    Case(
                        When(pallet__isnull=True, then=F("pcs")),
                        default=F("pallet__pcs"),
                        output_field=IntegerField(),
                    )
                ),
                total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                label=Max(
                    Case(
                        When(pallet__isnull=True, then=Value("EST")),
                        default=Value("ACT"),
                        output_field=CharField(),
                    )
                ),
            )
            .distinct()
            .order_by("destination", "container_number__container_number")
        )
    data = [i for i in packing_list]
    for item in data:
        item_id = item["id"]
        if item_id in id_check_id_map:
            item["check_id"] = id_check_id_map[item_id]
    keep = [
        "shipping_mark",
        "container_number__container_number",
        "fba_id",
        "ref_id",
        "total_pcs",
        "Pallet Count",
        "label",
        "check_id",
        "is_valid",
    ]
    df = pd.DataFrame.from_records(data)
    df["is_valid"] = None

    def get_est_pallet(n):
        if n < 1:
            return 1
        elif n % 1 >= 0.45:
            return int(n // 1 + 1)
        else:
            return int(n // 1)

    df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
    df["est"] = df["label"] == "EST"
    df["act"] = df["label"] == "ACT"
    df["Pallet Count"] = (
        df["total_n_pallet_act"] * df["act"] + df["total_n_pallet_est"] * df["est"]
    )
    df = df[keep].rename(
        {
            "fba_id": "PRO",
            "container_number__container_number": "BOL",
            "ref_id": "PO List (use , as separator) *",
            "total_pcs": "Carton Count",
        },
        axis=1,
    )
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f"attachment; filename=PO.csv"
    df.to_csv(path_or_buf=response, index=False)
    return response


def export_report(request: HttpRequest, export_format: str = "PO") -> HttpResponse:
    pl_ids = request.POST.getlist("pl_ids")
    pls = [pl.split(",") for pl in pl_ids]
    selections = request.POST.getlist("is_selected")
    ids = [o for s, co in zip(selections, pls) for o in co if s == "on"]
    return {"stauts": "ok"}


def export_po(request: HttpRequest, export_format: str = "PO") -> HttpResponse:
    ids = request.POST.get("pl_ids")
    ids = ids.replace("[", "").replace("]", "").split(", ")
    ids = [int(i) for i in ids]
    packing_list = (
        PackingList.objects.select_related("container_number", "pallet")
        .filter(id__in=ids)
        .values(
            "fba_id",
            "ref_id",
            "address",
            "zipcode",
            "destination",
            "delivery_method",
            "container_number__container_number",
            "shipping_mark",
        )
        .annotate(
            total_pcs=Sum(
                Case(
                    When(pallet__isnull=True, then=F("pcs")),
                    default=F("pallet__pcs"),
                    output_field=IntegerField(),
                )
            ),
            total_cbm=Sum(
                Case(
                    When(pallet__isnull=True, then=F("cbm")),
                    default=F("pallet__cbm"),
                    output_field=FloatField(),
                )
            ),
            total_weight_lbs=Sum(
                Case(
                    When(pallet__isnull=True, then=F("total_weight_lbs")),
                    default=F("pallet__weight_lbs"),
                    output_field=FloatField(),
                )
            ),
            total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
            total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
            label=Max(
                Case(
                    When(pallet__isnull=True, then=Value("EST")),
                    default=Value("ACT"),
                    output_field=CharField(),
                )
            ),
        )
        .distinct()
        .order_by("destination", "container_number__container_number")
    )
    for p in packing_list:
        try:
            pl = PoCheckEtaSeven.objects.get(
                container_number__container_number=p[
                    "container_number__container_number"
                ],
                shipping_mark=p["shipping_mark"],
                fba_id=p["fba_id"],
                ref_id=p["ref_id"],
            )

            if not pl.last_eta_checktime and not pl.last_retrieval_checktime:
                p["check"] = "未校验"
            elif pl.last_retrieval_checktime and not pl.last_retrieval_status:
                if pl.handling_method:
                    p["check"] = "失效," + str(pl.handling_method)
                else:
                    p["check"] = "失效未处理"
            elif (
                not pl.last_retrieval_checktime
                and pl.last_eta_checktime
                and not pl.last_eta_status
            ):
                if pl.handling_method:
                    p["check"] = "失效," + str(pl.handling_method)
                else:
                    p["check"] = "失效未处理"
            else:
                p["check"] = "有效"
        except PoCheckEtaSeven.DoesNotExist:
            p["check"] = "未找到记录"
        except MultipleObjectsReturned:
            p["check"] = "唛头FBA_REF重复"
    data = [i for i in packing_list]
    if export_format == "PO":
        keep = [
            "fba_id",
            "container_number__container_number",
            "ref_id",
            "Pallet Count",
            "total_pcs",
            "label",
            "check",
        ]
    elif export_format == "FULL_TABLE":
        keep = [
            "container_number__container_number",
            "destination",
            "delivery_method",
            "fba_id",
            "ref_id",
            "total_cbm",
            "total_pcs",
            "total_weight_lbs",
            "Pallet Count",
            "label",
            "check",
        ]
    else:
        raise ValueError(f"unknown export_format option: {export_format}")
    df = pd.DataFrame.from_records(data)

    def get_est_pallet(n):
        if n < 1:
            return 1
        elif n % 1 >= 0.45:
            return int(n // 1 + 1)
        else:
            return int(n // 1)

    df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
    df["est"] = df["label"] == "EST"
    df["act"] = df["label"] == "ACT"
    df["Pallet Count"] = (
        df["total_n_pallet_act"] * df["act"] + df["total_n_pallet_est"] * df["est"]
    )
    df = df[keep].rename(
        {
            "fba_id": "PRO",
            "container_number__container_number": "BOL",
            "ref_id": "PO List (use , as separator) *",
            "total_pcs": "Carton Count",
        },
        axis=1,
    )
    if export_format == "FULL_TABLE":
        df = df.rename(
            {
                "total_cbm": "CBM",
                "total_weight_lbs": "WEIGHT(LBS)",
            },
            axis=1,
        )
    response = HttpResponse(
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = f"attachment; filename=PO.xlsx"
    df.to_excel(excel_writer=response, index=False, columns=df.columns)
    return response


def export_do(request: HttpRequest) -> HttpResponse:
    selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
    if selected_orders:
        # 创建一个BytesIO对象来保存ZIP文件，我理解是前后端导出pdf只能响应一次，所以实现不了一次导出多个pdf，就将多个pdf合并为一个压缩包
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for container_number in selected_orders:
                pdf_response = export_do_branch(container_number)
                zip_file.writestr(f"DO_{container_number}.pdf", pdf_response.content)

        # 设置HTTP响应，格式为压缩包
        response = HttpResponse(zip_buffer.getvalue(), content_type="application/zip")
        response["Content-Disposition"] = 'attachment; filename="orders.zip"'
        zip_buffer.close()
        return response
    else:
        selected_orders = request.POST.get("container_number")
        pdf_response = export_do_branch(selected_orders)
        return pdf_response


def export_do_branch(container_number) -> Any:
    order = Order.objects.select_related(
        "container_number", "retrieval_id", "warehouse"
    ).get(container_number__container_number=container_number)
    container = order.container_number
    packing_list = PackingList.objects.filter(
        container_number__container_number=container_number
    )
    pcs, weight = 0, 0
    for pl in packing_list:
        pcs += pl.pcs if pl.pcs else 0
        weight += pl.total_weight_lbs if pl.total_weight_lbs else 0
    retrieval = order.retrieval_id
    vessel = order.vessel_id
    warehouse = order.warehouse
    context = {
        "order": order,
        "retrieval": retrieval,
        "vessel": vessel,
        "container": container,
        "warehouse": warehouse,
        "pcs": pcs,
        "weight": weight,
    }
    template_path = "export_file/do_template.html"
    template = get_template(template_path)
    html = template.render(context)
    response = HttpResponse(content_type="application/pdf")
    response["Content-Disposition"] = (
        f'attachment; filename="DO_{container_number}.pdf"'
    )
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        raise ValueError(
            "Error during PDF generation: %s" % pisa_status.err,
            content_type="text/plain",
        )
    return response


def export_invoice(
    request: HttpRequest,
) -> tuple[HttpResponse, str, BytesIO, dict[Any, Any]]:
    customer = request.POST.get("customer")
    chinese_char = False if customer.isascii() else True
    invoice_statement_id = request.POST.get("invoice_statement_id")
    invoice_terms = request.POST.get("invoice_terms")
    invoice_date = request.POST.get("invoice_date")
    due_date = request.POST.get("due_date")
    container_number = request.POST.getlist("container_number")
    invoice_number = request.POST.getlist("invoice_number")
    rate = [float(r) for r in request.POST.getlist("rate")]
    amount = [float(r) for r in request.POST.getlist("amount")]
    total_amount = sum(amount)
    cnt = list(range(1, len(container_number) + 1))
    invoice_details = zip(cnt, container_number, invoice_number, rate, amount)

    context = {
        "customer": customer,
        "chinese_char": chinese_char,
        "invoice_details": invoice_details,
        "total_amount": total_amount,
        "invoice_statement_id": invoice_statement_id,
        "invoice_terms": invoice_terms,
        "invoice_date": invoice_date,
        "due_date": due_date,
        "container_number": container_number,
        "ACCT_ACH_ROUTING_NUMBER": ACCT_ACH_ROUTING_NUMBER,
        "ACCT_BANK_NAME": ACCT_BANK_NAME,
        "ACCT_BENEFICIARY_ACCOUNT": ACCT_BENEFICIARY_ACCOUNT,
        "ACCT_BENEFICIARY_ADDRESS": ACCT_BENEFICIARY_ADDRESS,
        "ACCT_BENEFICIARY_NAME": ACCT_BENEFICIARY_NAME,
        "ACCT_SWIFT_CODE": ACCT_SWIFT_CODE,
    }

    template_path = "export_file/invoice_template.html"
    template = get_template(template_path)
    html = template.render(context)
    response = HttpResponse(content_type="application/pdf")
    response["Content-Disposition"] = (
        f'attachment; filename="invoice_{invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf"'
    )
    pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
    if pisa_status.err:
        raise ValueError(
            "Error during PDF generation: %s" % pisa_status.err,
            content_type="text/plain",
        )

    pdf_file = io.BytesIO()
    pisa_status = pisa.CreatePDF(html, dest=pdf_file, link_callback=link_callback)
    if pisa_status.err:
        return HttpResponse(
            "Error during PDF generation: %s" % pisa_status.err,
            content_type="text/plain",
        )
    pdf_file.seek(0)
    return (
        response,
        f"invoice_{invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf",
        pdf_file,
        context,
    )


def link_callback(uri: Any, rel: Any) -> Any:
    if uri.startswith(settings.STATIC_URL):
        path = os.path.join(settings.STATIC_ROOT, uri.replace(settings.STATIC_URL, ""))
        if not os.path.isfile(path):
            # Try to find the file using staticfiles finders
            result = finders.find(uri.replace(settings.STATIC_URL, ""))
            if not result:
                raise Exception(f"Static file not found: {uri}")
            if isinstance(result, (list, tuple)):
                result = result[0]
            path = result
        return path
    return uri
