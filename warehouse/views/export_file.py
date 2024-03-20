import pytz
import pandas as pd

from xhtml2pdf import pisa
from typing import Any
from datetime import datetime

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, FloatField, IntegerField, When, Count, DateTimeField, Max
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.forms import model_to_dict
from django.template.loader import get_template


from warehouse.models.packing_list import PackingList

@method_decorator(login_required(login_url='login'), name='dispatch')
class ExportFile(View):
    template_main = {
        "DO": "export_file/do.html",
        "PL": "export_file/packing_list.html"
    }
    file_name = {
        "DO": "D/O",
        "PL": "拆柜单"
    }

    def get(self, request: HttpRequest) -> HttpResponse:
        name = request.GET.get("name")
        template_path = self.template_main[name]
        template = get_template(template_path)
        context = {'sample_data': 'Hello, this is some sample data!'}
        html = template.render(context)

        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="{self.file_name[name]}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response

def export_bol(context: dict[str, Any]) -> HttpResponse:
    template_path = "export_file/bol_template.html"
    template = get_template(template_path)
    html = template.render(context)
    response = HttpResponse(content_type="application/pdf")
    response['Content-Disposition'] = f'attachment; filename="BOL_{context["batch_number"]}.pdf"'
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    return response

def export_palletization_list(request: HttpRequest) -> HttpResponse:
    status = request.POST.get("status")
    container_number = request.POST.get("container_number")
    if status == "non_palletized":
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn).strftime("%Y-%m-%d %H:%M:%S")
        packing_list = PackingList.objects.filter(container_number__container_number=container_number).annotate(
            custom_delivery_method=Case(
                When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                default=F('delivery_method'),
                output_field=CharField()
            ),
            str_id=Cast("id", CharField()),
            str_fba_id=Cast("fba_id", CharField()),
            str_ref_id=Cast("ref_id", CharField()),
        ).values(
            "container_number__container_number", "destination", "address", "custom_delivery_method"
        ).annotate(
            palletized_at=Value(current_time_cn, output_field=CharField()),
            fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
            ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
            weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
            pcs=Sum("pcs", output_field=IntegerField()),
            cbm=Sum("cbm", output_field=FloatField()),
            n_pallet=Value("", output_field=CharField()),
        ).order_by("-cbm")
    elif status == "palletized":
        packing_list = PackingList.objects.filter(container_number__container_number=container_number).annotate(
            custom_delivery_method=Case(
                When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                default=F('delivery_method'),
                output_field=CharField()
            ),
            str_id=Cast("id", CharField()),
            str_fba_id=Cast("fba_id", CharField()),
            str_ref_id=Cast("ref_id", CharField()),
        ).values(
            "container_number__container_number", "destination", "address", "custom_delivery_method"
        ).annotate(
            palletized_at=Cast(Max("container_number__order__offload_id__offload_at"), CharField()),
            fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
            ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
            weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
            pcs=Sum("pallet__pcs", output_field=IntegerField()),
            cbm=Sum("pallet__cbm", output_field=FloatField()),
            n_pallet=Count("pallet__pallet_id", distinct=True),
        ).order_by("-cbm")
    else:
        raise ValueError(f"Unknown container status: {status}\n{request.POST}")
    
    data = [i for i in packing_list]
    df = pd.DataFrame.from_records(data)
    df = df.rename({
        "container_number__container_number": "container_number",
        "custom_delivery_method": "delivery_method",
        "fba_ids": "fba_id",
        "ref_ids": "ref_id",
    }, axis=1)
    df["delivery_method"] = df["delivery_method"].apply(lambda x: x.split("-")[0])
    df = df[[
        "container_number", "palletized_at", "destination", "address", "delivery_method",
        "fba_id", "ref_id", "weight_lbs", "pcs", "cbm", "n_pallet",
    ]]
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f"attachment; filename={container_number}.xlsx"
    df.to_excel(excel_writer=response, index=False, columns=df.columns)
    return response

def export_po(request: HttpRequest) -> HttpResponse:
    # raise ValueError(f"{request.POST}")
    ids = request.POST.get("pl_ids")
    ids = ids.replace("[", "").replace("]", "").split(", ")
    ids = [int(i) for i in ids]
    packing_list = PackingList.objects.filter(
        id__in=ids
    ).values(
        'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
        'container_number__container_number',
    ).annotate(
        total_pcs=Sum(
            Case(
                When(pallet__isnull=True, then=F("pcs")),
                default=F("pallet__pcs"),
                output_field=IntegerField()
            )
        ),
        total_cbm=Sum(
            Case(
                When(pallet__isnull=True, then=F("cbm")),
                default=F("pallet__cbm"),
                output_field=FloatField()
            )
        ),
        total_weight_lbs=Sum(
            Case(
                When(pallet__isnull=True, then=F("total_weight_lbs")),
                default=F("pallet__weight_lbs"),
                output_field=FloatField()
            )
        ),
        total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
        total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
        label=Max(
            Case(
                When(pallet__isnull=True, then=Value("EST")),
                default=Value("ACT"),
                output_field=CharField()
            )
        ),
    ).distinct().order_by("destination", "container_number__container_number")
    data = [i for i in packing_list]
    keep = ["fba_id", "container_number__container_number", "ref_id", "Pallet Count", "total_pcs", "label"]
    df = pd.DataFrame.from_records(data)
    def get_est_pallet(n):
        if n < 1:
            return 1
        elif n%1 >= 0.45:
            return int(n//1 + 1)
        else:
            return int(n//1)
    df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
    df["est"] = df["label"] == "EST"
    df["act"] = df["label"] == "ACT"
    df["Pallet Count"] = df["total_n_pallet_act"] * df["act"] + df["total_n_pallet_est"] * df["est"]
    df = df[keep].rename({
        "fba_id": "PRO",
        "container_number__container_number": "BOL",
        "ref_id": "PO List (use , as separator) *",
        "total_pcs": "Carton Count",
    }, axis=1)
    response = HttpResponse(content_type="text/csv")
    response['Content-Disposition'] = f"attachment; filename=PO.csv"
    df.to_csv(path_or_buf=response, index=False)
    return response
