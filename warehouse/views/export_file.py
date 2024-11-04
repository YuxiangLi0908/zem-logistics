import io
import os
import pytz
import pandas as pd
import json
import zipfile

from io import BytesIO
from xhtml2pdf import pisa
from typing import Any
from datetime import datetime
from asgiref.sync import sync_to_async

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, FloatField, IntegerField, When, Count, DateTimeField, Max, Q
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.forms import model_to_dict
from django.template.loader import get_template
from django.conf import settings
from django.contrib.staticfiles import finders

from warehouse.models.packing_list import PackingList
from warehouse.models.order import Order
from warehouse.models.pallet import Pallet
from warehouse.utils.constants import (
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE
)

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

async def export_palletization_list(request: HttpRequest) -> HttpResponse:
    status = request.POST.get("status")
    container_number = request.POST.get("container_number")
    if status == "non_palletized":
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn).strftime("%Y-%m-%d %H:%M:%S")
        packing_list = await sync_to_async(list)(PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(container_number__container_number=container_number).annotate(
            custom_delivery_method=Case(
                When(Q(delivery_method='暂扣留仓(HOLD)') | Q(delivery_method='暂扣留仓'), then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                When(Q(delivery_method='客户自提') | Q(destination='客户自提'), then=Concat('delivery_method', Value('-'), 'destination',  Value('-'), 'shipping_mark')),
                default=F('delivery_method'),
                output_field=CharField()
            ),
            str_id=Cast("id", CharField()),
            str_fba_id=Cast("fba_id", CharField()),
            str_ref_id=Cast("ref_id", CharField()),
            str_shipping_mark=Cast("shipping_mark", CharField()),
        ).values(
            "container_number__container_number", "destination", "custom_delivery_method", "note"
        ).annotate(
            fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
            ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
            shipping_marks=StringAgg("str_shipping_mark", delimiter=",", distinct=True, ordering="str_shipping_mark"),
            pcs=Sum("pcs", output_field=IntegerField()),
            cbm=Sum("cbm", output_field=FloatField()),
            n_pallet=Value("", output_field=CharField()),
        ).order_by("-cbm"))
    elif status == "palletized":
        packing_list = await sync_to_async(list)(Pallet.objects.select_related(
                "container_number"
            ).filter(
                container_number__container_number=container_number
            ).values(
                "container_number__container_number", "delivery_method", "destination", "fba_id", "ref_id", "shipping_mark", "note"
            ).annotate(
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count("pallet_id", distinct=True),
            ).order_by("-cbm")
        )
    else:
        raise ValueError(f"Unknown container status: {status}\n{request.POST}")
    
    data = [i for i in packing_list]
    df = pd.DataFrame.from_records(data)
    df = df.rename({
        "container_number__container_number": "container_number",
        "custom_delivery_method": "delivery_method",
        "fba_ids": "fba_id",
        "ref_ids": "ref_id",
        "shipping_marks": "shipping_mark"
    }, axis=1)
    df["delivery_method"] = df["delivery_method"].apply(lambda x: x.split("-")[0])
    df = df[[
        "container_number", "destination", "delivery_method",
        "fba_id", "ref_id", "shipping_mark", "pcs", "cbm", "n_pallet", "note",
    ]]
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f"attachment; filename={container_number}.xlsx"
    df.to_excel(excel_writer=response, index=False, columns=df.columns)
    return response

def export_po_check(request: HttpRequest) -> HttpResponse:
    pl_ids = request.POST.getlist("pl_ids")   
    pls = [pl.split(",") for pl in pl_ids]
    selections = request.POST.getlist("is_selected")
    ids = [o for s, co in zip(selections, pls) for o in co if s == "on"]
    if ids:
        #查找柜号下的pl
        packing_list = PackingList.objects.select_related(
            "container_number", "pallet"
            ).filter(
                id__in=ids).values(
        'shipping_mark', 'fba_id', 'ref_id','address','zipcode',
        'container_number__container_number',
        ).annotate(total_pcs=Sum(
            Case(
                When(pallet__isnull=True, then=F("pcs")),
                default=F("pallet__pcs"),
                output_field=IntegerField()
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
    keep = [
            "shipping_mark", "container_number__container_number", "fba_id", "ref_id", 
            "total_pcs", "Pallet Count", "label","is_valid"
        ]
    df = pd.DataFrame.from_records(data)
    df['is_valid'] = None
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
        

def export_po(request: HttpRequest, export_format: str = "PO") -> HttpResponse:
    ids = request.POST.get("pl_ids")
    
    ids = ids.replace("[", "").replace("]", "").split(", ")
    ids = [int(i) for i in ids]
    packing_list = PackingList.objects.select_related(
        "container_number", "pallet"
    ).filter(
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
    if export_format == "PO":
        keep = ["fba_id", "container_number__container_number", "ref_id", "Pallet Count", "total_pcs", "label"]
    elif export_format == "FULL_TABLE":
        keep = [
            "container_number__container_number", "destination", "delivery_method", "fba_id", "ref_id", 
            "total_cbm", "total_pcs", "total_weight_lbs", "Pallet Count", "label"
        ]
    else:
        raise ValueError(f"unknown export_format option: {export_format}")
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
    if export_format == "FULL_TABLE":
        df = df.rename({
            "total_cbm": "CBM",
            "total_weight_lbs": "WEIGHT(LBS)",
        }, axis=1)
    response = HttpResponse(content_type="text/csv")
    response['Content-Disposition'] = f"attachment; filename=PO.csv"
    df.to_csv(path_or_buf=response, index=False)
    return response

def export_do(request: HttpRequest) -> HttpResponse:
    selected_orders = json.loads(request.POST.get('selectedOrders', '[]'))
 
    if len(selected_orders) > 1:
        # 创建一个BytesIO对象来保存ZIP文件，我理解是前后端导出pdf只能响应一次，所以实现不了一次导出多个pdf，就将多个pdf合并为一个压缩包
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for container_number in selected_orders:
                pdf_response = export_do_branch(container_number)
                zip_file.writestr(f'DO_{container_number}.pdf', pdf_response.content)

        # 设置HTTP响应，格式为压缩包
        response = HttpResponse(zip_buffer.getvalue(), content_type='application/zip')
        response['Content-Disposition'] = 'attachment; filename="orders.zip"'
        zip_buffer.close()
        return response
    else:
        pdf_response = export_do_branch(selected_orders[0])
        return pdf_response

    
def export_do_branch(container_number) -> Any:
    order = Order.objects.select_related(
        "container_number", "retrieval_id", "warehouse"
    ).get(container_number__container_number=container_number)
    container = order.container_number
    packing_list = PackingList.objects.filter(container_number__container_number=container_number)
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
    response['Content-Disposition'] = f'attachment; filename="DO_{container_number}.pdf"'
    pisa_status = pisa.CreatePDF(html, dest=response)
    if pisa_status.err:
        raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    return response

def export_invoice(request: HttpRequest) -> tuple[HttpResponse, str, BytesIO, dict[Any, Any]]:
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
    response['Content-Disposition'] = f'attachment; filename="invoice_{invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf"'
    pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
    if pisa_status.err:
        raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    
    pdf_file = io.BytesIO()
    pisa_status = pisa.CreatePDF(html, dest=pdf_file, link_callback=link_callback)
    if pisa_status.err:
        return HttpResponse('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
    pdf_file.seek(0)
    return response, f"invoice_{invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf", pdf_file, context

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