import io
import os
import openpyxl.workbook
import openpyxl.worksheet
import openpyxl.worksheet.worksheet
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from typing import Any

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, Count

from warehouse.models.order import Order
from warehouse.models.invoice import Invoice, InvoiceStatement
from warehouse.models.packing_list import PackingList
from warehouse.models.customer import Customer
from warehouse.forms.order_form import OrderForm
from warehouse.views.export_file import export_invoice
from warehouse.utils.constants import (
    APP_ENV,
    SP_USER,
    SP_PASS,
    SP_URL,
    SP_DOC_LIB,
    SYSTEM_FOLDER,
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE
)


@method_decorator(login_required(login_url='login'), name='dispatch')
class Accounting(View):
    template_pallet_data = "accounting/pallet_data.html"
    template_pl_data = "accounting/pl_data.html"
    template_invoice_management = "accounting/invoice_management.html"
    template_invoice_statement = "accounting/invoice_statement.html"
    template_invoice_container = "accounting/invoice_container.html"
    allowed_group = "accounting"
    conn = ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))

    def get(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")

        step = request.GET.get("step", None)
        if step == "pallet_data":
            template, context = self.handle_pallet_data_get()
            return render(request, template, context)
        elif step == "pl_data":
            template, context = self.handle_pl_data_get()
            return render(request, template, context)
        elif step == "invoice":
            template, context = self.handle_invoice_get()
            return render(request, template, context)
        elif step == "container_invoice":
            container_number = request.GET.get("container_number")
            template, context = self.handle_container_invoice_get(container_number)
            return render(request, template, context)
        else:
            raise ValueError(f"unknow request {step}")
        
    def post(self, request: HttpRequest) -> HttpResponse:
        if not self._validate_user_group(request.user):
            return HttpResponseForbidden("You are not authenticated to access this page!")

        step = request.POST.get("step", None)
        if step == "pallet_data_search":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            template, context = self.handle_pallet_data_get(start_date, end_date)
            return render(request, template, context)
        elif step == "pl_data_search":
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            container_number = request.POST.get("container_number")
            template, context = self.handle_pl_data_get(start_date, end_date, container_number)
            return render(request, template, context)
        elif step == "pallet_data_export":
            return self.handle_pallet_data_export_post(request)
        elif step == "pl_data_export":
            return self.handle_pl_data_export_post(request)
        elif step == "invoice_order_search":
            template, context = self.handle_invoice_order_search_post(request)
            return render(request, template, context)
        elif step == "invoice_order_select":
            return self.handle_invoice_order_select_post(request)
        elif step == "export_invoice":
            return self.handle_export_invoice_post(request)
        elif step == "create_container_invoice":
            return self.handle_create_container_invoice_post(request)
        else:
            raise ValueError(f"unknow request {step}")

    def handle_pallet_data_get(self, start_date: str = None, end_date: str = None) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        pallet_data = Order.objects.filter(
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) &
            models.Q(offload_id__offload_at__gte=start_date) &
            models.Q(offload_id__offload_at__lte=end_date)
        ).order_by("offload_id__offload_at")
        context = {
            "pallet_data": pallet_data,
            "start_date": start_date,
            "end_date": end_date,
        }
        return self.template_pallet_data, context
    
    def handle_pl_data_get(
        self, 
        start_date: str = None,
        end_date: str = None,
        container_number: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(container_number__order__eta__gte=start_date)
        criteria &= models.Q(container_number__order__eta__lte=end_date)
        if container_number == "None":
            container_number = None
        if container_number:
            criteria &= models.Q(container_number__container_number=container_number)
        pl_data = PackingList.objects.filter(criteria).values(
            'container_number__container_number', 'destination', 'delivery_method', 'cbm', 'pcs', 'total_weight_kg'
        ).order_by("container_number__container_number", "destination")
        context = {
            "start_date": start_date,
            "end_date": end_date,
            "container_number": container_number,
            "pl_data": pl_data,
        }
        return self.template_pl_data, context
    
    def handle_invoice_get(
        self, 
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            models.Q(offload_id__offload_required=True, offload_id__offload_at__isnull=False) |
            models.Q(offload_id__offload_required=False)
        )
        criteria &= models.Q(created_at__gte=start_date)
        criteria &= models.Q(created_at__lte=end_date)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        order = Order.objects.filter(criteria).order_by("created_at")
        order_no_invoice = [o for o in order if o.invoice_id is None]
        order_invoice = [o for o in order if o.invoice_id]
        context = {
            "order_form":OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "order": order,
            "customer": customer,
            "order_no_invoice": order_no_invoice,
            "order_invoice": order_invoice,
        }
        return self.template_invoice_management, context
    
    def handle_container_invoice_get(self, container_number: str) -> tuple[Any, Any]:
        order = Order.objects.get(container_number__container_number=container_number)
        packing_list = PackingList.objects.filter(
            container_number__container_number=container_number
        ).values(
            'container_number__container_number', 'destination'
        ).annotate(
            total_cbm=Sum("pallet__cbm", output_field=FloatField()),
            total_n_pallet=Count('pallet__pallet_id', distinct=True),
        ).order_by("destination", "-total_cbm")
        for pl in packing_list:
            if pl["total_cbm"] > 1:
                pl["total_n_pallet"] = round(pl["total_cbm"] / 2)
            elif pl["total_cbm"] >= 0.6 and pl["total_cbm"] <= 1:
                pl["total_n_pallet"] = 0.5
            else:
                pl["total_n_pallet"] = 0.25
        context = {
            "order": order,
            "packing_list": packing_list,
        }
        return self.template_invoice_container, context
    
    def handle_pallet_data_export_post(self, request: HttpRequest) -> HttpResponse:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet_data = Order.objects.filter(
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) &
            models.Q(eta__gte=start_date) &
            models.Q(eta__lte=end_date)
        ).order_by("offload_id__offload_at")
        data = [
            {
                "货柜号": d.container_number.container_number,
                "客户": d.customer_name.zem_name,
                "入仓仓库": d.warehouse.name,
                "柜型": d.container_number.container_type,
                "拆柜完成时间": d.offload_id.offload_at.strftime('%Y-%m-%d %H:%M:%S'),
                "打板数": d.offload_id.total_pallet,
            } for d in pallet_data
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f"attachment; filename=pallet_data_{start_date}_{end_date}.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response
    
    def handle_pl_data_export_post(self, request: HttpRequest) -> HttpResponse:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        container_number = request.POST.get("container_number")
        _, context = self.handle_pl_data_get(start_date, end_date, container_number)
        data = [
            {
                "货柜号": d["container_number__container_number"],
                "目的地": d["destination"],
                "派送方式": d["delivery_method"],
                "CBM": d["cbm"],
                "箱数": d["pcs"],
                "总重KG": d["total_weight_kg"],
            } for d in context["pl_data"]
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f"attachment; filename=packing_list_data_{start_date}_{end_date}.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response
    
    def handle_invoice_order_search_post(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        order_form = OrderForm(request.POST)
        if order_form.is_valid():
            customer = order_form.cleaned_data.get("customer_name")
        else:
            customer = None
        return self.handle_invoice_get(start_date, end_date, customer)

    def handle_invoice_order_select_post(self, request: HttpRequest) -> HttpResponse:
        order_ids = request.POST.getlist("order_id")
        selections = request.POST.getlist("is_order_selected")
        order_selected = [o for s, o in zip(selections, order_ids) if s == "on"]
        if order_selected:
            order = Order.objects.filter(
                order_id__in=order_selected
            )
            order_id = [o.id for o in order]
            customer = order[0].customer_name
            current_date = datetime.now().date().strftime("%Y-%m-%d")
            invoice_statement_id = f"{current_date.replace('-', '')}S{customer.id}{max(order_id)}"
            context = {
                "order": order,
                "customer": customer,
                "invoice_statement_id": invoice_statement_id,
                "current_date": current_date,
            }
            return render(request, self.template_invoice_statement, context)
        else:
            template, context = self.handle_invoice_order_search_post(request)
            return render(request, template, context)
        
    def handle_create_container_invoice_post(self, request: HttpRequest) -> HttpResponse:
        container_number = request.POST.get("container_number")
        description = request.POST.getlist("description")
        warehouse_code = request.POST.getlist("warehouse_code")
        cbm = request.POST.getlist("cbm")
        qty = request.POST.getlist("qty")
        rate = request.POST.getlist("rate")
        amount = request.POST.getlist("amount")
        order = Order.objects.get(container_number__container_number=container_number)
        context = {
            "order": order,
            "container_number": container_number,
            "data": zip(description, warehouse_code, cbm, qty, rate, amount)
        }
        return self._generate_invoice_excel(context)
    
    def handle_export_invoice_post(self, request: HttpRequest) -> HttpResponse:
        resp, file_name, pdf_file, context = export_invoice(request)
        pdf_file.seek(0)
        link = self._upload_excel_to_sharepoint(pdf_file, "invoice_statement", file_name)
        invoice = Invoice.objects.filter(models.Q(container_number__container_number__in=context["container_number"]))
        invoice_statement = InvoiceStatement(**{
            "invoice_statement_id": context["invoice_statement_id"],
            "statement_amount": context["total_amount"],
            "statement_date": context["invoice_date"],
            "due_date": context["due_date"],
            "invoice_terms": context["invoice_terms"],
            "customer": Customer.objects.get(accounting_name=context["customer"]),
            "statement_link": link,
        })
        invoice_statement.save()
        for invc in invoice:
            invc.statement_id = invoice_statement
            invc.save()
        
        return resp
    
    def _generate_invoice_excel(self, context: dict[Any, Any]) -> HttpResponse:
        current_date = datetime.now().date()
        order_id = str(context["order"].id)
        if len(order_id) < 5:
            order_id = '0' * (5 - len(order_id)) + order_id
        invoice_number = f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{order_id}"
        workbook = openpyxl.Workbook()
        worksheet = workbook.active
        worksheet.title = "Sheet1"

        cells_to_merge = [
            "A1:B1", "A3:A4", "B3:D3", "B4:D4", "E3:E4", "F3:G4", "A5:A6", "B5:D5", "B6:D6", "E5:E6", "F5:G6", "A9:B9", 
            "A10:B10", "F1:G1", "C1:E1", "A2:G2", "A7:G7", "A8:G8", "C9:G9", "C10:G10", "A11:G11"
        ]
        self._merge_ws_cells(worksheet, cells_to_merge)

        worksheet.column_dimensions['A'].width = 13
        worksheet.column_dimensions['B'].width = 15
        worksheet.column_dimensions['C'].width = 15
        worksheet.column_dimensions['D'].width = 7
        worksheet.column_dimensions['E'].width = 8
        worksheet.column_dimensions['F'].width = 7
        worksheet.column_dimensions['G'].width = 11
        worksheet.row_dimensions[1].height = 40

        worksheet["A1"] = "ZEM LOGISTICS INC."
        worksheet["A3"] = "NJ"
        worksheet["B3"] = "27 Engelhard Ave. Avenel NJ 07001"
        worksheet["B4"] = "Contact: Marstin Ma 929-810-9968"
        worksheet["A5"] = "SAV"
        worksheet["B5"] = "774 King George Blvd Savannah, GA 31419"
        worksheet["B6"] = "Contact: Darren Zheng 805-868-1682"
        worksheet["A7"] = "E-mail: OFFICE@ZEMLOGISTICS.COM"
        worksheet["A9"] = "BILL TO"
        worksheet["A10"] = context["order"].customer_name.accounting_name
        worksheet["F1"] = "Invoice"
        worksheet["E3"] = "Date"
        worksheet["F3"] = current_date.strftime('%Y-%m-%d')
        worksheet["E5"] = "Invoice #"
        worksheet["F5"] = invoice_number

        worksheet['A1'].font = Font(size=20)
        worksheet['F1'].font = Font(size=28)
        worksheet["A3"].alignment = Alignment(vertical="center")
        worksheet["A5"].alignment = Alignment(vertical="center")
        worksheet["E3"].alignment = Alignment(vertical="center")
        worksheet["E5"].alignment = Alignment(vertical="center")
        worksheet["F3"].alignment = Alignment(vertical="center")
        worksheet["F5"].alignment = Alignment(vertical="center")

        worksheet.append(["CONTAINER #", "DESCRIPTION", "WAREHOUSE CODE", "CBM", "QTY", "QTY", "AMOUNT"])
        row_count = 13
        total_amount = 0.0
        for d, wc, cbm, qty, r, amt in context["data"]:
            worksheet.append([context["container_number"], d, wc, cbm, qty, r, amt])
            total_amount += float(amt)
            row_count += 1
        self._merge_ws_cells(worksheet, [f"A{row_count}:G{row_count}"])
        row_count += 1

        bank_info = [
            f"Beneficiary Name: {ACCT_BENEFICIARY_NAME}",
            f"Bank Name: {ACCT_BANK_NAME}",
            f"SWIFT Code: {ACCT_SWIFT_CODE}",
            f"ACH/Wire Transfer Routing Number: {ACCT_ACH_ROUTING_NUMBER}",
            f"Beneficiary Account #: {ACCT_BENEFICIARY_ACCOUNT}",
            f"Beneficiary Address: {ACCT_BENEFICIARY_ADDRESS}",
            f"Email:FINANCE@ZEMLOGISTICS.COM",
            f"phone: 929-810-9968",
        ]
        for c in bank_info:
            worksheet.append([c])
            self._merge_ws_cells(worksheet, [f"A{row_count}:G{row_count}"])
            row_count += 1
        self._merge_ws_cells(worksheet, [f"A{row_count}:G{row_count}"])

        excel_file = io.BytesIO()
        workbook.save(excel_file)
        excel_file.seek(0)
        invoice_link = self._upload_excel_to_sharepoint(excel_file, "invoice", f"INVOICE-{context['container_number']}.xlsx")
        invoice = Invoice(**{
            "invoice_number": invoice_number,
            "invoice_date": current_date,
            "invoice_link": invoice_link,
            "customer": context["order"].customer_name,
            "container_number": context["order"].container_number,
            "total_amount": total_amount,
        })
        invoice.save()
        context["order"].invoice_id = invoice
        context["order"].save()

        worksheet['A9'].font = Font(color="00FFFFFF")
        worksheet['A9'].fill = PatternFill(start_color="00000000", end_color="00000000", fill_type="solid")
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename=INVOICE-{context["container_number"]}.xlsx'
        workbook.save(response)
        return response

    def _merge_ws_cells(self, ws: openpyxl.worksheet.worksheet, cells: list[str]) -> None:
        for c in cells:
            ws.merge_cells(c)

    def _upload_excel_to_sharepoint(
        self,
        file: BytesIO,
        schema: str,
        file_name: str
    ) -> str:
        file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/{schema}/{APP_ENV}")
        sp_folder = self.conn.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(f"{file_name}", file).execute_query()
        link = resp.share_link(SharingLinkKind.OrganizationView).execute_query().value.to_json()["sharingLinkInfo"]["Url"]
        return link
        
    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name=self.allowed_group).exists():
            return True
        else:
            return False