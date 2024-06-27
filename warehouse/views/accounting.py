import pandas as pd
from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.forms.order_form import OrderForm
from warehouse.views.export_file import export_invoice

@method_decorator(login_required(login_url='login'), name='dispatch')
class Accounting(View):
    template_pallet_data = "accounting/pallet_data.html"
    template_pl_data = "accounting/pl_data.html"
    template_invoice_management = "accounting/invoice_management.html"
    template_invoice = "accounting/invoice.html"
    allowed_group = "accounting"

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
            return export_invoice(request)
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
        criteria = models.Q(created_at__gte=start_date)
        criteria &= models.Q(created_at__lte=end_date)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        order = Order.objects.filter(criteria).order_by("created_at")
        context = {
            "order_form":OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "order": order,
            "customer": customer,
        } 
        return self.template_invoice_management, context
    
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
            customer = order[0].customer_name
            context = {
                "order": order,
                "customer": customer,
            }
            return render(request, self.template_invoice, context)
        else:
            template, context = self.handle_invoice_order_search_post(request)
            return render(request, template, context)
        
    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name=self.allowed_group).exists():
            return True
        else:
            return False