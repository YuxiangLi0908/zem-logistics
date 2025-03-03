import io
import math,re,ast
import os,json
import openpyxl.workbook
import openpyxl.worksheet
import openpyxl.worksheet.worksheet
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from typing import Any
from asgiref.sync import sync_to_async
from asgiref.sync import async_to_sync


from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from office365.runtime.client_request_exception import ClientRequestException

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Cast
from django.db.models import CharField
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count
from django.db.models import Case, When, F, Value

from warehouse.models.order import Order
from warehouse.models.invoice import Invoice, InvoiceItem, InvoiceStatement
from warehouse.models.invoice_details import InvoicePreport,InvoiceWarehouse,InvoiceDelivery
from warehouse.models.packing_list import PackingList
from warehouse.models.customer import Customer
from warehouse.models.pallet import Pallet
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
    ACCT_SWIFT_CODE,
    PICKUP_FEE,
    LOCAL_DELIVERY,NJ_AMAZON_DELIVERY,NJ_COMBINA,NJ_WALMART,
    SAV_AMAZON_DELIVERY,SAV_COMBINA,SAV_WALMART,
    LA_AMAZON_DELIVERY,LA_COMBINA,DIRECT_CONTAINER
)


@method_decorator(login_required(login_url='login'), name='dispatch')
class Accounting(View):
    template_pallet_data = "accounting/pallet_data.html"
    template_pl_data = "accounting/pl_data.html"
    template_invoice_management = "accounting/invoice_management.html"
    template_invoice_statement = "accounting/invoice_statement.html"
    template_invoice_container = "accounting/invoice_container.html"
    template_invoice_container_edit = "accounting/invoice_container_edit.html"
    template_invoice_preport = "accounting/invoice_preport.html"
    template_invoice_preport_edit = "accounting/invoice_preport_edit.html"
    template_invoice_warehouse = "accounting/invoice_warehouse.html"
    template_invoice_warehouse_edit = "accounting/invoice_warehouse_edit.html"
    template_invoice_delivery = "accounting/invoice_delivery.html"
    template_invoice_delievery_edit = "accounting/invoice_delivery_edit.html"
    template_invoice_confirm = "accounting/invoice_confirm.html"
    template_invoice_confirm_edit = "accounting/invoice_confirm_edit.html"
    template_invoice_direct = "accounting/invoice_direct.html"
    template_invoice_direct_edit = "accounting/invoice_direct_edit.html"
    allowed_group = "accounting"

    def get(self, request: HttpRequest) -> HttpResponse:
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.GET.get("step", None)
        if step == "pallet_data":
            template, context = self.handle_pallet_data_get()
            return render(request, template, context)
        elif step == "pl_data":
            template, context = self.handle_pl_data_get()
            return render(request, template, context)
        elif step == "invoice_direct":
            if self.validate_user_invoice_direct(request.user): 
                template, context = self.handle_invoice_direct_get(request)
                return render(request, template, context)      
            else:
                return HttpResponseForbidden("You are not authenticated to access this page!")
        elif step == "invoice_preport":  #提拆柜账单录入
            if self.validate_user_invoice_preport(request.user): 
                template, context = self.handle_invoice_preport_get(request)
                return render(request, template, context)      
            else:
                return HttpResponseForbidden("You are not authenticated to access this page!")
        elif step == "invoice_warehouse": #库内账单录入
            if self.validate_user_invoice_warehouse(request.user): 
                template, context = self.handle_invoice_warehouse_get(request)
                return render(request, template, context) 
            else:
                return HttpResponseForbidden("You are not authenticated to access this page!")
        elif step == "invoice_delivery":
            if self.validate_user_invoice_delivery(request.user): 
                template, context = self.handle_invoice_delivery_get(request)
                return render(request, template, context) 
            else:
                return HttpResponseForbidden("You are not authenticated to access this page!")
        elif step == "invoice_confirm":
            if self.validate_user_invoice_confirm(request.user): 
                template, context = self.handle_invoice_confirm_get(request)
                return render(request, template, context)      
            else:
                return HttpResponseForbidden("You are not authenticated to access this page!")
        elif step == "invoice":
            template, context = self.handle_invoice_get()
            return render(request, template, context)
        elif step == "container_invoice":
            container_number = request.GET.get("container_number")
            template, context = self.handle_container_invoice_get(container_number)
            return render(request, template, context)
        elif step == "container_direct":
            template, context = self.handle_container_invoice_direct_get(request)
            return render(request, template, context)
        elif step == "container_preport":
            template, context = self.handle_container_invoice_preport_get(request)
            return render(request, template, context)
        elif step == "container_warehouse":
            template, context = self.handle_container_invoice_warehouse_get(request)
            return render(request, template, context)
        elif step =="container_delivery":
            template, context = self.handle_container_invoice_delivery_get(request)
            return render(request, template, context)
        elif step == "container_confirm":
            template, context = self.handle_container_invoice_confirm_get(request)
            return render(request, template, context)
        elif step == "container_invoice_edit":
            container_number = request.GET.get("container_number")
            template, context = self.handle_container_invoice_edit_get(container_number)
            return render(request, template, context)
        elif step == "container_invoice_delete":
            template, context = self.handle_container_invoice_delete_get(request)
            return render(request, template, context)
        else:
            raise ValueError(f"unknow request {step}")
        
    def post(self, request: HttpRequest) -> HttpResponse:
        
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")

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
            template, context = self.handle_invoice_order_search_post(request,"old")
            return render(request, template, context)
        elif step == "invoice_order_direct":
            template, context = self.handle_invoice_order_search_post(request,"direct")
            return render(request, template, context)
        elif step == "invoice_order_preport":
            template, context = self.handle_invoice_order_search_post(request,"preport")
            return render(request, template, context)
        elif step == "invoice_order_warehouse":
            template, context = self.handle_invoice_order_search_post(request,"warehouse")
            return render(request, template, context)
        elif step == "invoice_order_delivery":
            template, context = self.handle_invoice_order_search_post(request,"delivery")
            return render(request, template, context)
        elif step == "invoice_order_confirm":
            template, context = self.handle_invoice_order_search_post(request,"confirm")
            return render(request, template, context)
        elif step == "invoice_order_select":
            return self.handle_invoice_order_select_post(request)
        elif step == "export_invoice":
            return self.handle_export_invoice_post(request)
        elif step == "create_container_invoice":
            return self.handle_create_container_invoice_post(request)
        elif step == "container_invoice_edit":
            return self.handle_container_invoice_edit_post(request)
        elif step == "direct_save":
            template, context = self.handle_invoice_direct_save_post(request)
            return render(request, template, context)
        elif step =="preport_save":
            template, context = self.handle_invoice_preport_save_post(request)
            return render(request, template, context)
        elif step == "warehouse_save":
            template, context = self.handle_invoice_warehouse_save_post(request)
            return render(request, template, context)
        elif step == "add_delivery_type":
            template, context = self.handle_invoice_delivery_type_save(request)
            return render(request, template, context)
        elif step == "update_delivery_invoice":
            template, context = self.handle_invoice_delivery_save(request)
            return render(request, template, context)
        elif step == "confirm_save":
            template, context = self.handle_invoice_confirm_save(request)
            return render(request, template, context)
        elif step == "dismiss":
            template, context = self.handle_invoice_dismiss_save(request)
            return render(request,template, context)
        else:
            raise ValueError(f"unknow request {step}")

    def handle_pallet_data_get(self, start_date: str = None, end_date: str = None) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        pallet_data = Order.objects.select_related(
            "container_number", "customer_name", "warehouse", "offload_id", "retrieval_id"
        ).filter(
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
        criteria = models.Q(
            container_number__order__eta__gte=start_date,
            container_number__order__eta__lte=end_date,
        )
        criteria |= models.Q(
            container_number__order__vessel_id__vessel_eta__gte=start_date,
            container_number__order__vessel_id__vessel_eta__lte=end_date,
        )
        if container_number == "None":
            container_number = None
        if container_number:
            criteria &= models.Q(container_number__container_number=container_number)
        pl_data = PackingList.objects.select_related("container_number").filter(criteria).values(
            'container_number__container_number', 'destination', 'delivery_method', 'cbm', 'pcs', 'total_weight_kg','total_weight_lbs'
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
        order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(criteria).order_by("created_at")
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
    
    def handle_invoice_direct_get(self, 
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None      
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date)
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        #查找直送，没有生成账单的柜子
        order = Order.objects.select_related(
            "customer_name", "container_number",
        ).filter(
            models.Q(
                models.Q(invoice_status="unrecorded") |
                models.Q(invoice_status="") |
                models.Q(invoice_status__exact="") |
                models.Q(invoice_status__isnull=True)
            ),
            criteria,
            order_type = "直送",
        )
        status = ['toBeConfirmed','confirmed']
        previous_order = Order.objects.select_related(
            "customer_name", "container_number", "invoice_id"
        ).values(
            "invoice_status","container_number__container_number","customer_name__zem_name","created_at"
        ).filter(
            criteria,
            order_type = "直送",
            invoice_status__in=status
        )
        context = {
            "order":order,
            "order_form":OrderForm(),
            "previous_order":previous_order,
            "start_date":start_date,
            "end_date":end_date,
            "customer": customer
        }
        return self.template_invoice_direct, context

    #港前账单，待开账单、已开账单、驳回账单
    def handle_invoice_preport_get(self, 
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None      
    ) -> tuple[Any, Any]:
        #拆送——港前提拆柜费
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date),
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        #查找未操作过的
        Invoice.objects.select_related(
            "container_number",""
        )
        order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(
            criteria & models.Q(
                models.Q(invoice_status="unrecorded") |
                models.Q(invoice_status="") |
                models.Q(invoice_status__exact="") |
                models.Q(invoice_status__isnull=True)
            ),
        )
        #查找操作过但被驳回的
        order_reject = Order.objects.filter(
            criteria,
            invoice_status="record_preport",
            invoice_reject="True"
        )
    
        #查找已操作过的，给港前组长看
        order_pending = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(
            criteria,
            invoice_status="record_preport",
            invoice_reject ="False"
        )
        #查找历史操作过的
        status = ['record_warehouse','record_delivery','toBeConfirmed','confirmed']
        previous_order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).values(
            "invoice_status","container_number__container_number","customer_name__zem_name","created_at"
        ).filter(
            criteria,
            models.Q(invoice_status__in=status)
        )
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
        context = {
            "order":order,
            "order_form":OrderForm(),
            "order_reject":order_reject,
            "order_pending":order_pending,
            "previous_order":previous_order,
            "start_date":start_date,
            "end_date":end_date,
            "customer": customer,
            "groups":groups
        }
        return self.template_invoice_preport, context

    def handle_invoice_warehouse_get(self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None
    )-> tuple[Any, Any]:
        #库内操作费
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date),
            
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        #查找未操作过的
        order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(
            criteria,
            invoice_status="record_warehouse"
        )
        #查找历史操作过的
        status = ['record_delivery','toBeConfirmed','confirmed']
        previous_order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).values(
            "invoice_status","container_number__container_number","customer_name__zem_name","created_at"
        ).filter(
            criteria,
            invoice_status__in=status
        )
        context = {
            "order":order,
            "order_form":OrderForm(),
            "start_date":start_date,
            "end_date":end_date,
            "customer": customer,
            "previous_order":previous_order
        }
        return self.template_invoice_warehouse, context

    def handle_invoice_confirm_get(self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None
    )-> tuple[Any, Any]:   
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date)
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        #客服录入完毕的账单
        order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(
            criteria,
            invoice_status="toBeConfirmed"
        )
        #已确认的账单
        previous_order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).values(
            "container_number__container_number","customer_name__zem_name","created_at","invoice_id__invoice_date","order_type",
            "invoice_id__preport_amount","invoice_id__warehouse_amount","invoice_id__delivery_amount","invoice_id__direct_amount",
            "invoice_id__invoice_number","invoice_id__invoice_link", "invoice_id__statement_id__invoice_statement_id",
            "invoice_id__statement_id__statement_link"
        ).filter(
            criteria,
            models.Q(invoice_status='confirmed')
        )
        previous_order = previous_order.annotate(
            total_amount=Case(
                When(order_type='转运', then=F('invoice_id__preport_amount') + F('invoice_id__warehouse_amount') + F('invoice_id__delivery_amount')),
                When(order_type='转运组合', then=F('invoice_id__preport_amount') + F('invoice_id__warehouse_amount') + F('invoice_id__delivery_amount')),
                When(order_type='直送', then=F('invoice_id__direct_amount')),
                default=Value(0),
                output_field=IntegerField()
            )   
        )
        context = {
            "order":order,
            "previous_order":previous_order,
            "order_form":OrderForm(),
            "start_date":start_date,
            "end_date":end_date,
            "customer": customer,
        }
        return self.template_invoice_confirm, context

    def handle_invoice_delivery_get(self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None
    )-> tuple[Any, Any]:   
         #库内操作费
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d') if not start_date else start_date
        end_date = current_date.strftime('%Y-%m-%d') if not end_date else end_date
        criteria = models.Q(
            (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date),
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        #查找未操作过的
        order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).filter(
            criteria,
            invoice_status="record_delivery"
        )
        #查找历史操作过的
        status = ['toBeConfirmed','confirmed']
        previous_order = Order.objects.select_related(
            "invoice_id", "customer_name", "container_number", "invoice_id__statement_id"
        ).values(
            "invoice_status","container_number__container_number","customer_name__zem_name","created_at"
        ).filter(
            criteria,
            invoice_status__in=status
        )
        context = {
            "order":order,
            "previous_order":previous_order,
            "order_form":OrderForm(),
            "start_date":start_date,
            "end_date":end_date,
            "customer": customer,
        }
        return self.template_invoice_delivery, context
    
    def handle_invoice_warehouse_save_post(self,request:HttpRequest) -> tuple[Any, Any]:
        data = request.POST.copy()
        container_number = data.get("container_number")
        invoice = Invoice.objects.select_related("container_number").get(
                container_number__container_number=container_number
            )
        #库内费用表记录
        warehouse_amount = request.POST.get("amount")
        invoice_warehouse = InvoiceWarehouse.objects.filter(invoice_number__invoice_number = invoice.invoice_number)
        if not invoice_warehouse.exists():
            invoice_content = InvoiceWarehouse(**{
                "invoice_number": invoice,
            })
            invoice_content.save()
        invoice_warehouse = InvoiceWarehouse.objects.get(invoice_number__invoice_number = invoice.invoice_number)
        for k,v in data.items():         
            if k not in ['csrfmiddlewaretoken','step','warehouse','container_number','invoice_number'] and v:
                setattr(invoice_warehouse, k, v)
        invoice_warehouse.save()

        #提拆柜记录到invoice表
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        invoice.warehouse_amount = warehouse_amount
        invoice.save()
        #账单状态记录
        invoice_warehouse = InvoiceWarehouse.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        order.invoice_status = "record_delivery"
        order.save()  
        return self.handle_invoice_warehouse_get(request)

    def handle_invoice_direct_save_post(self,request:HttpRequest) -> tuple[Any, Any]:
        data = request.POST.copy()
        container_number = data.get("container_number")
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        direct_amount = request.POST.get("amount")
        invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        for k,v in data.items():         
            if k not in ['csrfmiddlewaretoken','step','warehouse','container_number','invoice_number'] and v:
                setattr(invoice_preports, k, v)
        invoice_preports.save()
        invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        #账单状态记录
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        order.invoice_status="toBeConfirmed"
        order.save()
        invoice = Invoice.objects.get(container_number__container_number=container_number)
        invoice.direct_amount = direct_amount
        invoice.save()
        return self.handle_invoice_direct_get(request)

    def handle_invoice_preport_save_post(self,request:HttpRequest) -> tuple[Any, Any]:
        data = request.POST.copy()
        container_number = data.get("container_number")
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        preport_amount = request.POST.get("amount")
        #提拆柜表费用记录
        invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        for k,v in data.items():         
            if k not in ['csrfmiddlewaretoken','step','warehouse','container_number','invoice_number','pending',"invoice_reject_reason"]:
                if not v:
                    v = 0
                setattr(invoice_preports, k, v)
        #附加项费用和附加项说明
        fields = [
            'chassis', 'chassis_split', 'prepull', 'yard_storage', 
            'handling_fee', 'pier_pass', 'congestion_fee', 'hanging_crane', 
            'dry_run', 'exam_fee', 'hazmat', 'over_weight', 'urgent_fee', 
            'other_serive','demurrage','per_diem','second_pickup'
        ]
        surcharges = {}
        surcharge_notes = {}
        for field in fields:
            surcharge_key = f'{field}_surcharge'
            note_key = f'{field}_surcharge_note'

            surcharge = request.POST.get(surcharge_key, 0) or 0
            note = request.POST.get(note_key, '')
            surcharges[field] = float(surcharge)
            surcharge_notes[field] = note
        invoice_preports.surcharges = surcharges
        invoice_preports.surcharge_notes = surcharge_notes
        invoice_preports.save()
        invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        #账单状态记录
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        if data.get("pending") == "True":
            #审核通过，进入库内账单录入
            order.invoice_status = "record_warehouse"
            order.invoice_reject = "False"
            order.invoice_reject_reason = ''
            #提拆柜记录到invoice表
            invoice = Invoice.objects.select_related("container_number").get(
                container_number__container_number=container_number
            )
            invoice.preport_amount = preport_amount
            invoice.save()
        elif data.get("pending") == "False":
            #审核失败，驳回账单
            order.invoice_reject = "True"
            order.invoice_reject_reason = data.get("invoice_reject_reason", "")
        else:
            #提拆柜录入完毕
            order.invoice_status = "record_preport"
            order.invoice_reject = "False"
        order.save()
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
       
        return self.handle_invoice_preport_get(request,request.POST.get("start_date"),request.POST.get("end_date"))

    def handle_invoice_delivery_type_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        invoice = Invoice.objects.get(container_number__container_number=container_number)
        delivery_type = request.POST.get("alter_type") 
        selections = request.POST.getlist("is_type_added")
        plt_ids = request.POST.getlist("added_plt_ids")
        plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
        total_cbm = request.POST.getlist("cbm")
        total_cbm = [cbm for s, cbm in zip(selections, total_cbm) if s == "on"]
        total_weight_lbs = request.POST.getlist("weight_lbs")
        total_weight_lbs = [weight for s, weight in zip(selections, total_weight_lbs) if s == "on"]
        destination = request.POST.getlist("destination")
        destination = [des for s, des in zip(selections, destination) if s == "on"]
        zipcode = request.POST.getlist("zipcode")
        zipcode = [code for s, code in zip(selections, zipcode) if s == "on"]
        total_pallet = request.POST.getlist("total_pallet")
        total_pallet = [n for s, n in zip(selections, total_pallet) if s == "on"]
        #将前端的每一条记录存为invoice_delivery的一条
        for i in range(len((plt_ids))):
            ids = plt_ids[i].split(',')
            ids = [int(id) for id in ids]
            pallet = Pallet.objects.filter(id__in=ids)           
            current_date = datetime.now().date()
            invoice_delivery = f"{current_date.strftime('%Y-%m-%d').replace('-', '')}-{delivery_type}-{destination[i]}-{len(pallet)}"
            invoice_content = InvoiceDelivery(**{
                "invoice_delivery": invoice_delivery,
                "invoice_number": invoice,               
                "type": delivery_type,
                "zipcode": zipcode[i],
                "destination": destination[i],
                "total_pallet": total_pallet[i],
                "total_cbm": total_cbm[i],
                "total_weight_lbs": total_weight_lbs[i],
            })
            invoice_content.save()
            updated_pallets = []
            for plt in pallet:
                try:
                    invoice_delivery = plt.invoice_delivery
                    if invoice_delivery and hasattr(invoice_delivery, 'delete'):
                        invoice_delivery.delete() 
                except InvoiceDelivery.DoesNotExist:
                    pass
                #pallet指向InvoiceDelivery表
                plt.invoice_delivery = invoice_content
                updated_pallets.append(plt)
            Pallet.objects.bulk_update(updated_pallets, ["invoice_delivery"])
        return self.handle_container_invoice_delivery_get(request)

    def handle_invoice_confirm_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        invoice = Invoice.objects.get(container_number__container_number=container_number)
        description, warehouse_code, cbm, weight, qty, rate, amount, note = [], [], [], [], [], [], []
        if order.order_type == "直送":
            invoice_preport = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
            for field in invoice_preport._meta.fields:
                if isinstance(field, models.FloatField) and field.name != 'amount':
                    value = getattr(invoice_preport, field.name)                    
                    if value not in [None, 0]:
                        if field.verbose_name == "操作处理费":
                            description.append("等待费")
                        else:
                            description.append(field.verbose_name)
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append("")
                        rate.append("")
                        amount.append(value)
                        note.append("")
        else:
            invoice_preport = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
            invoice_warehouse = InvoiceWarehouse.objects.get(invoice_number__invoice_number=invoice.invoice_number)
            invoice_delivery = InvoiceDelivery.objects.filter(invoice_number__invoice_number=invoice.invoice_number)
            for field in invoice_preport._meta.fields:
                if isinstance(field, models.FloatField) and field.name != 'amount':
                    value = getattr(invoice_preport, field.name)                    
                    if value not in [None, 0]:
                        description.append(field.verbose_name)
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append("")
                        rate.append("")
                        amount.append(value)
                        note.append("")
            for field in invoice_warehouse._meta.fields:
                if isinstance(field, models.FloatField) and field.name != 'amount':
                    value = getattr(invoice_warehouse, field.name)                    
                    if value not in [None, 0]:
                        description.append(field.verbose_name)
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append("")
                        rate.append("")
                        amount.append(value)
                        note.append("")
            for delivery in invoice_delivery:
                description.append("派送费")
                warehouse_code.append(delivery.destination.upper())
                cbm.append(delivery.total_cbm)
                weight.append(delivery.total_weight_lbs)
                qty.append(delivery.total_pallet)
                amount.append(delivery.total_cost)
                note.append("")
                try:
                    rate.append(int(delivery.total_cost / delivery.total_pallet))
                except:
                    rate.append("")
        context = {
            "order": order,
            "container_number": container_number,
            "data": zip(description, warehouse_code, cbm, weight, qty, rate, amount, note)
        }
        workbook, invoice_data = self._generate_invoice_excel(context)
        invoice.invoice_date = invoice_data["invoice_date"]
        invoice.invoice_link = invoice_data["invoice_link"]
        invoice.total_amount = (
            float(invoice.preport_amount or 0) +
            float(invoice.warehouse_amount or 0) +
            float(invoice.delivery_amount or 0) +
            float(invoice.direct_amount or 0)
        )
        invoice.save()
        order.invoice_status = "confirmed"
        order.save()
        return self.handle_invoice_confirm_get(request)


    def handle_invoice_dismiss_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        status = request.POST.get("status")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        reject_reason = request.POST.get("reject_reason")
        order = Order.objects.select_related("container_number").get(container_number__container_number=container_number)
        order.invoice_status = status
        order.invoice_reject = "True"
        order.invoice_reject_reason = reject_reason
        order.save()
        return self.handle_invoice_confirm_get(request,start_date,end_date)



    def handle_invoice_delivery_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        type_value = request.POST.get("type")
        total = request.POST.get("amount")
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        #如果派送方式都填完了，invoice记录派送价格和账单状态
        if type_value == "amount":
            invoice.delivery_amount = total
            order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
            order.invoice_status = "toBeConfirmed"
            order.save()
            invoice.save()
        else:
            #记录其中一种派送方式到invoice_delivery表
            plt_ids = request.POST.getlist("plt_ids")
            new_plt_ids = [ast.literal_eval(sub_plt_id) for sub_plt_id in plt_ids] 
            cost = request.POST.getlist("cost")
            #将前端的每一条记录存为invoice_delivery的一条
            for i in range(len((new_plt_ids))):
                ids = [int(id) for id in new_plt_ids[i]]
                pallet = Pallet.objects.filter(id__in=ids)
                #因为每一条记录中所有的板子都是对应一条invoice_delivery，建表的时候就是这样存的，所以取其中一个的外键就可以
                pallet_obj = pallet[0]
                invoice_content = pallet_obj.invoice_delivery
                #除价格外，其他在新建记录的时候就存了
                invoice_content.total_cost = cost[i]         
                invoice_content.save()
        return self.handle_container_invoice_delivery_get(request)

    def handle_container_invoice_warehouse_get(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        warehouse = order.retrieval_id.retrieval_destination_area
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        try:
            invoice_warehouse = InvoiceWarehouse.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        except InvoiceWarehouse.DoesNotExist:
            context = {
                "warehouse":warehouse,
                "invoice":invoice,
                "container_number":container_number,
            }
            return self.template_invoice_warehouse_edit, context
        context = {
            "warehouse":warehouse,
            "invoice_warehouse":invoice_warehouse,
            "invoice":invoice,
            "container_number":container_number,
        }
        return self.template_invoice_warehouse_edit, context
    
    def handle_container_invoice_confirm_get(self,request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        invoice = Invoice.objects.get(
            container_number__container_number=container_number
        )
        order = Order.objects.select_related("container_number").get(container_number__container_number=container_number)
        invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        if order.order_type == "转运" or order.order_type == "转运组合":
            invoice_warehouse = InvoiceWarehouse.objects.get(invoice_number__invoice_number=invoice.invoice_number)
            invoice_delivery = InvoiceDelivery.objects.filter(invoice_number__invoice_number=invoice.invoice_number)
            amazon = []
            local = []
            combine = []
            walmart = []
            for delivery in invoice_delivery:
                if delivery.type == "amazon":
                    amazon.append(delivery)
                elif delivery.type == "local":
                    local.append(delivery)
                elif delivery.type == "combine":
                    combine.append(delivery)
                elif delivery.type == "walmart":
                    walmart.append(delivery)
            context = {
                "invoice":invoice,
                "order_type":order.order_type,
                "invoice_preports":invoice_preports,
                "invoice_warehouse":invoice_warehouse,
                "amazon":amazon,
                "local":local,
                "combine":combine,
                "walmart":walmart,
                "container_number":container_number,
                "start_date":start_date,
                "end_date":end_date
            }
        elif order.order_type == "直送":
            context = {
                "invoice":invoice,
                "order_type":order.order_type,
                "invoice_preports":invoice_preports,
                "container_number":container_number,
                "start_date":start_date,
                "end_date":end_date
            }
        return self.template_invoice_confirm_edit, context

    def handle_container_invoice_delivery_get(self,request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        warehouse = order.retrieval_id.retrieval_destination_area
        #把pallet汇总
        pallet =Pallet.objects.prefetch_related(
            "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number",
            "container_number__order__offload_id", "container_number__order__customer_name", "container_number__order__retrieval_id",
        ).select_related(
            'invoice_delivery'
        ).filter(
            container_number__container_number=container_number
        ).annotate(
            str_id=Cast("id", CharField()),
        ).values(
            'container_number__container_number',
            'destination',
            'zipcode',
            'address',
            'delivery_method',
            'invoice_delivery__type'
        ).annotate(  
            ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),     
            total_cbm=Sum("cbm", output_field=FloatField()),
            total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
            total_n_pallet=Count("pallet_id", distinct=True)
        ).order_by(F('invoice_delivery__type').asc(nulls_first=True)) 
        amazon = []
        local = []
        combine = []
        walmart = []
        if warehouse == 'NJ':
            selected_amazon = NJ_AMAZON_DELIVERY   
            selected_local = LOCAL_DELIVERY
            selected_combina = NJ_COMBINA
            selected_walmart = NJ_WALMART
        elif warehouse == 'SAV':
            selected_amazon = SAV_AMAZON_DELIVERY  
            selected_combina = SAV_COMBINA
            selected_local = None
            selected_walmart = SAV_WALMART
        elif warehouse == 'LA':
            selected_amazon = LA_AMAZON_DELIVERY 
            selected_combina = LA_COMBINA
            selected_local = None    
            selected_walmart = None
        #先查询是不是有Invoice_delivery表了
        invoice_delivery = InvoiceDelivery.objects.prefetch_related(
            "pallet_delivery"
        ).filter(invoice_number__invoice_number=invoice.invoice_number)
        if invoice_delivery:
            for delivery in invoice_delivery:
                destination = delivery.destination.split('-')[1] if '-' in delivery.destination else delivery.destination
                plt_ids = []
                pallets = delivery.pallet_delivery.all()
                for plt in pallets:
                    plt_ids.append(plt.id)
                setattr(delivery, 'plt_ids', plt_ids)
                setattr(delivery, 'total_n_pallet', len(plt_ids))
                if delivery.type == "amazon":
                    for k,v in selected_amazon.items():
                        if destination in v:
                            setattr(delivery, 'cost', k)
                            if not delivery.total_cost:
                                setattr(delivery, 'total_cost', int(k)*int(len(plt_ids)))
                    amazon.append(delivery)
                elif delivery.type == "local": 
                    if selected_local: #NJ的          
                        for k,v in selected_local.items():
                            if delivery.zipcode in v:
                                n_pallet = int(len(plt_ids))
                                costs = k.split(",")
                                if n_pallet <= 5:
                                    cost = int(costs[0])
                                elif n_pallet >= 5:
                                    cost = int(costs[1])
                                setattr(delivery, 'cost', cost)
                                if not delivery.total_cost:   
                                    setattr(delivery, 'total_cost', max(cost*n_pallet,int(costs[2])))
                                break                      
                    local.append(delivery)
                elif delivery.type == "combine":
                    container_type = order.container_number.container_type
                    for k,v in selected_combina.items():
                        if destination in v:
                            cost = k.split(",")
                            if "45HQ/GP" in container_type:
                                setattr(delivery, 'cost', int(cost[1]))   
                                if not delivery.total_cost:     
                                    setattr(delivery, 'total_cost', int(cost[1]))           
                                    setattr(delivery, 'total_cost', int(cost[1]))
                            elif "40HQ/GP" in container_type:
                                setattr(delivery, 'cost', int(cost[0])) 
                                if not delivery.total_cost:
                                    setattr(delivery, 'total_cost', int(cost[0]))   
                    combine.append(delivery)
                elif delivery.type == "walmart":
                    for k,v in selected_walmart.items():
                        if destination in v:
                            setattr(delivery, 'cost', k)
                            if not delivery.total_cost:
                                setattr(delivery, 'total_cost', int(k)*int(len(plt_ids)))
                    walmart.append(delivery)
        else:
            #该柜子没有建表的情况下，系统再根据报表单汇总派送方式                         
            for plt in pallet:       
                destination = plt["destination"].split('-')[1] if '-' in plt["destination"] else plt["destination"] 
                if plt["invoice_delivery__type"] == "amazon":                           
                    for k,v in selected_amazon.items():
                        if destination in v:
                            plt["cost"] = k
                            if not plt["total_cost"]:
                                plt["total_cost"] = int(k)*int(plt["total_n_pallet"])
                            break
                    amazon.append(plt)      
                elif plt["invoice_delivery__type"] == "local":                   
                    if selected_local: #NJ的          
                        for k,v in selected_local.items():
                            if plt["zipcode"] in v:
                                n_pallet = int(plt["total_n_pallet"])
                                costs = k.split(",")
                                if n_pallet <= 5:
                                    cost = int(costs[0])
                                elif n_pallet >= 5:
                                    cost = int(costs[1])
                                plt["cost"] = cost
                                if not plt["total_cost"]:   
                                    plt["total_cost"] = max(cost*n_pallet,int(costs[2]))
                                break               
                    local.append(plt)
                elif plt["invoice_delivery__type"] == "combine":                           
                    container_type = order.container_number.container_type
                    for k,v in selected_combina.items():
                        if destination in v:
                            cost = k.split(",")
                            if "45HQ/GP" in container_type:
                                plt["cost"] = int(cost[1])    
                                if not plt["total_cost"]:                 
                                    plt["total_cost"] = int(cost[1])
                            elif "40HQ/GP" in container_type:
                                plt["cost"] = int(cost[0])
                                if not plt["total_cost"]: 
                                    plt["total_cost"] = int(cost[0])
                    combine.append(plt)
                elif plt["invoice_delivery__type"] == "walmart":                           
                    for k,v in selected_walmart.items():
                        if destination in v:
                            plt["cost"] = k
                            if not plt["total_cost"]:
                                plt["total_cost"] = int(k)*int(plt["total_n_pallet"])
                            break
                    walmart.append(plt)
        context = {
            "warehouse":warehouse,
            "invoice":invoice,
            "container_number":container_number,
            "pallet":pallet,
            "amazon":amazon,
            "local":local,
            "combine":combine,
            "walmart":walmart,
            "invoice_delivery":invoice_delivery
        }
        return self.template_invoice_delievery_edit, context

    def handle_container_invoice_direct_get(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        warehouse = order.retrieval_id.retrieval_destination_area
        try:
            invoice = Invoice.objects.select_related("customer", "container_number").get(
                container_number__container_number=container_number
            )
        except Invoice.DoesNotExist:
            order = Order.objects.select_related("customer_name", "container_number").get(
                container_number__container_number=container_number
            )
            current_date = datetime.now().date()
            order_id = str(order.id)
            customer_id = order.customer_name.id
            invoice = Invoice(**{
                "invoice_number":f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}",
                "customer": order.customer_name,
                "container_number": order.container_number,
                "delivery_amount":0
            })
            invoice.save()
            order.invoice_id = invoice
            order.save()
        destination = order.retrieval_id.retrieval_destination_area
        new_destination = destination.replace(' ', '') if destination else ''
        second_delivery = 0 
        if new_destination in ["ONT8","LGB8","LAX9","SBD2","SBD3","KRB1"]:
            second_delivery = 750
        elif new_destination in ["GYR2","GYR3","GYR3","PHX5","PHX7","SMF3","SCK1","SCK4","SJC7","OAK3","LAS1","LAS3"]:
            second_delivery = 1750
        try:
            invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        except InvoicePreport.DoesNotExist:    
            #获取直送柜子的提拆柜费用
            #如果之前没有录过费用，就根据报价表生成提+派送费用
            pickup_fee = 0
            for k,v in DIRECT_CONTAINER.items():
                if new_destination in v:
                    pickup_fee = k
            invoice_preports = InvoicePreport(**{
                "invoice_number": invoice,
                "pickup": pickup_fee,
            })
            invoice_preports.save()
        context = {
            "warehouse": warehouse,
            "invoice_preports":invoice_preports,
            "container_number":container_number,
            "second_delivery":second_delivery
        }
        return self.template_invoice_direct_edit, context
    
    def handle_container_invoice_preport_get(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        order = Order.objects.select_related("retrieval_id","container_number").get(container_number__container_number=container_number)
        #查看仓库和柜型，计算提拆费
        warehouse = order.retrieval_id.retrieval_destination_area
        container_type = order.container_number.container_type
        pickup_key = (warehouse, container_type)
        pickup_fee = PICKUP_FEE.get(pickup_key, 1500)
        try:
            invoice = Invoice.objects.select_related("customer", "container_number").get(
                container_number__container_number=container_number
            )
        except Invoice.DoesNotExist:
            #没有账单就创建
            order = Order.objects.select_related("customer_name", "container_number").get(
                container_number__container_number=container_number
            )
            current_date = datetime.now().date()
            order_id = str(order.id)
            customer_id = order.customer_name.id
            invoice = Invoice(**{
                "invoice_number":f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}",
                "customer": order.customer_name,
                "container_number": order.container_number,
                "preport_amount": 0,
                "warehouse_amount": 0,
                "delivery_amount": 0,
            })
            invoice.save()
            order.invoice_id = invoice
            order.save()
        try:
            invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        except InvoicePreport.DoesNotExist:
            invoice_preports = InvoicePreport(**{
                "invoice_number": invoice,
                "pickup": pickup_fee,
            })
            invoice_preports.save()
            invoice_preports = InvoicePreport.objects.get(invoice_number__invoice_number=invoice.invoice_number)
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
        context = {
            "warehouse": warehouse,
            "reject_reason": order.invoice_reject_reason,
            "invoice_preports":invoice_preports,
            "surcharges":invoice_preports.surcharges,
            "surcharges_notes":invoice_preports.surcharge_notes,
            "container_number":container_number,
            "groups":groups,
            "start_date":request.GET.get("start_date"),
            "end_date":request.GET.get("end_date")
        }
        print(context["surcharges"])
        return self.template_invoice_preport_edit, context

    def handle_container_invoice_get(self, container_number: str) -> tuple[Any, Any]:
        order = Order.objects.select_related("offload_id").get(container_number__container_number=container_number)
        if order.offload_id.offload_at == None:
            packing_list = PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(
                container_number__container_number=container_number
            ).values(
                'container_number__container_number', 'destination'
            ).annotate(
                total_cbm=Sum("pallet__cbm", output_field=FloatField()),
                total_weight=Sum("pallet__weight_lbs", output_field=FloatField()),
                total_n_pallet=Count('pallet__pallet_id', distinct=True),
            ).order_by("destination", "-total_cbm")
        else:
            packing_list = Pallet.objects.select_related(
                "container_number"
            ).filter(
                container_number__container_number=container_number
            ).values(
                'container_number__container_number', 'destination'
            ).annotate(
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet=Count('pallet_id', distinct=True),
            ).order_by("destination", "-total_cbm")
        for pl in packing_list:
            c_p = math.ceil(pl["total_cbm"] / 1.8)
            w_p = math.ceil(pl["total_weight"] / 1000)
            pl["total_n_pallet"] = max(c_p,w_p)
            # if pl["total_cbm"] > 1:
            #     pl["total_n_pallet"] = round(pl["total_cbm"] / 2)
            # elif pl["total_cbm"] >= 0.6 and pl["total_cbm"] <= 1:
            #     pl["total_n_pallet"] = 0.5
            # else:
            #     pl["total_n_pallet"] = 0.25
        context = {
            "order": order,
            "packing_list": packing_list,
        }
        return self.template_invoice_container, context
    
    def handle_container_invoice_edit_get(self, container_number: str) -> tuple[Any, Any]:
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        invoice_item = InvoiceItem.objects.filter(invoice_number__invoice_number=invoice.invoice_number)
        context = {
            "invoice": invoice,
            "invoice_item": invoice_item,
        }
        return self.template_invoice_container_edit, context
    
    def handle_container_invoice_delete_get(self, request: HttpRequest) -> tuple[Any, Any]:
        invoice_number = request.GET.get("invoice_number")
        invoice = Invoice.objects.select_related("container_number").get(invoice_number=invoice_number)
        invoice_item = InvoiceItem.objects.filter(invoice_number__invoice_number=invoice_number)
        container_number = invoice.container_number.container_number
        # delete file from sharepoint
        try:
            self._delete_file_from_sharepoint("invoice", f"INVOICE-{container_number}.xlsx")
        except FileNotFoundError as e:
            pass
        except RuntimeError as e:
            raise RuntimeError(e)
        # delete invoice item
        invoice_item.delete()
        # delete invoice
        invoice.delete()
        return self.handle_invoice_get()
    
    def handle_pallet_data_export_post(self, request: HttpRequest) -> HttpResponse:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet_data = Order.objects.select_related(
            "container_number", "customer_name", "warehouse", "offload_id", "retrieval_id"
        ).filter(
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) &
            models.Q(offload_id__offload_at__gte=start_date) &
            models.Q(offload_id__offload_at__lte=end_date)
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
                "总重lbs": d["total_weight_lbs"],
            } for d in context["pl_data"]
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f"attachment; filename=packing_list_data_{start_date}_{end_date}.xlsx"
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response
    
    def handle_invoice_order_search_post(self, request: HttpRequest,status) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        order_form = OrderForm(request.POST)
        if order_form.is_valid():
            customer = order_form.cleaned_data.get("customer_name")
        else:
            customer = None
        if status == "direct":
            return self.handle_invoice_direct_get(request, start_date, end_date, customer)
        elif status == "preport":
            return self.handle_invoice_preport_get(request, start_date, end_date, customer)
        elif status == "warehouse":
            return self.handle_invoice_warehouse_get(request, start_date, end_date, customer)
        elif status == "delivery":
            return self.handle_invoice_delivery_get(request, start_date, end_date, customer)
        elif status == "confirm":
            return self.handle_invoice_confirm_get(request, start_date, end_date, customer)
        else:
            return self.handle_invoice_get(start_date, end_date, customer)

    def handle_invoice_order_select_post(self, request: HttpRequest) -> HttpResponse:
        selected_orders = json.loads(request.POST.get('selectedOrders', '[]'))
        selected_orders = list(set(selected_orders))
        if selected_orders:
            order = Order.objects.select_related(
                "customer_name", "container_number", "invoice_id"
            ).filter(
                container_number__container_number__in=selected_orders
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
        if self._check_invoice_exist(container_number):
            raise RuntimeError(f"货柜-{container_number}已生成invoice!")
        description = request.POST.getlist("description")
        warehouse_code = request.POST.getlist("warehouse_code")
        cbm = request.POST.getlist("cbm")
        weight = request.POST.getlist("weight")
        qty = request.POST.getlist("qty")
        rate = request.POST.getlist("rate")
        amount = request.POST.getlist("amount")
        note = request.POST.getlist("note")
        order = Order.objects.select_related("customer_name", "container_number").get(
            container_number__container_number=container_number
        )
        context = {
            "order": order,
            "container_number": container_number,
            "data": zip(description, warehouse_code, cbm, weight, qty, rate, amount, note)
        }
        workbook, invoice_data = self._generate_invoice_excel(context)
        invoice = Invoice(**{
            "invoice_number": invoice_data["invoice_number"],
            "invoice_date": invoice_data["invoice_date"],
            "invoice_link": invoice_data["invoice_link"],
            "customer": context["order"].customer_name,
            "container_number": context["order"].container_number,
            "total_amount": invoice_data["total_amount"],
        })
        invoice.save()
        order.invoice_id = invoice
        order.save()
        invoice_item_data = []
        for d, wc, c, w, q, r, a, n in zip(description, warehouse_code, cbm, weight, qty, rate, amount, note):
            invoice_item_data.append({
                "invoice_number": invoice,
                "description": d,
                "warehouse_code": wc,
                "cbm": c if c else None,
                "weight": w if w else None,
                "qty": q if q else None,
                "rate": r if r else None,
                "amount": a if a else None,
                "note": n if n else '',
            })
        InvoiceItem.objects.bulk_create([
            InvoiceItem(**inv_itm_data) for inv_itm_data in invoice_item_data
        ])
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename=INVOICE-{container_number}.xlsx'
        workbook.save(response)
        return response
    
    def handle_export_invoice_post(self, request: HttpRequest) -> HttpResponse:
        resp, file_name, pdf_file, context = export_invoice(request)
        pdf_file.seek(0)
        invoice = Invoice.objects.select_related("statement_id").filter(
            models.Q(container_number__container_number__in=context["container_number"])
        )
        invoice_statement = InvoiceStatement.objects.filter(
            models.Q(invoice__container_number__container_number__in=context["container_number"])
        )
        for invc_stmt in invoice_statement.distinct():
            try:
                self._delete_file_from_sharepoint(
                    "invoice_statement",
                    f"invoice_{invc_stmt.invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf"
                )
            except:
                pass
        invoice_statement.delete()
        link = self._upload_excel_to_sharepoint(pdf_file, "invoice_statement", file_name)
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
        Invoice.objects.bulk_update(invoice, ["statement_id"])
        return resp
    
    def handle_container_invoice_edit_post(self, request: HttpRequest) -> HttpResponse:
        invoice_number = request.POST.get("invoice_number")
        invoice = Invoice.objects.select_related("container_number").get(invoice_number=invoice_number)
        container_number = invoice.container_number.container_number
        description = request.POST.getlist("description")
        warehouse_code = request.POST.getlist("warehouse_code")
        cbm = request.POST.getlist("cbm")
        weight = request.POST.getlist("weight")
        qty = request.POST.getlist("qty")
        rate = request.POST.getlist("rate")
        amount = request.POST.getlist("amount")
        note = request.POST.getlist("note")
        order = Order.objects.select_related("customer_name").get(invoice_id__invoice_number=invoice_number)
        context = {
            "order": order,
            "container_number": container_number,
            "data": zip(description, warehouse_code, cbm, weight, qty, rate, amount, note)
        }

        # delete old file from sharepoint
        try:
            self._delete_file_from_sharepoint("invoice", f"INVOICE-{container_number}.xlsx")
        except FileNotFoundError as e:
            pass
        except RuntimeError as e:
            raise RuntimeError(f"Error: {e}")
        # create new file and upload to sharepoint
        workbook, invoice_data = self._generate_invoice_excel(context)
        # update invoice information
        # invoice.invoice_number = invoice_data["invoice_number"]
        # invoice.invoice_date = invoice_data["invoice_date"]
        invoice.invoice_link = invoice_data["invoice_link"]
        invoice.total_amount = invoice_data["total_amount"]
        invoice.save()
        # update invoice item information
        InvoiceItem.objects.filter(invoice_number__invoice_number=invoice_number).delete()
        invoice_item_data = []
        for d, wc, c, q, r, a, n in zip(description, warehouse_code, cbm, qty, rate, amount, note):
            invoice_item_data.append({
                "invoice_number": invoice,
                "description": d,
                "warehouse_code": wc,
                "cbm": c if c else None,
                "qty": q if q else None,
                "rate": r if r else None,
                "amount": a if a else None,
                "note": n if n else None,
            })
        InvoiceItem.objects.bulk_create([
            InvoiceItem(**inv_itm_data) for inv_itm_data in invoice_item_data
        ])
        # export new file
        response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename=INVOICE-{container_number}.xlsx'
        workbook.save(response)
        return response
        
    def _generate_invoice_excel(
            self, context: dict[Any, Any],
        ) -> tuple[openpyxl.workbook.Workbook, dict[Any, Any]]:
        current_date = datetime.now().date()
        order_id = str(context["order"].id)
        customer_id = context["order"].customer_name.id
        invoice_number = f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}"
        workbook = openpyxl.Workbook()  #创建一个工作簿对象
        worksheet = workbook.active     #获取工作簿的活动工作表
        worksheet.title = "Sheet1"      #给表命名
        cells_to_merge = [              #要合并的单元格
            "A1:E1", "A3:A4", "B3:D3", "B4:D4", "E3:E4", "F3:I4", "A5:A6", "B5:D5", "B6:D6", "E5:E6", "F5:I6", "A9:B9", 
            "A10:B10", "F1:I1", "C1:E1", "A2:H2", "A7:H7", "A8:H8", "C9:H9", "C10:H10", "A11:H11"
        ]
        self._merge_ws_cells(worksheet, cells_to_merge)   #进行合并

        worksheet.column_dimensions['A'].width = 18
        worksheet.column_dimensions['B'].width = 18
        worksheet.column_dimensions['C'].width = 15
        worksheet.column_dimensions['D'].width = 7
        worksheet.column_dimensions['E'].width = 8
        worksheet.column_dimensions['F'].width = 7
        worksheet.column_dimensions['G'].width = 11
        worksheet.column_dimensions['H'].width = 11
        worksheet.column_dimensions['I'].width = 11
        worksheet.row_dimensions[1].height = 40

        worksheet["A1"] = "Zem Elitelink Logistics Inc"
        worksheet["A3"] = "NJ"
        worksheet["B3"] = "27 Engelhard Ave. Avenel NJ 07001"
        worksheet["B4"] = "Contact: Marstin Ma 929-810-9968"
        worksheet["A5"] = "SAV"
        worksheet["B5"] = "1001 Trade Center Pkwy, Rincon, GA 31326, USA"
        worksheet["B6"] = "Contact: Ken 929-329-4323"
        worksheet["A7"] = "E-mail: OFFICE@ZEMLOGISTICS.COM"
        worksheet["A9"] = "BILL TO"
        worksheet["A10"] = context["order"].customer_name.zem_name
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

        worksheet.append(["CONTAINER #", "DESCRIPTION", "WAREHOUSE CODE", "CBM", "WEIGHT", "QTY", "RATE", "AMOUNT", "NOTE"]) #添加表头
        invoice_item_starting_row = 12
        invoice_item_row_count = 0
        row_count = 13
        total_amount = 0.0
        total_cbm = 0.0
        total_weight = 0.0
        for d, wc, cbm, weight, qty, r, amt, n in context["data"]:
            worksheet.append([context["container_number"], d, wc, cbm, weight, qty, r, amt, n])  #添加数据
            total_amount += float(amt)  #计算总金额
            total_cbm += float(cbm) if cbm else 0
            total_weight += float(weight) if weight else 0
            row_count += 1
            invoice_item_row_count += 1
        worksheet.append(["Total", None, None, total_cbm, total_weight, None, None, total_amount, None])   #工作表末尾添加总金额
        invoice_item_row_count += 1
        for row in worksheet.iter_rows(  #单元格设置样式
            min_row=invoice_item_starting_row,
            max_row=invoice_item_starting_row + invoice_item_row_count,
            min_col=1,
            max_col=9,
        ):
            for cell in row:
                cell.border = Border(
                    left=Side(style="thin"),
                    right=Side(style="thin"),
                    top=Side(style="thin"),
                    bottom=Side(style="thin"),
                )  
        self._merge_ws_cells(worksheet, [f"A{row_count}:C{row_count}"])
        self._merge_ws_cells(worksheet, [f"F{row_count}:G{row_count}"])
        worksheet[f"A{row_count}"].alignment = Alignment(horizontal="center")
        worksheet[f"G{row_count}"].number_format = numbers.FORMAT_NUMBER_00
        worksheet[f"G{row_count}"].alignment = Alignment(horizontal="left")
        row_count += 1
        self._merge_ws_cells(worksheet, [f"A{row_count}:H{row_count}"])
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
            self._merge_ws_cells(worksheet, [f"A{row_count}:H{row_count}"])
            row_count += 1
        self._merge_ws_cells(worksheet, [f"A{row_count}:H{row_count}"])

        excel_file = io.BytesIO()  #创建一个BytesIO对象
        workbook.save(excel_file)  #将workbook保存到BytesIO中
        excel_file.seek(0)         #将文件指针移动到文件开头
        invoice_link = self._upload_excel_to_sharepoint(excel_file, "invoice", f"INVOICE-{context['container_number']}.xlsx")

        worksheet['A9'].font = Font(color="00FFFFFF")
        worksheet['A9'].fill = PatternFill(start_color="00000000", end_color="00000000", fill_type="solid")
        invoice_data = {
            "invoice_number": invoice_number,
            "invoice_date": current_date.strftime('%Y-%m-%d'),
            "invoice_link": invoice_link,
            "total_amount": total_amount,
        }
        return workbook, invoice_data

    def _merge_ws_cells(self, ws: openpyxl.worksheet.worksheet, cells: list[str]) -> None:
        for c in cells:
            ws.merge_cells(c)

    def _get_sharepoint_auth(self) -> ClientContext:
        return  ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))

    def _upload_excel_to_sharepoint(
        self,
        file: BytesIO,
        schema: str,
        file_name: str
    ) -> str:
        conn = self._get_sharepoint_auth()
        file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/{schema}/{APP_ENV}")
        sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(f"{file_name}", file).execute_query()
        link = resp.share_link(SharingLinkKind.OrganizationView).execute_query().value.to_json()["sharingLinkInfo"]["Url"]
        return link
    
    def _delete_file_from_sharepoint(
        self,
        schema: str,
        file_name: str,
    ) -> None:
        conn = self._get_sharepoint_auth()
        file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/{schema}/{APP_ENV}/{file_name}")
        try:
            conn.web.get_file_by_server_relative_url(file_path).delete_object().execute_query()
        except ClientRequestException as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(e)
            else:
                raise RuntimeError(e)

    #按照权限分组，有三个分组：客服组（添加账单详情）、组长组（确认客服组操作）、财务组（确认账单）
    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name=self.allowed_group).exists():
            return True
        else:
            return False
    def validate_user_invoice_direct(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_direct").exists():
            return True
        else:
            return False
        
    def validate_user_invoice_preport(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_preport").exists():
            return True
        elif user.groups.filter(name="invoice_preport_leader").exists():
            return True
        else:
            return False

    def validate_user_invoice_warehouse(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_warehouse").exists():
            return True
        else:
            return False
    
    def validate_user_invoice_delivery(self, user:User)-> bool:
        if user.is_staff or user.groups.filter(name="invoice_delivery").exists():
            return True
        else:
            return False
        
    def validate_user_invoice_confirm(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_confirm").exists():
            return True
        else:
            return False 
        
    def _check_invoice_exist(self, container_number: str) -> bool:
        return Invoice.objects.filter(container_number__container_number=container_number).exists()