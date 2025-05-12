import ast
import io
import json
import math
import os
import re
import zipfile
from datetime import datetime, timedelta,date
from io import BytesIO
from typing import Any
from itertools import chain

import openpyxl
import openpyxl.workbook
import openpyxl.worksheet
import openpyxl.worksheet.worksheet
import pandas as pd
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Sum,
    Value,
    When,
    Prefetch,
    BooleanField,
    Min,
    Exists,
    OuterRef,
    ExpressionWrapper
)
from collections import defaultdict
from django.db.models.functions import Cast, Coalesce
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.forms.order_form import OrderForm
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.container import Container
from warehouse.models.invoice import (
    Invoice,
    InvoiceItem,
    InvoiceStatement,
    InvoiceStatus,
)
from warehouse.models.invoice_details import (
    InvoiceDelivery,
    InvoicePreport,
    InvoiceWarehouse,
)
from django.db.models.query import QuerySet
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.quotation_master import QuotationMaster
from warehouse.utils.constants import (
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE,
    APP_ENV,
    SP_DOC_LIB,
    SP_PASS,
    SP_URL,
    SP_USER,
    SYSTEM_FOLDER,
)
from warehouse.views.export_file import export_invoice


@method_decorator(login_required(login_url="login"), name="dispatch")
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
    template_invoice_delievery_public_edit = (
        "accounting/invoice_delivery_public_edit.html"
    )
    template_invoice_delievery_other_edit = (
        "accounting/invoice_delivery_other_edit.html"
    )
    template_invoice_confirm = "accounting/invoice_confirm.html"
    template_invoice_confirm_edit = "accounting/invoice_confirm_edit.html"
    # template_invoice_confirm_edit = "accounting/invoice_confirm_combina3.html"
    template_invoice_direct = "accounting/invoice_direct.html"
    template_invoice_direct_edit = "accounting/invoice_direct_edit.html"
    template_invoice_combina = "accounting/invoice_combina.html"
    template_invoice_combina_edit = "accounting/invoice_combina_edit.html"
    template_invoice_search = "accounting/invoice_search.html"
    allowed_group = "accounting"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }

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
            if self._validate_user_invoice_direct(request.user):
                template, context = self.handle_invoice_direct_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "invoice_combina":
            if self._validate_user_invoice_combina(request.user):
                template, context = self.handle_invoice_combina_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "invoice_preport":  # 提拆柜账单录入
            if self._validate_user_invoice_preport(request.user):
                template, context = self.handle_invoice_preport_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "invoice_warehouse":  # 库内账单录入
            if self._validate_user_invoice_warehouse(request.user):
                template, context = self.handle_invoice_warehouse_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "invoice_delivery":
            if self._validate_user_invoice_delivery(request.user):
                template, context = self.handle_invoice_delivery_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "invoice_confirm":
            if self._validate_user_invoice_confirm(request.user):
                template, context = self.handle_invoice_confirm_get(request)
                return render(request, template, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
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
        elif step == "container_combina":
            template, context = self.handle_container_invoice_combina_get(request)
            return render(request, template, context)
        elif step == "container_preport":
            template, context = self.handle_container_invoice_preport_get(request)
            return render(request, template, context)
        elif step == "container_warehouse":
            template, context = self.handle_container_invoice_warehouse_get(request)
            return render(request, template, context)
        elif step == "container_delivery":
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
        elif step == "invoice_search":
            template, context = self.handle_invoice_search_get(request)
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
            template, context = self.handle_pl_data_get(
                start_date, end_date, container_number
            )
            return render(request, template, context)
        elif step == "pallet_data_export":
            return self.handle_pallet_data_export_post(request)
        elif step == "pl_data_export":
            return self.handle_pl_data_export_post(request)
        elif step == "invoice_order_search":
            template, context = self.handle_invoice_order_search_post(request, "old")
            return render(request, template, context)
        elif step == "invoice_order_direct":
            template, context = self.handle_invoice_order_search_post(request, "direct")
            return render(request, template, context)
        elif step == "invoice_order_combina":
            template, context = self.handle_invoice_order_search_post(request, "combina")
            return render(request, template, context)
        elif step == "invoice_order_preport":
            template, context = self.handle_invoice_order_search_post(
                request, "preport"
            )
            return render(request, template, context)
        elif step == "invoice_order_warehouse":
            template, context = self.handle_invoice_order_search_post(
                request, "warehouse"
            )
            return render(request, template, context)
        elif step == "invoice_order_delivery":
            template, context = self.handle_invoice_order_search_post(
                request, "delivery"
            )
            return render(request, template, context)
        elif step == "invoice_order_confirm":
            template, context = self.handle_invoice_order_search_post(
                request, "confirm"
            )
            return render(request, template, context)
        elif step == "invoice_search":
            template, context = self.handle_invoice_order_search_post(
                request, "search"
            )
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
        elif step == "preport_save":
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
            return render(request, template, context)
        elif step == "redirect":
            template, context = self.handle_invoice_redirect_post(request)
            return render(request, template, context)
        elif step == "invoice_order_batch_export":
            return self.handle_invoice_order_batch_export(request)
        elif step == "invoice_order_delivered":
            return self.handle_invoice_order_batch_delivered(request)
        elif step == "invoice_order_reject":
            return self.handle_invoice_order_batch_reject(request)
        elif step == "migrate_payable_receivable_amount":
            template, context = self.migrate_payable_to_receivable()
            return render(request, template, context)
        elif step == "migrate_status":
            template, context = self.migrate_status()
            return render(request, template, context)
        elif step == "confirm_combina_save":
            template, context = self.handle_invoice_confirm_combina_save(request)
            return render(request, template, context)
        else:
            raise ValueError(f"unknow request {step}")

    def migrate_payable_to_receivable(self) -> tuple[Any, Any]:
        invoices = Invoice.objects.all()
        for invoice in invoices:
            invoice.receivable_total_amount = invoice.payable_total_amount
            invoice.receivable_preport_amount = invoice.payable_preport_amount
            invoice.receivable_warehouse_amount = invoice.payable_warehouse_amount
            invoice.receivable_delivery_amount = invoice.payable_delivery_amount
            invoice.receivable_direct_amount = invoice.payable_direct_amount

            # 将payable字段重置为0.0
            invoice.payable_total_amount = 0.0
            invoice.payable_preport_amount = 0.0
            invoice.payable_warehouse_amount = 0.0
            invoice.payable_delivery_amount = 0.0
            invoice.payable_direct_amount = 0.0

            invoice.save()
        context = {}
        return self.template_invoice_preport, context

    def get_special_stages(self, main_stage) -> tuple[str, str]:
        if main_stage == "delivery":
            return "warehouse_completed", "warehouse_completed"
        elif main_stage in ("tobeconfirmed", "confirmed"):
            return "delivery_completed", "delivery_completed"
        else:
            return "pending", "pending"  # 默认返回原状态

    def migrate_status(self) -> tuple[Any, Any]:
        #把派送表里，total_cost为空的，自动计算
        updated = InvoiceDelivery.objects.filter(
            total_cost__isnull=True
        ).exclude(
            cost=0
        ).exclude(
            total_pallet__isnull=True
        ).update(
            total_cost=Coalesce(F('cost') * F('total_pallet'), Value(0))
        )
        context = {}
        return self.template_invoice_preport, context


        for item in InvoiceStatus.objects.all():   
            if item.stage == "tobeconfirmed":
                container_number = item.container_number
                invoice = Invoice.objects.get(container_number = item.container_number)
                delivery_amount = (
                    InvoiceDelivery.objects.filter(
                        invoice_number=invoice,
                        invoice_type="receivable",               
                    ).aggregate(total_amount=Sum("total_cost"))["total_amount"]
                    or 0
                )
                invoice.receivable_delivery_amount = delivery_amount
                invoice.save()      
        # 提取单价信息
        excluded_fields = {
            "id",
            "invoice_number",
            "invoice_type",
            "amount",
            "qty",
            "rate",
            "other_fees",
            "surcharges",
            "surcharge_notes",
            "history",
        }
        vessel_etd = order.vessel_id.vessel_etd
        quotation = QuotationMaster.objects.filter(
            effective_date__lte=vessel_etd
        ).order_by('-effective_date').first()
        if not quotation:
            raise ValueError('找不到报价表')
        PICKUP_FEE = FeeDetail.objects.get(
            quotation_id=quotation.id, fee_type="preport"
        )
        # 提拆、打托缠膜费用
        #match = re.match(r"\d+", container_type)

        FS_constrain = {  
            key: float(re.search(r"\d+(\.\d+)?", value).group())
            for key, value in PICKUP_FEE.details.items()
            if not isinstance(value, dict)
            and "/" in str(value)
            and re.search(r"\d+(\.\d+)?", value)
        }
        for item in InvoicePreport.objects.all():   
            invoice = item.invoice_number
            order = Order.objects.get(
                container_number=invoice.container_number,
            )
            if order:
                warehouse = order.warehouse
                #container_type = invoice.container_number.container_type if invoice.container_number else None
                order_type = order.order_type
            if order_type != '直送':
                if not item.qty or not item.rate:             
                    qty_data, rate_data = self._extract_unit_price(
                        model=InvoicePreport,
                        unit_prices=FS_constrain,
                        pickup_fee=item.pickup,
                        excluded_fields=excluded_fields,
                    )
                    item.qty = qty_data
                    item.rate = rate_data
                    item.save()

        for st in InvoiceStatus.objects.all():
            if st.stage_public == "delivery_completed" and st.stage_other == "delivery_completed" and st.stage =="preport":
                st.stage = "tobeconfirmed"
                st.save()
        context = {}
        return self.template_invoice_preport, context

        deliverys = InvoiceDelivery.objects.all()
        for dl in deliverys:
            if dl.type == "self_delivery":
                dl.type = "selfdelivery"
                dl.save()
        context = {}
        STATUS_MAPPING = {
            "record_preport": "preport",
            "record_warehouse": "warehouse",
            "record_delivery": "delivery",  # 修正为正确的映射
            "tobeconfirmed": "tobeconfirmed",
            "confirmed": "confirmed",
            None: "unstarted",  # 处理空值情况
        }
        orders = (
            Order.objects.select_related("container_number").filter(
                invoice_status__isnull=False
            )
        ).exclude(invoice_status="")
        for order in orders:
            main_stage = STATUS_MAPPING.get(order.invoice_status, "unstarted")
            stage_public, stage_other = self.get_special_stages(main_stage)

            invoice_status, created = InvoiceStatus.objects.get_or_create(
                container_number=order.container_number,
                invoice_type="receivable",
                defaults={
                    "stage": main_stage,
                    "stage_public": stage_public,
                    "stage_other": stage_other,
                },
            )

            if not created:
                invoice_status.stage = main_stage
                invoice_status.stage_public = stage_public
                invoice_status.stage_other = stage_other
                invoice_status.save()

            order.receivable_status = invoice_status
            order.save()
        context = {}
        return self.template_invoice_preport, context

    def replace_keywords(data):
        KEYWORD_MAPPING = {
            "等待费": "港口拥堵费",
            "查验": "查验费",
            "车架费": "托架费",
            "车架分离费": "托架提取费",
            "货柜储存费": "货柜放置费",
        }
        if isinstance(data, str):
            for old, new in KEYWORD_MAPPING.items():
                if old in data:
                    return new
            return data
        elif isinstance(data, dict):
            return {
                (KEYWORD_MAPPING.get(key, key) if isinstance(key, str) else key): value
                for key, value in data.items()
            }
        else:
            return data
    def handle_pallet_data_get(
        self, start_date: str = None, end_date: str = None
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        pallet_data = (
            Order.objects.select_related(
                "container_number",
                "customer_name",
                "warehouse",
                "offload_id",
                "retrieval_id",
            )
            .filter(
                models.Q(offload_id__offload_required=True)
                & models.Q(offload_id__offload_at__isnull=False)
                & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
                & models.Q(offload_id__offload_at__gte=start_date)
                & models.Q(offload_id__offload_at__lte=end_date)
            )
            .order_by("offload_id__offload_at")
        )
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
        start_date = (
            (current_date + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
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
        pl_data = (
            PackingList.objects.select_related("container_number")
            .filter(criteria)
            .values(
                "container_number__container_number",
                "destination",
                "delivery_method",
                "cbm",
                "pcs",
                "total_weight_kg",
                "total_weight_lbs",
            )
            .order_by("container_number__container_number", "destination")
        )
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
        start_date = (
            (current_date + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        criteria = models.Q(
            models.Q(
                offload_id__offload_required=True, offload_id__offload_at__isnull=False
            )
            | models.Q(offload_id__offload_required=False)
        )
        criteria &= models.Q(created_at__gte=start_date)
        criteria &= models.Q(created_at__lte=end_date)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        order = (
            Order.objects.select_related(
                "invoice_id",
                "customer_name",
                "container_number",
                "invoice_id__statement_id",
            )
            .filter(criteria)
            .order_by("created_at")
        )
        order_no_invoice = [o for o in order if o.invoice_id is None]
        order_invoice = [o for o in order if o.invoice_id]
        context = {
            "order_form": OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "order": order,
            "customer": customer,
            "order_no_invoice": order_no_invoice,
            "order_invoice": order_invoice,
        }
        return self.template_invoice_management, context

    def handle_invoice_combina_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        criteria = models.Q(
            cancel_notification=False,
            vessel_id__vessel_etd__gte=start_date,
            vessel_id__vessel_etd__lte=end_date
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)

        invoice_type = request.POST.get("invoice_type") or "receivable"
        status_field = f"{invoice_type}_status"

        # 查找转运组合，没有生成账单的柜子
        order = Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id"
        ).filter(
            criteria,
            models.Q(**{f"{status_field}__isnull": True})
            | models.Q(  # 考虑账单编辑点的是暂存的情况
                **{
                    f"{invoice_type}_status__invoice_type": invoice_type,
                    f"{invoice_type}_status__stage__in": ["unstarted"],
                }
            ),
            order_type="转运组合",
            container_number__account_order_type="转运组合",
        )
        order = self.process_orders_display_status(
            order, invoice_type
        )
        # 已录入账单
        previous_order = (
            Order.objects.select_related(
                "invoice_id",
                "customer_name",
                "container_number",
                "invoice_id__statement_id",
                f"{invoice_type}_status",
            )
            .values(
                "invoice_status",
                "container_number__container_number",
                "customer_name__zem_name",
                "created_at",
                f"{invoice_type}_status",
            )
            .filter(
                criteria,
                order_type="转运组合",
                container_number__account_order_type="转运组合",
                **{
                    f"{invoice_type}_status__isnull": False,
                    f"{invoice_type}_status__invoice_type": invoice_type,
                },
            )
            .exclude(
                **{
                    f"{invoice_type}_status__stage__in": ["preport", "unstarted"]
                }
            )
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        context = {
            "order": order,
            "order_form": OrderForm(),
            "previous_order": previous_order,
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "invoice_type_filter": invoice_type,
        }
        return self.template_invoice_combina, context
    
    def handle_invoice_direct_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        criteria = models.Q(
            cancel_notification=False,
            vessel_id__vessel_etd__gte=start_date,
            vessel_id__vessel_etd__lte=end_date
        )
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)

        invoice_type = request.POST.get("invoice_type") or "receivable"
        status_field = f"{invoice_type}_status"

        # 查找直送，没有生成账单的柜子
        order = Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id"
        ).filter(
            criteria,
            models.Q(**{f"{status_field}__isnull": True})
            | models.Q(  # 考虑账单编辑点的是暂存的情况
                **{
                    f"{invoice_type}_status__invoice_type": invoice_type,
                    f"{invoice_type}_status__stage__in": ["unstarted"],
                }
            ),
            order_type="直送",
        )
        order = self.process_orders_display_status(
            order, invoice_type
        )
        # 已录入账单
        previous_order = (
            Order.objects.select_related(
                "invoice_id",
                "customer_name",
                "container_number",
                "invoice_id__statement_id",
                f"{invoice_type}_status",
            )
            .values(
                "invoice_status",
                "container_number__container_number",
                "customer_name__zem_name",
                "created_at",
                f"{invoice_type}_status",
            )
            .filter(
                criteria,
                order_type="直送",
                **{
                    f"{invoice_type}_status__isnull": False,
                    f"{invoice_type}_status__invoice_type": invoice_type,
                },
            )
            .exclude(
                **{
                    f"{invoice_type}_status__stage__in": ["preport", "unstarted"]
                }
            )
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        context = {
            "order": order,
            "order_form": OrderForm(),
            "previous_order": previous_order,
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "invoice_type_filter": invoice_type,
        }
        return self.template_invoice_direct, context

    def handle_invoice_search_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        criteria = models.Q(
            vessel_id__vessel_etd__gte=start_date, vessel_id__vessel_etd__lte=end_date
        )
        if warehouse:
            criteria &= models.Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        invoice_type = request.POST.get("invoice_type") or "receivable"
        orders = Order.objects.select_related(
            "customer_name", "container_number", f"{invoice_type}_status"
        ).filter(criteria)
        orders = self.process_orders_display_status(orders, invoice_type)
        context = {
            "order": orders,
            "order_form": OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "invoice_type_filter": invoice_type,
        }
        return self.template_invoice_search, context

    def process_orders_display_status(self, orders, invoice_type):
        STAGE_MAPPING = {
            "unstarted": "未录入",
            "preport": "提拆柜录入阶段",
            "warehouse": "仓库录入阶段",
            "delivery": "派送录入阶段",
            "tobeconfirmed": "待财务确认",
            "confirmed": "财务已确认",
        }

        SUB_STAGE_MAPPING = {
            "pending": "仓库待处理",
            "warehouse_completed": "仓库已完成",
            "delivery_completed": "派送已完成",
            "warehouse_rejected": "仓库已驳回",
            "delivery_rejected": "派送已驳回",
        }

        is_dict_type = orders and isinstance(orders[0], dict)

        if is_dict_type:
            status_ids = [
                o[f"{invoice_type}_status"]
                for o in orders
                if o.get(f"{invoice_type}_status")
            ]
            status_objects = InvoiceStatus.objects.filter(id__in=status_ids).in_bulk()

        processed_orders = []
        for order in orders:
            if is_dict_type:
                status_id = order.get(f"{invoice_type}_status")
                status_obj = status_objects.get(status_id) if status_id else None

                if status_obj:
                    raw_stage = status_obj.stage
                    raw_public_stage = status_obj.stage_public
                    raw_other_stage = status_obj.stage_other
                    raw_is_rejected = status_obj.is_rejected
                    raw_reject_reason = status_obj.reject_reason
                else:
                    raw_stage = None
                    raw_public_stage = None
                    raw_other_stage = None
                    raw_is_rejected = False
                    raw_reject_reason = ""

                order_data = order.copy()
            else:
                status_obj = getattr(order, f"{invoice_type}_status", None)

                if status_obj:
                    raw_stage = status_obj.stage
                    raw_public_stage = status_obj.stage_public
                    raw_other_stage = status_obj.stage_other
                    raw_is_rejected = status_obj.is_rejected
                    raw_reject_reason = status_obj.reject_reason
                else:
                    raw_stage = None
                    raw_public_stage = None
                    raw_other_stage = None
                    raw_is_rejected = False
                    raw_reject_reason = ""

                order_data = order

            if raw_stage in ["warehouse", "delivery"]:
                stage1 = SUB_STAGE_MAPPING.get(raw_public_stage, str(raw_public_stage))
                stage2 = SUB_STAGE_MAPPING.get(raw_other_stage, str(raw_other_stage))
                display_stage = f"公仓: {stage1}\n私仓: {stage2}"
            else:
                display_stage = (
                    STAGE_MAPPING.get(raw_stage, str(raw_stage))
                    if raw_stage
                    else "未知状态"
                )

            if is_dict_type:
                order_data.update(
                    {
                        "display_stage": display_stage,
                        "display_is_rejected": "已驳回" if raw_is_rejected else "正常",
                        "display_reject_reason": raw_reject_reason or " ",
                    }
                )
            else:
                order.display_stage = display_stage
                order.display_is_rejected = "已驳回" if raw_is_rejected else "正常"
                order.display_reject_reason = raw_reject_reason or " "

            processed_orders.append(order_data)

        return processed_orders

    # 港前账单，待开账单、已开账单、驳回账单
    def handle_invoice_preport_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        # 拆送——港前提拆柜费
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date

        criteria = models.Q(
            cancel_notification=False
        ) & (
            models.Q(order_type="转运") | models.Q(order_type="转运组合")
        ) & models.Q(
            vessel_id__vessel_etd__gte=start_date,
            vessel_id__vessel_etd__lte=end_date
        )
        if warehouse:
            criteria &= models.Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        invoice_type = request.POST.get("invoice_type") or "receivable"
        status_field = f"{invoice_type}_status"
        # 查找待录入账单（未操作过的）
        order = Order.objects.select_related(
            "invoice_id",
            "customer_name",
            "container_number",
            "invoice_id__statement_id",
        ).filter(
            criteria,
            models.Q(**{f"{status_field}__isnull": True})
            | models.Q(  # 考虑账单编辑点的是暂存的情况
                **{
                    f"{invoice_type}_status__invoice_type": invoice_type,
                    f"{invoice_type}_status__stage": "unstarted",
                }
            ),
        )
        # 查找驳回账单
        order_reject = Order.objects.filter(
            criteria,
            **{
                f"{invoice_type}_status__invoice_type": invoice_type,
                f"{invoice_type}_status__is_rejected": True,
                f"{invoice_type}_status__stage": "preport",
            },
        )

        # 查找待审核账单，给港前组长看
        order_pending = Order.objects.select_related(
            "invoice_id",
            "customer_name",
            "container_number",
            "invoice_id__statement_id",
        ).filter(
            criteria,
            **{
                f"{invoice_type}_status__invoice_type": invoice_type,
                f"{invoice_type}_status__is_rejected": False,
                f"{invoice_type}_status__stage": "preport",
            },
        )
        # 查找已录入账单
        previous_order = (
            Order.objects.select_related(
                "invoice_id",
                "customer_name",
                "container_number",
                "invoice_id__statement_id",
                f"{invoice_type}_status",
            )
            .values(
                "invoice_status",
                "container_number__container_number",
                "customer_name__zem_name",
                "created_at",
                f"{invoice_type}_status",
            )
            .filter(
                criteria,
                **{
                    f"{invoice_type}_status__isnull": False,
                    f"{invoice_type}_status__invoice_type": invoice_type,
                },
            )
            .exclude(**{f"{invoice_type}_status__stage__in": ["preport", "unstarted"]})
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
        context = {
            "order": order,
            "order_form": OrderForm(),
            "order_reject": order_reject,
            "order_pending": order_pending,
            "previous_order": previous_order,
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "groups": groups,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "invoice_type_filter": invoice_type,
        }
        return self.template_invoice_preport, context

    def handle_invoice_warehouse_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        # 库内操作费
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        criteria = models.Q(
            (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date),
        )
        if warehouse:
            criteria &= models.Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        invoice_type = request.POST.get("invoice_type") or "receivable"

        groups = [group.name for group in request.user.groups.all()]
        #表示是否可以看见公仓和私仓
        display_mix = False
        if "NJ_mix_account" in groups:
            if warehouse == "NJ-07001":
                display_mix = True
        if "mix_account" in groups:
            display_mix = True

        delivery_type_filter = None
        if display_mix: #这个权限的，NJ公仓私仓都能看见
            delivery_type_filter = models.Q()
        else:
            if "warehouse_public" in groups and "warehouse_other" not in groups:
                delivery_type_filter = models.Q(
                    container_number__delivery_type__in=["public", "mixed"]
                )
            elif "warehouse_other" in groups and "warehouse_public" not in groups:
                delivery_type_filter = models.Q(
                    container_number__delivery_type__in=["other", "mixed"]
                )
            else:
                raise ValueError('没有权限')
        # 基础查询
        base_query = Order.objects.select_related(
            "invoice_id",
            f"{invoice_type}_status",
            "customer_name",
            "container_number",
            "invoice_id__statement_id",
        ).filter(criteria)

        if delivery_type_filter:
            base_query = base_query.filter(delivery_type_filter)

        # 查找未操作过的
        if display_mix:
            order = base_query.filter(
                **{f"{invoice_type}_status__stage": "warehouse"},
            ).order_by(f"{invoice_type}_status__reject_reason")
        else:
            if "warehouse_public" in groups and "warehouse_other" not in groups:
                # 如果是公仓人员
                order = base_query.filter(
                    **{f"{invoice_type}_status__stage": "warehouse"},
                ).filter(
                    models.Q(**{f"{invoice_type}_status__stage_public": "pending"}) |
                    models.Q(**{f"{invoice_type}_status__stage_public": "warehouse_rejected"})
                ).order_by(f"{invoice_type}_status__reject_reason")
            elif "warehouse_other" in groups and "warehouse_public" not in groups:
                # 如果是私仓人员
                order = base_query.filter(
                    **{f"{invoice_type}_status__stage": "warehouse"},
                ).filter(
                    models.Q(**{f"{invoice_type}_status__stage_other": "pending"}) |
                    models.Q(**{f"{invoice_type}_status__stage_other": "warehouse_rejected"})
                ).order_by(f"{invoice_type}_status__reject_reason")
        order = self.process_orders_display_status(order, invoice_type)

        # 查找历史操作过的，状态是warehouse时，对应group的stage为completed，或者状态是库内之后的
        base_condition = ~models.Q(
            **{f"{invoice_type}_status__stage__in": ["unstarted", "preport"]}
        )
        other_stages = models.Q(
            **{
                f"{invoice_type}_status__stage__in": [
                    "delivery",
                    "tobeconfirmed",
                    "confirmed",
                ]
            }
        )
        
        if display_mix:
            warehouse_condition = models.Q(
                **{f"{invoice_type}_status__stage": "delivery"}
            )
        else:
            if "warehouse_public" in groups and "warehouse_other" not in groups:
                warehouse_condition = models.Q(
                    **{f"{invoice_type}_status__stage_public": "warehouse_completed"}
                )
            elif "warehouse_other" in groups and "warehouse_public" not in groups:
                warehouse_condition = models.Q(
                    **{f"{invoice_type}_status__stage_other": "warehouse_completed"}
                )
        previous_order = base_query.filter(
            base_condition,
            warehouse_condition | other_stages,  # 满足仓库条件或其他阶段
            **{f"{invoice_type}_status__invoice_type": invoice_type},
        ).select_related(
            "customer_name", "container_number", "receivable_status", "payable_status"
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        groups = [group.name for group in request.user.groups.all()]
        context = {
            "order": order,
            "order_form": OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "previous_order": previous_order,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "invoice_type_filter": invoice_type,
            "groups": groups,
            "display_mix":display_mix
        }
        return self.template_invoice_warehouse, context

    def handle_invoice_confirm_get(
        self,
        request: HttpRequest,
        start_date_confirm: str = None,
        end_date_confirm: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        start_date_confirm = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date_confirm
            else start_date_confirm
        )
        end_date_confirm = (
            current_date.strftime("%Y-%m-%d")
            if not end_date_confirm
            else end_date_confirm
        )
        criteria = models.Q(
            models.Q(vessel_id__vessel_etd__gte=start_date_confirm),
            models.Q(vessel_id__vessel_etd__lte=end_date_confirm),
        )
        if warehouse:
            criteria &= models.Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)

        # 客服录入完毕的账单
        invoice_type = request.POST.get("invoice_type") or "receivable"
        order = Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id"
        ).filter(criteria, **{f"{invoice_type}_status__stage": "tobeconfirmed"})

        previous_order = (
            Order.objects.select_related(
                "customer_name", "container_number", "retrieval_id"
            )
            .values(
                "container_number__container_number",
                "customer_name__zem_name",
                "created_at",
                "invoice_id__invoice_date",
                "order_type",
                f"invoice_id__{invoice_type}_total_amount",
                f"invoice_id__{invoice_type}_preport_amount",
                f"invoice_id__{invoice_type}_warehouse_amount",
                f"invoice_id__{invoice_type}_delivery_amount",
                f"invoice_id__{invoice_type}_direct_amount",
                "invoice_id__invoice_number",
                "invoice_id__invoice_link",
                "invoice_id__statement_id__invoice_statement_id",
                "invoice_id__statement_id__statement_link",
                "invoice_id__is_invoice_delivered",
            )
            .filter(criteria, **{f"{invoice_type}_status__stage": "confirmed"})
        )

        # 已确认的账单
        previous_order = previous_order.annotate(
            total_amount=Case(
                When(
                    order_type="转运",
                    then=F(f"invoice_id__{invoice_type}_preport_amount")
                    + F(f"invoice_id__{invoice_type}_warehouse_amount")
                    + F(f"invoice_id__{invoice_type}_delivery_amount"),
                ),
                When(
                    order_type="转运组合",
                    then=F(f"invoice_id__{invoice_type}_preport_amount")
                    + F(f"invoice_id__{invoice_type}_warehouse_amount")
                    + F(f"invoice_id__{invoice_type}_delivery_amount"),
                ),
                When(
                    order_type="直送",
                    then=F(f"invoice_id__{invoice_type}_direct_amount"),
                ),
                default=Value(0),
                output_field=IntegerField(),
            )
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        context = {
            "order": order,
            "previous_order": previous_order,
            "order_form": OrderForm(),
            "start_date_confirm": start_date_confirm,
            "end_date_confirm": end_date_confirm,
            "customer": customer,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "invoice_type_filter": invoice_type,
        }
        return self.template_invoice_confirm, context

    def handle_invoice_delivery_get(
        self,
        request: HttpRequest,
        start_date: str = None,
        end_date: str = None,
        customer: str = None,
        warehouse: str = None,
    ) -> tuple[Any, Any]:
        # 库内操作费
        current_date = datetime.now().date()
        start_date = request.POST.get("start_date") or (
            current_date + timedelta(days=-90)
        ).strftime("%Y-%m-%d")
        end_date = request.POST.get("end_date") or current_date.strftime("%Y-%m-%d")

        criteria = models.Q(
            (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
            models.Q(vessel_id__vessel_etd__gte=start_date),
            models.Q(vessel_id__vessel_etd__lte=end_date),
            models.Q(offload_id__offload_at__isnull=False)
        )
        if warehouse is None:
            warehouse = request.POST.get("warehouse")
        if warehouse and warehouse!= "None":
            criteria &= models.Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= models.Q(customer_name__zem_name=customer)
        invoice_type = request.POST.get("invoice_type") or "receivable"

        groups = [group.name for group in request.user.groups.all()]
        delivery_type_filter = None
        display_mix = False
        if "mix_account" in groups:
            display_mix = True
        
        if display_mix:
            delivery_type_filter = models.Q()
            delivery_type = 'mix'
        elif ("warehouse_other" in groups and "warehouse_public" not in groups) or ("NJ_mix_account" in groups):
            delivery_type_filter = models.Q(
                container_number__delivery_type__in=["other", "mixed"]
            )
            delivery_type = 'other'
        elif "warehouse_public" in groups and "warehouse_other" not in groups:
            delivery_type_filter = models.Q(
                container_number__delivery_type__in=["public", "mixed"]
            )
            delivery_type = 'public'     

        # 基础查询
        base_query = Order.objects.select_related(
            "invoice_id",
            f"{invoice_type}_status",
            "customer_name",
            "container_number",
            "invoice_id__statement_id",
        ).filter(criteria)

        if delivery_type_filter:
            base_query = base_query.filter(delivery_type_filter)
        #判断是否有暂扣的板子
        hold_subquery = Pallet.objects.filter(
            container_number=OuterRef('container_number'),  # 关联到Order的container
            delivery_method__contains="暂扣留仓"
        )
        if delivery_type != 'mix':
            hold_subquery = hold_subquery.filter(delivery_type=delivery_type)

         # 查找未操作过的
        if "NJ_mix_account" in groups or ("warehouse_other" in groups and "warehouse_public" not in groups):  #这个权限的，要看NJ的私仓
             order = base_query.filter(
                    models.Q(
                        **{f"{invoice_type}_status__stage_other": "warehouse_completed"}
                    )
                    | models.Q(
                        **{f"{invoice_type}_status__stage_other": "delivery_rejected"}
                    )
                ).annotate(
                    is_priority=Case(
                        When(**{f"{invoice_type}_status__stage_other": "delivery_rejected"}, then=Value(0)),
                        When(**{f"{invoice_type}_status__stage_other": "warehouse_completed"}, then=Value(1)),
                        output_field=IntegerField(),
                    ),
                    is_hold=Exists(hold_subquery)  #表示柜子是否有暂扣的 
                ).order_by('is_priority')
        else:        
            if display_mix:   #这个权限的，都能看
                order = base_query.filter(
                    models.Q(**{f"{invoice_type}_status__stage": "delivery"})
                ).annotate(
                    is_priority=Case(
                        When(
                            models.Q(**{f"{invoice_type}_status__stage_other": "delivery_rejected"}) |
                            models.Q(**{f"{invoice_type}_status__stage_public": "delivery_rejected"}),
                            then=Value(0)
                        ),
                        default=Value(1),
                        output_field=IntegerField(),
                    ),
                    is_hold=Exists(hold_subquery)  
                ).order_by('is_priority')
            elif "warehouse_public" in groups and "warehouse_other" not in groups:
                # 只看公仓人员
                order = base_query.filter(
                    models.Q(
                        **{f"{invoice_type}_status__stage_public": "warehouse_completed"}
                    )
                    | models.Q(
                        **{f"{invoice_type}_status__stage_public": "delivery_rejected"}
                    )
                ).annotate(
                    is_priority=Case(
                        When(**{f"{invoice_type}_status__stage_public": "delivery_rejected"}, then=Value(0)),
                        When(**{f"{invoice_type}_status__stage_public": "warehouse_completed"}, then=Value(1)),
                        output_field=IntegerField(),
                    ),
                    is_hold=Exists(hold_subquery)    
                ).order_by('is_priority')
        order = self.process_orders_display_status(
            order, invoice_type
        )       

        # 查找历史操作过的
        base_condition = ~models.Q(
            **{f"{invoice_type}_status__stage__in": ["unstarted", "preport"]}
        )
        other_stages = models.Q(
            **{f"{invoice_type}_status__stage__in": ["tobeconfirmed", "confirmed"]}
        )
        delivery_completed_condition = models.Q()
        if "NJ_mix_account" in groups:
            delivery_completed_condition = models.Q(
                **{f"{invoice_type}_status__stage_other": "delivery_completed"}
            )
        else:
            if display_mix:
                delivery_completed_condition = models.Q()
            elif "warehouse_public" in groups and "warehouse_other" not in groups:
                delivery_completed_condition = models.Q(
                    **{f"{invoice_type}_status__stage_public": "delivery_completed"}
                )
            elif "warehouse_other" in groups and "warehouse_public" not in groups:
                delivery_completed_condition = models.Q(
                    **{f"{invoice_type}_status__stage_other": "delivery_completed"}
                )
        previous_order = base_query.filter(
            base_condition,
            other_stages | delivery_completed_condition,  # 满足任意一个条件即可
            **{f"{invoice_type}_status__invoice_type": invoice_type},
        ).select_related(
            "customer_name", "container_number", "receivable_status", "payable_status"
        )
        previous_order = self.process_orders_display_status(
            previous_order, invoice_type
        )
        context = {
            "order": order,
            "previous_order": previous_order,
            "order_form": OrderForm(),
            "start_date": start_date,
            "end_date": end_date,
            "customer": customer,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "invoice_type_filter": invoice_type,
            "warehouse":warehouse
        }
        return self.template_invoice_delivery, context

    def handle_invoice_warehouse_save_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        data = request.POST.copy()
        save_type = request.POST.get("save_type")
        container_number = data.get("container_number")
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        invoice_type = data.get("invoice_type")

        invoice_warehouse = InvoiceWarehouse.objects.filter(
            invoice_number__invoice_number=invoice.invoice_number,
            invoice_type=invoice_type,
        )
        if not invoice_warehouse.exists():
            invoice_content = InvoiceWarehouse(
                **{
                    "invoice_number": invoice,
                }
            )
            invoice_content.save()
        delivery_type = request.POST.get("delivery_type")
        invoice_warehouse = InvoiceWarehouse.objects.get(
            invoice_number__invoice_number=invoice.invoice_number,
            invoice_type=invoice_type,
            delivery_type=delivery_type,
        )
        names = data.getlist("others_feename")[:-1]
        amounts = data.getlist("others_feeamount")[:-1]
        other_fees = dict(zip(names, map(float, amounts)))
        invoice_warehouse.other_fees = {k: v for k, v in other_fees.items() if k}
        exclude_fields = {
            "csrfmiddlewaretoken",
            "step",
            "warehouse",
            "container_number",
            "invoice_number",
            "others_feename",
            "others_feeamount",
        }

        # 附加项费用和附加项说明
        fields = [
            "sorting",
            "intercept",
            "po_activation",
            "self_pickup",
            "re_pallet",
            "counting",
            "warehouse_rent",
            "specified_labeling",
            "inner_outer_box",
            "pallet_label",
            "open_close_box",
            "destroy",
            "take_photo",
            "take_video",
            "repeated_operation_fee",
            "per_diem",
            "second_pickup",
        ]

        # 初始化单价和数量字典
        qty_data = {}
        rate_data = {}
        s_fields = fields
        for field in s_fields:
            # 保存数量
            quantity_key = f"{field}_quantity"
            if quantity_key in data:
                qty_data[field] = float(data.get(quantity_key, 0)) if data.get(quantity_key) is not None else 0

            # 保存单价
            price_key = f"{field}_price"
            if price_key in data:
                rate_data[field] = float(data.get(price_key, 1)) if data.get(price_key) is not None else 1

            # 保存原有字段
            if field in data and field not in exclude_fields and data[field]:
                setattr(invoice_warehouse, field, data[field])
        setattr(invoice_warehouse, 'amount', data['amount'])
        # 保存单价和数量
        invoice_warehouse.qty = qty_data
        invoice_warehouse.rate = rate_data
        surcharges = {}
        surcharge_notes = {}
        for field in fields:
            
            surcharge_key = f"{field}_surcharge"
            note_key = f"{field}_surcharge_note"
            surcharge = data.get(surcharge_key, 0) or 0
            note = data.get(note_key, "")
            surcharges[field] = float(surcharge)
            surcharge_notes[field] = note
        invoice_warehouse.surcharges = surcharges
        invoice_warehouse.surcharge_notes = surcharge_notes
        invoice_warehouse.save()

        # 因为现在分公仓私仓两条记录，所以汇总的时候，要从数据库查一遍
        if save_type == "complete":
            invoice = Invoice.objects.select_related("container_number").get(
                container_number__container_number=container_number,
            )
            warehouse_amount = (
                InvoiceWarehouse.objects.filter(
                    invoice_number=invoice, invoice_type=invoice_type
                ).aggregate(total_amount=Sum("amount"))["total_amount"]
                or 0
            )
            if invoice_type == "receivable":
                invoice.receivable_warehouse_amount = warehouse_amount
            elif invoice_type == "payable":
                invoice.payable_warehouse_amount = warehouse_amount
            invoice.save()

        order = Order.objects.select_related("retrieval_id", "container_number").get(
            container_number__container_number=container_number
        )

        if save_type == "complete":
            # 开始准备改变状态，先找到状态表
            if invoice_type == "receivable":
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="receivable"
                )
            elif invoice_type == "payable":
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="payable"
                )
            container_delivery_type = invoice_status.container_number.delivery_type
            if container_delivery_type in ["public", "other"]:
                # 如果这个柜子只有一类仓，就直接改变状态
                invoice_status.stage = "delivery"
                invoice_status.is_rejected = False
                invoice_status.reject_reason = ""
                invoice_status.stage_public = "warehouse_completed"
                invoice_status.stage_other = "warehouse_completed"
            elif container_delivery_type == "mixed":
                if delivery_type == "public":
                    # 公仓组录完了，改变stage_public
                    if invoice_status.stage_public not in ["delivery_completed","delivery_rejected"]:
                        invoice_status.stage_public = "warehouse_completed"
                        # 如果私仓也做完了，就改变主状态到派送阶段
                        if invoice_status.stage_other not in ["pending","warehouse_rejected"]:
                            invoice_status.stage = "delivery"
                elif delivery_type == "other":
                    # 私仓租录完了，改变stage_other
                    if invoice_status.stage_other not in ["delivery_completed","delivery_rejected"]:
                        invoice_status.stage_other = "warehouse_completed"
                        # 如果公仓也做完了，就改变主状态
                        if invoice_status.stage_public not in ["pending","warehouse_rejected"]:
                            invoice_status.stage = "delivery"
                else:
                    raise ValueError("没有派送类别")
                # 既有公仓权限，又有私仓权限的不知道咋处理，而且编辑页面也不好搞
                invoice_status.is_rejected = False
                invoice_status.reject_reason = ""
            invoice_status.save()
        elif save_type == "account_comlete":
            modified_get = request.GET.copy()
            modified_get["start_date_confirm"] = request.POST.get("start_date_confirm")
            modified_get["end_date_confirm"] = request.POST.get("end_date_confirm")
            new_request = request
            new_request.GET = modified_get
            return self.handle_container_invoice_confirm_get(new_request)
        
        order_form = OrderForm(request.POST)
        if order_form.is_valid():
            customer = order_form.cleaned_data.get("customer_name")
        else:
            customer = None
        return self.handle_invoice_warehouse_get(
            request, request.POST.get("start_date"), request.POST.get("end_date"),customer,request.POST.get("warehouse")
        )

    def handle_invoice_direct_save_post(self, request: HttpRequest) -> tuple[Any, Any]:
        data = request.POST.copy()
        save_type = request.POST.get("save_type")
        container_number = data.get("container_number")
        direct_amount = request.POST.get("amount")
        invoice_type = request.POST.get("invoice_type")
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number,
        )
        order = Order.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        try:
            invoice_preports = InvoicePreport.objects.get(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
            )
        except InvoicePreport.DoesNotExist:
            # 获取直送柜子的提拆柜费用
            # 如果之前没有录过费用，就根据报价表生成提+派送费用
            invoice_preports = InvoicePreport(
                **{"invoice_number": invoice, "invoice_type": invoice_type}
            )
            invoice_preports.save()

        names = data.getlist("others_feename")[:-1]
        amounts = data.getlist("others_feeamount")[:-1]
        other_fees = dict(zip(names, map(float, amounts)))
        invoice_preports.other_fees = {k: v for k, v in other_fees.items() if k}
        exclude_fields = {
            "csrfmiddlewaretoken",
            "step",
            "warehouse",
            "container_number",
            "invoice_number",
            "others_feename",
            "others_feeamount",
        }
        # 附加项费用和附加项说明
        fields = [
            "exam_fee",
            "second_pickup",
            "demurrage",
            "per_diem",
            "congestion_fee",
            "chassis",
            "prepull",
            "yard_storage",
            "handling_fee",
            "chassis_split",
            "over_weight",
        ]
        # 初始化单价和数量字典
        qty_data = {}
        rate_data = {}
        s_fields = ["pickup"] + ["amount"] + fields
        for field in s_fields:
            # 保存单价
            quantity_key = f"{field}_quantity"
            if quantity_key in data:
                qty_data[field] = float(data.get(quantity_key, 0)) or 0
            # 保存数量
            price_key = f"{field}_price"
            if price_key in data:
                rate_data[field] = float(data.get(price_key, 1)) or 1
            # 保存原有字段
            if field in data and field not in exclude_fields and data[field]:
                setattr(invoice_preports, field, data[field])
        # 保存单价和数量
        invoice_preports.qty = qty_data
        invoice_preports.rate = rate_data

        surcharges = {}
        surcharge_notes = {}
        for field in fields:
            surcharge_key = f"{field}_surcharge"
            note_key = f"{field}_surcharge_note"
            surcharge = request.POST.get(surcharge_key, 0) or 0
            note = request.POST.get(note_key, "")
            surcharges[field] = float(surcharge)
            surcharge_notes[field] = note
        invoice_preports.surcharges = surcharges
        invoice_preports.surcharge_notes = surcharge_notes
        invoice_preports.save()

        if save_type == "complete":  # 如果是普通账户确认，订单转为待财务确认状态
            # 更新invoice表和状态表
            if invoice_type == "receivable":
                invoice.receivable_direct_amount = direct_amount
                invoice_status, created = InvoiceStatus.objects.get_or_create(
                    container_number=order.container_number, invoice_type="receivable"
                )
            elif invoice_type == "payable":
                invoice.payable_direct_amount = direct_amount
                invoice_status, created = InvoiceStatus.objects.get_or_create(
                    container_number=order.container_number, invoice_type="payable"
                )
            invoice.save()
            invoice_status.stage = "tobeconfirmed"
            invoice_status.is_rejected = "False"
            invoice_status.reject_reason = ""
            invoice_status.save()
        elif save_type == "account_complete":
            # 如果是财务确认，订单转为已确认状态
            invoice = Invoice.objects.select_related("container_number").get(
                container_number__container_number=container_number
            )
            if invoice_type == "receivable":
                invoice.receivable_direct_amount = direct_amount
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="receivable"
                )
            elif invoice_type == "payable":
                invoice.payable_direct_amount = direct_amount
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="payable"
                )
            invoice.save()
            invoice_status.stage = "confirmed"
            invoice_status.is_rejected = "False"
            invoice_status.reject_reason = ""
            invoice_status.save()
        elif save_type == "reject":
            # 如果是财务拒绝，退回到未编辑状态，并记录驳回原因和驳回状态
            if invoice_type == "receivable":
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="receivable"
                )
            elif invoice_type == "payable":
                invoice_status = InvoiceStatus.objects.get(
                    container_number=order.container_number, invoice_type="payable"
                )
            invoice_status.stage = "unstarted"
            invoice_status.is_rejected = "True"
            invoice_status.reject_reason = data.get("invoice_reject_reason", "")
            invoice_status.save()

        if save_type in [
            "account_complete",
            "reject",
        ]:
            # 如果是从账单确认那里点进来的操作，就跳转回账单确认界面
            return self.handle_invoice_confirm_save(request)
        else:
            return self.handle_invoice_direct_get(
                request, request.POST.get("start_date"), request.POST.get("end_date")
            )

    def handle_invoice_preport_save_post(self, request: HttpRequest) -> tuple[Any, Any]:
        data = request.POST.copy()
        save_type = request.POST.get("save_type")
        container_number = data.get("container_number")
        invoice_type = data.get("invoice_type")
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        preport_amount = data["amount"]
        # 提拆柜表费用记录
        invoice_preports = InvoicePreport.objects.get(
            invoice_number__invoice_number=invoice.invoice_number,
            invoice_type=invoice_type,
        )
        names = [name for name in data.getlist("others_feename") if name.strip()]
        amounts = [
            amount for amount in data.getlist("others_feeamount") if amount.strip()
        ]
        other_fees = dict(zip(names, map(float, amounts)))
        invoice_preports.other_fees = {k: v for k, v in other_fees.items() if k}
        exclude_fields = {
            "csrfmiddlewaretoken",
            "step",
            "warehouse",
            "container_number",
            "invoice_number",
            "pending",
            "others_feename",
            "others_feeamount",
        }
        # 附加项费用和附加项说明,qty多了一个pickup
        fields = [
            "chassis",
            "chassis_split",
            "prepull",
            "yard_storage",
            "handling_fee",
            "pier_pass",
            "congestion_fee",
            "hanging_crane",
            "dry_run",
            "exam_fee",
            "hazmat",
            "over_weight",
            "urgent_fee",
            "other_serive",
            "demurrage",
            "per_diem",
            "second_pickup",
        ]

        # 初始化单价和数量字典
        qty_data = {}
        rate_data = {}
        s_fields = ["pickup"] + ["amount"] + fields
        for field in s_fields:
            # 保存单价
            quantity_key = f"{field}_quantity"
            if quantity_key in data:
                qty_data[field] = float(data.get(quantity_key, 0)) or 0

            # 保存数量
            price_key = f"{field}_price"
            
            
            if price_key in data:
                rate_data[field] = float(data.get(price_key, 1)) or 1

            # 保存原有字段
            if field in data and field not in exclude_fields and data[field]:
                setattr(invoice_preports, field, data[field])
        # 保存单价和数量
        invoice_preports.qty = qty_data
        invoice_preports.rate = rate_data

        surcharges = {}
        surcharge_notes = {}
        for field in fields:
            surcharge_key = f"{field}_surcharge"
            note_key = f"{field}_surcharge_note"

            surcharge = request.POST.get(surcharge_key, 0) or 0
            note = request.POST.get(note_key, "")
            surcharges[field] = float(surcharge)
            surcharge_notes[field] = note
        invoice_preports.surcharges = surcharges
        invoice_preports.surcharge_notes = surcharge_notes
        invoice_preports.save()
        invoice_preports = InvoicePreport.objects.get(
            invoice_number__invoice_number=invoice.invoice_number,
            invoice_type=invoice_type,
        )
        # 只要更新了港前拆柜数据，就要计算一次总数，更新invoice的preport
        invoice = Invoice.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        if invoice_type == "receivable":
            invoice.receivable_preport_amount = preport_amount
        elif invoice_type == "payable":
            invoice.payable_preport_amount = preport_amount
        invoice.save()
        # 账单状态记录
        order = Order.objects.select_related("retrieval_id", "container_number").get(
            container_number__container_number=container_number
        )

        if invoice_type == "receivable":
            invoice_status, created = InvoiceStatus.objects.get_or_create(
                container_number=order.container_number, invoice_type="receivable"
            )
        elif invoice_type == "payable":
            invoice_status, created = InvoiceStatus.objects.get_or_create(
                container_number=order.container_number, invoice_type="payable"
            )
        if data.get("pending") == "True":
            # 审核通过，进入库内账单录入
            invoice_status.stage = "warehouse"
            invoice_status.is_rejected = "False"
            invoice_status.reject_reason = ""

        elif data.get("pending") == "False":
            # 审核失败，驳回账单
            invoice_status.is_rejected = "True"
            invoice_status.reject_reason = data.get("invoice_reject_reason", "")
        else:
            # 提拆柜录入完毕,如果是complete表示客服录入完成，订单状态进入下一步，否则不改状态
            invoice_status.is_rejected = "False"
            if save_type == "complete":
                invoice_status.stage = "preport"
                invoice_status.is_rejected = "False"
            elif (
                save_type == "account_complete"
            ):  # 如果是财务从确认界面跳转过来的，就要return回账单确认界面
                modified_get = request.GET.copy()
                modified_get["start_date_confirm"] = request.POST.get(
                    "start_date_confirm"
                )
                modified_get["end_date_confirm"] = request.POST.get("end_date_confirm")
                new_request = request
                new_request.GET = modified_get
                return self.handle_container_invoice_confirm_get(new_request)

        order.save()
        invoice_status.save()
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")

        return self.handle_invoice_preport_get(
            request, request.POST.get("start_date"), request.POST.get("end_date")
        )

    def handle_invoice_delivery_type_save(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        invoice_type = request.POST.get("invoice_type")
        invoice = Invoice.objects.get(
            container_number__container_number=container_number
        )
        selections = request.POST.getlist("is_type_added")
        plt_ids = request.POST.getlist("added_plt_ids")
        plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
        alter_type = request.POST.get("alter_type")
        
        #如果是改公仓/私仓，是一个方法，否则是旧方法
        if alter_type == "transferDes":
            #更改公仓/私仓类别
            plt_delivery_type = request.POST.getlist("plt_delivery_type")
            plt_delivery_type = [des for s, des in zip(selections, plt_delivery_type) if s == "on"]
            for i in range(len((plt_ids))):
                ids = plt_ids[i].split(",")
                ids = [int(id) for id in ids]
                pallet = Pallet.objects.filter(id__in=ids)
                updated_pallets = []
                for plt in pallet:
                    plt.delivery_type = plt_delivery_type[i]
                    updated_pallets.append(plt)
                bulk_update_with_history(
                    updated_pallets, Pallet, fields=["delivery_type"]
                )

        else:           
            total_cbm = request.POST.getlist("cbm")
            total_cbm = [cbm for s, cbm in zip(selections, total_cbm) if s == "on"]
            total_weight_lbs = request.POST.getlist("weight_lbs")
            total_weight_lbs = [
                weight for s, weight in zip(selections, total_weight_lbs) if s == "on"
            ]
            destination = request.POST.getlist("destination")
            destination = [des for s, des in zip(selections, destination) if s == "on"]
            #更改amazon/walmart等类别
            delivery_type = request.POST.getlist("delivery_type")
            delivery_type = [des for s, des in zip(selections, delivery_type) if s == "on"]
            zipcode = request.POST.getlist("zipcode")
            zipcode = [code for s, code in zip(selections, zipcode) if s == "on"]
            total_pallet = request.POST.getlist("total_pallet")
            total_pallet = [n for s, n in zip(selections, total_pallet) if s == "on"]
            
            # 将前端的每一条记录存为invoice_delivery的一条
            for i in range(len((plt_ids))):
                ids = plt_ids[i].split(",")
                ids = [int(id) for id in ids]
                pallet = Pallet.objects.filter(id__in=ids)
                current_date = datetime.now().date()
                invoice_delivery = f"{current_date.strftime('%Y-%m-%d').replace('-', '')}-{alter_type}-{destination[i]}-{len(pallet)}"
                invoice_content = InvoiceDelivery(
                    **{
                        "invoice_delivery": invoice_delivery,
                        "invoice_number": invoice,
                        "invoice_type": invoice_type,
                        "delivery_type": delivery_type[i],
                        "type": alter_type,  # 沃尔玛/亚马逊等
                        "zipcode": zipcode[i],
                        "destination": destination[i],
                        "total_pallet": total_pallet[i],
                        "total_cbm": total_cbm[i],
                        "total_weight_lbs": total_weight_lbs[i],
                    }
                )
                invoice_content.save()
                #找单价
                order = Order.objects.select_related("retrieval_id", "container_number","vessel_id").get(
                    container_number__container_number=container_number
                )
                container_type = order.container_number.container_type
                warehouse = order.retrieval_id.retrieval_destination_area
                vessel_etd = order.vessel_id.vessel_etd
                cutoff_date = date(2025, 4, 1)
                is_new_rule = vessel_etd >= cutoff_date

                fee_details = self._get_fee_details(warehouse,vessel_etd)
                self._calculate_and_set_delivery_cost(
                    invoice_content,
                    container_type,
                    fee_details,
                    warehouse,
                    is_new_rule
                )
                if invoice_content.cost is not None:
                    if invoice_content.type != 'combine':
                        invoice_content.total_cost = float(invoice_content.cost) * float(invoice_content.total_pallet)
                    elif invoice_content.type == 'combine':  #组合柜总价=单价
                        invoice_content.total_cost = float(invoice_content.cost)
                    else:
                        invoice_content.total_cost = 0
                invoice_content.save()

                updated_pallets = []
                for plt in pallet:
                    try:
                        invoice_delivery = plt.invoice_delivery
                        if invoice_delivery and hasattr(invoice_delivery, "delete"):
                            invoice_delivery.delete()
                    except InvoiceDelivery.DoesNotExist:
                        pass
                    # pallet指向InvoiceDelivery表
                    plt.invoice_delivery = invoice_content
                    updated_pallets.append(plt)
                bulk_update_with_history(
                    updated_pallets, Pallet, fields=["invoice_delivery"]
                )

        return self.handle_container_invoice_delivery_get(request)

    def handle_invoice_confirm_save(self, request: HttpRequest) -> tuple[Any, Any]:
        save_type = request.POST.get("save_type", None)
        if save_type == "reject":
            return self.handle_invoice_confirm_get(request)
        invoice_type = request.POST.get("invoice_type")
        container_number = request.POST.get("container_number")
        order = Order.objects.select_related("retrieval_id", "container_number").get(
            container_number__container_number=container_number
        )
        invoice = Invoice.objects.get(
            container_number__container_number=container_number
        )
        # 更新状态
        invoice_type = request.POST.get("invoice_type")
        if invoice_type == "receivable":
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type="receivable"
            )
        elif invoice_type == "payable":
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type="payable"
            )
        invoice_status.stage = "confirmed"
        invoice_status.save()

        context = self._parse_invoice_excel_data(order, invoice, invoice_type)
        workbook, invoice_data = self._generate_invoice_excel(context)
        invoice.invoice_date = invoice_data["invoice_date"]
        if invoice_type == "receivable":
            invoice.invoice_link = invoice_data["invoice_link"]
            if order.order_type =="直送":
                invoice.receivable_total_amount = float(invoice.receivable_direct_amount or 0)
            else:
                invoice.receivable_total_amount = (
                    float(invoice.receivable_preport_amount or 0)
                    + float(invoice.receivable_warehouse_amount or 0)
                    + float(invoice.receivable_delivery_amount or 0)
                )
        elif invoice_type == "payable":
            if order.order_type =="直送":
                invoice.payable_total_amount = float(invoice.payable_direct_amount or 0)
            else:
                invoice.payable_total_amount = (
                    float(invoice.payable_preport_amount or 0)
                    + float(invoice.payable_warehouse_amount or 0)
                    + float(invoice.payable_delivery_amount or 0)
                )
        else:
            raise ValueError(f"Unknown invoice_type: {invoice_type}")
        invoice.save()
        if invoice_type == "receivable":
            receivable_status = order.receivable_status
            receivable_status.stage = "confirmed"
            receivable_status.save()
        else:
            payable_status = order.payable_status
            payable_status.stage = "confirmed"
            payable_status.save() 
        order.save()
        return self.handle_invoice_confirm_get(request)

    def handle_invoice_confirm_combina_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get('container_number')
        invoice_number = request.POST.get("invoice_number")
        invoice = Invoice.objects.get(invoice_number=invoice_number)
        invoice_type = request.POST.get("invoice_type")
        total_fee = float(request.POST.get('totalAmountDisplay',0))
        base_fee = float(request.POST.get('base_fee', 0))
        overweight_fee = float(request.POST.get('overweight_fee', 0))
        overpallet_fee = float(request.POST.get('overpallet_fee', 0))
        overregion_pickup_fee = float(request.POST.get('overregion_pickup_fee', 0))
        overregion_delivery_fee = float(request.POST.get('overregion_delivery_fee', 0))    

        plts_by_destination = Pallet.objects.filter(
            container_number__container_number=container_number   
        ).values('destination').annotate(total_cbm=Sum('cbm'),total_weight=Sum('weight_lbs'))
        invoice_item_data = []

        combina_data_des_key = request.POST.getlist("combina_data_des_key")
        combina_data_des_cbm = request.POST.getlist("combina_data_des_cbm")
        combina_data_des_price = request.POST.getlist("combina_data_des_price")
        combina_data_des_location = request.POST.getlist("combina_data_des_location")
        combina_data_des_rate = request.POST.getlist("combina_data_des_rate")
        base_location=[]
        for i in range(len(combina_data_des_key)):
            location = combina_data_des_location[i].split(',')
            for num in range(len(location)):
                base_location.append(location[num])
                if num == 0:
                    qty = float(combina_data_des_rate[i])
                    rate = float(combina_data_des_price[i])
                    amount = qty*rate
                else:
                    qty = rate = amount = 0.00
                for item in plts_by_destination:
                    cleaned_item_dest = item['destination'].strip()
                    cleaned_location = location[num].strip()
                    if cleaned_item_dest == cleaned_location:
                        cbm_d = item['total_cbm']
                        weight_d = item['total_weight']
                        break
                invoice_item_data.append({
                    'invoice_number': invoice,
                    'description': '派送费',
                    'warehouse_code': location[num],
                    'cbm': cbm_d,
                    'weight': weight_d,
                    'qty': qty,
                    'rate': rate,
                    'amount': amount,
                    'note': combina_data_des_key[i]
                })
        if overweight_fee > 0:
            overweight_extra_weight = request.POST.get("overweight_extra_weight")
            invoice_item_data.append({
                'invoice_number': invoice,
                'description': '超重费',
                'warehouse_code': None,
                'cbm': None,
                'weight': overweight_extra_weight,
                'qty': 1.0,
                'rate': overweight_fee,
                'amount': overweight_fee,
                'note': None
            })
        
        if overpallet_fee > 0:
            current_pallets = request.POST.get("current_pallets")
            limit_pallets = request.POST.get("limit_pallets")
            over_count = float(current_pallets) - float(limit_pallets)
            invoice_item_data.append({
                'invoice_number': invoice,
                'description': '超板费',
                'warehouse_code': None,
                'cbm': None,
                'weight': None,
                'qty': over_count,
                'rate': overpallet_fee / over_count if over_count > 0 else 0,
                'amount': overpallet_fee,
                'note': None
            })
        
        if overregion_pickup_fee > 0:
            overregion_pickup_non_combina_cbm = request.POST.get('overregion_pickup_non_combina_cbm')
            overregion_pickup_non_combina_base_fee = request.POST.get('overregion_pickup_non_combina_base_fee')
            invoice_item_data.append({
                'invoice_number': invoice,
                'description': '提拆费',
                'warehouse_code': None,
                'cbm': None,
                'weight': None,
                'qty': overregion_pickup_non_combina_cbm,
                'rate': overregion_pickup_non_combina_base_fee,
                'amount': overregion_pickup_fee,
                'note': None
            })
            
            overregion_delivery_destination = request.POST.getlist("overregion_delivery_destination")
            overregion_delivery_pallets = request.POST.getlist("overregion_delivery_pallets")
            overregion_delivery_cbm = request.POST.getlist("overregion_delivery_cbm")
            overregion_delivery_price = request.POST.getlist("overregion_delivery_price")
            overregion_delivery_subtotal = request.POST.getlist("overregion_delivery_subtotal")
            for i in range(len(overregion_delivery_destination)):
                if overregion_delivery_destination[i] not in base_location:
                    invoice_item_data.append({
                        'invoice_number': invoice,
                        'description': '派送费',
                        'warehouse_code': overregion_delivery_destination[i],
                        'cbm': overregion_delivery_cbm[i],
                        'weight': None,
                        'qty': overregion_delivery_pallets[i],
                        'rate': overregion_delivery_price[i],
                        'amount': overregion_delivery_subtotal[i],
                        'note': None
                    })
    
        invoice_item_instances = [
            InvoiceItem(**inv_itm_data) for inv_itm_data in invoice_item_data
        ]
        bulk_create_with_history(invoice_item_instances, InvoiceItem)

        order = Order.objects.get(container_number__container_number=container_number)
        
        context = self._parse_invoice_excel_data(order, invoice, invoice_type)
        workbook, invoice_data = self._generate_invoice_excel(context)
        invoice.invoice_date = invoice_data["invoice_date"]
        if invoice_type == "receivable":
            invoice.invoice_link = invoice_data["invoice_link"]
            invoice.receivable_total_amount = total_fee
        elif invoice_type == "payable":
            invoice.invoice_link = invoice_data["invoice_link"]
            invoice.payable_total_amount = total_fee
        else:
            raise ValueError(f"Unknown invoice_type: {invoice_type}")
        invoice.save()
        save_type = request.POST.get('save_type')
        if save_type == "complete": #暂存的不改变状态
            if invoice_type == "receivable":
                receivable_status = order.receivable_status
                receivable_status.stage = "confirmed"
                receivable_status.save()
            else:
                payable_status = order.payable_status
                payable_status.stage = "confirmed"
                payable_status.save() 
        order.save()
    
        return self.handle_invoice_combina_get(
                request, request.POST.get("start_date"), request.POST.get("end_date"), request.POST.get("customer"), request.POST.get("warehouse")
            )


    def handle_invoice_dismiss_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        status = request.POST.get("status")
        start_date_confirm = request.POST.get("start_date_confirm")
        end_date_confirm = request.POST.get("end_date_confirm")
        reject_reason = request.POST.get("reject_reason")
        order = Order.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        # 更新状态
        invoice_type = request.POST.get("invoice_type")
        if invoice_type == "receivable":
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type="receivable"
            )
        elif invoice_type == "payable":
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type="payable"
            )
        invoice_status.stage = status
        if status == "warehouse":
            delivery_type = container_number = request.POST.get("delivery_type")
            if delivery_type == "public":
                invoice_status.stage_public = "warehouse_rejected"
            elif delivery_type == "other":
                invoice_status.stage_other = "warehouse_rejected"
        elif status == "delivery":
            invoice_status.stage_public = "delivery_rejected"
            invoice_status.stage_other = "delivery_rejected"
        invoice_status.is_rejected = "True"
        invoice_status.reject_reason = reject_reason
        invoice_status.save()
        return self.handle_invoice_confirm_get(
            request, start_date_confirm, end_date_confirm
        )

    def handle_invoice_redirect_post(self, request: HttpRequest) -> tuple[Any, Any]:
        status = request.POST.get("status")
        if status == "preport":
            return self.handle_container_invoice_preport_get(request)
        elif status == "warehouse":
            delivery_type = request.POST.get("delivery_type")
            return self.handle_container_invoice_warehouse_get(request,delivery_type)
        elif status == "delivery":
            return self.handle_container_invoice_delivery_get(request)
        elif status == "direct":
            return self.handle_container_invoice_direct_get(request)

    def handle_invoice_delivery_save(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        invoice_type = request.POST.get("invoice_type")
        delivery_type = request.POST.get("delivery_type")
        total_cost = request.POST.getlist("total_cost")
        cost = request.POST.getlist("cost")
        total_pallet = request.POST.getlist("total_pallet")
        po_activation = request.POST.getlist("po_activation")
        type_value = request.POST.get("type")
        redirect_step = request.POST.get("redirect_step")
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        # 如果派送方式都填完了，invoice记录派送价格和账单状态
        if type_value == "amount":
            delivery_amount = (
                InvoiceDelivery.objects.filter(
                    invoice_number=invoice,
                    invoice_type=invoice_type,               
                ).aggregate(total_amount=Sum("total_cost"))["total_amount"]
                or 0
            )
            if invoice_type == "receivable":
                invoice.receivable_delivery_amount = delivery_amount
            elif invoice_type == "payable":
                invoice.payable_delivery_amount = delivery_amount
            else:
                raise ValueError("应收应付类型错误")
            invoice.save()

            order = Order.objects.select_related(
                "retrieval_id", "container_number"
            ).get(container_number__container_number=container_number)
            if redirect_step == "False":
                # 如果不是从财务确认界面跳转来的，才需要改变状态
                if invoice_type == "receivable":
                    invoice_status = InvoiceStatus.objects.get(
                        container_number=order.container_number,
                        invoice_type="receivable",
                    )
                elif invoice_type == "payable":
                    invoice_status = InvoiceStatus.objects.get(
                        container_number=order.container_number, invoice_type="payable"
                    )
                else:
                    raise ValueError(f"unknown invoice_type: {invoice_type}")
                container_delivery_type = invoice_status.container_number.delivery_type
                groups = [group.name for group in request.user.groups.all()]
                if "mix_account" in groups and "NJ_mix_account" not in groups:
                    invoice_status.stage_public = "delivery_completed"
                    invoice_status.stage_other = "delivery_completed"
                    invoice_status.stage = "tobeconfirmed"
                    invoice_status.is_rejected = False
                    invoice_status.reject_reason = ""
                else:
                    if container_delivery_type in ["public", "other"]:
                        # 如果这个柜子只有一类仓，就直接改变状态
                        invoice_status.stage = "tobeconfirmed"
                        invoice_status.is_rejected = False
                        invoice_status.reject_reason = ""
                        invoice_status.stage_public = "delivery_completed"
                        invoice_status.stage_other = "delivery_completed"
                    elif container_delivery_type == "mixed":
                        if delivery_type == "public":
                            # 公仓组录完了，改变stage_public
                            invoice_status.stage_public = "delivery_completed"
                            # 如果私仓也做完了，就改变主状态到派送阶段
                            if invoice_status.stage_other == "delivery_completed":
                                invoice_status.stage = "tobeconfirmed"
                        elif delivery_type == "other":
                            # 私仓租录完了，改变stage_other
                            invoice_status.stage_other = "delivery_completed"
                            # 如果公仓也做完了，就改变主状态
                            if invoice_status.stage_public == "delivery_completed":
                                invoice_status.stage = "tobeconfirmed"
                        invoice_status.is_rejected = False
                        invoice_status.reject_reason = ""

                invoice_status.save()
        else:
            # 记录其中一种派送方式到invoice_delivery表
            plt_ids = request.POST.getlist("plt_ids")
            new_plt_ids = [ast.literal_eval(sub_plt_id) for sub_plt_id in plt_ids]
            expense = request.POST.getlist("expense")
            if type_value == "selfdelivery" or type_value == "selfpickup":  #自发的要加备注
                note = request.POST.getlist("note")
            else:
                note = None
            # 将前端的每一条记录存为invoice_delivery的一条
            for i in range(len((new_plt_ids))):
                ids = [int(id) for id in new_plt_ids[i]]
                pallet = Pallet.objects.filter(id__in=ids)
                # 因为每一条记录中所有的板子都是对应一条invoice_delivery，建表的时候就是这样存的，所以取其中一个的外键就可以
                pallet_obj = pallet[0]
                invoice_content = pallet_obj.invoice_delivery
                # 除价格外，其他在新建记录的时候就存了
                if total_cost[i] is None:
                    raise ValueError('总价为空')
                invoice_content.total_cost = total_cost[i]
                invoice_content.cost = cost[i]
                invoice_content.total_pallet = total_pallet[i]
                if expense[i]:
                    invoice_content.expense = expense[i]
                if po_activation[i]:
                    invoice_content.po_activation = po_activation[i]
                if note:
                    if 'None' in note[i]:
                        invoice_content.note = None
                    else:
                        invoice_content.note = note[i]
                invoice_content.save()
        # 如果是财务确认界面跳转的，需要重定向到财务确认界面，并且执行派送界面的账单确认操作
        if redirect_step == "True":
            # 派送界面，一种派送方式点确认后，自动计算总费用
            return self.handle_container_invoice_confirm_get(request)
        else:
            return self.handle_invoice_delivery_get(request)

    def handle_container_invoice_warehouse_get(
        self, request: HttpRequest,delivery_type: str = None,
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        order = Order.objects.select_related("retrieval_id", "container_number").get(
            container_number__container_number=container_number
        )
        warehouse = order.retrieval_id.retrieval_destination_area
        vessel_etd = order.vessel_id.vessel_etd
        quotation = QuotationMaster.objects.filter(
            effective_date__lte=vessel_etd
        ).order_by('-effective_date').first()
        
        if not quotation:
            raise ValueError('找不到报价表')
        WAREHOUSE_FEE = FeeDetail.objects.get(
            quotation_id=quotation.id, fee_type="warehouse"
        )
        invoice_type = request.GET.get("invoice_type")
        # 提拆、打托缠膜费用

        # FS_constrain = {
        #     key: float(re.search(r'\d+(\.\d+)?', value).group())
        #     for key, value in WAREHOUSE_FEE.details.items()
        #     if '/' in str(value) and re.search(r'\d+(\.\d+)?', value)
        # }
        FS_constrain = {}
        for (
            key,
            value,
        ) in (
            WAREHOUSE_FEE.details.items()
        ):  # 把details里面的键值对改成值是纯数字的，用于在费用表单提交前，验证数据合规性
            if not isinstance(value, dict):
                match = re.findall(r"\$(\d+(\.\d+)?)", str(value))
                if match and len(match) == 1:
                    if "（" in str(key):
                        key = key.split("（")[0]
                    FS_constrain[key] = float(match[0][0])
        fs_json = json.dumps(FS_constrain, ensure_ascii=False)
        # 其他费用
        FS = {
            "sorting": f"{WAREHOUSE_FEE.details.get('分拣费', 'N/A')}",  # 分拣费
            "intercept": f"{WAREHOUSE_FEE.details.get('拦截费', 'N/A')}",  # 拦截费
            "po_activation": f"{WAREHOUSE_FEE.details.get('亚马逊PO激活', 'N/A')}",  # 拦截费
            "self_pickup": f"{WAREHOUSE_FEE.details.get('客户自提', 'N/A')}",  # 客户自提
            "re_pallet": f"{WAREHOUSE_FEE.details.get('重新打板', 'N/A')}",  # 重新打板
            "counting": f"{WAREHOUSE_FEE.details.get('货品清点费', 'N/A')}",  # 货品清点费
            "warehouse_rent": WAREHOUSE_FEE.details.get("仓租", "N/A"),  # 仓租
            "specified_labeling": f"{WAREHOUSE_FEE.details.get('指定贴标', 'N/A')}",  # 指定贴标
            "inner_outer_box": f"{WAREHOUSE_FEE.details.get('内外箱', 'N/A')}",  # 内外箱
            "pallet_label": f"{WAREHOUSE_FEE.details.get('托盘标签', 'N/A')}",  # 内外箱
            "open_close_box": f"{WAREHOUSE_FEE.details.get('开封箱', 'N/A')}",  # 开封箱
            "destroy": f"{WAREHOUSE_FEE.details.get('销毁', 'N/A')}",  # 销毁
            "take_photo": f"{WAREHOUSE_FEE.details.get('拍照', 'N/A')}",  # 拍照
            "take_video": f"{WAREHOUSE_FEE.details.get('拍视频', 'N/A')}",  # 拍视频
            "repeated_operation_fee": f"{WAREHOUSE_FEE.details.get('重复操作费', 'N/A')}",  # 重复操作费
        }

        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        groups = [group.name for group in request.user.groups.all()]
        if not delivery_type:
            delivery_type = request.GET.get("delivery_type")
        if delivery_type is None:
            # 确定delivery_type
            if "warehouse_public" in groups and "warehouse_other" not in groups:
                delivery_type = "public"
            elif "warehouse_other" in groups and "warehouse_public" not in groups:
                delivery_type = "other"
        # 不需要赋值单价的字段
        excluded_fields = {
            "id",
            "invoice_number",
            "invoice_type",
            "delivery_type",
            "amount",
            "qty",
            "rate",
            "other_fees",
            "surcharges",
            "surcharge_notes",
            "history",
        }

        try:
            invoice_warehouse = InvoiceWarehouse.objects.get(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
                delivery_type=delivery_type,
            )
        except InvoiceWarehouse.DoesNotExist:
            invoice_warehouse,created = InvoiceWarehouse.objects.get_or_create(
                invoice_number=invoice,
                invoice_type=invoice_type,
                delivery_type=delivery_type,
            )
            qty_data, rate_data = self._extract_unit_price(
                model=InvoiceWarehouse,
                unit_prices=FS_constrain,
                pickup_fee=None,
                excluded_fields=excluded_fields,
            )
            invoice_warehouse.qty = qty_data
            invoice_warehouse.rate = rate_data
            invoice_warehouse.save()

            context = {
                "warehouse": request.GET.get("warehouse"),
                "invoice": invoice,
                "invoice_type": invoice_type,
                "container_number": container_number,
                "FS": FS,
                "fs_json": fs_json,
                "delivery_type": delivery_type,
                "start_date": request.GET.get("start_date"),
                "end_date": request.GET.get("end_date"),
                "invoice_type": invoice_type,
                "invoice_warehouse":invoice_warehouse,
                "surcharges": invoice_warehouse.surcharges,
                "surcharge_notes": invoice_warehouse.surcharge_notes,
            }
            return self.template_invoice_warehouse_edit, context
        # 如果单价和数量都为空的话，就初始化
        if not invoice_warehouse.qty and not invoice_warehouse.rate:
            qty_data, rate_data = self._extract_unit_price(
                model=InvoiceWarehouse,
                unit_prices=FS_constrain,
                pickup_fee=None,
                excluded_fields=excluded_fields,
            )
            invoice_warehouse.qty = qty_data
            invoice_warehouse.rate = rate_data
            invoice_warehouse.save()
        step = request.POST.get("step")
        redirect_step = step == "redirect"
        context = {
            "warehouse": request.GET.get("warehouse"),
            "invoice_warehouse": invoice_warehouse,
            "invoice": invoice,
            "container_number": container_number,
            "surcharges": invoice_warehouse.surcharges,
            "surcharge_notes": invoice_warehouse.surcharge_notes,
            "start_date": request.GET.get("start_date"),
            "end_date": request.GET.get("end_date"),
            "redirect_step": redirect_step,
            "FS": FS,
            "fs_json": fs_json,
            "status": (
                order.receivable_status.stage
                if invoice_type == "receivable"
                else order.payable_status.stage
            ),
            "start_date_confirm": request.POST.get("start_date_confirm") or None,
            "end_date_confirm": request.POST.get("end_date_confirm") or None,
            "invoice_type": invoice_type,
            "delivery_type": delivery_type,
            "invoice_warehouse":invoice_warehouse
        }
        return self.template_invoice_warehouse_edit, context

    def handle_container_invoice_confirm_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        start_date_confirm = request.GET.get("start_date_confirm")
        end_date_confirm = request.GET.get("end_date_confirm")
        invoice_type = request.GET.get("invoice_type")
        invoice = Invoice.objects.get(
            container_number__container_number=container_number
        )
        order = Order.objects.select_related("container_number").get(
            container_number__container_number=container_number
        )
        invoice_preports = InvoicePreport.objects.get(
            invoice_number__invoice_number=invoice.invoice_number,
            invoice_type=invoice_type,
        )
        if order.order_type in ["转运", "转运组合"]:
            invoice_warehouses = InvoiceWarehouse.objects.filter(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
            )
            invoice_delivery = InvoiceDelivery.objects.filter(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
            )
            amazon = []
            local = []
            combine = []
            walmart = []
            selfdelivery = []
            upsdelivery = []
            selfpickup = []
            for delivery in invoice_delivery:
                if delivery.type == "amazon":
                    amazon.append(delivery)
                elif delivery.type == "local":
                    local.append(delivery)
                elif delivery.type == "combine":
                    combine.append(delivery)
                elif delivery.type == "walmart":
                    walmart.append(delivery)
                elif delivery.type == "selfdelivery":
                    selfdelivery.append(delivery)
                elif delivery.type == "upsdelivery":
                    upsdelivery.append(delivery)
                elif delivery.type == "selfpickup":
                    selfpickup.append(delivery)
            context = {
                "invoice": invoice,
                "order_type": order.order_type,
                "invoice_preports": invoice_preports,
                "invoice_warehouses": invoice_warehouses,
                "amazon": amazon,
                "local": local,
                "combine": combine,
                "walmart": walmart,
                "selfdelivery": selfdelivery,
                "upsdelivery":upsdelivery,
                "selfpickup":selfpickup,
                "container_number": container_number,
                "start_date_confirm": start_date_confirm,
                "end_date_confirm": end_date_confirm,
                "invoice_type": invoice_type,
                'delivery_amount': getattr(invoice, f'{invoice_type}_delivery_amount', 0),
                'total_amount': getattr(invoice, f'{invoice_type}_total_amount', 0),
            }
            return self.template_invoice_confirm_edit, context
        elif order.order_type == "直送":
            modified_get = request.GET.copy()
            modified_get["start_date_confirm"] = request.GET.get("start_date_confirm")
            modified_get["end_date_confirm"] = request.GET.get("end_date_confirm")
            modified_get["confirm_step"] = True
            new_request = request
            new_request.GET = modified_get
            return self.handle_container_invoice_direct_get(request)

    def handle_container_invoice_delivery_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        invoice_type = request.GET.get("invoice_type")
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        order = Order.objects.select_related("retrieval_id", "container_number","vessel_id").get(
            container_number__container_number=container_number
        )
        warehouse = order.retrieval_id.retrieval_destination_area
        vessel_etd = order.vessel_id.vessel_etd
        # 把pallet汇总
        base_query = Pallet.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "container_number__order__customer_name",
                "invoice_delivery",
                Prefetch("invoice_delivery__pallet_delivery", to_attr="delivered_pallets")
            ).filter(container_number__container_number=container_number)
        common_annotations = {
            'str_id': Cast("id", CharField()),
            'is_hold': Case(
                When(delivery_method__contains="暂扣留仓", then=Value(True)),
                default=Value(False),
                output_field=BooleanField()
            ),
            'has_delivery': Case(
                When(invoice_delivery__isnull=False, then=Value(True)),
                default=Value(False),
                output_field=BooleanField()
            ),
            'is_self_pickup': Case(
                When(delivery_method__contains="客户自提", then=Value(True)),
                default=Value(False),
                output_field=BooleanField()
            )   
        }
        common_values = [
            "container_number__container_number",
            "destination",
            "zipcode",
            "address",
            "delivery_method",
            "invoice_delivery__type",
            "delivery_type",
            "is_hold",
            "has_delivery"   
        ]
        common_aggregates = {
            'ids': StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
            'total_cbm': Sum("cbm", output_field=FloatField()),
            'total_weight_lbs': Sum("weight_lbs", output_field=FloatField()),
            'total_pallet': Count("pallet_id", distinct=True),   
        }
        common_ordering = [
            Case(
                When(is_hold=True, then=Value(1)),
                default=Value(0),
                output_field=IntegerField()
            ),
            F("invoice_delivery__type").asc(nulls_first=True)   
        ]
        if 'NJ' in warehouse:
            pallets = (
                base_query.annotate(**common_annotations)
                .values(*common_values)
                .annotate(**common_aggregates)
                .order_by(*common_ordering)
            )
        else:  #sav和la的自提要按唛头分组
            self_pickup_pallets = (
                base_query.annotate(**common_annotations)
                .filter(is_self_pickup=True)              
                .values(*common_values, "shipping_mark")  # 自提组需要包含shipping_mark
                .annotate(**common_aggregates)
                .order_by(*common_ordering)   
            )
            non_self_pickup_pallets = (
                base_query.annotate(**common_annotations)
                .filter(is_self_pickup=False)
                .annotate(**common_annotations)
                .values(*common_values)  # 非自提组不需要shipping_mark
                .annotate(**common_aggregates)
                .order_by(*common_ordering)   
            )
            pallets_list = list(self_pickup_pallets) + list(non_self_pickup_pallets)
            pallets = [
                {k: v for k, v in item.items() if k in common_values}
                for item in pallets_list
            ]
            

        has_delivery = True
        for plt in pallets:
            if plt["has_delivery"] == False:
                has_delivery = False
                break
        # 需要重新规范板数，就是total_n_pallet
        cutoff_date = date(2025, 4, 1)
        is_new_rule = vessel_etd >= cutoff_date

        fee_details = self._get_fee_details(warehouse,vessel_etd)
        delivery_groups = self._process_delivery_records(
            invoice.invoice_number,
            pallets,
            order.container_number.container_type,
            fee_details,
            warehouse,
            is_new_rule
        )

        groups = [group.name for group in request.user.groups.all()]

        step = request.POST.get("step")
        redirect_step = (step == "redirect") or (
            request.POST.get("redirect_step") == "True"
        )
        context = {
            "warehouse": request.GET.get("warehouse"),
            "invoice": invoice,
            "container_number": container_number,
            "pallet": pallets,
            "amazon": delivery_groups["amazon"],
            "local": delivery_groups["local"],
            "combine": delivery_groups["combine"],
            "walmart": delivery_groups["walmart"],
            "selfdelivery": delivery_groups["selfdelivery"],
            "upsdelivery":delivery_groups["upsdelivery"],
            "selfpickup":delivery_groups["selfpickup"],
            "redirect_step": redirect_step,
            "start_date": request.GET.get("start_date") or None,
            "end_date": request.GET.get("end_date") or None,
            "start_date_confirm": request.POST.get("start_date_confirm") or None,
            "end_date_confirm": request.POST.get("end_date_confirm") or None,
            "invoice_type": invoice_type,
            "delivery_types": [
                ("", ""),
                ("公仓", "public"),
                ("其他", "other"),
            ],
        }
        if "mix_account" in groups:  # 如果公仓私仓都能看，就进总页面
            context["delivery_type"] = "mixed"
            context["has_delivery"] = has_delivery
            return self.template_invoice_delievery_edit, context
        else:
            if "NJ_mix_account" in groups or ("warehouse_other" in groups and "warehouse_public" not in groups):  #只看私仓
                print(pallets)
                print('---------------------')
                pallet = self._filter_pallets(pallets,'other')
                print(pallet)
                has_delivery = True
                for plt in pallet:
                    if plt["has_delivery"] == False:
                        has_delivery = False
                        break
                context["delivery_type"] = "other"
                context["pallet"] = pallet
                context["has_delivery"] = has_delivery
                return self.template_invoice_delievery_other_edit, context
            elif "warehouse_public" in groups and "warehouse_other" not in groups:
                pallet = self._filter_pallets(pallets,'public')
                has_delivery = True
                for plt in pallet:
                    if plt["has_delivery"] == False:
                        has_delivery = False
                        break
                context["pallet"] = pallet
                context["delivery_type"] = "public"
                context["has_delivery"] = has_delivery
                return self.template_invoice_delievery_public_edit, context
            else:
                raise ValueError("没有权限")

    def _filter_pallets(self,pallets:Any,delivery_type:str) -> Any:
        if isinstance(pallets, QuerySet):
            return pallets.filter(delivery_type=delivery_type)
        elif isinstance(pallets, list):
            return [
                pallet for pallet in pallets 
                if (
                    # 支持对象属性、字典键、或 getattr 安全访问
                    pallet.get("delivery_type") == delivery_type 
                    if isinstance(pallet, dict)
                    else getattr(pallet, "delivery_type", None) == delivery_type
                )
            ]
        else:
            raise ValueError("pallets must be QuerySet or list")
        
    def _get_fee_details(self, warehouse: str,vessel_etd) -> dict:
        try:
            quotation = QuotationMaster.objects.filter(
                effective_date__lte=vessel_etd
            ).order_by('-effective_date').first()
            if not quotation:
                raise ValueError('找不到报价表')
            id = quotation.id
            fee_types = {
                'NJ': ['NJ_LOCAL', 'NJ_PUBLIC', 'NJ_COMBINA'],
                'SAV': ['SAV_PUBLIC', 'SAV_COMBINA'],
                'LA': ['LA_PUBLIC', 'LA_COMBINA']
            }.get(warehouse, [])
            
            return {
                fee.fee_type: fee
                for fee in FeeDetail.objects.filter(
                    quotation_id=id,
                    fee_type__in=fee_types
                )
            }
        except QuotationMaster.DoesNotExist:
            raise ValueError("没有找到有效的报价单")
    
    def _process_delivery_records(self, invoice_number: str, pallets: Any, 
                       container_type: str, fee_details: dict, warehouse: str, is_new_rule: bool) -> dict:
        invoice_deliveries = InvoiceDelivery.objects.prefetch_related(
            Prefetch("pallet_delivery", queryset=Pallet.objects.all())
        ).filter(invoice_number__invoice_number=invoice_number)
        
        delivery_groups = {
            "amazon": [],
            "local": [],
            "combine": [],
            "walmart": [],
            "selfdelivery": [],
            "upsdelivery": [],
            "selfpickup": [],
            "invoice_delivery": invoice_deliveries
        }
        #没有单价的找单价，再根据type汇总派送方式
        for delivery in invoice_deliveries:
            pallets_in_delivery = delivery.pallet_delivery.all()
            pallet_ids = [str(p.id) for p in pallets_in_delivery] 
            setattr(delivery, "plt_ids", pallet_ids)
            if delivery.cost is None:
                self._calculate_and_set_delivery_cost(
                    delivery,
                    container_type,
                    fee_details,
                    warehouse,
                    is_new_rule
                )
            if delivery.type in delivery_groups:
                delivery_groups[delivery.type].append(delivery)

        return delivery_groups
    
    #根据报价表找单价
    def _calculate_and_set_delivery_cost(
        self,
        delivery: InvoiceDelivery,
        container_type: str,
        fee_details: dict,
        warehouse: str,
        is_new_rule:bool,
    ) -> None:
        destination = (
            delivery.destination.split("-")[1]
            if "-" in delivery.destination
            else delivery.destination
        )
        cost = 0
        is_niche_warehouse = True  #本地派送算冷门仓点
        #根据报价表找单价，ups和自发没有报价，所以不用找
        if delivery.type == "amazon":
            if "LA" in warehouse:
                amazon_data = fee_details.get(f"{warehouse}_PUBLIC").details
            else:
                amazon_data = fee_details.get(f"{warehouse}_PUBLIC").details.get(f"{warehouse}_AMAZON")
            for k, v in amazon_data.items():
                if destination in v:
                    cost = k
                    break
            niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
            if destination in niche_warehouse:
                is_niche_warehouse = True
            else:
                is_niche_warehouse = False
        elif delivery.type == "local" and warehouse == "NJ":
            local_data = fee_details.get("NJ_LOCAL").details
            for k, v in local_data.items():
                if str(delivery.zipcode) in map(str,v["zipcodes"]):
                    n_pallet = int(delivery.total_pallet)  # 板数
                    costs = v["prices"]
                    if n_pallet <= 5:
                        cost = int(costs[0])
                    elif n_pallet >= 5:
                        cost = int(costs[1])
                    break
        elif delivery.type == "combine":
            combine_data = fee_details.get(f"{warehouse}_COMBINA").details
            cost = None
            for region, price_groups in combine_data.items():
                for price_group in price_groups:
                    if destination in price_group["location"]:
                        if "45HQ/GP" in container_type:
                            cost = int(price_group["prices"][1])
                        elif "40HQ/GP" in container_type:
                            cost = int(price_group["prices"][0])
                        break
                if cost is not None:
                    break
            niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
            if destination in niche_warehouse:
                is_niche_warehouse = True
            else:
                is_niche_warehouse = False
        elif delivery.type == "walmart":
            if "LA" in warehouse:
                walmart_data = fee_details.get(f"{warehouse}_PUBLIC").details
            else:
                walmart_data = fee_details.get(f"{warehouse}_PUBLIC").details.get(f"{warehouse}_WALMART")
            for price, locations in walmart_data.items():
                if str(destination).upper() in [loc.upper() for loc in locations]:
                    cost = price
                    break
            niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
            if destination in niche_warehouse:
                is_niche_warehouse = True
            else:
                is_niche_warehouse = False
        if cost is not None:
            delivery.cost = cost
        else:
            delivery.cost = 0
        #计算板数
        if delivery.type == "local" or (delivery.type == "amazon" and warehouse == "LA"):
            is_new_rule = False
        total_pallet = self._calculate_total_pallet(delivery.total_cbm,is_new_rule,is_niche_warehouse)
        delivery.total_pallet = total_pallet
        delivery.save()

    def _calculate_total_pallet(self,cbm: float,is_new_rule: bool,is_niche_warehouse:bool) -> float:
        raw_p = float(cbm)/1.8
        integer_part = int(raw_p)
        decimal_part = raw_p - integer_part
        #本地派送的按照4.1之前的规则
        if decimal_part > 0:
            if is_new_rule: #etd4.1之后的
                if decimal_part > 0:
                    if is_niche_warehouse:
                        additional = 2 if decimal_part > 0.5 else 1
                    else:
                        additional = 1 if decimal_part > 0.5 else 0
            else:
                if decimal_part > 0:
                    if is_niche_warehouse:
                        additional = 1
                    else:
                        additional = 1 if decimal_part > 0.9 else 0.5
            total_pallet = integer_part + additional
        elif decimal_part == 0:
            total_pallet = integer_part
        else:
            ValueError("板数计算错误")
        return total_pallet

    def find_matching_regions(self,plts_by_destination: dict, combina_fee: dict,container_type) -> dict:
        matching_regions = defaultdict(float)
        des_match_quote = {}
        destination_matches = set()
        non_combina_dests = set()
        price_display = defaultdict(lambda: {
            "price": 0.0,
            "location": set()
        })

        for plts in plts_by_destination:
            dest = plts['destination']
            cbm = plts['total_cbm']
            dest_matches = []
            matched = False
            # 遍历所有区域和location
            for region, fee_data_list in combina_fee.items():
                for fee_data in fee_data_list:
                    if dest in fee_data["location"]:
                        temp_cbm = matching_regions[region] + cbm
                        matching_regions[region] = temp_cbm
                        dest_matches.append({
                            "region": region,
                            "location": dest,
                            "prices": fee_data["prices"],
                            'cbm':cbm,
                        })
                        price_display[region]["price"] = fee_data["prices"][container_type]
                        price_display[region]["location"].add(dest)
                        matched = True
            
            # 记录匹配结果
            if dest_matches:
                des_match_quote[dest] = dest_matches
                destination_matches.add(dest)
            elif not matched:
                non_combina_dests.add(dest)  # 未匹配的仓点
   
        return {
            "des_match_quote":des_match_quote,
            "matching_regions": matching_regions,
            "combina_dests": destination_matches,
            "non_combina_dests": non_combina_dests,
            "price_display":price_display
        }
    
    def is_mixed_region(self,matched_regions, warehouse) -> bool:
        regions = list(matched_regions.keys())
        
        # LA仓库的特殊规则：CDEF区不能混
        if warehouse == "LA":
            forbidden_zones = {"C区", "D区", "E区", "F区"}
            if any(zone in regions for zone in forbidden_zones):
                # 如果有CDEF区，则检查是否有其他区混合
                if len(set(regions) - forbidden_zones) > 1:
                    return True
        # 其他仓库无限制
        return False
    
    def handle_container_invoice_combina_get(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        container = Container.objects.get(container_number=container_number)
        order = Order.objects.select_related(
            "retrieval_id", "container_number", "vessel_id"
        ).get(container_number__container_number=container_number)
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        invoice_type = request.GET.get('invoice_type')
        #先查找是不是已经报错过数据了，如果保存了就读保存的数据，没有保存的数据就去匹配查找
        invoice_item = InvoiceItem.objects.filter(
            invoice_number__invoice_number=invoice.invoice_number
        )
        if invoice_item:
            context = {
                "invoice": invoice,
                "invoice_item": invoice_item,
            }
            return self.template_invoice_container_edit, context
        warehouse = order.retrieval_id.retrieval_destination_area
        vessel_etd = order.vessel_id.vessel_etd
        
        container_type = container.container_type 
        # 1. 基础数据统计
        plts = Pallet.objects.filter(
            container_number__container_number=container_number
        ).aggregate(
            unique_destinations=Count('destination', distinct=True),
            total_weight=Sum('weight_lbs'),
            total_cbm=Sum('cbm'),
            total_pallets=Count('id'),
        )
        plts['total_cbm'] = round(plts['total_cbm'], 2)
        plts['total_weight'] = round(plts['total_weight'], 2)
        # 3. 获取匹配的报价表
        matching_quotation = QuotationMaster.objects.filter(
            effective_date__lte=vessel_etd
        ).order_by('-effective_date').first()
        if not matching_quotation:
            return self.template_invoice_combina_edit, {'reason': '找不到匹配报价表'}
        # 4. 获取费用规则
        try:
            PICKUP_FEE = FeeDetail.objects.get(
                quotation_id=matching_quotation.id, fee_type="preport"
            )
            combina_fee = FeeDetail.objects.get(
                quotation_id=matching_quotation.id,
                fee_type=f"{warehouse}_COMBINA"
            ).details
            fee_details = self._get_fee_details(warehouse,vessel_etd)
            stipulate = FeeDetail.objects.get(
                quotation_id=matching_quotation.id,
                fee_type="COMBINA_STIPULATE"
            ).details
        except FeeDetail.DoesNotExist:
            return self.template_invoice_combina_edit, {'reason': '费用规则不完整'}
        if isinstance(combina_fee, str):
            combina_fee = json.loads(combina_fee)
        # 2. 检查基本条件
        if plts['unique_destinations'] == 0:
            return self.template_invoice_combina_edit, {'reason': '未录入拆柜数据'}
            
        if plts['unique_destinations'] > stipulate["global_rules"]["bulk_threshold"]["default"]:
            container.account_order_type = '转运'
            container.save()
            return self.template_invoice_combina_edit, {'reason': '超过14个仓点'}

        # 按区域统计
        destinations = Pallet.objects.filter(
            container_number__container_number=container_number
        ).values_list('destination', flat=True).distinct()
        plts_by_destination = Pallet.objects.filter(
            container_number__container_number=container_number   
        ).values('destination').annotate(total_cbm=Sum('cbm'))
        #区分组合柜区域和非组合柜区域
        container_type = 0 if container_type == '40HQ/GP' else 1
        matched_regions = self.find_matching_regions(plts_by_destination, combina_fee,container_type)
        # 判断是否混区，除了LA的CDEF不能混，别的都能混
        is_mix = self.is_mixed_region(matched_regions["matching_regions"], warehouse)

        #非组合柜区域
        non_combina_region_count = len(matched_regions['non_combina_dests'])
        #组合柜区域
        combina_region_count = len(matched_regions["combina_dests"])
        #组合柜对应的区
        des_match_quote = matched_regions["des_match_quote"]
        #组合柜区域，各区及总cbm
        matching_regions = matched_regions["matching_regions"]
        if combina_region_count + non_combina_region_count != len(destinations):
            raise ValueError('计算组合柜和非组合柜区域有误')

        non_combina_cbm = Pallet.objects.filter(
            container_number__container_number=container_number,
                destination__in=matched_regions['non_combina_dests']  # 过滤非组合柜仓点
            ).aggregate(
                Sum('cbm')
            )['cbm__sum'] or 0  # 处理可能为None的情况
        non_combina_cbm = round(float(non_combina_cbm), 3) if non_combina_cbm else 0.000
        # 4. 计算占比
        if plts['total_cbm'] and plts['total_cbm'] > 0:
            non_combina_cbm_ratio = round(non_combina_cbm / plts['total_cbm'], 3)
        else:
            non_combina_cbm_ratio = 0

        if combina_region_count > stipulate["global_rules"]["max_mixed"]["default"] or non_combina_region_count > (stipulate["global_rules"]["bulk_threshold"]["default"] - stipulate["global_rules"]["max_mixed"]["default"]):
            container.account_order_type = '转运'
            container.save()
            if combina_region_count > stipulate["global_rules"]["max_mixed"]["default"]:
                reason = f"规定{stipulate["global_rules"]["max_mixed"]["default"]}组合柜区,但实际有{combina_region_count}个:matched_regions['combina_dests']"
            elif non_combina_region_count > (stipulate["global_rules"]["bulk_threshold"]["default"] - stipulate["global_rules"]["max_mixed"]["default"]):
                stipulate_non_combina = stipulate["global_rules"]["bulk_threshold"]["default"] - stipulate["global_rules"]["max_mixed"]["default"]
                reason = f"规定{stipulate_non_combina}个非组合柜区，但是有{non_combina_region_count}个：{matched_regions['non_combina_dests']}"
            return self.template_invoice_combina_edit, {'reason': reason}
        # 7.2 计算基础费用
        base_fee = 0
        extra_fees = {
            'overweight': 0,
            'overpallets': 0,
            'overregion_pickup': 0,
            'overregion_delivery': 0
        }
        base_fee = 0.0
        price_display = matched_regions['price_display']
        if not is_mix:
            # 单一区域情况
            if des_match_quote == 1: 
                region = des_match_quote[0]          
                base_fee = combina_fee[region][0]['prices'][0 if container_type == '40HQ/GP' else 1]
                price_display_new = [
                    {"key": region, 'cbm':matching_regions[region], 'rate':100, "price": data['price'], "location":  ", ".join(data['location'])}
                    for region, data in price_display.items()   
                ]
            else:  #允许混区的情况
                total_cbm_sum = sum(total_cbm for region,total_cbm in matching_regions.items())
                for region,total_cbm in matching_regions.items():
                    fee = combina_fee[region][0]['prices'][0 if container_type == '40HQ/GP' else 1]
                    base_fee += fee * total_cbm/total_cbm_sum
                base_fee = round(base_fee,2)
                price_display_new = [
                    {"key": region, 'cbm':matching_regions[region], 'rate':round(matching_regions[region]/total_cbm_sum,3), "price": data['price'], "location":  ", ".join(data['location'])}
                    for region, data in price_display.items()   
                ]
        # 7.3 检查超限情况
        # 超重检查
        if plts['total_weight'] > stipulate["global_rules"]["weight_limit"]["default"]:
            extra_fees['overweight'] = '需人工录入'  # 实际业务中应有默认费率

        # 超板检查——确定上限的板数
        if container_type == '40HQ/GP':
            std_plt = stipulate["global_rules"]['std_40ft_plts']
        else:
            std_plt = stipulate["global_rules"]['std_45ft_plts']
        exceptions_dict = std_plt["exceptions"]
              
        max_pallets = 0
        for exception_des, exception_plt in exceptions_dict.items():
            exception_warehouse = exception_des.split('_')[0]
            exception_regions = list(exception_des.split('_')[1])
            # 检查是否匹配当前 warehouse 和 region
            if (exception_warehouse == warehouse and 
                any(region in exception_regions for region,value in matched_regions["matching_regions"].items())):
                max_pallets = exception_plt
                break   
        if max_pallets== 0:
            max_pallets = std_plt["default"]
        #处理超的板数
        #先计算实际板数
        total_pallets = math.ceil(plts['total_cbm'] / 1.8)  #取上限
        if total_pallets > max_pallets:
            over_count = total_pallets - max_pallets
        else:
            over_count = 0
        # 找每个仓点的单价，倒序排序，方便计算超板的（没有超板的也要查，可能前端会改板数）
        plts_by_destination = Pallet.objects.filter(
            container_number__container_number=container_number   
        ).values('destination').annotate(
            total_cbm=Sum('cbm'),
            price=Value(None, output_field=models.FloatField()),
            is_fixed_price=Value(False, output_field=BooleanField()),
            total_pallet=Count('id',output_field=FloatField())   
        ) #形如{'destination': 'A', 'total_cbm': 10.5，'price':31.5,'is_fixed_price':True},
        plts_by_destination = self._calculate_delivery_fee_cost(fee_details,warehouse,plts_by_destination,destinations,over_count)
        max_price = 0
        max_single_price = 0
        for plt_d in plts_by_destination:
            if plt_d['is_fixed_price']:   #一口价的不用乘板数
                max_price = max(float(plt_d['price']),max_price)
                max_single_price = max(max_price,max_single_price)
            else:
                max_price = max(float(plt_d['price'])*over_count,max_price)
                max_single_price = max(float(plt_d['price']),max_single_price)
        extra_fees['overpallets'] = max_price

        # 计算非组合柜费用的提拆费和派送费
        if non_combina_region_count:
            #提拆费，要计算下非组合柜区域占当前柜子的cbm比例*对应的提拆费
            container_type = order.container_number.container_type
            match = re.match(r"\d+", container_type)
            if match:
                pick_subkey = match.group()
                pickup_fee = PICKUP_FEE.details[warehouse][pick_subkey]
            extra_fees['overregion_pickup'] = non_combina_cbm_ratio*pickup_fee
            #派送费
            for item in matched_regions['non_combina_dests']:
                #计算改区域的板数
                plts_by_destination_overregion = Pallet.objects.filter(
                    container_number__container_number=container_number,
                    destination__in=matched_regions['non_combina_dests']   
                ).values('destination').annotate(
                    total_cbm=Sum('cbm'),
                    total_pallet=Count('id',output_field=FloatField()),
                    total_weight=Sum('weight_lbs'),
                    price=Value(None, output_field=models.FloatField()),
                    is_fixed_price=Value(False, output_field=BooleanField()) 
                )
                

                plts_by_destination_overregion = self._calculate_delivery_fee_cost(fee_details,warehouse,plts_by_destination_overregion,destinations,None)

                sum_price = 0
                for plt_d in plts_by_destination_overregion:
                    if plt_d['is_fixed_price']:   #一口价的不用乘板数
                        sum_price += float(plt_d['price'])
                    else:
                        sum_price += float(plt_d['price'])*plt_d['total_pallet']
            extra_fees['overregion_delivery'] = sum_price
        display_data = {
            # 基础信息
            'plts_by_destination':plts_by_destination,
            'container_info': {
                'number': container_number,
                'type': container_type,
                'warehouse': warehouse
            },
            
            # 组合柜信息
            'combina_data': {
                'base_fee': base_fee,
                'regions': [],
                'destinations': []
            },
            
            # 超限费用
            'extra_fees': {
                'overweight': {
                    'is_over': plts['total_weight'] > stipulate["global_rules"]["weight_limit"]["default"],
                    'current_weight': plts['total_weight'],
                    'limit_weight': stipulate["global_rules"]["weight_limit"]["default"],
                    'extra_weight': round(plts['total_weight'] - stipulate["global_rules"]["weight_limit"]["default"],2),
                    'fee': extra_fees['overweight'],
                    'input_field': True  # 显示输入框
                },
                'overpallets': {
                    'is_over': total_pallets > max_pallets,
                    'current_pallets': total_pallets,
                    'limit_pallets': max_pallets,
                    'over_count': over_count if total_pallets > max_pallets else 0,
                    'pallet_details': [],
                    'max_price_used': None,
                    'fee': extra_fees['overpallets']
                },
                'overregion': {
                    'pickup': {
                        'non_combina_cbm': non_combina_cbm,
                        'total_cbm': plts['total_cbm'],
                        'ratio': non_combina_cbm_ratio*100,
                        'base_fee': pickup_fee,
                        'fee': extra_fees['overregion_pickup']
                    },
                    'delivery': {
                        'fee': extra_fees['overregion_delivery'],
                        'details': [],
                    }
                }
                
            }
        }
        display_data['combina_data']['destinations'] = price_display_new
        
        # 填充超板费详细信息，不超板也要展示详情，因为前端可以修改超的板数
        #if total_pallets > max_pallets:
        for plt in plts_by_destination:
            display_data['extra_fees']['overpallets']['pallet_details'].append({
                'destination': plt['destination'],
                'price': plt['price'],
                'is_fixed_price': plt['is_fixed_price'],
                'is_max_used': float(plt['price']) == max_single_price  # 标记是否被采用
            })
        display_data['extra_fees']['overpallets']['max_price_used'] = max_price
        
        # 填充超区派送费详细信息
        for plt in plts_by_destination_overregion:
            display_data['extra_fees']['overregion']['delivery']['details'].append({
                'destination': plt['destination'],
                'pallets': plt['total_pallet'],
                'price': plt['price'],
                'cbm':round(plt['total_cbm'],3),
                'subtotal': float(plt['price']) * plt['total_pallet']
            })
        total_amount = base_fee + extra_fees['overpallets'] + extra_fees['overregion_pickup'] + extra_fees['overregion_delivery']
        # 8. 返回结果
        context = {
            'display_data': display_data,
            'total_amount': total_amount,
            "invoice_number":invoice.invoice_number,
            "container_number":container_number,
            'invoice_type':invoice_type,
        }
        return self.template_invoice_combina_edit, context
    
    def _calculate_delivery_fee_cost(
        self,
        fee_details: dict,
        warehouse: str,
        plts_by_destination: list,
        is_new_rule:bool,
        over_count: float,
    ) -> str:
        is_niche_warehouse = True
        if "LA" in warehouse:
            amazon_data = fee_details.get(f"{warehouse}_PUBLIC").details
            walmart_data = None
        else:
            amazon_data = fee_details.get(f"{warehouse}_PUBLIC").details.get(f"{warehouse}_AMAZON")
            walmart_data = fee_details.get(f"{warehouse}_PUBLIC").details.get(f"{warehouse}_WALMART")
        local_data = fee_details.get("NJ_LOCAL").details
        for pl in plts_by_destination:
            #从亚马逊、沃尔玛、本地报价表中挨个找
            #先找亚马逊       
            for price, locations in amazon_data.items():
                if pl['destination'] in locations:
                    pl['price'] = price
                    break
            if pl['price']:  #说明这个仓点在这个报价表里，确实是不是冷门仓点
                niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
                if pl['destination'] in niche_warehouse:
                    is_niche_warehouse = True
                else:
                    is_niche_warehouse = False

            #再找沃尔玛
            if not pl['price'] and walmart_data:
                for price, locations in walmart_data.items():
                    if str(pl['destination']).upper() in [loc.upper() for loc in locations]:
                        pl['price'] = price
                        break
                if pl['price']:
                    niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
                    if pl['destination'] in niche_warehouse:
                        is_niche_warehouse = True
                    else:
                        is_niche_warehouse = False
            #再找本地派送
            if not pl['price'] and warehouse == "NJ":               
                destination = re.sub(r'[^0-9]', '', str(pl['destination']))
                for price, locations in local_data.items():
                    if str(destination) in map(str,locations["zipcodes"]):
                        pl['is_fixed_price'] = True  #表示一口价，等会就不会再乘以板数了
                        costs = locations["prices"]
                        if not over_count:
                            total_pallet = self._calculate_total_pallet(pl['total_cbm'],is_new_rule,is_niche_warehouse)
                        else:
                            total_pallet = over_count
                        if total_pallet <= 5:
                            pl['price'] = int(costs[0])
                        elif total_pallet >= 5:
                            pl['price'] = int(costs[1])
                        break
            if not pl['price']:
                pl['price'] = 0
            if not over_count:
                total_pallet = self._calculate_total_pallet(pl['total_cbm'],is_new_rule,is_niche_warehouse)
            else:
                total_pallet = over_count
            pl['total_pallet'] = total_pallet
        return plts_by_destination

    def handle_container_invoice_direct_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        order = Order.objects.select_related(
            "retrieval_id", "container_number", "receivable_status", "payable_status"
        ).get(container_number__container_number=container_number)
        warehouse = order.retrieval_id.retrieval_destination_area
        invoice_type = request.GET.get("invoice_type")
        vessel_etd = order.vessel_id.vessel_etd
        quotation = QuotationMaster.objects.filter(
            effective_date__lte=vessel_etd
        ).order_by('-effective_date').first()
        if not quotation:
            raise ValueError('找不到报价表')
        PICKUP_FEE = FeeDetail.objects.get(quotation_id=quotation.id, fee_type="direct")
        # 提拆、打托缠膜费用
        pickup_fee = 0
        pickup = PICKUP_FEE.details["pickup"]
        for fee, location in pickup.items():
            if warehouse in location:
                pickup_fee = fee
        # 其他费用
        destination = order.retrieval_id.retrieval_destination_area
        new_destination = destination.replace(" ", "") if destination else ""
        second_delivery = PICKUP_FEE.details.get("二次派送")
        second_pickup = None
        for fee, location in second_delivery.items():
            if new_destination in location:
                second_pickup = fee
        FS = {
            "exam_fee": f"{PICKUP_FEE.details.get('查验柜运费', 'N/A')}",  # 查验费
            "second_delivery": second_pickup,  # 二次派送
            "demurrage": f"{PICKUP_FEE.details.get('滞港费', 'N/A')}",  # 滞港费
            "per_diem": f"{PICKUP_FEE.details.get('滞箱费', 'N/A')}",  # 滞箱费
            "congestion_fee": f"{PICKUP_FEE.details.get('港口拥堵费', 'N/A')}",  # 港口拥堵费
            "chassis": f"{PICKUP_FEE.details.get('车架费', 'N/A')}",  # 车架费
            "prepull": f"{PICKUP_FEE.details.get('预提费', 'N/A')}",  # 预提费
            "yard_storage": f"{PICKUP_FEE.details.get('货柜储存费', 'N/A')}",  # 货柜储存费
            "chassis_split": f"{PICKUP_FEE.details.get('车架分离费', 'N/A')}",  # 车架分离费
            "over_weight": f"{PICKUP_FEE.details.get('超重费', 'N/A')}",  # 超重费
        }
        FS_constrain = {}
        for key, value in PICKUP_FEE.details.items():
            if not isinstance(value, dict):
                match = re.findall(r"\$(\d+(\.\d+)?)", str(value))
                if match and len(match) == 1:
                    FS_constrain[key] = float(match[0][0])
        fs_json = json.dumps(FS_constrain, ensure_ascii=False)
        try:
            invoice = Invoice.objects.select_related(
                "customer", "container_number"
            ).get(container_number__container_number=container_number)
        except Invoice.DoesNotExist:
            current_date = datetime.now().date()
            order_id = str(order.id)
            customer_id = order.customer_name.id
            invoice = Invoice(
                **{
                    "invoice_number": f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}",
                    "customer": order.customer_name,
                    "container_number": order.container_number,
                }
            )
            invoice.save()
            order.invoice_id = invoice

        # 建立invoicestatus表
        try:
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type=invoice_type
            )
        except InvoiceStatus.DoesNotExist:
            invoice_status = InvoiceStatus(
                container_number=order.container_number,
                invoice_type=invoice_type,
            )
            invoice_status.save()
            if invoice_type == "receivable":
                order.receivable_status = invoice_status
            else:
                order.payable_status = invoice_status
        order.save()

        invoice_preports, created = InvoicePreport.objects.get_or_create(
            invoice_number=invoice,
            invoice_type=invoice_type,
            defaults={
                "pickup": pickup_fee,
            },
        )
        # 如果单价和数量都为空的话，就初始化
        renamed_FS_constrain = (
            {  # 因为报价表中，直送和提拆名字不一致，但是表名一致，名称就无法匹配
                (
                    "港口拥堵费"
                    if key == "等待费"
                    else (
                        "查验费"
                        if "查验" in key
                        else (
                            "托架费"
                            if "车架费" in key
                            else (
                                "托架提取费"
                                if key == "车架分离费"
                                else "货柜放置费" if key == "货柜储存费" else key
                            )
                        )
                    )
                ): value
                for key, value in FS_constrain.items()
            }
        )
        if not invoice_preports.qty and not invoice_preports.rate:
            # 提取单价信息
            excluded_fields = {
                "id",
                "invoice_number",
                "invoice_type",
                "amount",
                "qty",
                "rate",
                "other_fees",
                "surcharges",
                "surcharge_notes",
                "history",
            }
            qty_data, rate_data = self._extract_unit_price(
                model=InvoicePreport,
                unit_prices=renamed_FS_constrain,
                pickup_fee=pickup_fee,
                excluded_fields=excluded_fields,
            )
            invoice_preports.qty = qty_data
            invoice_preports.rate = rate_data
            invoice_preports.save()
        else:
            qty_data = invoice_preports.qty
            rate_data = invoice_preports.rate

        context = {
            "warehouse": warehouse,
            "invoice_preports": invoice_preports,
            "container_number": container_number,
            "start_date": request.GET.get("start_date"),
            "end_date": request.GET.get("end_date"),
            "surcharges": invoice_preports.surcharges,
            "surcharges_notes": invoice_preports.surcharge_notes,
            "FS": FS,
            "fs_json": fs_json,
            "status": (
                order.receivable_status.stage
                if invoice_type == "receivable"
                else order.payable_status.stage
            ),
            "start_date_confirm": request.GET.get("start_date_confirm") or None,
            "end_date_confirm": request.GET.get("end_date_confirm") or None,
            "confirm_step": request.GET.get("confirm_step") or None,
            "invoice_type": invoice_type,
            "qty_data": qty_data,
            "rate_data": rate_data,
        }
        return self.template_invoice_direct_edit, context

    def _extract_unit_price(self, model, unit_prices, pickup_fee, excluded_fields):
        # 构建qty JSON
        qty_data = {}
        rate_data = {}
        # 遍历模型的所有FloatField字段
        for field in model._meta.get_fields():
            if not (
                isinstance(field, models.FloatField)
                and field.name not in excluded_fields
            ):
                continue
            price = unit_prices.get(field.verbose_name, 1.0)
            rate_data[field.name] = float(price) if price not in [None, "N/A"] else 1.0
            qty_data[field.name] = 0
        if pickup_fee:
            rate_data["pickup"] = pickup_fee
            qty_data["pickup"] = 1
        return qty_data, rate_data

    def handle_container_invoice_preport_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        invoice_type = request.GET.get("invoice_type")
        order = Order.objects.select_related(
            "retrieval_id", "container_number", "warehouse"
        ).get(container_number__container_number=container_number)
        # 查看仓库和柜型，计算提拆费
        warehouse = order.retrieval_id.retrieval_destination_area
        container_type = order.container_number.container_type
        vessel_etd = order.vessel_id.vessel_etd
        quotation = QuotationMaster.objects.filter(
            effective_date__lte=vessel_etd
        ).order_by('-effective_date').first()
        if not quotation:
            raise ValueError('找不到报价表')
        PICKUP_FEE = FeeDetail.objects.get(
            quotation_id=quotation.id, fee_type="preport"
        )
        # 提拆、打托缠膜费用
        match = re.match(r"\d+", container_type)
        if match:
            pick_subkey = match.group()
            pickup_fee = PICKUP_FEE.details[warehouse][pick_subkey]

        FS_constrain = {  # 把details里面的键值对改成值是纯数字的，用于在费用表单提交前，验证数据合规性
            key: float(re.search(r"\d+(\.\d+)?", value).group())
            for key, value in PICKUP_FEE.details.items()
            if not isinstance(value, dict)
            and "/" in str(value)
            and re.search(r"\d+(\.\d+)?", value)
        }
        fs_json = json.dumps(FS_constrain, ensure_ascii=False)
        # 其他费用
        FS = {
            "chassis": f"{PICKUP_FEE.details.get('托架费', 'N/A')}",  # 托架费
            "chassis_split": f"{PICKUP_FEE.details.get('托架提取费', 'N/A')}",  # 托架提取费
            "prepull": f"{PICKUP_FEE.details.get('预提费', 'N/A')}",  # 预提费
            "yard_storage": f"{PICKUP_FEE.details.get('货柜放置费', 'N/A')}",  # 货柜放置费
            "handling": f"{PICKUP_FEE.details.get('操作处理费', 'N/A')}",  # 操作处理费
            "pier_pass": PICKUP_FEE.details.get("码头", "N/A"),  # 码头费
            "congestion": f"{PICKUP_FEE.details.get('港口拥堵费', 'N/A')}",  # 港口拥堵费
            "hanging_crane": f"{PICKUP_FEE.details.get('火车站吊柜费', 'N/A')}",  # 火车站吊柜费
            "dry_run": f"{PICKUP_FEE.details.get('空跑费', 'N/A')}",  # 空跑费
            "exam_fee": f"{PICKUP_FEE.details.get('查验费', 'N/A')}",  # 查验费
            "hazmat": f"{PICKUP_FEE.details.get('危险品', 'N/A')}",  # 危险品
            "over_weight": f"{PICKUP_FEE.details.get('超重费', 'N/A')}",  # 超重费
            "urgent_fee": f"{PICKUP_FEE.details.get('加急费', 'N/A')}",  # 加急费
            "other_serive": f"{PICKUP_FEE.details.get('其他服务', 'N/A')}",  # 其他服务
        }
        # 提拆柜费用读取对应表
        try:
            invoice = Invoice.objects.select_related(
                "customer", "container_number"
            ).get(container_number__container_number=container_number)
        except Invoice.DoesNotExist:
            # 没有账单就创建
            order = Order.objects.select_related(
                "customer_name", "container_number"
            ).get(container_number__container_number=container_number)
            current_date = datetime.now().date()
            order_id = str(order.id)
            customer_id = order.customer_name.id
            invoice = Invoice(
                **{
                    "invoice_number": f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}",
                    "customer": order.customer_name,
                    "container_number": order.container_number,
                    "receivable_total_amount": 0.0,
                    "receivable_preport_amount": 0.0,
                    "receivable_warehouse_amount": 0.0,
                    "receivable_delivery_amount": 0.0,
                    "receivable_direct_amount": 0.0,
                    "payable_total_amount": 0.0,
                    "payable_preport_amount": 0.0,
                    "payable_warehouse_amount": 0.0,
                    "payable_delivery_amount": 0.0,
                    "payable_direct_amount": 0.0,
                }
            )
            invoice.save()
            order.invoice_id = invoice

        # 建立invoicestatus表
        try:
            invoice_status = InvoiceStatus.objects.get(
                container_number=order.container_number, invoice_type=invoice_type
            )
        except InvoiceStatus.DoesNotExist:
            invoice_status = InvoiceStatus(
                container_number=order.container_number,
                invoice_type=invoice_type,
            )
            invoice_status.save()
            if invoice_type == "receivable":
                order.receivable_status = invoice_status
            else:
                order.payable_status = invoice_status
        order.save()
        # 建立invoicepreport表
        invoice_preports, created = InvoicePreport.objects.get_or_create(
            invoice_number=invoice,
            invoice_type=invoice_type,
            defaults={
                "pickup": pickup_fee,
            },
        )
        # 如果单价和数量都为空的话，就初始化
        if not invoice_preports.qty and not invoice_preports.rate:
            # 提取单价信息
            excluded_fields = {
                "id",
                "invoice_number",
                "invoice_type",
                "amount",
                "qty",
                "rate",
                "other_fees",
                "surcharges",
                "surcharge_notes",
                "history",
            }
            qty_data, rate_data = self._extract_unit_price(
                model=InvoicePreport,
                unit_prices=FS_constrain,
                pickup_fee=pickup_fee,
                excluded_fields=excluded_fields,
            )
            invoice_preports.qty = qty_data
            invoice_preports.rate = rate_data
            invoice_preports.save()
        else:
            qty_data = invoice_preports.qty
            rate_data = invoice_preports.rate
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
        step = request.POST.get("step")
        redirect_step = step == "redirect"
        context = {
            "warehouse": warehouse,
            "order_type": order.order_type,
            "container_type": container_type,
            "reject_reason": order.invoice_reject_reason,
            "invoice_preports": invoice_preports,
            "surcharges": invoice_preports.surcharges,
            "surcharges_notes": invoice_preports.surcharge_notes,
            "container_number": container_number,
            "groups": groups,
            "start_date": request.GET.get("start_date"),
            "end_date": request.GET.get("end_date"),
            "FS": FS,
            "fs_json": fs_json,
            "status": (
                order.receivable_status.stage
                if invoice_type == "receivable"
                else order.payable_status.stage
            ),
            "redirect_step": redirect_step,
            "start_date_confirm": request.POST.get("start_date_confirm") or None,
            "end_date_confirm": request.POST.get("end_date_confirm") or None,
            "invoice_type": invoice_type,
            "qty_data": qty_data,
            "rate_data": rate_data,
        }
        return self.template_invoice_preport_edit, context

    def handle_container_invoice_get(self, container_number: str) -> tuple[Any, Any]:
        order = Order.objects.select_related("offload_id").get(
            container_number__container_number=container_number
        )
        if order.offload_id.offload_at == None:
            packing_list = (
                PackingList.objects.select_related("container_number", "pallet")
                .filter(container_number__container_number=container_number)
                .values("container_number__container_number", "destination")
                .annotate(
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight=Sum("total_weight_lbs", output_field=FloatField()),
                    total_n_pallet=Count("id", distinct=True),
                )
                .order_by("destination", "-total_cbm")
            )
        else:
            packing_list = (
                Pallet.objects.select_related("container_number")
                .filter(container_number__container_number=container_number)
                .values("container_number__container_number", "destination")
                .annotate(
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet=Count("pallet_id", distinct=True),
                )
                .order_by("destination", "-total_cbm")
            )
        for pl in packing_list:
            c_p = math.ceil(pl["total_cbm"] / 1.8)
            w_p = math.ceil(pl["total_weight"] / 1000)
            pl["total_n_pallet"] = max(c_p, w_p)
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

    def handle_container_invoice_edit_get(
        self, container_number: str
    ) -> tuple[Any, Any]:
        invoice = Invoice.objects.select_related("customer", "container_number").get(
            container_number__container_number=container_number
        )
        invoice_item = InvoiceItem.objects.filter(
            invoice_number__invoice_number=invoice.invoice_number
        )
        context = {
            "invoice": invoice,
            "invoice_item": invoice_item,
        }
        return self.template_invoice_container_edit, context

    def handle_container_invoice_delete_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        invoice_number = request.GET.get("invoice_number")
        invoice = Invoice.objects.select_related("container_number").get(
            invoice_number=invoice_number
        )
        invoice_item = InvoiceItem.objects.filter(
            invoice_number__invoice_number=invoice_number
        )
        container_number = invoice.container_number.container_number
        # delete file from sharepoint
        try:
            self._delete_file_from_sharepoint(
                "invoice", f"INVOICE-{container_number}.xlsx"
            )
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
        pallet_data = (
            Order.objects.select_related(
                "container_number",
                "customer_name",
                "warehouse",
                "offload_id",
                "retrieval_id",
            )
            .filter(
                models.Q(offload_id__offload_required=True)
                & models.Q(offload_id__offload_at__isnull=False)
                & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
                & models.Q(offload_id__offload_at__gte=start_date)
                & models.Q(offload_id__offload_at__lte=end_date)
            )
            .order_by("offload_id__offload_at")
        )
        data = [
            {
                "货柜号": d.container_number.container_number,
                "客户": d.customer_name.zem_name,
                "入仓仓库": d.warehouse.name,
                "柜型": d.container_number.container_type,
                "拆柜完成时间": d.offload_id.offload_at.strftime("%Y-%m-%d %H:%M:%S"),
                "打板数": d.offload_id.total_pallet,
            }
            for d in pallet_data
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = (
            f"attachment; filename=pallet_data_{start_date}_{end_date}.xlsx"
        )
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
            }
            for d in context["pl_data"]
        ]
        df = pd.DataFrame.from_records(data)
        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = (
            f"attachment; filename=packing_list_data_{start_date}_{end_date}.xlsx"
        )
        df.to_excel(excel_writer=response, index=False, columns=df.columns)
        return response

    def handle_invoice_order_search_post(
        self, request: HttpRequest, status
    ) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        order_form = OrderForm(request.POST)
        warehouse = request.POST.get("warehouse_filter")
        if order_form.is_valid():
            customer = order_form.cleaned_data.get("customer_name")
        else:
            customer = None
        if status == "direct":
            return self.handle_invoice_direct_get(
                request, start_date, end_date, customer, warehouse
            )
        elif status == "combina":
            return self.handle_invoice_combina_get(
                request, start_date, end_date, customer, warehouse
            )
        elif status == "preport":
            return self.handle_invoice_preport_get(
                request, start_date, end_date, customer, warehouse
            )
        elif status == "warehouse":
            return self.handle_invoice_warehouse_get(
                request, start_date, end_date, customer, warehouse
            )
        elif status == "delivery":
            return self.handle_invoice_delivery_get(
                request, start_date, end_date, customer, warehouse
            )
        elif status == "confirm":
            start_date_confirm = request.POST.get("start_date_confirm")
            end_date_confirm = request.POST.get("end_date_confirm")
            return self.handle_invoice_confirm_get(
                request, start_date_confirm, end_date_confirm, customer, warehouse
            )
        elif status == "search":
            return self.handle_invoice_search_get(
                request, start_date, end_date, customer, warehouse
            )
        else:
            return self.handle_invoice_get(start_date, end_date, customer)

    def handle_invoice_order_select_post(self, request: HttpRequest) -> HttpResponse:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        invoice_type = request.POST.get("invoice_type")
        if selected_orders:
            order = Order.objects.select_related(
                "customer_name", "container_number", "invoice_id"
            ).filter(container_number__container_number__in=selected_orders)
            order_id = [o.id for o in order]
            customer = order[0].customer_name
            current_date = datetime.now().date().strftime("%Y-%m-%d")
            invoice_statement_id = (
                f"{current_date.replace('-', '')}S{customer.id}{max(order_id)}"
            )
            context = {
                "order": order,
                "customer": customer,
                "invoice_statement_id": invoice_statement_id,
                "current_date": current_date,
                "invoice_type":invoice_type
            }
            return render(request, self.template_invoice_statement, context)
        else:
            template, context = self.handle_invoice_order_search_post(request)
            return render(request, template, context)

    def handle_create_container_invoice_post(
        self, request: HttpRequest
    ) -> HttpResponse:
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
            "data": zip(
                description, warehouse_code, cbm, weight, qty, rate, amount, note
            ),
        }
        workbook, invoice_data = self._generate_invoice_excel(context)
        invoice = Invoice(
            **{
                "invoice_number": invoice_data["invoice_number"],
                "invoice_date": invoice_data["invoice_date"],
                "invoice_link": invoice_data["invoice_link"],
                "customer": context["order"].customer_name,
                "container_number": context["order"].container_number,
                "receivable_total_amount": invoice_data["total_amount"],
            }
        )
        invoice.save()
        order.invoice_id = invoice
        order.save()
        invoice_item_data = []
        for d, wc, c, w, q, r, a, n in zip(
            description, warehouse_code, cbm, weight, qty, rate, amount, note
        ):
            invoice_item_data.append(
                {
                    "invoice_number": invoice,
                    "description": d,
                    "warehouse_code": wc,
                    "cbm": c if c else None,
                    "weight": w if w else None,
                    "qty": q if q else None,
                    "rate": r if r else None,
                    "amount": a if a else None,
                    "note": n if n else "",
                }
            )
        invoice_item_instances = [
            InvoiceItem(**inv_itm_data) for inv_itm_data in invoice_item_data
        ]
        bulk_create_with_history(invoice_item_instances, InvoiceItem)
        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = (
            f"attachment; filename=INVOICE-{container_number}.xlsx"
        )
        workbook.save(response)
        return response

    def handle_export_invoice_post(self, request: HttpRequest) -> HttpResponse:
        resp, file_name, pdf_file, context = export_invoice(request)
        pdf_file.seek(0)
        invoice = Invoice.objects.select_related("statement_id").filter(
            models.Q(container_number__container_number__in=context["container_number"])
        )
        invoice_statement = InvoiceStatement.objects.filter(
            models.Q(
                invoice__container_number__container_number__in=context[
                    "container_number"
                ]
            )
        )
        for invc_stmt in invoice_statement.distinct():
            try:
                self._delete_file_from_sharepoint(
                    "invoice_statement",
                    f"invoice_{invc_stmt.invoice_statement_id}_from_ZEM_ELITELINK LOGISTICS_INC.pdf",
                )
            except:
                pass
        invoice_statement.delete()
        link = self._upload_excel_to_sharepoint(
            pdf_file, "invoice_statement", file_name
        )
        invoice_statement = InvoiceStatement(
            **{
                "invoice_statement_id": context["invoice_statement_id"],
                "statement_amount": context["total_amount"],
                "statement_date": context["invoice_date"],
                "due_date": context["due_date"],
                "invoice_terms": context["invoice_terms"],
                "customer": Customer.objects.get(accounting_name=context["customer"]),
                "statement_link": link,
            }
        )
        invoice_statement.save()
        for invc in invoice:
            invc.statement_id = invoice_statement
        bulk_update_with_history(invoice, Invoice, fields=["statement_id"])
        return resp

    def handle_container_invoice_edit_post(self, request: HttpRequest) -> HttpResponse:
        invoice_number = request.POST.get("invoice_number")
        invoice = Invoice.objects.select_related("container_number").get(
            invoice_number=invoice_number
        )
        container_number = invoice.container_number.container_number
        description = request.POST.getlist("description")
        warehouse_code = request.POST.getlist("warehouse_code")
        cbm = request.POST.getlist("cbm")
        weight = request.POST.getlist("weight")
        qty = request.POST.getlist("qty")
        rate = request.POST.getlist("rate")
        amount = request.POST.getlist("amount")
        note = request.POST.getlist("note")
        order = Order.objects.select_related("customer_name").get(
            invoice_id__invoice_number=invoice_number
        )
        context = {
            "order": order,
            "container_number": container_number,
            "data": zip(
                description, warehouse_code, cbm, weight, qty, rate, amount, note
            ),
        }

        # delete old file from sharepoint
        try:
            self._delete_file_from_sharepoint(
                "invoice", f"INVOICE-{container_number}.xlsx"
            )
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
        invoice.receivable_total_amount = invoice_data["total_amount"]
        invoice.save()
        # update invoice item information
        InvoiceItem.objects.filter(
            invoice_number__invoice_number=invoice_number
        ).delete()
        invoice_item_data = []
        for d, wc, c, q, r, a, n in zip(
            description, warehouse_code, cbm, qty, rate, amount, note
        ):
            invoice_item_data.append(
                {
                    "invoice_number": invoice,
                    "description": d,
                    "warehouse_code": wc,
                    "cbm": c if c else None,
                    "qty": q if q else None,
                    "rate": r if r else None,
                    "amount": a if a else None,
                    "note": n if n else None,
                }
            )
        invoice_item_instances = [
            InvoiceItem(**inv_itm_data) for inv_itm_data in invoice_item_data
        ]
        bulk_create_with_history(invoice_item_instances, InvoiceItem)
        # export new file
        response = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = (
            f"attachment; filename=INVOICE-{container_number}.xlsx"
        )
        workbook.save(response)
        return response

    def handle_invoice_order_batch_reject(self, request: HttpRequest) -> HttpResponse:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        invoice_status = InvoiceStatus.objects.filter(container_number__container_number__in=selected_orders)
        for item in invoice_status:
            item.stage = 'tobeconfirmed'
            item.save()
        return self.handle_invoice_confirm_get(request,request.POST.get("start_date_confirm"),request.POST.get("end_date_confirm"))

    def handle_invoice_order_batch_delivered(self, request: HttpRequest) -> HttpResponse:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        invoices = Invoice.objects.prefetch_related(
            "order", "order__container_number", "container_number"
        ).filter(container_number__container_number__in=selected_orders)
        for invoice in invoices:
            invoice.is_invoice_delivered = True
            invoice.save()
        return self.handle_invoice_confirm_get(request,request.POST.get("start_date_confirm"),request.POST.get("end_date_confirm"))
    
    def handle_invoice_order_batch_export(self, request: HttpRequest) -> HttpResponse:
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        orders = Order.objects.select_related(
            "retrieval_id", "container_number"
        ).filter(container_number__container_number__in=selected_orders)
        invoices = Invoice.objects.prefetch_related(
            "order", "order__container_number", "container_number"
        ).filter(container_number__container_number__in=selected_orders)
        data = [
            (
                order,
                invoices.get(
                    container_number__container_number=order.container_number.container_number
                ),
            )
            for order in orders
        ]
        invoice_type = request.POST.get("invoice_type")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for order, invoice in data:
                context = self._parse_invoice_excel_data(order, invoice,invoice_type)
                workbook, _ = self._generate_invoice_excel(
                    context, save_to_sharepoint=False
                )
                excel_file = io.BytesIO()
                workbook.save(excel_file)
                excel_file.seek(0)  # Go to the beginning of the in-memory file
                zip_file.writestr(
                    f"INVOICE-{order.container_number.container_number}.xlsx",
                    excel_file.read(),
                )
        zip_buffer.seek(0)
        response = HttpResponse(zip_buffer.read(), content_type="application/zip")
        response["Content-Disposition"] = 'attachment; filename="invoices.zip"'
        return response

    def _generate_invoice_excel(
        self,
        context: dict[Any, Any],
        save_to_sharepoint: bool = True,
    ) -> tuple[openpyxl.workbook.Workbook, dict[Any, Any]]:
        current_date = datetime.now().date()
        order_id = str(context["order"].id)
        customer_id = context["order"].customer_name.id
        invoice_number = f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}"
        workbook = openpyxl.Workbook()  # 创建一个工作簿对象
        worksheet = workbook.active  # 获取工作簿的活动工作表
        worksheet.title = "Sheet1"  # 给表命名
        cells_to_merge = [  # 要合并的单元格
            "A1:E1",
            "A3:A4",
            "B3:D3",
            "B4:D4",
            "E3:E4",
            "F3:I4",
            "A5:A6",
            "B5:D5",
            "B6:D6",
            "E5:E6",
            "F5:I6",
            "A9:B9",
            "A10:B10",
            "F1:I1",
            "C1:E1",
            "A2:I2",
            "A7:I7",
            "A8:I8",
            "C9:I9",
            "C10:I10",
            "A11:I11",
        ]
        self._merge_ws_cells(worksheet, cells_to_merge)  # 进行合并

        worksheet.column_dimensions["A"].width = 18
        worksheet.column_dimensions["B"].width = 18
        worksheet.column_dimensions["C"].width = 15
        worksheet.column_dimensions["D"].width = 7
        worksheet.column_dimensions["E"].width = 8
        worksheet.column_dimensions["F"].width = 7
        worksheet.column_dimensions["G"].width = 11
        worksheet.column_dimensions["H"].width = 11
        worksheet.column_dimensions["I"].width = 11
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
        worksheet["F3"] = current_date.strftime("%Y-%m-%d")
        worksheet["E5"] = "Invoice #"
        worksheet["F5"] = invoice_number

        worksheet["A1"].font = Font(size=20)
        worksheet["F1"].font = Font(size=28)
        worksheet["A3"].alignment = Alignment(vertical="center")
        worksheet["A5"].alignment = Alignment(vertical="center")
        worksheet["E3"].alignment = Alignment(vertical="center")
        worksheet["E5"].alignment = Alignment(vertical="center")
        worksheet["F3"].alignment = Alignment(vertical="center")
        worksheet["F5"].alignment = Alignment(vertical="center")

        worksheet.append(
            [
                "CONTAINER #",
                "DESCRIPTION",
                "WAREHOUSE CODE",
                "CBM",
                "WEIGHT",
                "QTY",
                "RATE",
                "AMOUNT",
                "NOTE",
            ]
        )  # 添加表头
        invoice_item_starting_row = 12
        invoice_item_row_count = 0
        row_count = 13
        total_amount = 0.0
        total_cbm = 0.0
        total_weight = 0.0
        for d, wc, cbm, weight, qty, r, amt, n in context["data"]:
            worksheet.append(
                [context["container_number"], d, wc, cbm, weight, qty, r, amt, n]
            )  # 添加数据
            total_amount += float(amt)  # 计算总金额
            total_cbm += float(cbm) if cbm else 0
            total_weight += float(weight) if weight else 0
            row_count += 1
            invoice_item_row_count += 1
        worksheet.append(
            [
                "Total",
                None,
                None,
                total_cbm,
                total_weight,
                None,
                None,
                total_amount,
                None,
            ]
        )  # 工作表末尾添加总金额
        invoice_item_row_count += 1
        for row in worksheet.iter_rows(  # 单元格设置样式
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
        self._merge_ws_cells(worksheet, [f"A{row_count}:I{row_count}"])
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
            self._merge_ws_cells(worksheet, [f"A{row_count}:I{row_count}"])
            row_count += 1
        self._merge_ws_cells(worksheet, [f"A{row_count}:I{row_count}"])

        excel_file = io.BytesIO()  # 创建一个BytesIO对象
        workbook.save(excel_file)  # 将workbook保存到BytesIO中
        excel_file.seek(0)  # 将文件指针移动到文件开头
        if save_to_sharepoint:
            invoice_link = self._upload_excel_to_sharepoint(
                excel_file, "invoice", f"INVOICE-{context['container_number']}.xlsx"
            )
        else:
            invoice_link = ""

        worksheet["A9"].font = Font(color="00FFFFFF")
        worksheet["A9"].fill = PatternFill(
            start_color="00000000", end_color="00000000", fill_type="solid"
        )
        invoice_data = {
            "invoice_number": invoice_number,
            "invoice_date": current_date.strftime("%Y-%m-%d"),
            "invoice_link": invoice_link,
            "total_amount": total_amount,
        }
        return workbook, invoice_data

    def _merge_ws_cells(
        self, ws: openpyxl.worksheet.worksheet, cells: list[str]
    ) -> None:
        for c in cells:
            ws.merge_cells(c)

    def _parse_invoice_excel_data(
        self, order: Order, invoice: Invoice, invoice_type: str
    ) -> dict[str, Any]:
        description = []
        warehouse_code = []
        cbm = []
        weight = []
        qty = []
        rate = []
        amount = []
        note = []
        if order.order_type == "直送":
            invoice_preport = InvoicePreport.objects.get(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
            )
            for field in invoice_preport._meta.fields:
                if isinstance(field, models.FloatField) and field.name != "amount":
                    value = getattr(invoice_preport, field.name)
                    if value not in [None, 0]:
                        if field.verbose_name == "操作处理费":
                            description.append("等待费")
                        else:
                            description.append(field.verbose_name)
                        surcharge = invoice_preport.surcharges.get(field.name, 0)
                        if surcharge > 0:
                            value += surcharge
                            note.append(
                                f"{invoice_preport.surcharge_notes.get(field.name)}: ${surcharge}"
                            )
                        else:
                            note.append("")
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append(invoice_preport.qty[field.name])
                        rate.append(invoice_preport.rate[field.name])
                        amount.append(value)

            for k, v in invoice_preport.other_fees.items():
                description.append(k)
                amount.append(v)
                warehouse_code.append("")
                cbm.append("")
                weight.append("")
                qty.append(1)
                rate.append(v)
                note.append("")
        else:
            invoice_preport = InvoicePreport.objects.get(
                invoice_number__invoice_number=invoice.invoice_number,
                invoice_type=invoice_type,
            )
            invoice_warehouse = InvoiceWarehouse.objects.filter(
                invoice_number__invoice_number=invoice.invoice_number
            )
            invoice_delivery = InvoiceDelivery.objects.filter(
                invoice_number__invoice_number=invoice.invoice_number
            )
            
            for field in invoice_preport._meta.fields:
                if isinstance(field, models.FloatField) and field.name != "amount":
                    value = getattr(invoice_preport, field.name)
                    if value not in [None, 0]:
                        description.append(field.verbose_name)
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append(invoice_preport.qty[field.name])
                        rate.append(invoice_preport.rate[field.name])
                        amount.append(value)
                        note.append("")
            for k, v in invoice_preport.other_fees.items():
                if v not in [None, 0]:
                    description.append(k)
                    amount.append(v)
                    warehouse_code.append("")
                    cbm.append("")
                    weight.append("")
                    qty.append(1)
                    rate.append(v)
                    note.append("")
            for warehouse in invoice_warehouse:
                for field in warehouse._meta.fields:
                    if isinstance(field, models.FloatField) and field.name != "amount":
                        value = getattr(warehouse, field.name)
                        if value not in [None, 0]:
                            description.append(field.verbose_name)
                            warehouse_code.append("")
                            cbm.append("")
                            weight.append("")
                            qty.append(warehouse.qty[field.name])
                            rate.append(warehouse.rate[field.name])
                            amount.append(value)
                            note.append("")
            for warehouse in invoice_warehouse:
                for k, v in warehouse.other_fees.items():
                    if v not in [None, 0]:
                        description.append(k)
                        warehouse_code.append("")
                        cbm.append("")
                        weight.append("")
                        qty.append(1)
                        rate.append(v)
                        amount.append(v)
                        note.append("")
            for delivery in invoice_delivery:
                if delivery.total_cost is None:
                    raise ValueError('派送费为空')
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
            "container_number": order.container_number.container_number,
            "data": zip(
                description, warehouse_code, cbm, weight, qty, rate, amount, note
            ),
        }
        return context

    def _get_sharepoint_auth(self) -> ClientContext:
        return ClientContext(SP_URL).with_credentials(UserCredential(SP_USER, SP_PASS))

    def _upload_excel_to_sharepoint(
        self, file: BytesIO, schema: str, file_name: str
    ) -> str:
        conn = self._get_sharepoint_auth()
        file_path = os.path.join(SP_DOC_LIB, f"{SYSTEM_FOLDER}/{schema}/{APP_ENV}")
        sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(f"{file_name}", file).execute_query()
        link = (
            resp.share_link(SharingLinkKind.OrganizationView)
            .execute_query()
            .value.to_json()["sharingLinkInfo"]["Url"]
        )
        return link

    def _delete_file_from_sharepoint(
        self,
        schema: str,
        file_name: str,
    ) -> None:
        conn = self._get_sharepoint_auth()
        file_path = os.path.join(
            SP_DOC_LIB, f"{SYSTEM_FOLDER}/{schema}/{APP_ENV}/{file_name}"
        )
        try:
            conn.web.get_file_by_server_relative_url(
                file_path
            ).delete_object().execute_query()
        except ClientRequestException as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(e)
            else:
                raise RuntimeError(e)

    # 按照权限分组，有三个分组：客服组（添加账单详情）、组长组（确认客服组操作）、财务组（确认账单）
    def _validate_user_group(self, user: User) -> bool:
        if user.groups.filter(name=self.allowed_group).exists():
            return True
        else:
            return False

    def _validate_user_invoice_direct(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_direct").exists():
            return True
        else:
            return False
    
    def _validate_user_invoice_combina(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_combina").exists():
            return True
        else:
            return False

    def _validate_user_invoice_preport(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_preport").exists():
            return True
        elif user.groups.filter(name="invoice_preport_leader").exists():
            return True
        else:
            return False

    def _validate_user_invoice_warehouse(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_warehouse").exists():
            return True
        else:
            return False

    def _validate_user_invoice_delivery(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_delivery").exists():
            return True
        else:
            return False

    def _validate_user_invoice_confirm(self, user: User) -> bool:
        if user.is_staff or user.groups.filter(name="invoice_confirm").exists():
            return True
        else:
            return False

    def _check_invoice_exist(self, container_number: str) -> bool:
        return Invoice.objects.filter(
            container_number__container_number=container_number
        ).exists()
