import ast
import io
import json
import math
import os
import re
import json
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, time as datetime_time
from io import BytesIO
from itertools import chain, groupby
from operator import attrgetter

from asgiref.sync import sync_to_async, async_to_sync

from django.db import transaction
from django.db.models.fields.json import KeyTextTransform
from django.core.paginator import Paginator

from typing import Any, Dict, List, Tuple

import openpyxl
import openpyxl.workbook
import openpyxl.worksheet
import openpyxl.worksheet.worksheet
import pandas as pd
import pytz
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User
from django.contrib.postgres.aggregates import ArrayAgg, StringAgg
from django.core.paginator import Paginator
from django.db import models, transaction
from django.db.models import (
    BooleanField,
    Case,
    CharField,
    Count,
    Exists,
    F,
    FloatField,
    IntegerField,
    OuterRef,
    Prefetch,
    Subquery,
    Sum,
    Value,
    When,
    Q
)
import logging
import traceback
from decimal import Decimal
from django.utils.safestring import mark_safe

from django.db import transaction
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast, Coalesce
from django.db.models.query import QuerySet
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden, QueryDict
from django.shortcuts import render
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side, numbers
from openpyxl.utils.dataframe import dataframe_to_rows
from simple_history.utils import bulk_create_with_history, bulk_update_with_history
from sqlalchemy.util import await_only

from warehouse.forms.order_form import OrderForm
from warehouse.models.container import Container
from warehouse.models.shipment import Shipment
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoicev2 import (
    Invoicev2,
    InvoiceItemv2,
    InvoiceStatusv2,
)
from warehouse.models.invoice_details import (
    InvoiceDelivery,
    InvoicePreport,
    InvoiceWarehouse,
)
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.retrieval import Retrieval
from warehouse.models.transaction import Transaction
from warehouse.views.quote_management import QuoteManagement
from warehouse.forms.order_form import OrderForm
from warehouse.utils.constants import (
    ACCT_ACH_ROUTING_NUMBER,
    ACCT_BANK_NAME,
    ACCT_BENEFICIARY_ACCOUNT,
    ACCT_BENEFICIARY_ADDRESS,
    ACCT_BENEFICIARY_NAME,
    ACCT_SWIFT_CODE,
    APP_ENV,
    CARRIER_FLEET,
    CONTAINER_PICKUP_CARRIER,
    SP_DOC_LIB,
    SP_CLIENT_ID,
    SP_DOC_LIB,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    SYSTEM_FOLDER,
    DELIVERY_METHOD_OPTIONS
)
from warehouse.views.export_file import export_invoice


class ReceivableAccounting(View):
    template_progress_overview = "receivable_accounting/progress_overview.html"
    template_alert_monitoring = "receivable_accounting/alert_monitoring.html"

    template_preport_entry = "receivable_accounting/preport_entry.html"
    template_preport_edit= "receivable_accounting/preport_edit.html"
    template_warehouse_entry = "receivable_accounting/warehouse_entry.html"
    template_warehouse_edit = "receivable_accounting/warehouse_edit.html"

    template_delivery_entry = "receivable_accounting/delivery_entry.html"
    template_delivery_public_edit = "receivable_accounting/delivery_public__edit.html"
    template_delivery_other_edit = "receivable_accounting/delivery_other_edit.html"

    template_pending_confirmation = "receivable_accounting/pending_confirmation.html"
    template_completed_bills = "receivable_accounting/completed_bills.html"
    template_supplementary = "receivable_accounting/supplementary.html"
    template_financial_statistics = "receivable_accounting/financial_statistics.html"
    template_quotation_management = "receivable_accounting/quotation_management.html"
    
    allowed_group = "accounting"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "LA-91748": "LA-91748",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
        "LA-91789": "LA-91789",
        "直送": "直送",
    }
    CATEGORY_STATUS_FIELD = {
        "preport": "preport_status",
        "warehouse_public": "warehouse_public_status",
        "warehouse_other": "warehouse_other_status",
        "delivery_public": "delivery_public_status",
        "delivery_other": "delivery_other_status",
    }

    def get(self, request: HttpRequest) -> HttpResponse:
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.GET.get("step", None)
        if step == "progress":  #账单进度
            template, context = self.handle_progress_overview_get()
            return render(request, template, context)
        elif step == "alert":  #预警监控
            template, context = self.handle_alert_monitoring_get()
            return render(request, template, context)
        elif step == "preport":  #港前
            context = {"warehouse_options": self.warehouse_options,"order_form": OrderForm()}
            return render(request, self.template_preport_entry, context)
        elif step == "warehouse":  #库内
            context = {"warehouse_options": self.warehouse_options,"order_form": OrderForm()}
            return render(request, self.template_warehouse_entry, context)    
        elif step == "delivery":  # 派送
            context = {"warehouse_options": self.warehouse_options,"order_form": OrderForm()}
            return render(request, self.template_delivery_entry, context)
        
        elif step == "delivery_other":  # 私仓派送
            context = {"warehouse_options": self.warehouse_options,"order_form": OrderForm()}
            return render(request, self.template_self_delivery_entry, context)
        elif step == "confirm":  # 财务确认
            template, context = self.handle_pending_confirmation_get(request)
            return render(request, template, context)
        elif step == "completed": #历史完成账单
            template, context = self.handle_completed_bills_get(request)
            return render(request, template, context)
        elif step == "supplementary": #补开账单
            template, context = self.handle_supplementary_get(request)
            return render(request, template, context)
        elif step == "finance_stats": #财务统计分析
            template, context = self.handle_financial_statistics_get(request)
            return render(request, template, context)
        elif step == "quotation_management": #报价表管理
            quotes = QuotationMaster.objects.filter(quote_type="receivable")
            context = {"order_form": OrderForm(), "quotes": quotes}
            return render(request, self.template_quotation_management, context)  
        elif step == "container_preport":
            context = self.handle_container_preport_post(request)
            return render(request, self.template_preport_edit, context)
        elif step == "container_warehouse":
            context = self.handle_container_warehouse_post(request)
            return render(request, self.template_warehouse_edit, context)       
        elif step == "container_delivery":
            template, context = self.handle_container_delivery_post(request)
            return render(request, template, context)   
        else:
            raise ValueError(f"unknow request {step}")

    def post(self, request: HttpRequest) -> HttpResponse:
        # if not self._validate_user_group(request.user):
        #     return HttpResponseForbidden("You are not authenticated to access this page!")
        step = request.POST.get("step", None)
        if step == "preport_search":  #港前
            context = self.handle_preport_entry_post(request)
            return render(request, self.template_preport_entry, context)
        elif step == "warehouse_search":  #库内
            context = self.handle_warehouse_entry_post(request)
            return render(request, self.template_warehouse_entry, context)
        elif step == "delivery_search":
            context = self.handle_delivery_entry_post(request)
            return render(request, self.template_delivery_entry, context)
        elif step == "release_hold":
            template, context = self.handle_release_hold_post(request)
            return render(request, template, context) 
        elif step == "save_single":
            template, context = self.handle_save_single_post(request)
            return render(request, template, context)
        elif step == "save_all":
            context = self.handle_save_all_post(request)
            return render(request, self.template_delivery_entry, context)
        elif step == "preport_save":
            context = self.handle_invoice_preport_save(request)
            return render(request, self.template_preport_entry, context)
        elif step == "modify_order_type":
            context = self.handle_modify_order_type(request)
            return render(request, self.template_preport_edit, context)
        elif step == "warehouse_save":
            context = self.handle_invoice_warehouse_save(request)
            return render(request, self.template_warehouse_entry, context)
    
    def handle_save_all_post(self, request: HttpRequest):
        """处理解扣操作"""
        context = {}
        container_number = request.POST.getlist("container_number")[0]
        invoice_id = request.POST.getlist("invoice_id")[0]
        delivery_type = request.POST.get("delivery_type")
        if delivery_type == "other":
            item_category = "delivery_other"
        else:
            item_category = "delivery_public"
        items_data_json = request.POST.get("items_data")
        if not items_data_json:
            context.update({"error_messages": "没有接收到数据"})
            return self.handle_delivery_entry_post(request, context)
        
        try:
            items_data = json.loads(items_data_json)
        except json.JSONDecodeError as e:
            context.update({"error_messages": f"数据格式错误: {str(e)}"})
            return self.handle_delivery_entry_post(request, context)
        
        success_count = 0
        error_messages = []
        
        try:
            container = Container.objects.get(container_number=container_number)
            invoice = Invoicev2.objects.get(id=invoice_id)
        except Container.DoesNotExist:
            context.update({"error_messages": f"柜号 {container_number} 不存在"})
            return self.handle_delivery_entry_post(request, context)
        except Invoicev2.DoesNotExist:
            context.update({"error_messages": f"账单ID {invoice_id} 不存在"})
            return self.handle_delivery_entry_post(request, context)
        
        # 遍历每条数据
        for item_data in items_data:
            try:
                # 提取数据
                delivery_category = item_data.get("delivery_category", "")
                if not delivery_category:
                    error_messages.append(f"第{row_index + 1}行: 派送类型不能为空")
                    continue
                #暂扣的不记录
                if delivery_category == "hold":
                    continue
                row_index = item_data.get("rowIndex")
                item_id = item_data.get("item_id")
                po_id = item_data.get("po_id", "")
                destination = item_data.get("destination", "")
                
                rate = item_data.get("rate")
                pallets = item_data.get("pallets")
                surcharges = item_data.get("surcharges")
                amount = item_data.get("amount")
                note = item_data.get("note", "")
                
                if not po_id:
                    error_messages.append(f"第{row_index + 1}行: PO号不能为空")
                    continue                 
         
                # 转换数据类型
                def to_float(val):
                    if val is None or val == "":
                        return None
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None
                
                rate_float = to_float(rate)
                pallets_float = to_float(pallets)
                surcharges_float = to_float(surcharges)
                amount_float = to_float(amount)
                
                # 更新或创建记录
                if item_id:
                    # 更新现有记录
                    try:
                        item = InvoiceItemv2.objects.get(id=item_id)
                    except InvoiceItemv2.DoesNotExist:
                        error_messages.append(f"第{row_index + 1}行: 未查询到ID为 {item_id} 的记录")
                        continue
                else:
                    # 新建记录
                    item = InvoiceItemv2(
                        container_number=container,
                        invoice_number=invoice,
                        invoice_type="receivable",
                        item_category=item_category,
                        PO_ID=po_id,
                    )
                
                # 更新字段
                item.delivery_type = delivery_category
                item.invoice_number = invoice
                item.container_number = container
                item.PO_ID = po_id
                item.rate = rate_float
                item.qty = pallets_float
                item.surcharges = surcharges_float
                item.amount = amount_float
                item.description = note
                item.warehouse_code = destination
                
                # 保存
                item.save()
                success_count += 1
                
            except Exception as e:
                error_messages.append(f"第{row_index + 1}行处理失败: {str(e)}")
                continue
        
        # 准备返回消息
        success_messages = []
        if success_count > 0:
            success_messages.append(f"{container_number}成功保存 {success_count} 条记录")
        
        # 更新上下文
        if success_messages:
            context.update({"success_messages": success_messages})
        if error_messages:
            context.update({"error_messages": error_messages})

        container_delivery_type = getattr(container, 'delivery_type', 'mixed')

        status_obj = InvoiceStatusv2.objects.get(
                invoice=invoice,
                invoice_type='receivable'
            )
        # 根据柜子类型自动更新另一边的状态
        if delivery_type == "public" and container_delivery_type == "public":
            status_obj.delivery_other_status = "completed"

        elif delivery_type == "other" and container_delivery_type == "other":
            status_obj.delivery_public_status = "completed"
            
        status_obj.save()
        return self.handle_delivery_entry_post(request, context)
    
    def handle_save_single_post(self, request: HttpRequest):
        """处理解扣操作"""
        context = {}
        container_number = request.POST.get("container_number")
        invoice_id = request.POST.get("invoice_id")
        item_id = request.POST.get("item_id")
        po_id = request.POST.get("po_id")
        delivery_category = request.POST.get("delivery_category")
        delivery_type = request.POST.get("delivery_type")
        if delivery_type == "other":
            item_category = "delivery_other"
        else:
            item_category = "delivery_public"

        rate = request.POST.get("rate") or None
        pallets = request.POST.get("pallets") or None
        surcharges = request.POST.get("surcharges") or None
        amount = request.POST.get("amount") or None
        note = request.POST.get("note") or ""
        destination = request.POST.get("destination") or ""
        
        note = request.POST.get("note") or ""

        # 转类型
        def to_float(val):
            try:
                return float(val)
            except:
                return None

        rate = to_float(rate)
        pallets = to_float(pallets)
        surcharges = to_float(surcharges)
        amount = to_float(amount)

        # 外键对象
        container = Container.objects.get(container_number=container_number)
        invoice = Invoicev2.objects.get(id=invoice_id)

        if item_id:
            # 更新
            try:
                item = InvoiceItemv2.objects.get(id=item_id)
            except InvoiceItemv2.DoesNotExist:
                success_messages = f"未查询到invoice_item的id为 {item_id}的记录"
                context.update({"error_messages":success_messages})
                return self.handle_container_delivery_post(request,context)
        else:
            # 新建
            item = InvoiceItemv2(
                container_number=container,
                invoice_number=invoice,
                invoice_type="receivable",
                item_category=item_category,
                PO_ID=po_id,
            )
           
        item.delivery_type = delivery_category
        item.invoice_number = invoice
        item.container_number = container
        item.PO_ID = po_id

        item.rate = rate
        item.qty = pallets
        item.surcharges = surcharges
        item.amount = amount
        item.description = note
        item.warehouse_code = destination

        # 保存
        item.save()

        # 构造新的 GET 查询参数
        get_params = QueryDict(mutable=True)
        get_params["container_number"] = container_number
        get_params["delivery_type"] = delivery_type
        get_params["invoice_id"] = invoice_id

        request.GET = get_params
        return self.handle_container_delivery_post(request,context)
    
    def handle_release_hold_post(self, request: HttpRequest):
        """处理解扣操作"""
        po_id = request.POST.get("po_id")
        container_number = request.POST.get("container_number")
        delivery_method = request.POST.get("delivery_method")
        # 更新托盘状态，移除暂扣标记
        qs = Pallet.objects.filter(
            container_number__container_number=container_number,
            PO_ID=po_id
        )

        updated = qs.update(delivery_method=delivery_method)

        delivery_type = request.POST.get("delivery_type")
        invoice_id = request.POST.get("invoice_id")

        # 构造新的 GET 查询参数
        get_params = QueryDict(mutable=True)
        get_params["container_number"] = container_number
        get_params["delivery_type"] = delivery_type
        get_params["invoice_id"] = invoice_id

        request.GET = get_params
        success_messages = f"解扣成功！共更新 {updated} 个板子派送方式为{delivery_method}"
        context = {"success_messages":success_messages}
        return self.handle_container_delivery_post(request,context)

    def handle_preport_entry_post(self, request:HttpRequest, context: dict| None = None,) -> Dict[str, Any]:
        warehouse = request.POST.get("warehouse_filter")
        customer = request.POST.get("customer")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")

        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date

        criteria = (
            Q(cancel_notification=False)
            & (Q(order_type="转运") | Q(order_type="转运组合"))
            & Q(vessel_id__vessel_etd__gte=start_date)
            & Q(vessel_id__vessel_etd__lte=end_date)
            & Q(offload_id__offload_at__isnull=False)
        )

        if warehouse:
            criteria &= Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= Q(customer_name__zem_name=customer)

        # 获取基础订单数据
        base_orders = (
            Order.objects
            .select_related(
                'retrieval_id', 
                'offload_id', 
                'container_number',
                'customer_name'
            )
            .annotate(
                retrieval_time=F("retrieval_id__actual_retrieval_timestamp"),
                empty_returned_time=F("retrieval_id__empty_returned_at"),
                offload_time=F("offload_id__offload_at"),
            )
            .filter(criteria)
            .order_by("-retrieval_time")
            .distinct()
        )

        preport_to_record_orders = [] #待录入
        preport_recorded_orders = []  #已录入
        preport_pending_review_orders = []  #待审核
        preport_completed_orders = []  #已审核

        for order in base_orders:
            container = order.container_number
            
            if not container:
                continue
                
            # 查询这个柜子的所有应收账单
            invoices = Invoicev2.objects.filter(container_number=container)
            
            if not invoices.exists():
                # 没有账单的情况 - 归到待录入
                order_data = {
                    'order': order,
                    'invoice_number': None,
                    'invoice_id': None,
                    'invoice_created_at': None,
                    'preport_status': None,
                    'finance_status': None,
                    'has_invoice': False
                }
                preport_to_record_orders.append(order_data)
            else:
                # 有账单的情况 - 每个账单都要单独处理
                for invoice in invoices:
                    # 查询这个账单对应的状态
                    try:
                        status_obj = InvoiceStatusv2.objects.get(
                            invoice=invoice,
                            invoice_type='receivable'
                        )
                        preport_status = status_obj.preport_status
                        finance_status = status_obj.finance_status
                    except InvoiceStatusv2.DoesNotExist:
                        preport_status = None
                        finance_status = None
                    
                    order_data = {
                        'order': order,
                        'invoice_number': invoice.invoice_number,
                        'invoice_id': invoice.id,
                        'invoice_created_at': invoice.created_at if hasattr(invoice, 'created_at') else invoice.history.first().history_date if invoice.history.exists() else None,
                        'preport_status': preport_status,
                        'finance_status': finance_status,
                        'has_invoice': True
                    }
                    
                    # 根据状态分组
                    if preport_status in ["unstarted", "in_progress"] or preport_status is None:
                        preport_to_record_orders.append(order_data)
                    elif preport_status == "pending_review":
                        preport_pending_review_orders.append(order_data)
                        preport_recorded_orders.append(order_data)
                    elif preport_status == "completed":
                        preport_completed_orders.append(order_data)
                        preport_recorded_orders.append(order_data)
                    elif preport_status == "rejected":
                        preport_recorded_orders.append(order_data)

        # 对已录入的订单按状态排序（rejected置顶）
        preport_recorded_orders.sort(key=lambda x: {
            "rejected": 0,
            "pending_review": 1, 
            "completed": 2
        }.get(x['preport_status'], 3))

        # 判断用户权限，决定默认标签页
        groups = [group.name for group in request.user.groups.all()]
        if not context:
            context = {}
        # 如果用户有 invoice_preport_leader 权限，默认打开审核标签页，否则打开录入标签页
        is_leader = False
        if 'invoice_preport_leader' in groups:
            is_leader = True
            default_tab = 'review'
        else:
            default_tab = 'entry'
        context.update({
            'start_date': start_date,
            'end_date': end_date,
            'preport_to_record_orders': preport_to_record_orders,
            'preport_recorded_orders': preport_recorded_orders,
            'preport_pending_review_orders': preport_pending_review_orders,
            'preport_completed_orders': preport_completed_orders,
            "order_form": OrderForm(),
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "default_tab": default_tab, 
            "is_leader": is_leader,
        })
        return context

    def handle_warehouse_entry_post(self, request:HttpRequest, context: dict| None = None,) -> Dict[str, Any]:
        warehouse = request.POST.get("warehouse_filter")
        customer = request.POST.get("customer")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")

        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date

        criteria = (
            Q(cancel_notification=False)
            & (Q(order_type="转运") | Q(order_type="转运组合"))
            & Q(vessel_id__vessel_etd__gte=start_date)
            & Q(vessel_id__vessel_etd__lte=end_date)
            & Q(offload_id__offload_at__isnull=False)
        )

        if warehouse:
            criteria &= Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= Q(customer_name__zem_name=customer)

        # 获取基础订单数据
        base_orders = (
            Order.objects
            .select_related(
                'retrieval_id', 
                'offload_id', 
                'container_number',
                'customer_name'
            )
            .annotate(
                retrieval_time=F("retrieval_id__actual_retrieval_timestamp"),
                empty_returned_time=F("retrieval_id__empty_returned_at"),
                offload_time=F("offload_id__offload_at"),
            )
            .filter(criteria)
            .distinct()
        )
        wh_public_to_record_orders = [] #公仓待录入
        wh_public_recorded_orders = []  #公仓已录入
        wh_self_to_record_orders = []  #私仓待录入
        wh_self_recorded_orders = []  #私仓已录入
        
        for order in base_orders:
            container = order.container_number
            
            if not container:
                continue
            
            container_delivery_type = getattr(container, 'delivery_type', 'mixed')
        
            # 判断是否应该处理公仓或私仓
            should_process_public = container_delivery_type in ['public', 'mixed']
            should_process_self = container_delivery_type in ['other', 'mixed']
            
            # 如果没有柜子类型信息，默认都处理
            if container_delivery_type not in ['public', 'other', 'mixed']:
                should_process_public = True
                should_process_self = True

            # 查询这个柜子的所有应收账单
            invoices = Invoicev2.objects.filter(container_number=container)
            
            if not invoices.exists():
                # 没有账单的情况 - 归到待录入
                order_data = {
                    'order': order,
                    'container_number': order.container_number,
                    'invoice_number': None,
                    'invoice_id': None,
                    'invoice_created_at': None,
                    'preport_status': None,
                    'finance_status': None,
                    'has_invoice': False,
                    'offload_time': order.offload_time,
                }
                # 根据柜子类型决定添加到哪个列表
                if should_process_public:
                    wh_public_to_record_orders.append(order_data)
                if should_process_self:
                    wh_self_to_record_orders.append(order_data)
            else:
                # 有账单的情况 - 每个账单都要单独处理
                for invoice in invoices:
                    # 查询这个账单对应的状态
                    try:
                        status_obj = InvoiceStatusv2.objects.get(
                            invoice=invoice,
                            invoice_type='receivable'
                        )
                        public_status = status_obj.warehouse_public_status #公仓状态
                        self_status = status_obj.warehouse_other_status  #私仓状态
                        finance_status = status_obj.finance_status #财务状态
                    except InvoiceStatusv2.DoesNotExist:
                        public_status = None
                        self_status = None
                        finance_status = None
                    
                    order_data = {
                        'order': order,
                        'container_number': order.container_number,
                        'invoice_number': invoice.invoice_number,
                        'invoice_id': invoice.id,
                        'invoice_created_at': invoice.created_at if hasattr(invoice, 'created_at') else invoice.history.first().history_date if invoice.history.exists() else None,
                        'public_status': public_status,
                        'self_status': self_status,
                        'finance_status': finance_status,
                        'has_invoice': True,
                        'offload_time': order.offload_time,
                    }
                    
                    # 根据状态分组，同时考虑柜子类型
                    if should_process_public:
                        if public_status in ["unstarted", "in_progress"] or public_status is None:
                            wh_public_to_record_orders.append(order_data)
                        elif public_status in ["completed", "rejected", "confirmed"]:
                            wh_public_recorded_orders.append(order_data)

                    if should_process_self:
                        if self_status in ["unstarted", "in_progress"] or self_status is None:
                            wh_self_to_record_orders.append(order_data)
                        elif self_status in ["completed", "rejected", "confirmed"]:
                            wh_self_recorded_orders.append(order_data)
        
        #按照出库比例排序
        wh_self_to_record_orders = self._add_shipment_group_stats(wh_self_to_record_orders, "other")
        
        # 判断用户权限，决定默认标签页
        groups = [group.name for group in request.user.groups.all()]
        if not context:
            context = {}

        # 根据权限，决定打开的标签页
        if 'warehouse_other' in groups:
            default_tab = 'self'
        else:
            default_tab = 'public'
        context.update({
            'start_date': start_date,
            'end_date': end_date,
            'wh_public_to_record_orders': wh_public_to_record_orders,
            'wh_public_recorded_orders': wh_public_recorded_orders,
            'wh_self_to_record_orders': wh_self_to_record_orders,
            'wh_self_recorded_orders': wh_self_recorded_orders,
            "order_form": OrderForm(),
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "default_tab": default_tab, 
        })
        return context
    
    def handle_delivery_entry_post(self, request:HttpRequest, context: dict| None = None,) -> Dict[str, Any]:
        warehouse = request.POST.get("warehouse_filter")
        customer = request.POST.get("customer")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")

        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-90)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date

        criteria = (
            Q(cancel_notification=False)
            & (Q(order_type="转运") | Q(order_type="转运组合"))
            & Q(vessel_id__vessel_etd__gte=start_date)
            & Q(vessel_id__vessel_etd__lte=end_date)
            & Q(offload_id__offload_at__isnull=False)
        )

        if warehouse:
            criteria &= Q(retrieval_id__retrieval_destination_precise=warehouse)
        if customer:
            criteria &= Q(customer_name__zem_name=customer)
    
        # 获取基础订单数据
        base_orders = (
            Order.objects
            .select_related(
                'retrieval_id', 
                'offload_id', 
                'container_number',
                'customer_name'
            )
            .annotate(
                retrieval_time=F("retrieval_id__actual_retrieval_timestamp"),
                empty_returned_time=F("retrieval_id__empty_returned_at"),
                offload_time=F("offload_id__offload_at"),
            )
            .filter(criteria)
            .distinct()
        )
        dl_public_to_record_orders = [] #公仓待录入
        dl_public_recorded_orders = []  #公仓已录入
        dl_self_to_record_orders = []  #私仓待录入
        dl_self_recorded_orders = []  #私仓已录入
        
        for order in base_orders:
            container = order.container_number
            
            if not container:
                continue

            container_delivery_type = getattr(container, 'delivery_type', 'mixed')   
            # 判断是否应该处理公仓或私仓 
            should_process_public = container_delivery_type in ['public', 'mixed']
            should_process_self = container_delivery_type in ['other', 'mixed']
            # 如果没有柜子类型信息，默认都处理
            if container_delivery_type not in ['public', 'other', 'mixed']:
                should_process_public = True
                should_process_self = True

            is_hold = False
            if should_process_public:
                public_hold_subquery = Pallet.objects.filter(
                    container_number=container,
                    delivery_method__contains="暂扣留仓",
                    delivery_type="public"
                )
                if public_hold_subquery.exists():
                    is_hold = True
            
            # 私仓暂扣板子查询（只查询delivery_type为other的）
            if should_process_self:
                self_hold_subquery = Pallet.objects.filter(
                    container_number=container,
                    delivery_method__contains="暂扣留仓",
                    delivery_type="other"
                )
                if self_hold_subquery.exists():
                    is_hold = True

            # 查询这个柜子的所有应收账单
            invoices = Invoicev2.objects.filter(container_number=container)
            
            if not invoices.exists():
                # 没有账单的情况 - 归到待录入
                order_data = {
                    'order': order,
                    'container_number': order.container_number,
                    'invoice_number': None,
                    'invoice_id': None,
                    'invoice_created_at': None,
                    'preport_status': None,
                    'finance_status': None,
                    'has_invoice': False,
                    'offload_time': order.offload_time,
                    "is_hold": is_hold,
                }
                if should_process_public:
                    dl_public_to_record_orders.append(order_data)
                if should_process_self:
                    dl_self_to_record_orders.append(order_data)
            else:
                # 有账单的情况 - 每个账单都要单独处理
                for invoice in invoices:
                    # 查询这个账单对应的状态
                    try:
                        status_obj = InvoiceStatusv2.objects.get(
                            invoice=invoice,
                            invoice_type='receivable'
                        )
                        public_status = status_obj.delivery_public_status #公仓状态
                        self_status = status_obj.delivery_other_status  #私仓状态
                        finance_status = status_obj.finance_status #财务状态
                    except InvoiceStatusv2.DoesNotExist:
                        public_status = None
                        self_status = None
                        finance_status = None
                    
                    order_data = {
                        'order': order,
                        'container_number': order.container_number,
                        'invoice_number': invoice.invoice_number,
                        'invoice_id': invoice.id,
                        'invoice_created_at': invoice.created_at if hasattr(invoice, 'created_at') else invoice.history.first().history_date if invoice.history.exists() else None,
                        'public_status': public_status,
                        'self_status': self_status,
                        'finance_status': finance_status,
                        'has_invoice': True,
                        'offload_time': order.offload_time,
                        'is_hold': is_hold,
                    }
                    # 根据状态分组
                    if should_process_public:                 
                        if public_status in ["unstarted", "in_progress"] or public_status is None:
                            dl_public_to_record_orders.append(order_data)
                        elif public_status in ["completed", "rejected"]:
                            dl_public_recorded_orders.append(order_data)
                    if should_process_self:
                        if self_status in ["unstarted", "in_progress"] or self_status is None:
                            dl_self_to_record_orders.append(order_data)
                        elif self_status in ["completed", "rejected"]:
                            dl_self_recorded_orders.append(order_data)
        
        #按照出库比例排序
        dl_public_to_record_orders = self._add_shipment_group_stats(dl_public_to_record_orders, "public")
        dl_self_to_record_orders = self._add_shipment_group_stats(dl_self_to_record_orders, "other")
        
        # 判断用户权限，决定默认标签页
        groups = [group.name for group in request.user.groups.all()]
        if not context:
            context = {}

        # 根据权限，决定打开的标签页
        if 'warehouse_other' in groups:
            default_tab = 'self'
        else:
            default_tab = 'public'
        context.update({
            'start_date': start_date,
            'end_date': end_date,
            'wh_public_to_record_orders': dl_public_to_record_orders,
            'wh_public_recorded_orders': dl_public_recorded_orders,
            'wh_self_to_record_orders': dl_self_to_record_orders,
            'wh_self_recorded_orders': dl_self_recorded_orders,
            "order_form": OrderForm(),
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "default_tab": default_tab, 
        })
        return context
    
    def _add_shipment_group_stats(self, orders, display_mix):
        """
        为每个order添加分组统计信息
        """
        # 获取用户权限对应的delivery_type筛选条件
        for order in orders:
            # 查找该order关联的packinglist和pallet
            packinglist_stats = self.get_shipment_group_stats(
                PackingList.objects.filter(
                    container_number__container_number=order["container_number"],
                    container_number__order__offload_id__offload_at__isnull=True,
                ).select_related('shipment_batch_number'),
                Q(delivery_type=display_mix)
            )
            pallet_stats = self.get_shipment_group_stats(
                Pallet.objects.filter(
                    container_number__container_number=order['container_number'],
                    container_number__order__offload_id__offload_at__isnull=False,
                ).select_related('shipment_batch_number'),
                Q(delivery_type=display_mix)
            )
            
            # 合并统计结果
            total_groups = packinglist_stats['total_groups'] + pallet_stats['total_groups']
            shipped_groups = packinglist_stats['shipped_groups'] + pallet_stats['shipped_groups']
            unshipped_groups = packinglist_stats['unshipped_groups'] + pallet_stats['unshipped_groups']
            
            # 添加到order对象（不改变原有结构）
            order['total_shipment_groups'] = total_groups
            order['shipped_shipment_groups'] = shipped_groups
            order['unshipped_shipment_groups'] = unshipped_groups
            order['completion_ratio'] = shipped_groups / total_groups if total_groups > 0 else 0
            
        sorted_orders = sorted(orders, key=lambda x: x['completion_ratio'], reverse=True)
        return sorted_orders
    
    def get_shipment_group_stats(self, queryset, delivery_type_q):
        """
        获取分组统计信息
        """
        # 应用delivery_type筛选
        if delivery_type_q:
            queryset = queryset.filter(delivery_type_q)
        
        # 按destination和shipment_batch_number分组
        groups = queryset.values('destination', 'shipment_batch_number__shipment_batch_number').annotate(
            group_count=Count('id')
        )
        total_groups = groups.count()
        
        # 统计已出库和未出库的分组
        shipped_groups = 0
        unshipped_groups = 0
        
        for group in groups:
            shipment_batch_number = group['shipment_batch_number__shipment_batch_number']
            
            if shipment_batch_number:
                # 检查shipment是否已出库
                shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
                if shipment.shipped_at:
                    shipped_groups += 1
                else:
                    unshipped_groups += 1
            else:
                # 没有shipment_batch_number的视为未出库
                unshipped_groups += 1
        
        return {
            'total_groups': total_groups,
            'shipped_groups': shipped_groups,
            'unshipped_groups': unshipped_groups
        }
    
    def handle_modify_order_type(self, request:HttpRequest) -> Dict[str, Any]:
        container_number = request.POST.get("container_number")
        new_order_type = request.POST.get("new_order_type")

        container = Container.objects.get(container_number=container_number)
        if new_order_type == "转运":
            actual_non_combina_reason = request.POST.get("actual_non_combina_reason")
            container.manually_order_type = "转运"
            container.non_combina_reason = actual_non_combina_reason
        elif new_order_type == "转运组合":
            container.manually_order_type = "转运组合"
        container.save()
        context = {"success_messages": f"{container_number}修改类型成功！"}
        return self.handle_container_preport_post(request, context)

    def handle_container_preport_post(self, request:HttpRequest, context: dict|None=None) -> Dict[str, Any]:
        """处理柜号点击进入港前账单编辑页面"""
        if not context:
            context = {}
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")

        container_number = request.GET.get("container_number")
        invoice_id = request.GET.get("invoice_id")
        
        #获取订单信息
        order = Order.objects.select_related(
            "retrieval_id", "container_number", "warehouse"
        ).get(container_number__container_number=container_number)
        if invoice_id:
            #找到要修改的那份账单
            invoice = Invoicev2.objects.get(id=invoice_id)
            invoice_status, created = InvoiceStatusv2.objects.get_or_create(
                invoice=invoice,
                invoice_type="receivable",
                defaults={
                    "container_number": order.container_number,
                    "invoice": invoice,
                }
            )
        else:
            #说明这个柜子没有创建过账单，需要创建
            invoice, invoice_status = self._create_invoice_and_status(container_number)

        # 查看仓库和柜型，计算提拆费
        warehouse = order.retrieval_id.retrieval_destination_area
        container_type = order.container_number.container_type
        
        #查找报价表
        quotation, quotation_error = self._get_quotation_for_order(order, 'receivable')
        if quotation_error:
            context.update({"error_messages": quotation_error})
            return context

        order_type = order.order_type
        if order_type == "转运":
            iscombina = False
            non_combina_reason = None
        else:
            container = Container.objects.get(container_number=container_number)
            if container.manually_order_type == "转运":
                iscombina = False
                non_combina_reason = container.non_combina_reason
            elif container.manually_order_type == "转运组合":
                iscombina = True
                non_combina_reason = None
            else:
                combina_context, iscombina,non_combina_reason = self._is_combina(container_number)
                if combina_context.get("error_messages"):
                    return combina_context
            
        fee_detail, fee_error = self._get_fee_details_from_quotation(quotation, "preport")
        if fee_error:
            context.update({"error_messages": fee_error})
            return context
        
        # 计算提拆费   
        match = re.match(r"\d+", container_type)
        pickup_fee = 0
        if match:
            pick_subkey = match.group()
            try:
                pickup_fee = fee_detail.details[warehouse][pick_subkey]
            except KeyError:
                pickup_fee = 0
                context.update({"error_messages": f"在报价表中找不到{warehouse}仓库{pick_subkey}柜型的提拆费"})
                return context
        # 构建费用提示信息
        FS = {
            "提拆/打托缠膜": f"{pickup_fee}",
            "托架费": f"{fee_detail.details.get('托架费', 'N/A')}",
            "托架提取费": f"{fee_detail.details.get('托架提取费', 'N/A')}",
            "预提费": f"{fee_detail.details.get('预提费', 'N/A')}",
            "货柜放置费": f"{fee_detail.details.get('货柜放置费', 'N/A')}",
            "操作处理费": f"{fee_detail.details.get('操作处理费', 'N/A')}",
            "码头": fee_detail.details.get("码头", "N/A"),
            "港口拥堵费": f"{fee_detail.details.get('港口拥堵费', 'N/A')}",
            "吊柜费": f"{fee_detail.details.get('火车站吊柜费', 'N/A')}",
            "空跑费": f"{fee_detail.details.get('空跑费', 'N/A')}",
            "查验费": f"{fee_detail.details.get('查验费', 'N/A')}",
            "危险品": f"{fee_detail.details.get('危险品', 'N/A')}",
            "超重费": f"{fee_detail.details.get('超重费', 'N/A')}",
            "加急费": f"{fee_detail.details.get('加急费', 'N/A')}",
            "其他服务": f"{fee_detail.details.get('其他服务', 'N/A')}",
            "港内滞期费": f"{fee_detail.details.get('港内滞期费', 'N/A')}",
            "港外滞期费": f"{fee_detail.details.get('港外滞期费', 'N/A')}",
            "二次提货": f"{fee_detail.details.get('二次提货', 'N/A')}",
        }
        # 获取现有的费用项目
        existing_items = InvoiceItemv2.objects.filter(
            invoice_number=invoice,
            item_category="preport"
        )
        # 获取已存在的费用描述列表，用于前端过滤
        existing_descriptions = [item.description for item in existing_items]

        # 标准费用项目列表
        standard_fee_items = [
            "提拆/打托缠膜", "托架费", "托架提取费", "预提费", "货柜放置费", 
            "操作处理费", "码头", "港口拥堵费", "吊柜费", "空跑费", 
            "查验费", "危险品", "超重费", "加急费", "其他服务", 
            "港内滞期费", "港外滞期费", "二次提货"
        ]
        # 构建费用数据
        fee_data = []
        for item in existing_items:
            fee_data.append({
                'id': item.id,
                'description': item.description,
                'qty': item.qty or 1,
                'rate': item.rate or 0,
                'surcharges': item.surcharges or 0, 
                'amount': item.amount or 0,
                'note': item.note or ''
            })
        # 如果是第一次录入且没有费用记录，添加提拆费作为默认
        if not existing_items.exists() and invoice_status.preport_status == 'unstarted' and pickup_fee > 0:
            for fee_name in standard_fee_items:              
                if fee_name == '提拆/打托缠膜':
                    # 提拆费特殊处理
                    pickup_qty = 0 if iscombina else 1
                    pickup_amount = pickup_fee * pickup_qty
                    fee_data.append({
                        'id': None,
                        'description': fee_name,
                        'qty': pickup_qty,
                        'rate': pickup_fee,
                        'surcharges': 0,
                        'amount': pickup_amount,
                        'note': '',
                    })
                else:
                    ref_price = FS.get(fee_name, 0)
                    numeric_price = self._extract_number(ref_price)
                    # 其他费用默认显示，但数量和金额为0
                    fee_data.append({
                        'id': None,
                        'description': fee_name,
                        'qty': 0,
                        'rate': numeric_price,
                        'surcharges': 0,
                        'amount': 0,
                        'note': '',
                    })
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
            
        context.update({
            "warehouse": warehouse,
            "order_type": order_type,
            "container_type": container_type,
            "reject_reason": order.invoice_reject_reason,
            "container_number": container_number,
            "groups": groups,
            "start_date": start_date,
            "end_date": end_date,
            "FS": FS,
            "receivable_is_locked": invoice.receivable_is_locked,
            "invoice_type": "receivable",
            "non_combina_reason": non_combina_reason,
            "fee_data": fee_data,
            "invoice_number": invoice.invoice_number,
            "invoice": invoice,  # 传递整个invoice对象
            "quotation_info": {
                "quotation_id": quotation.quotation_id,
                "version": quotation.version,
                "effective_date": quotation.effective_date,
                "is_user_exclusive": quotation.is_user_exclusive,
                "exclusive_user": quotation.exclusive_user,
                "filename": quotation.filename,  # 添加文件名
            },
            "pickup_fee": pickup_fee,
            "standard_fee_items": standard_fee_items,
            "existing_descriptions": existing_descriptions,  # 用于前端过滤
            "preport_status": invoice_status.preport_status,
        })
        return context
    
    def  handle_container_warehouse_post(self, request:HttpRequest, context: dict|None=None) -> Dict[str, Any]:
        if not context:
            context = {}
        start_date = request.GET.get("start_date")
        end_date = request.GET.get("end_date")
        container_number = request.GET.get("container_number")
        invoice_id = request.GET.get("invoice_id")
        delivery_type = request.GET.get("delivery_type")
        # 获取订单信息
        order = Order.objects.select_related(
            "retrieval_id", "container_number", "warehouse", "customer_name", "vessel_id"
        ).get(container_number__container_number=container_number)
        
        # 获取或创建账单和状态
        if invoice_id:
            #找到要修改的那份账单
            invoice = Invoicev2.objects.get(id=invoice_id)
            invoice_status, created = InvoiceStatusv2.objects.get_or_create(
                invoice=invoice,
                invoice_type="receivable",
                defaults={
                    "container_number": order.container_number,
                    "invoice": invoice,
                }
            )
        else:
            #说明这个柜子没有创建过账单，需要创建
            invoice, invoice_status = self._create_invoice_and_status(container_number)
        
        # 确定delivery_type
        groups = [group.name for group in request.user.groups.all()]
        if request.user.is_staff:
            groups.append("staff")
        
        # 设置item_category
        item_category = f"warehouse_{delivery_type}"
        
        # 获取报价表
        quotation, quotation_error = self._get_quotation_for_order(order, 'receivable')
        if quotation_error:
            context.update({"error_messages": quotation_error})
            return context
        
        # 获取仓库费用详情
        fee_detail, fee_error = self._get_fee_details_from_quotation(quotation, "warehouse")
        if fee_error:
            context.update({"error_messages": fee_error})
            return context
        
        # 仓库费用项目列表
        standard_fee_items = [
            "分拣费", "拦截费", "亚马逊PO激活", "客户自提", "重新打板",
            "货品清点费", "仓租", "指定贴标", "内外箱", "托盘标签",
            "开封箱", "销毁", "拍照", "拍视频", "重复操作费"
        ]
        
        # 构建参考费用信息
        FS = {
            "分拣费": fee_detail.details.get("分拣费", "N/A"),
            "拦截费": fee_detail.details.get("拦截费", "N/A"),
            "亚马逊PO激活": fee_detail.details.get("亚马逊PO激活", "N/A"),
            "客户自提": fee_detail.details.get("客户自提", "N/A"),
            "重新打板": fee_detail.details.get("重新打板", "N/A"),
            "货品清点费": fee_detail.details.get("货品清点费", "N/A"),
            "仓租": fee_detail.details.get("仓租", "N/A"),
            "指定贴标": fee_detail.details.get("指定贴标", "N/A"),
            "内外箱": fee_detail.details.get("内外箱", "N/A"),
            "托盘标签": fee_detail.details.get("托盘标签", "N/A"),
            "开封箱": fee_detail.details.get("开封箱", "N/A"),
            "销毁": fee_detail.details.get("销毁", "N/A"),
            "拍照": fee_detail.details.get("拍照", "N/A"),
            "拍视频": fee_detail.details.get("拍视频", "N/A"),
            "重复操作费": fee_detail.details.get("重复操作费", "N/A"),
        }
        
        # 获取现有的费用项目
        existing_items = InvoiceItemv2.objects.filter(
            invoice_number=invoice,
            item_category=item_category
        )
        
        # 构建费用数据
        fee_data = []
        for item in existing_items:
            fee_data.append({
                'id': item.id,
                'description': item.description,
                'qty': item.qty or 0,
                'rate': item.rate or 0,
                'surcharges': item.surcharges or 0,
                'amount': item.amount or 0,
                'note': item.note or ''
            })
        # 如果是第一次录入且没有费用记录，添加所有标准费用项目为默认
        if not existing_items.exists():
            status_field = f"warehouse_{delivery_type}_status"
            current_status = getattr(invoice_status, status_field, 'unstarted')
            
            if current_status == 'unstarted':
                for fee_name in standard_fee_items:
                    ref_price = FS.get(fee_name, 0)
                    numeric_price = self._extract_number(str(ref_price)) if ref_price != "N/A" else 0
                    
                    fee_data.append({
                        'id': None,
                        'description': fee_name,
                        'qty': 0,
                        'rate': numeric_price,
                        'surcharges': 0,
                        'amount': 0,
                        'note': '',
                    })
        
        # 获取已存在的费用描述列表，用于前端过滤
        existing_descriptions = [item.description for item in existing_items]
        # 计算可用的标准费用项目（还没有被添加的）
        available_standard_items = [item for item in standard_fee_items if item not in existing_descriptions]
        # 确定当前状态
        if invoice_status.finance_status == "completed":
            current_status = "confirmed"
        else:
            status_field = f"warehouse_{delivery_type}_status"
            current_status = getattr(invoice_status, status_field, 'unstarted')
        context.update({
            "warehouse": order.retrieval_id.retrieval_destination_area,
            "container_number": container_number,
            "groups": groups,
            "start_date": start_date,
            "end_date": end_date,
            "FS": FS,
            "receivable_is_locked": invoice.receivable_is_locked,
            "invoice_type": "receivable",
            "delivery_type": delivery_type,
            "item_category": item_category,
            "fee_data": fee_data,
            "invoice_number": invoice.invoice_number,
            "invoice": invoice,
            "quotation_info": {
                "quotation_id": quotation.quotation_id,
                "version": quotation.version,
                "effective_date": quotation.effective_date,
                "is_user_exclusive": quotation.is_user_exclusive,
                "exclusive_user": quotation.exclusive_user,
                "filename": quotation.filename,
            },
            "standard_fee_items": standard_fee_items,
            "available_standard_items": available_standard_items,
            "existing_descriptions": existing_descriptions,
            "status": current_status,
            "warehouse_status": current_status,  # 兼容性
        })
        
        return context

    def handle_container_delivery_post(self, request:HttpRequest, context: dict| None = None) -> Dict[str, Any]:
        if not context:
            context = {}
        container_number = request.GET.get("container_number")
        delivery_type = request.GET.get("delivery_type", "public")
        if delivery_type == "public":
            template = self.template_delivery_public_edit
        else:
            template = self.template_delivery_other_edit
        
        invoice_id = request.GET.get("invoice_id")
        order = Order.objects.select_related(
            'container_number',
            'customer_name',
            'warehouse',
            'vessel_id',
            'retrieval_id'
        ).get(container_number__container_number=container_number)
         
        if invoice_id:
            #找到要修改的那份账单
            invoice = Invoicev2.objects.get(id=invoice_id)
            invoice_status, created = InvoiceStatusv2.objects.get_or_create(
                invoice=invoice,
                invoice_type="receivable",
                defaults={
                    "container_number": order.container_number,
                    "invoice": invoice,
                }
            )
        else:
            #说明这个柜子没有创建过账单，需要创建
            invoice, invoice_status = self._create_invoice_and_status(container_number)

        # 获取板子数据
        pallet_groups, other_pallet_groups, context = self._get_pallet_groups_by_po(container_number, delivery_type)
        if context.get('error_messages'):
            return template, context
 
        # 获取已录入的账单项
        existing_items = self._get_existing_invoice_items(
            invoice, "delivery_" + delivery_type
        )
        # 检查是否所有PO都已录入
        all_po_ids = {group["PO_ID"] for group in pallet_groups if group.get("PO_ID")}
        existing_po_ids = set(existing_items.keys())
        
        #查看是不是组合柜
        is_combina = False
        if delivery_type == "public":
            is_combina = False
            if order.container_number.manually_order_type == "转运组合":
                is_combina = True
            elif order.container_number.manually_order_type == "转运":
                is_combina = False
            else:
                # 未定义，直接去判断
                if self._is_combina(order.container_number.container_number):
                    is_combina = True

        # 如果所有PO都已录入，直接返回已有数据
        if all_po_ids and all_po_ids.issubset(existing_po_ids):
            billing_result = self._separate_existing_items(existing_items, pallet_groups)
        else:
            # 有未录入的PO，需要进一步处理
            billing_result = self._process_unbilled_items(
                pallet_groups=pallet_groups,
                existing_items=existing_items,
                container=order.container_number,
                order=order,
                delivery_type=delivery_type,
                invoice=invoice,
                is_combina=is_combina
            )
            if isinstance(billing_result, dict) and billing_result.get('error_messages'):
                return template, billing_result
            
        # 构建上下文
        context.update({
            "container_number": container_number,
            "container_type": order.container_number.container_type,
            "delivery_type": delivery_type,
            "warehouse": order.warehouse.name if order.warehouse else "",
            "customer_name": order.customer_name.zem_name if order.customer_name else "",
            "manually_order_type": order.container_number.manually_order_type,
            # 分组数据
            "combina_items": billing_result.get("combina_items", []),
            "normal_items": billing_result.get("normal_items", []),
            "combina_info": billing_result.get("combina_info", {}),
            "combina_groups": billing_result.get("combina_groups", []),
            "invoice_id": invoice.id,
            "is_combina": is_combina,
            "invoice_number": invoice.invoice_number,
            "delivery_method_options": DELIVERY_METHOD_OPTIONS,
            "other_pallet_groups": other_pallet_groups,
        })
        print('combina_groups',context['combina_groups'])
        if delivery_type == "public":
            return template, context
        else:
            return template, context
    
    def _separate_existing_items(self, existing_items, pallet_groups):
        """将已有数据按组合柜和非组合柜分开"""
        result = {
            "combina_items": [],
            "normal_items": [],
            "combina_groups": [],
            "combina_info": {}
        }
        
        # 按区域分组组合柜数据
        combina_items_by_region = {}
        
        for po_id, existing_item in existing_items.items():
            # 找到对应的pallet组
            pallet_group = next((g for g in pallet_groups if g.get("PO_ID") == po_id), None)
            if pallet_group:
                item_data = self._create_item_from_existing(existing_item, pallet_group)
                
                # 根据类型分类
                if existing_item.delivery_type == 'combine':
                    result["combina_items"].append(item_data)
                    
                    # 按区域分组
                    region = item_data.get("combina_region", "未知")
                    if region not in combina_items_by_region:
                        combina_items_by_region[region] = []
                    combina_items_by_region[region].append(item_data)
                else:
                    result["normal_items"].append(item_data)
        
        # 构建组合柜分组数据
        for region, items in combina_items_by_region.items():
            if items:
                price = items[0].get("combina_price", 0)
                total_cbm = sum(item.get("total_cbm", 0) for item in items)
                
                result["combina_groups"].append({
                    "region": region,
                    "price": price,
                    "total_cbm": round(total_cbm, 2),
                    "destinations": list(set(item.get("destination", "") for item in items)),
                    "items": items
                })
        
        # 计算组合柜总信息
        if result["combina_items"]:
            total_base_fee = sum(item.get("amount", 0) for item in result["combina_items"])
            total_cbm = sum(item.get("total_cbm", 0) for item in result["combina_items"])
            
            result["combina_info"] = {
                "base_fee": round(total_base_fee, 2),
                "total_cbm": round(total_cbm, 2),
                "region_count": len(result["combina_groups"])
            }
        
        return result

    def _process_unbilled_items(
        self,
        pallet_groups: List[Dict],
        existing_items: Dict[str, Any],
        container,
        order,
        delivery_type: str,
        invoice,
        is_combina
    ) -> List[Dict[str, Any]]:
        """处理未录入费用的PO组"""
        result = {
            "combina_items": [],  # 组合柜数据（按区域分组）
            "normal_items": [],   # 非组合柜数据（普通表格）
            "combina_groups": [],   # 按区域分组的组合柜数据
            "combina_info": {}    # 组合柜总信息
        }
        # 按区域分组组合柜数据
        combina_items_by_region = {}

        # 先处理已有记录的PO
        for po_id, existing_item in existing_items.items():
            # 找到对应的pallet组
            pallet_group = next((g for g in pallet_groups if g.get("PO_ID") == po_id), None)
            if pallet_group:
                item_data = self._create_item_from_existing(existing_item, pallet_group)
                if existing_item['delivery_category'] == 'combine' or existing_item['is_combina_item']:
                    result["combina_items"].append(item_data)
                    #按区域分组
                    region = item_data.get("combina_region", "未知")
                    if region not in combina_items_by_region:
                        combina_items_by_region[region] = []
                    combina_items_by_region[region].append(item_data)
                else:
                    result["normal_items"].append(item_data)
        
        # 获取已经处理过的PO ID（包括已有记录和组合柜记录）
        processed_po_ids = set(existing_items.keys())  # 先添加已有记录的PO
        pallet_groups = [g for g in pallet_groups if g.get("PO_ID") not in processed_po_ids]

        if delivery_type == "public":
            # 获取报价表，私仓不用找报价表
            quotations = self._get_fee_details(order, order.retrieval_id.retrieval_destination_area, 
                                          order.customer_name.zem_name)
            fee_details = quotations['fees']
            quotation_info = quotations['quotation']
            if is_combina:
                combina_result = self._process_combina_items_with_grouping(
                    pallet_groups=pallet_groups,
                    container=container,
                    order=order,
                    fee_details=fee_details
                )
                
                if isinstance(combina_result, dict) and combina_result.get('error_messages'):
                    return combina_result
                
                # 合并组合柜数据
                new_combina_items = combina_result.get("items", [])
                result["combina_items"].extend(new_combina_items)
                result["combina_info"] = combina_result.get("info", {})
                
                # 将新的组合柜数据按区域分组
                for item in new_combina_items:
                    region = item.get("combina_region", "未知")
                    if region not in combina_items_by_region:
                        combina_items_by_region[region] = []
                    combina_items_by_region[region].append(item)
                
                # 从待处理的pallet_groups中移除已处理的组合柜记录
                combina_po_ids = {item.get("PO_ID") for item in new_combina_items}
                processed_po_ids.update(combina_po_ids)

        pallet_groups = [g for g in pallet_groups if g.get("PO_ID") not in processed_po_ids]

        # 处理未录入的PO
        if pallet_groups:
            for group in pallet_groups:
                po_id = group.get("PO_ID")
                destination = group.get("destination", "")
                location = group.get("location")
                
                if delivery_type == "public":
                    # 公仓：尝试自动计算费用
                    item_data = self._process_public_unbilled(
                        group=group,
                        container=container,
                        order=order,
                        destination=destination,
                        location=location,
                        is_combina=is_combina,
                        fee_details=fee_details
                    )
                    
                else:
                    # 私仓：只确定类型，不创建记录
                    item_data = self._process_private_unbilled(
                        group=group,
                        invoice=invoice
                    )

                if item_data:
                    if item_data.get('error_messages'):
                        extra_msg = f"（报价表：{quotation_info.filename}，版本：{quotation_info.version}）"
                        item_data["error_messages"] = f"{item_data['error_messages']} {extra_msg}"
                        return item_data
                    else:
                        # 如果是组合柜项目，添加到对应的分组
                        if item_data.get('delivery_category') == 'combine' or item_data.get('is_combina_item'):
                            result["combina_items"].append(item_data)
                            region = item_data.get("combina_region", "未知")
                            if region not in combina_items_by_region:
                                combina_items_by_region[region] = []
                            combina_items_by_region[region].append(item_data)
                        else:
                            result["normal_items"].append(item_data)
        for region, items in combina_items_by_region.items():
            if items:
                # 使用第一个item的价格作为该区域的价格
                price = items[0].get("combina_price", 0) if items else 0
                total_cbm = sum(item.get("total_cbm", 0) for item in items)
                
                result["combina_groups"].append({
                    "region": region,
                    "price": price,
                    "total_cbm": round(total_cbm, 2),
                    "destinations": list(set(item.get("destination", "") for item in items if item.get("destination"))),
                    "items": items
                })
        
        # 计算组合柜总信息（如果还没有计算过）
        if not result["combina_info"] and result["combina_items"]:
            total_base_fee = sum(item.get("amount", 0) for item in result["combina_items"])
            total_cbm = sum(item.get("total_cbm", 0) for item in result["combina_items"])
            
            result["combina_info"] = {
                "base_fee": round(total_base_fee, 2),
                "total_cbm": round(total_cbm, 2),
                "region_count": len(result["combina_groups"])
            }
        return result
    
    def _process_combina_items_with_grouping(
        self,
        pallet_groups: List[Dict],
        container,
        order,
        fee_details
    ) -> tuple[List[Dict], List[Dict]]:
        """处理组合柜区域的计费逻辑返回: (更新后的billing_items, 已处理的pallet_groups)"""
        warehouse = order.retrieval_id.retrieval_destination_area
        container_type_temp = 0 if "40" in container.container_type else 1
        
        # 1. 获取组合柜报价规则
        combina_key = f"{warehouse}_COMBINA"
        if combina_key not in fee_details:
            context = {
                "error_messages": f"未找到组合柜报价表规则 {combina_key}"
            }          
            return (context, [])  # 返回错误，空列表
        
        rules = fee_details.get(combina_key).details
        
        # 2. 筛选出属于组合区域的pallet_groups
        combina_pallet_groups = []
        processed_po_ids = set()
        
        for group in pallet_groups:
            po_id = group.get("PO_ID", "")
            destination = group.get("destination", "")
            
            # 检查是否属于组合区域
            is_combina_region = False
            for region, region_data in rules.items():
                for item in region_data:
                    if destination in item["location"]:
                        is_combina_region = True
                        break
                if is_combina_region:
                    break
            
            if is_combina_region:
                print('是组合柜',destination)
                combina_pallet_groups.append(group)
                processed_po_ids.add(po_id)
            else:
                print('不是组合柜',destination)
        
        # 如果没有组合区域，直接返回原数据和空列表，都按转运算
        if not combina_pallet_groups:
            return {"items": [], "info": {}}
        
        # 3. 计算组合区域总CBM和总板数
        total_combina_cbm = 0
        total_combina_pallets = 0
        combina_destinations_cbm = {}  # 记录每个目的地的CBM
        
        for group in combina_pallet_groups:
            po_id = group.get("PO_ID")
            destination = group.get("destination", "")
            
            cbm = group.get("total_cbm")
            total_pallets = group.get("total_pallets", 0)
            
            total_combina_cbm += cbm
            total_combina_pallets += total_pallets
            
            # 记录每个目的地的CBM
            if destination in combina_destinations_cbm:
                combina_destinations_cbm[destination] += cbm
            else:
                combina_destinations_cbm[destination] = cbm
        
        # 4. 计算组合柜总费用
        combina_regions_data = {}  # 记录每个区域的费用数据
        destination_region_map = {}
        destination_price_map = {}

        # 按区域计算费用， combina_destinations_cbm = {"LAX": 25.5, "ONT": 18.2,"SFO": 12.8 }
        for destination, cbm in combina_destinations_cbm.items():
            region_found = False
            #rules = {"CA1": [ {"location": ["LAX", "ONT"], "prices": [1500, 1800]}],
                #{"CA2": [  {"location": ["SFO", "SJC"], "prices": [1600, 1900]}]}
            for region, region_data in rules.items():
                for item in region_data:
                    if destination in item["location"]:
                        destination_region_map[destination] = region
                        destination_price_map[destination] = item["prices"][container_type_temp]

                        if region not in combina_regions_data:
                            combina_regions_data[region] = {
                                "total_cbm": 0,
                                "destinations": [],
                                "items": [],
                                "price": item["prices"][container_type_temp]
                            }
                        combina_regions_data[region]["total_cbm"] += cbm
                        combina_regions_data[region]["destinations"].append(destination)

                        region_found = True
                        break
                if region_found:
                    break
                
        # 5. 计算组合柜总费用
        combina_base_fee = 0
        for region, data in combina_regions_data.items():
            cbm_ratio = data["total_cbm"] / total_combina_cbm if total_combina_cbm > 0 else 0
            region_fee = data["price"] * cbm_ratio
            combina_base_fee += region_fee

        # 6. 构建组合柜项目数据（按区域分组）
        combina_items = []
        region_groups = []
        for region, region_data in combina_regions_data.items():
            region_items = []
            region_price = region_data["price"]
            region_total_cbm = region_data["total_cbm"]
            
            # 对该区域内的每个目的地构建item
            for group in combina_pallet_groups:
                destination = group.get("destination", "")
                if destination not in region_data["destinations"]:
                    continue
                
                po_id = group.get("PO_ID")
                cbm = group.get("total_cbm", 0)
                total_weight_lbs = group.get("total_weight_lbs", 0)
                total_pallets = group.get("total_pallets", 0)
                
                # 按CBM比例分摊费用
                if total_combina_cbm > 0:
                    cbm_ratio = cbm / total_combina_cbm
                    amount = combina_base_fee * cbm_ratio
                else:
                    amount = 0
                
                item_data = {
                    "id": None,
                    "PO_ID": po_id,
                    "destination": destination,
                    "delivery_method": group.get("delivery_method", ""),
                    "delivery_category": "combine",
                    "total_pallets": total_pallets,
                    "total_cbm": round(cbm, 2),
                    "total_weight_lbs": round(total_weight_lbs, 2),
                    "shipping_marks": group.get("shipping_marks", ""),
                    "pallet_ids": group.get("pallet_ids", []),
                    "rate": region_price,
                    "description": '',
                    "surcharges": 0,
                    "note": "",
                    "amount": round(amount, 2),
                    "is_existing": False,
                    "need_manual_input": False,
                    "is_combina_item": True,
                    "combina_region": region,
                    "combina_price": region_price,
                    "cbm_ratio": round(cbm_ratio, 4) if total_combina_cbm > 0 else 0,
                }
                
                region_items.append(item_data)
                combina_items.append(item_data)
            
            # 添加区域分组信息
            region_groups.append({
                "region": region,
                "price": region_price,
                "total_cbm": round(region_total_cbm, 2),
                "destinations": region_data["destinations"],
                "items": region_items
            })
        # 7. 返回组合柜数据
        return {
            "items": combina_items,
            "groups": region_groups,
            "info": {
                "base_fee": round(combina_base_fee, 2),
                "total_cbm": round(total_combina_cbm, 2),
                "total_pallets": total_combina_pallets,
                "region_count": len(combina_regions_data)
            }
        }

    def _process_private_unbilled(
        self,
        group: Dict,
        invoice
    ) -> Dict[str, Any]:
        """处理私仓未录入的PO"""
        po_id = group.get("PO_ID")
        destination = group.get("destination", "")
        delivery_method = group.get("delivery_method", "")
        
        # 确定派送类型
        rate = None
        amount = None
        need_manual_input = True
        if "暂扣" in delivery_method:
            delivery_category = "hold"
            rate = 0
            amount = 0
            need_manual_input = False
        elif delivery_method and "客户自提" in delivery_method:
            delivery_category = "selfpickup"
        else:
            delivery_category = "selfdelivery"
        # 返回数据（不自动创建记录）
        return {
            "id": None,
            "PO_ID": po_id,
            "destination": destination,
            "zipcode": group.get("zipcode", ""),
            "delivery_method": delivery_method,
            "delivery_category": delivery_category,
            "total_pallets": group.get("total_pallets", 0),
            "total_cbm": group.get("total_cbm", 0),
            "total_weight_lbs": group.get("total_weight_lbs", 0),
            "shipping_marks": group.get("shipping_marks", ""),
            "pallet_ids": group.get("pallet_ids", []),
            "rate": rate,
            "description": '',
            "surcharges": 0,
            "note": "",
            "amount": amount,
            "is_existing": False,
            "need_manual_input": need_manual_input,  # 私仓都需要手动录入
            "invoice_id": invoice.id,  # 记录invoice_id，用于后续创建
        }
    
    def _process_public_unbilled(
        self,
        group: Dict,
        container,
        order,
        destination,
        location,
        is_combina,
        fee_details,
    ) -> Dict[str, Any]:
        """处理公仓未录入的PO"""
        context = {}
        po_id = group.get("PO_ID")
        delivery_method = group.get("delivery_method", "")
        warehouse = order.retrieval_id.retrieval_destination_area
        #柜型
        container_type_temp = 0 if "40" in container.container_type else 1 

        # 获取结果，如果为空则设置为0.0
        total_cbm = group.get("total_cbm")
        total_weight_lbs = group.get("total_weight_lbs")
        need_manual_input = False
        # 1. 确定派送类型
        if delivery_method and any(courier in delivery_method.upper() 
                                 for courier in ["UPS", "FEDEX", "DHL", "DPD", "TNT"]):
            delivery_category = "upsdelivery"
            rate = 0
            amount = 0
            total_pallets = group.get("total_pallets")     
            need_manual_input = True      
        else:      
            rate_found = False
            # 使用统一的组合柜判断
            if is_combina:
                delivery_category = "combine"
                #用组合柜方式计算费用,按建单的仓库
                combina_key = f"{warehouse}_COMBINA"
                if combina_key not in fee_details:
                    
                    context.update({'error_messages':'未找到组合柜报价表,报价表是'})
                    return context
                
                rules = fee_details.get(f"{warehouse}_COMBINA").details
                for region, region_data in rules.items():
                    for item in region_data:
                        if destination in item["location"]:
                            rate = item["prices"][container_type_temp]
                            total_pallets = group.get("total_pallets")
                            amount = rate * total_pallets
                            rate_found = True
                            break
                    if rate_found:
                        break
                if not rate_found:
                    need_manual_input = True
                    rate = 0
                    amount = 0
                    total_pallets = group.get("total_pallets")     


            else:
                if "准时达" in order.customer_name.zem_name:
                    #准时达根据板子实际仓库找报价表，其他用户是根据建单
                    warehouse = location.split('-')[0]

                #用转运方式计算费用
                public_key = f"{warehouse}_PUBLIC"
                if public_key not in fee_details:
                    context.update({'error_messages':'未找到亚马逊沃尔玛报价表'})
                    return context
                rules = fee_details.get(f"{warehouse}_PUBLIC").details
                niche_warehouse = fee_details.get(f"{warehouse}_PUBLIC").niche_warehouse
                if destination in niche_warehouse:
                    is_niche_warehouse = True
                else:
                    is_niche_warehouse = False
                #LA和其他的存储格式有点区别
                details = (
                    {"LA_AMAZON": rules}
                    if "LA" in warehouse and "LA_AMAZON" not in rules
                    else rules
                )
                delivery_category = None
                for category, zones in details.items():
                    for zone, locations in zones.items():
                        if destination in locations:
                            if "AMAZON" in category:
                                delivery_category = "amazon"
                                rate = zone
                                rate_found = True
                            elif "WALMART" in category:
                                delivery_category = "walmart"
                                rate = zone
                                rate_found = True
                    if rate_found:
                        break
                if not rate_found:
                    need_manual_input = True
                    rate = 0
                    amount = 0
                    total_pallets = group.get("total_pallets")   
                else:            
                    total_pallets = self._calculate_total_pallet(
                        total_cbm, True, is_niche_warehouse
                    )               
                    amount = rate * total_pallets

        # 返回数据（不创建InvoiceItemv2记录）
        return {
            "id": None,
            "PO_ID": po_id,
            "destination": destination,
            "delivery_method": delivery_method,
            "delivery_category": delivery_category,
            "total_pallets": total_pallets,
            "total_cbm": total_cbm,
            "total_weight_lbs": total_weight_lbs,
            "shipping_marks": group.get("shipping_marks", ""),
            "pallet_ids": group.get("pallet_ids", []),
            "rate": rate,
            "description": '',
            "surcharges": 0,
            "note": "",
            "amount": amount,
            "is_existing": False,
            "need_manual_input": need_manual_input,  # 新增：是否需要手动录入
        }
    
    def _calculate_total_pallet(
        self, cbm: float, is_new_rule: bool, is_niche_warehouse: bool
    ) -> float:
        raw_p = float(cbm) / 1.8
        integer_part = int(raw_p)
        decimal_part = raw_p - integer_part
        # 本地派送的按照4.1之前的规则
        if decimal_part > 0:
            if is_new_rule:  # etd4.1之后的
                if is_niche_warehouse:
                    additional = 1 if decimal_part > 0.5 else 0.5
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

    def _generate_items_from_existing_only(
        self,
        existing_items: Dict[str, Any],
        pallet_groups: List[Dict]
    ) -> List[Dict[str, Any]]:
        """从已有记录生成账单数据（全部已录入的情况）"""
        billing_items = []
        
        for po_id, existing_item in existing_items.items():
            # 找到对应的pallet组
            pallet_group = next((g for g in pallet_groups if g.get("PO_ID") == po_id), None)
            if pallet_group:
                item_data = self._create_item_from_existing(existing_item, pallet_group)
                billing_items.append(item_data)
        
        return billing_items
    
    def _create_item_from_existing(
        self,
        existing_item,
        pallet_group: Dict
    ) -> Dict[str, Any]:
        """从已有InvoiceItemv2记录创建账单数据"""
        is_hold = False
        if "暂扣" in pallet_group.get("delivery_method"):
            is_hold = True
        return {
            "id": existing_item.id,
            "PO_ID": existing_item.PO_ID,
            "destination": existing_item.warehouse_code or pallet_group.get("destination", ""),
            "zipcode": "",
            #"delivery_method": existing_item.delivery_method or pallet_group.get("delivery_method", ""),
            "delivery_method": "",
            "delivery_category": existing_item.delivery_type,
            "total_pallets": pallet_group.get("total_pallets", 0),
            "total_cbm": pallet_group.get("total_cbm", 0),
            "total_weight_lbs": pallet_group.get("total_weight_lbs", 0),
            "shipping_marks": pallet_group.get("shipping_marks", ""),
            "pallet_ids": pallet_group.get("pallet_ids", []),
            "rate": existing_item.rate,
            "description": existing_item.description,
            "surcharges": existing_item.surcharges,
            "note": existing_item.note,
            "amount": existing_item.amount,
            "is_existing": True,
            "need_manual_input": False,
            "is_hold": is_hold,
        }
    
    def _get_fee_details(self, order: Order, warehouse, customer_name) -> dict:
        context = {}
        quotation, quotation_error = self._get_quotation_for_order(order, 'receivable')
        if quotation_error:
            context.update({"error_messages": quotation_error})
            return context
        id = quotation.id
        if "准时达" in customer_name:
            #准时达的，如果转仓，要根据pallet实际仓库去计算报价
            fee_types = ["NJ_LOCAL", "NJ_PUBLIC", "NJ_COMBINA","SAV_PUBLIC", "SAV_COMBINA","LA_PUBLIC", "LA_COMBINA"]
        else:
            fee_types = {
                "NJ": ["NJ_LOCAL", "NJ_PUBLIC", "NJ_COMBINA"],
                "SAV": ["SAV_PUBLIC", "SAV_COMBINA"],
                "LA": ["LA_PUBLIC", "LA_COMBINA"],
            }.get(warehouse, [])

        return {
            "quotation": quotation,
            "fees": {
                fee.fee_type: fee
                for fee in FeeDetail.objects.filter(
                    quotation_id=id, fee_type__in=fee_types
                )
            }
        }
        
    def _get_pallet_groups_by_po(self, container_number: str, delivery_type: str) -> list:
        """获取托盘数据"""
        context = {}
        error_messages = []
        base_query = Pallet.objects.filter(
            container_number__container_number=container_number,
            delivery_type=delivery_type
        ).exclude(
            PO_ID__isnull=True
        ).exclude(
            PO_ID=""
        )
        other_query = Pallet.objects.filter(
            container_number__container_number=container_number
        ).exclude(
            delivery_type=delivery_type               # ← 只反转这个
        ).exclude(
            PO_ID__isnull=True
        ).exclude(
            PO_ID=""
        )
        # 按PO_ID分组统计
        pallet_groups = list(
            base_query.values(
                "PO_ID",
                "destination",
                "zipcode",
                "delivery_method"
            ).annotate(
                total_pallets=models.Count("pallet_id"),
                total_cbm=models.Sum("cbm"),
                total_weight_lbs=models.Sum("weight_lbs"),
                pallet_ids=ArrayAgg("pallet_id"),
                shipping_marks=StringAgg("shipping_mark", delimiter=", ", distinct=True),
            ).order_by("PO_ID")
        )
        other_pallet_groups = list(
            other_query.values(
                "PO_ID",
                "destination",
                "zipcode",
                "delivery_method"
            ).annotate(
                total_pallets=models.Count("pallet_id"),
                total_cbm=models.Sum("cbm"),
                total_weight_lbs=models.Sum("weight_lbs"),
                pallet_ids=ArrayAgg("pallet_id"),
                shipping_marks=StringAgg("shipping_mark", delimiter=", ", distinct=True),
            ).order_by("PO_ID")
        )
        if not pallet_groups:
            error_messages.append("未找到板子数据")
            context['error_messages'] = error_messages
            return [], context
        
        # 对每个PO组，从PackingList表中获取准确的CBM和重量数据
        for group in pallet_groups:
            po_id = group.get("PO_ID")
            if po_id:
                try:
                    aggregated = PackingList.objects.filter(PO_ID=po_id).aggregate(
                        total_cbm=Sum('cbm'),
                        total_weight_lbs=Sum('total_weight_lbs')
                    )
                    
                    group['total_cbm'] = aggregated['total_cbm'] or 0.0
                    group['total_weight_lbs'] = aggregated['total_weight_lbs'] or 0.0
                    
                except Exception as e:
                    # 如果查询出错，设置默认值
                    group['total_cbm'] = 0.0
                    group['total_weight_lbs'] = 0.0
                    error_messages.append(f"获取PO_ID {po_id} (目的地: {destination}) 的PackingList数据时出错: {str(e)}")
                    
            else:
                # 没有PO_ID的情况
                group['total_cbm'] = 0.0
                group['total_weight_lbs'] = 0.0
                destination = group.get("destination")
                error_messages.append(f"缺少PO_ID，目的地是 {destination}")
        if error_messages:
            context['error_messages'] = error_messages
        return pallet_groups, other_pallet_groups, context
    
    def _get_existing_invoice_items(
        self,
        invoice,
        item_category: str
    ) -> Dict[str, Any]:
        """获取已存在的InvoiceItemv2记录，按PO_ID索引"""
        items = InvoiceItemv2.objects.filter(
            invoice_number=invoice,
            item_category=item_category,
            invoice_type="receivable"
        )
        
        # 按PO_ID建立索引
        item_dict = {}
        for item in items:
            if item.PO_ID:
                item_dict[item.PO_ID] = item
                
        return item_dict
    
    def handle_invoice_warehouse_save(self, request:HttpRequest) -> Dict[str, Any]:
        """保存仓库账单"""
        context = {} 
        save_type = request.POST.get("save_type")       
        invoice_id = request.POST.get("invoice_id")
        delivery_type = request.POST.get("delivery_type")

        try:
            invoice = Invoicev2.objects.get(id=invoice_id)
            invoice_status = InvoiceStatusv2.objects.get(invoice=invoice, invoice_type="receivable")
            
            container_number = request.POST.get("container_number")
            order = Order.objects.select_related(
                "customer_name", "container_number"
            ).get(container_number__container_number=container_number)
            
            container = order.container_number  # 获取柜子对象
            container_delivery_type = getattr(container, 'delivery_type', 'mixed')
            # 设置item_category
            item_category = f"warehouse_{delivery_type}"
            
            # 费用详情
            fee_ids = request.POST.getlist("fee_id")
            descriptions = request.POST.getlist("fee_description")
            rates = request.POST.getlist("fee_rate")
            qtys = request.POST.getlist("fee_qty")
            surcharges = request.POST.getlist("fee_surcharges")
            notes = request.POST.getlist("fee_note")

            total_amount = Decimal("0.00")
            with transaction.atomic():
                # 找下仓库账单之前存的费用记录，和现在所有费用比较，差集就是前端删除的记录
                existing_items = InvoiceItemv2.objects.filter(
                    invoice_number=invoice, 
                    item_category=item_category
                )
                existing_ids = set(item.id for item in existing_items if item.id is not None)
                submitted_ids = set(int(fid) for fid in fee_ids if fid)  # 只包含已有的id
                to_delete_ids = existing_ids - submitted_ids
                if to_delete_ids:
                    InvoiceItemv2.objects.filter(id__in=to_delete_ids).delete()

                for i in range(len(descriptions)):
                    fee_id = fee_ids[i] or None
                    description = descriptions[i]
                    rate = Decimal(rates[i] or 0)
                    qty = Decimal(qtys[i] or 0)
                    surcharge = Decimal(surcharges[i] or 0)
                    
                    # 计算总价：总价 = 单价 * 数量 + 附加费
                    amount = rate * qty + surcharge
                    
                    note = notes[i] or ""
                    
                    # 如果单价、数量和附加费都为0，则跳过
                    if qty == 0 and surcharge == 0:
                        continue
                        
                    total_amount += amount

                    if fee_id:  # 已存在的费用项，更新
                        try:
                            item = InvoiceItemv2.objects.get(id=fee_id, invoice_number=invoice)
                            item.rate = rate
                            item.qty = qty
                            item.surcharges = surcharge
                            item.amount = amount
                            item.note = note
                            item.save()
                        except InvoiceItemv2.DoesNotExist:
                            # 防止前端传了错误 id，查不到就新增
                            InvoiceItemv2.objects.create(
                                container_number=order.container_number,
                                invoice_number=invoice,
                                invoice_type="receivable",
                                item_category=item_category,
                                description=description,
                                rate=rate,
                                qty=qty,
                                surcharges=surcharge,
                                amount=amount,
                                note=note
                            )
                    else:  # 新增费用项
                        InvoiceItemv2.objects.create(
                            container_number=order.container_number,
                            invoice_number=invoice,
                            invoice_type="receivable",
                            item_category=item_category,
                            description=description,
                            rate=rate,
                            qty=qty,
                            surcharges=surcharge,
                            amount=amount,
                            note=note
                        )

                # 更新账单总金额
                if delivery_type == "public":
                    invoice.receivable_wh_public_amount = total_amount
                else:
                    invoice.receivable_wh_other_amount = total_amount
                def to_decimal(value, default='0.0'):
                    """安全转换为 Decimal"""
                    if value is None:
                        return Decimal(default)
                    if isinstance(value, Decimal):
                        return value
                    if isinstance(value, float):
                        return Decimal(str(value))
                    return Decimal(str(value))
                # 计算仓库总金额
                wh_public = to_decimal(invoice.receivable_wh_public_amount)
                wh_other = to_decimal(invoice.receivable_wh_other_amount)
                warehouse_total = wh_public + wh_other

                delivery_public = to_decimal(invoice.receivable_delivery_public_amount)
                delivery_other = to_decimal(invoice.receivable_delivery_other_amount)
                delivery_total = delivery_public + delivery_other

                preport_amount = to_decimal(invoice.receivable_preport_amount)
                # 更新总金额
                invoice.receivable_total_amount = preport_amount + warehouse_total + delivery_total      
                invoice.save()

                # 更新仓库账单状态
                status_field = f"warehouse_{delivery_type}_status"
                setattr(invoice_status, status_field, save_type)

                if save_type == "rejected":
                    reason_field = f"warehouse_{delivery_type}_reason"
                    setattr(invoice_status, reason_field, request.POST.get("reject_reason", ""))
                
                # 根据柜子类型自动更新另一边的状态
                if delivery_type == "public" and container_delivery_type == "public":
                    invoice_status.warehouse_other_status = "completed"

                elif delivery_type == "other" and container_delivery_type == "other":
                    invoice_status.warehouse_public_status = "completed"
                    
                invoice_status.save()
            delivery_type_chinese = "公仓" if delivery_type == "public" else "私仓"
            status_mapping = {
                'unstarted': '未录入',
                'in_progress': '录入中',
                'completed': '已完成',
                'rejected': '已拒绝'
            }
            status_chinese = status_mapping.get(save_type, '未知状态')
            success_msg = mark_safe(
                f"{container_number} 仓库账单保存成功！<br>"
                f"总费用: <strong>${total_amount:.2f}</strong><br>"
                f"类型: {delivery_type_chinese}<br>"
                f"状态更新为:{status_chinese}"
            )
            context["success_messages"] = success_msg
            
        except Exception as e:
            context["error_messages"] = f"操作失败: {str(e)}"
        
        # 重新加载页面
        return self.handle_warehouse_entry_post(request, context)

    def handle_invoice_preport_save(self, request:HttpRequest) -> Dict[str, Any]:
        context = {} 
        save_type = request.POST.get("save_type")       
        invoice_id = request.POST.get("invoice_id")
    
        try:
            invoice = Invoicev2.objects.get(id=invoice_id)
            invoice_status = InvoiceStatusv2.objects.get(invoice=invoice, invoice_type="receivable")
            
            container_number = request.POST.get("container_number")
            order = Order.objects.select_related(
                "customer_name", "container_number"
            ).get(container_number__container_number=container_number)
            #费用详情
            fee_ids = request.POST.getlist("fee_id")
            descriptions = request.POST.getlist("fee_description")
            rates = request.POST.getlist("fee_rate")
            qtys = request.POST.getlist("fee_qty")
            surcharges = request.POST.getlist("fee_surcharges")
            notes = request.POST.getlist("fee_note")

            total_amount = Decimal("0.00")
            with transaction.atomic():
                #找下港前账单之前存的费用记录，和现在所有费用比较，差集就是前端删除的记录
                existing_items = InvoiceItemv2.objects.filter(invoice_number=invoice, item_category="preport")
                existing_ids = set(item.id for item in existing_items if item.id is not None)
                submitted_ids = set(int(fid) for fid in fee_ids if fid)  # 只包含已有的id
                to_delete_ids = existing_ids - submitted_ids
                if to_delete_ids:
                    InvoiceItemv2.objects.filter(id__in=to_delete_ids).delete()

                for i in range(len(descriptions)):
                    fee_id = fee_ids[i] or None
                    description = descriptions[i]
                    rate = Decimal(rates[i] or 0)
                    qty = Decimal(qtys[i] or 0)
                    surcharge = Decimal(surcharges[i] or 0)

                    amount = rate * qty + surcharge
                    note = notes[i] or ""
                    if rate == 0 and qty == 0 and surcharge == 0:
                        continue
                    total_amount += amount

                    if fee_id:  # 已存在的费用项，更新
                        try:
                            item = InvoiceItemv2.objects.get(id=fee_id, invoice_number=invoice)
                            item.rate = rate
                            item.qty = qty
                            item.surcharges = surcharge
                            item.amount = amount
                            item.note = note
                            item.save()
                        except InvoiceItemv2.DoesNotExist:
                            # 防止前端传了错误 id，查不到就新增
                            InvoiceItemv2.objects.create(
                                container_number=order.container_number,
                                invoice_number=invoice,
                                invoice_type="receivable",
                                item_category="preport",
                                description=description,
                                rate=rate,
                                qty=qty,
                                surcharges=surcharge,
                                amount=amount,
                                note=note
                            )
                    else:  # 新增费用项
                        InvoiceItemv2.objects.create(
                            container_number=order.container_number,
                            invoice_number=invoice,
                            invoice_type="receivable",
                            item_category="preport",
                            description=description,
                            rate=rate,
                            qty=qty,
                            surcharges=surcharge,
                            amount=amount,
                            note=note
                        )

                # 更新账单总金额
                def to_decimal(value, default='0.0'):
                    """安全转换为 Decimal"""
                    if value is None:
                        return Decimal(default)
                    if isinstance(value, Decimal):
                        return value
                    if isinstance(value, float):
                        return Decimal(str(value))
                    return Decimal(str(value))
                invoice.receivable_preport_amount = total_amount
                warehouse_amount = to_decimal(invoice.receivable_wh_public_amount) + to_decimal(invoice.receivable_wh_other_amount)
                delivery_amount = to_decimal(invoice.receivable_delivery_public_amount) + to_decimal(invoice.receivable_delivery_other_amount)
                preport_amount = to_decimal(total_amount)  # 当前保存的金额

                invoice.receivable_total_amount = warehouse_amount + preport_amount + delivery_amount        
                invoice.save()

                # 更新港前账单状态
                invoice_status.preport_status = save_type
                if save_type == "rejected":
                    invoice_status.preport_reason = request.POST.get("reject_reason", "")
                invoice_status.save()
            status_mapping = {
                'pending_review': '待审核',
                'in_progress': '录入中',
                'completed': '已完成',
                'rejected': '已拒绝'
            }
            status_chinese = status_mapping.get(save_type, '未知状态')
            success_msg = mark_safe(
                f"{container_number} 柜号仓库账单保存成功！<br>"
                f"总费用: <strong>${total_amount:.2f}</strong><br>"
                f"状态更新为:{status_chinese}"
            )
            context["success_messages"] = success_msg
        except Exception as e:
            # 失败消息
            context["error_messages"] = f"操作失败: {str(e)}"    
        return self.handle_preport_entry_post(request,context)

    def _extract_number(self, value):
        """从字符串里提取数字，失败则返回 0"""
        if value is None:
            return 0
        try:
            # 直接是数字
            return float(value)
        except:
            pass

        # 尝试从文本里提取数字
        match = re.search(r"[-+]?\d*\.?\d+", str(value))
        if match:
            try:
                return float(match.group())
            except:
                return 0

        return 0
        
    def query_invoice_basic(
        self,
        request,
        category: str,    
    ):
        """统一查询方法，用于五种账单类型"""
        
        warehouse = request.GET.get("warehouse", None)
        customer = request.GET.get("customer", None)

        status_field = self.CATEGORY_STATUS_FIELD[category]

        status_kwargs = {f"{status_field}__in": ["unstarted", "in_progress"]}

        # --- 基础过滤（Container） ---
        container_filter = Q()
        if warehouse:
            container_filter &= Q(retrieval_destination_precise=warehouse)

        # --- 所有应收账单对应的 InvoiceStatus ---
        qs = InvoiceStatusv2.objects.select_related(
            "invoice",
            "container_number",
            "invoice__customer",
        ).filter(
            invoice_type="receivable",
            container_number__in=Container.objects.filter(container_filter),
        )

        # =========================
        # 待录入（unstarted + in_progress）
        # =========================
        pending_input = qs.filter(**{status_field + "__in": ["unstarted", "in_progress"]})

        # =========================
        # 驳回
        # =========================
        rejected = qs.filter(**{status_field: "rejected"})

        # =========================
        # 待审核（pending_review）
        # =========================
        pending_review = qs.filter(**{status_field: "pending_review"})

        # =========================
        # 已完成（completed）
        # =========================
        completed = qs.filter(**{status_field: "completed"}).order_by("-invoice__invoice_date")

        context = {
            "pending_input": pending_input,
            "rejected": rejected,
            "pending_review": pending_review,
            "completed": completed,
            "warehouse_options": self.warehouse_options,
            "warehouse_filter": warehouse,
            "category": category,
        }

        return context
        
    def _create_invoice_and_status(self, container_number: str) -> tuple[Invoicev2, InvoiceStatusv2]:
        """创建账单和状态记录"""
        order = Order.objects.select_related(
            "customer_name", "container_number"
        ).get(container_number__container_number=container_number)
        # 创建 Invoicev2
        current_date = datetime.now().date()
        order_id = str(order.id)
        customer_id = order.customer_name.id
        invoice = Invoicev2.objects.create(
            container_number=order.container_number,
            invoice_number=f"{current_date.strftime('%Y-%m-%d').replace('-', '')}C{customer_id}{order_id}",
            invoice_date=current_date
        )
        
        # 创建 InvoiceStatusv2
        invoice_status = InvoiceStatusv2.objects.create(
            container_number=order.container_number,
            invoice=invoice,
            invoice_type="receivable"
        )
        invoice.save()
        invoice_status.save()
        return invoice, invoice_status
    
    def _is_combina(self, container_number: str) -> Any:
        context = {}
        try:
            container = Container.objects.get(container_number=container_number)
            order = Order.objects.select_related(
                "retrieval_id", "container_number", "vessel_id"
            ).get(container_number__container_number=container_number)
            customer = order.customer_name
            customer_name = customer.zem_name
            # 从报价表找+客服录的数据
            warehouse = order.retrieval_id.retrieval_destination_area
            vessel_etd = order.vessel_id.vessel_etd

            container_type = container.container_type
            #  基础数据统计
            plts = Pallet.objects.filter(
                container_number__container_number=container_number
            ).aggregate(
                unique_destinations=Count("destination", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
                total_pallets=Count("id"),
            )
            plts["total_cbm"] = round(plts["total_cbm"], 2)
            plts["total_weight"] = round(plts["total_weight"], 2)
            # 获取匹配的报价表
            matching_quotation = (
                QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=True,
                    exclusive_user=customer_name,
                    quote_type='receivable',
                )
                .order_by("-effective_date")
                .first()
            )
            if not matching_quotation:
                matching_quotation = (
                    QuotationMaster.objects.filter(
                        effective_date__lte=vessel_etd,
                        is_user_exclusive=False,  # 非用户专属的通用报价单
                        quote_type='receivable',
                    )
                    .order_by("-effective_date")
                    .first()
                )
            if not matching_quotation:
                context.update({"error_messages": f"找不到{container_number}可用的报价表！"})
                return context, None, None
            # 获取组合柜规则
            try:
                stipulate_fee_detail = FeeDetail.objects.get(
                    quotation_id=matching_quotation.id, fee_type="COMBINA_STIPULATE"
                )
                stipulate = stipulate_fee_detail.details
            except FeeDetail.DoesNotExist:
                context.update({
                    "error_messages": f"报价表《{matching_quotation.filename}》-{matching_quotation.id}中找不到<报价表规则>分表，请截此图给技术员！"
                })
                return context, None, None
            
            combina_fee = FeeDetail.objects.get(
                quotation_id=matching_quotation.id, fee_type=f"{warehouse}_COMBINA"
            ).details
            if isinstance(combina_fee, str):
                combina_fee = json.loads(combina_fee)
            # 看是否超出组合柜限定仓点,NJ/SAV是14个
            default_combina = stipulate["global_rules"]["max_mixed"]["default"]
            exceptions = stipulate["global_rules"]["max_mixed"].get("exceptions", {})
            combina_threshold = exceptions.get(warehouse, default_combina) if exceptions else default_combina

            default_threshold = stipulate["global_rules"]["bulk_threshold"]["default"]
            exceptions = stipulate["global_rules"]["bulk_threshold"].get("exceptions", {})
            uncombina_threshold = exceptions.get(warehouse, default_threshold) if exceptions else default_threshold
            if plts["unique_destinations"] > uncombina_threshold:
                container.account_order_type = "转运"
                container.non_combina_reason = (
                    f"总仓点超过{uncombina_threshold}个"
                )
                container.save()
                return context, False, f"总仓点超过{uncombina_threshold}个" # 不是组合柜

            # 按区域统计
            destinations = (
                Pallet.objects.filter(container_number__container_number=container_number)
                .values_list("destination", flat=True)
                .distinct()
            )
            plts_by_destination = (
                Pallet.objects.filter(container_number__container_number=container_number)
                .values("destination")
                .annotate(total_cbm=Sum("cbm"))
            )
            total_cbm_sum = sum(item["total_cbm"] for item in plts_by_destination)
            # 区分组合柜区域和非组合柜区域
            container_type_temp = 0 if "40" in container_type else 1
            matched_regions = self.find_matching_regions(
                plts_by_destination, combina_fee, container_type_temp, total_cbm_sum, combina_threshold
            )
            # 判断是否混区，False表示满足混区条件
            is_mix = self.is_mixed_region(
                matched_regions["matching_regions"], warehouse, vessel_etd
            )
            if is_mix:
                container.account_order_type = "转运"
                container.non_combina_reason = "混区不符合标准"
                container.save()
                return context, False, "混区不符合标准"
            
            filtered_non_destinations = [key for key in matched_regions["non_combina_dests"].keys() if "UPS" not in key]
            # 非组合柜区域
            non_combina_region_count = len(filtered_non_destinations)
            # 组合柜区域
            combina_region_count = len(matched_regions["combina_dests"])

            filtered_destinations = self._filter_ups_destinations(destinations)
            if combina_region_count + non_combina_region_count != len(filtered_destinations):
                raise ValueError(
                    f"计算组合柜和非组合柜区域有误\n"
                    f"组合柜目的地：{matched_regions['combina_dests']}，数量：{combina_region_count}\n"
                    f"非组合柜目的地：{filtered_non_destinations}，数量：{non_combina_region_count}\n"
                    f"目的地集合：{filtered_destinations}\n"
                    f"目的地总数：{len(filtered_destinations)}"
                )
            if non_combina_region_count > (
                uncombina_threshold
                - combina_threshold
            ):
                # 当非组合柜的区域数量超出时，不能按转运组合
                container.account_order_type = "转运"
                container.non_combina_reason = "非组合柜区的数量不符合标准"
                container.save()
                return context, False,"非组合柜区的数量不符合标准"
            container.non_combina_reason = None
            container.account_order_type = "转运组合"
            container.save()
            return context, True, None
        except Exception as e:
            error_message = f"组合柜检查错误: {str(e)[:200]}"
            context.update({
                "error_messages": error_message,
            })
            return context, False, None

    def is_mixed_region(self, matched_regions, warehouse, vessel_etd) -> bool:
        regions = list(matched_regions.keys())
        # LA仓库的特殊规则：CDEF区不能混
        if warehouse == "LA":
            if vessel_etd.month > 7 or (
                vessel_etd.month == 7 and vessel_etd.day >= 15
            ):  # 715之后没有混区限制
                return False
            if len(regions) <= 1:  # 只有一个区，就没有混区的情况
                return False
            if set(regions) == {"A区", "B区"}:  # 如果只有A区和B区，也满足混区规则
                return False
            return True
        # 其他仓库无限制
        return False
         
    def _filter_ups_destinations(self, destinations):
        """过滤掉包含UPS的目的地，支持列表和QuerySet"""
        if hasattr(destinations, '__iter__') and not isinstance(destinations, (str, dict)):
            destinations_list = list(destinations)
        else:
            destinations_list = destinations
        filtered_destinations = [dest for dest in destinations_list if 'UPS' not in str(dest) and 'FEDEX' not in str(dest)]
        return filtered_destinations
    
    def find_matching_regions(
        self,
        plts_by_destination: dict,
        combina_fee: dict,
        container_type,
        total_cbm_sum: FloatField,
        combina_threshold: int,
    ) -> dict:
        matching_regions = defaultdict(float)  # 各区的cbm总和
        des_match_quote = {}  # 各仓点的匹配详情
        destination_matches = set()  # 组合柜的仓点
        non_combina_dests = {}  # 非组合柜的仓点
        price_display = defaultdict(
            lambda: {"price": 0.0, "location": set()}
        )  # 各区的价格和仓点
        dest_cbm_list = []  # 临时存储初筛组合柜内的cbm和匹配信息

        region_counter = {}
        region_price_map = {}
        for plts in plts_by_destination:
            destination = plts["destination"]
            if ('UPS' in destination) or ('FEDEX' in destination):
                continue
            # 如果是沃尔玛的，只保留后面的名字，因为报价表里就是这么保留的
            dest = destination.replace("沃尔玛", "").split("-")[-1].strip()
            cbm = plts["total_cbm"]
            dest_matches = []
            matched = False
            # 遍历所有区域和location
            for region, fee_data_list in combina_fee.items():           
                for fee_data in fee_data_list:
                    prices_obj = fee_data["prices"]
                    price = self._extract_price(prices_obj, container_type)
                    
                    # 如果匹配到组合柜仓点，就登记到组合柜集合中
                    if dest in fee_data["location"]:
                        # 初始化
                        if region not in region_price_map:
                            region_price_map[region] = [price]
                            region_counter[region] = 0
                            actual_region = region
                        else:
                            # 如果该 region 下已有相同价格 → 不加编号
                            found = None
                            for r_key, r_val in price_display.items():
                                if r_key.startswith(region) and r_val["price"] == price:
                                    found = r_key
                                    break
                            if found:
                                actual_region = found
                            else:                                
                                # 新价格 → 需要编号
                                region_counter[region] += 1
                                actual_region = f"{region}{region_counter[region]}"
                                region_price_map[region].append(price)

                        temp_cbm = matching_regions.get(actual_region, 0) + cbm
                        matching_regions[actual_region] = temp_cbm
                        dest_matches.append(
                            {
                                "region": actual_region,
                                "location": dest,
                                "prices": fee_data["prices"],
                                "cbm": cbm,
                            }
                        )
                        if actual_region not in price_display:
                            price_display[actual_region] = {
                                "price": price,
                                "location": set([dest]),
                            }
                        else:
                            # 不要覆盖，更新集合
                            price_display[actual_region]["location"].add(dest)
                        matched = True
            
            if not matched:
                # 非组合柜仓点
                non_combina_dests[dest] = {"cbm": cbm}
            # 记录匹配结果
            if dest_matches:
                des_match_quote[dest] = dest_matches
                # 将组合柜内的记录下来，后续方便按照cbm排序
                dest_cbm_list.append(
                    {"dest": dest, "cbm": cbm, "matches": dest_matches}
                )
                destination_matches.add(dest)
        if len(destination_matches) > combina_threshold:
            # 按cbm降序排序，将cbm大的归到非组合
            sorted_dests = sorted(dest_cbm_list, key=lambda x: x["cbm"], reverse=True)
            # 重新将排序后的前12个加入里面
            destination_matches = set()
            matching_regions = defaultdict(float)
            price_display = defaultdict(lambda: {"price": 0.0, "location": set()})
            for item in sorted_dests[:combina_threshold]:
                dest = item["dest"]
                destination_matches.add(dest)

                # 重新计算各区域的CBM总和
                for match in item["matches"]:
                    region = match["region"]
                    matching_regions[region] += item["cbm"]
                    price_display[region]["price"] = self._extract_price(match["prices"], container_type)
                    
                    price_display[region]["location"].add(dest)

            # 其余仓点转为非组合柜
            for item in sorted_dests[combina_threshold:]:
                non_combina_dests[item["dest"]] = {"cbm": item["cbm"]}
                # 将cbm大的从组合柜集合中删除
                des_match_quote.pop(item["dest"], None)

        # 下面开始计算组合柜和非组合柜各仓点占总体积的比例
        total_ratio = 0.0
        ratio_info = []

        # 处理组合柜仓点的cbm_ratio
        for dest, matches in des_match_quote.items():
            cbm = matches[0]["cbm"]  # 同一个dest的cbm在所有matches中相同
            ratio = round(cbm / total_cbm_sum, 4)
            total_ratio += ratio
            ratio_info.append((dest, ratio, cbm, True))  # 最后一个参数表示是否是组合柜
            for match in matches:
                match["cbm_ratio"] = ratio

        # 处理非组合柜仓点的cbm_ratio
        for dest, data in non_combina_dests.items():
            cbm = data["cbm"]
            ratio = round(cbm / total_cbm_sum, 4)
            total_ratio += ratio
            ratio_info.append((dest, ratio, cbm, False))
            data["cbm_ratio"] = ratio

        # 处理四舍五入导致的误差
        if abs(total_ratio - 1.0) > 0.0001:  # 考虑浮点数精度
            # 找到CBM最大的仓点
            ratio_info.sort(key=lambda x: x[2], reverse=True)
            largest_dest, largest_ratio, largest_cbm, is_combi = ratio_info[0]

            # 调整最大的仓点的ratio
            diff = 1.0 - total_ratio
            if is_combi:
                for match in des_match_quote[largest_dest]:
                    match["cbm_ratio"] = round(match["cbm_ratio"] + diff, 4)
            else:
                non_combina_dests[largest_dest]["cbm_ratio"] = round(
                    non_combina_dests[largest_dest]["cbm_ratio"] + diff, 4
                )
        return {
            "des_match_quote": des_match_quote,
            "matching_regions": matching_regions,
            "combina_dests": destination_matches,
            "non_combina_dests": non_combina_dests,
            "price_display": price_display,
        }
    
    def _extract_price(self, prices_obj, container_type):
        """
        安全地从 prices_obj 中提取数值 price：
        - 如果 prices_obj 是 dict，按键取（container_type 可为字符串或整型）。
        - 如果是 list/tuple，且 container_type 是 int，则尝试取 prices_obj[container_type]。
        若越界或该项不是数值，则回退到列表中第一个数值项。
        - 如果是单值（int/float），直接返回。
        - 其它情况返回 None。
        """
        # 优先处理 dict
        if isinstance(prices_obj, dict):
            # 允许 container_type 是 str 或 int（int 转为索引的情况不常见）
            val = prices_obj.get(container_type)
            if isinstance(val, (int, float)):
                return val
            # 如果取到的不是数字，尝试找 dict 的第一个数字值作为回退
            for v in prices_obj.values():
                if isinstance(v, (int, float)):
                    return v
            return None

        # list/tuple 按 index 选
        if isinstance(prices_obj, (list, tuple)):
            # 当 container_type 是整数索引时，优先使用该索引
            if isinstance(container_type, int):
                try:
                    candidate = prices_obj[container_type]
                    if isinstance(candidate, (int, float)):
                        return candidate
                except Exception:
                    pass
            # 回退：选第一个数字项
            first_num = next((x for x in prices_obj if isinstance(x, (int, float))), None)
            return first_num

        # 直接是数字
        if isinstance(prices_obj, (int, float)):
            return prices_obj

        # 其他（字符串等），不能作为 price
        return None
    
    def _get_quotation_for_order(self, order: Order, quote_type: str = 'receivable') :
        """获取订单对应的报价表"""
        try:
            vessel_etd = order.vessel_id.vessel_etd
            customer = order.customer_name
            customer_name = customer.zem_name
            
            # 先查找用户专属报价表
            quotation = (
                QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=True,
                    exclusive_user=customer_name,
                    quote_type=quote_type,
                )
                .order_by("-effective_date")
                .first()
            )
            
            if not quotation:
                # 查找通用报价表
                quotation = (
                    QuotationMaster.objects.filter(
                        effective_date__lte=vessel_etd,
                        is_user_exclusive=False,
                        quote_type=quote_type,
                    )
                    .order_by("-effective_date")
                    .first()
                )
            
            if quotation:
                return quotation, None
            else:
                error_msg = f"找不到生效日期在{vessel_etd}之前的{quote_type}报价表"
                return None, error_msg
                
        except Exception as e:
            error_msg = f"查询报价表时发生错误: {str(e)}"
            return None, error_msg

    def _get_fee_details_from_quotation(self, quotation: QuotationMaster, fee_type: str = "preport") :
        """从报价表中获取费用详情"""
        try:
            fee_detail = FeeDetail.objects.get(
                quotation_id=quotation.id,
                fee_type=fee_type
            )
            return fee_detail, None
        except FeeDetail.DoesNotExist:
            error_msg = f"报价表中找不到{fee_type}类型的费用详情"
            return None, error_msg
        except Exception as e:
            error_msg = f"获取费用详情时发生错误: {str(e)}"
            return None, error_msg
   
        