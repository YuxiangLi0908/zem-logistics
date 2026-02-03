from typing import Any, Coroutine
from django.db.models.functions import Trim
from django.db.models.functions import TruncMonth
from collections import Counter
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.template.response import TemplateResponse
from django.utils.decorators import method_decorator
from django.db import models
from django.core.exceptions import FieldDoesNotExist
from django.views import View
import pandas as pd
import re, json
import asyncio
from django.db.models import Q
from decimal import Decimal
from django.utils.timezone import make_aware, now
from datetime import datetime, timedelta, date
from django.contrib import messages
from simple_history.manager import HistoryManager

from django.contrib.postgres.aggregates import ArrayAgg, StringAgg
from warehouse.forms.upload_file import UploadFileForm
from asgiref.sync import sync_to_async
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from warehouse.models.order import Order
from django.contrib.auth.models import User
from warehouse.models.container import Container
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.invoice import Invoice
from warehouse.models.invoicev2 import Invoicev2
from warehouse.models.invoicev2 import InvoiceItemv2

from warehouse.models.invoice import InvoiceItem
from warehouse.models.invoice import InvoiceStatement
from warehouse.models.invoice_details import InvoiceDelivery
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice_details import InvoicePreport
from warehouse.models.invoice_details import InvoiceWarehouse
from warehouse.models.invoice import InvoiceStatus
from warehouse.models.invoicev2 import InvoiceStatusv2
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet_destroyed import PalletDestroyed
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.transfer_location import TransferLocation
from warehouse.models.vessel import Vessel
from warehouse.forms.warehouse_form import ZemWarehouseForm
from django.db import transaction
from asgiref.sync import sync_to_async
from warehouse.views.terminal49_webhook import T49Webhook
import logging

logger = logging.getLogger(__name__)
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Round, Cast, Coalesce
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    Func,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from warehouse.views.receivable_accounting import ReceivableAccounting

class ExceptionHandling(View):
    template_container_pallet = "exception_handling/shipment_actual.html"
    template_post_port_status = "exception_handling/post_port_status.html"
    template_delivery_invoice = "exception_handling/delivery_invoice.html"
    template_excel_formula_tool = "exception_handling/excel_formula_tool.html"
    template_find_all_table = "exception_handling/find_all_table_id.html"   
    template_restore_sorted_data_get = "exception_handling/restore_sorted_data_get.html"
    template_restore_sorted_data_get_packinglist = "exception_handling/restore_sorted_data_get_packinglist.html"
    template_query_pallet_packinglist = "exception_handling/query_pallet_packinglist.html"
    template_temporary_function = "exception_handling/temporary_function.html"
    template_receivable_status_migrate = "exception_handling/receivable_status_migrate.html"
    template_recaculate_combine = "exception_handling/recalculate_combine.html"
    shipment_type_options = {
        "": "",
        "FTL": "FTL",
        "LTL": "LTL",
        "外配": "外配",
        "快递": "快递",
        "客户自提": "客户自提",
    }
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
        "LA-91766": "LA-91766",
    }

    async def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "post_port_status":
            if self._validate_user_exception_handling(request.user):
                return await sync_to_async(render)(request, self.template_post_port_status)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                ) 
        elif step == "delivery_invoice":
            return await sync_to_async(render)(request, self.template_delivery_invoice)     
        elif step == "excel_formula_tool":
            return await sync_to_async(render)(request, self.template_excel_formula_tool)   
        elif step == "find_table_id":
            return await sync_to_async(render)(request, self.template_find_all_table)
        elif step == "restore_sorted_data_get":
            return await sync_to_async(render)(request, self.template_restore_sorted_data_get)
        elif step == "restore_sorted_data_get_packinglist":
            return await sync_to_async(render)(request, self.template_restore_sorted_data_get_packinglist)
        elif step == "pl_plt_detail":
            if self._validate_user_exception_handling(request.user):
                context = {"warehouse_form": ZemWarehouseForm()}
                return await sync_to_async(render)(request, self.template_query_pallet_packinglist,context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                ) 
        elif step == "shipment_actual":
            if self._validate_user_exception_handling(request.user):
                return await sync_to_async(render)(request, self.template_container_pallet)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "temporary_function":
            if self._validate_user_exception_handling(request.user):
                context = {"warehouse_form": ZemWarehouseForm()}
                return await sync_to_async(render)(request, self.template_temporary_function, context)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "receivable_status_migrate":
            if self._validate_super_user(request.user):
                return await sync_to_async(render)(request, self.template_receivable_status_migrate, {})
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        elif step == "recalculate_combine":
            return await sync_to_async(render)(request, self.template_recaculate_combine)
        
    async def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        print('step',step)
        #修改约状态相关的
        if step == "search_shipment":
            template, context = await self.handle_search_shipment(request)
            return await sync_to_async(render)(request, template, context)    
        elif step == "update_shipment_status":
            template, context = await self.handle_update_shipment_status(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_shipment_in_use":
            template, context = await self.handle_update_shipment_in_use(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_fleet_type":
            template, context = await self.handle_update_fleet_type(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "update_shipment_type":
            template, context = await self.handle_update_shipment_type(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_fleet_origin":
            template, context = await self.handle_update_fleet_origin(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_fleet_is_canceled":
            template, context = await self.handle_update_fleet_is_canceled(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_shipment_is_canceled":
            template, context = await self.handle_update_shipment_is_canceled(request)
            return await sync_to_async(render)(request, template, context)
        #修改主约和实际约
        elif step == "search_container":
            template, context = await self.handle_search_container(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "query_history":
            template, context = await self.query_history(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "query_history_packinglist":
            template, context = await self.query_history_packinglist(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "single_restore":
            template, context = await self.single_restore(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "single_restore_packinglist":
            template, context = await self.single_restore_packinglist(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "single_delete":
            template, context = await self.single_delete(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "single_delete_packinglist":
            template, context = await self.single_delete_packinglist(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_restore":
            template, context = await self.batch_restore(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_restore_packinglist":
            template, context = await self.batch_restore_packinglist(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_delete":
            template, context = await self.batch_delete(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_delete_packinglist":
            template, context = await self.batch_delete_packinglist(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "_get_query_context":
            template, context = await self._get_query_context(request)
            return await sync_to_async(render)(request, template, context)

        elif step == "update_pallet_master_shipment":
            template, context = await self.handle_update_pallet_master_shipment(request)
            return await sync_to_async(render)(request, template, context)
        #派送账单相关的
        elif step == "search_invoice_delivery":
            template, context = await self.handle_search_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delete_pallet_invoice_delivery":
            template, context = await self.handle_delete_pallet_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context)
        
        elif step == "delete_container_invoice_deliveries":
            template, context = await self.handle_delete_container_invoice_deliveries(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_invoice_delivery":
            template, context = await self.handle_delete_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_all_invoice_delivery":
            template, context = await self.handle_delete_all_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_all_invoice_items":
            template, context = await self.handle_delete_all_invoice_items(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_all_invoice_items_public":
            template, context = await self.delete_all_invoice_items_public(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_invoice_item":
            template, context = await self.handle_delete_invoice_item(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "search_data":
            template, context = await self.handle_search_data(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "query_pallet_packinglist":
            template, context = await self.handle_query_pallet_packinglist(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "process_excel":
            template, context = await self.handle_process_excel(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "search_outbound_pos":
            template, context = await self.handle_search_outbound_pos(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "receivale_status_migrate":
            template, context = await self.handle_receivale_status_migrate(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "receivale_status_migrate_delete_old":
            template, context = await self.handle_receivale_status_migrate_delete_old(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "receivale_item_migrate":
            template, context = await self.handle_receivale_item_migrate(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "search_extra_invoice":
            template, context = await self.handle_search_extra_invoice(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "search_wrong_status":
            template, context = await self.handle_search_wrong_status(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "recalculate_combine":
            template, context = await self.handle_recalculate_combine(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "recalculate_by_containers":
            template, context = await self.handle_recalculate_by_containers(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "search_wrong_fee":
            template, context = await self.handle_search_wrong_fee(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "receivale_status_search":
            template, context = await self.handle_receivale_status_search(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "modify_direct_status":
            template, context = await self.handle_modify_direct_status(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "update_shipment_type_to_fleet_type":
            template, context = await self.handle_update_shipment_type_to_fleet_type(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "search_receivable_total_fee":
            template, context = await self.handle_search_receivable_total_fee(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_receivable_total_fee":
            template, context = await self.handle_update_receivable_total_fee(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_shipment":
            template, context = await self.handle_delete_shipment(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delete_fleet":
            template, context = await self.handle_delete_fleet(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delete_shipment_batch_number":
            template, context = await self.handle_delete_shipment_batch_number(request)
            return await sync_to_async(render)(request, template, context)
        elif step =="update_invoice_status":
            template, context = await self.handle_update_invoice_status(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "overweight_single_save":
            template, context = await self.handle_overweight_single_save(request)
            return await sync_to_async(render)(request, template, context)
        else:
            return await sync_to_async(T49Webhook().post)(request)
        
    async def handle_update_invoice_status(self, request):
        '''更新账单的状态'''
        record_id = request.POST.get("record_id")
        field = request.POST.get("field")
        value = request.POST.get("value")
        allow_fields = [
            "preport_status",
            "warehouse_public_status",
            "warehouse_other_status",
            "delivery_public_status",
            "delivery_other_status",
            "finance_status",
        ]
        if field in allow_fields:
            obj = await sync_to_async(InvoiceStatusv2.objects.get)(id=record_id)
            setattr(obj, field, value)
            await sync_to_async(obj.save)(update_fields=[field])

        return await self.handle_search_shipment(request)

    async def handle_search_receivable_total_fee(self, request):
        '''查询invoicev2的应收总费用是0'''
        invoices_without_amount = await sync_to_async(list)(
            Invoicev2.objects.filter(
                (Q(receivable_total_amount__isnull=True) | Q(receivable_total_amount=0) | Q(remain_offset=0) | Q(remain_offset__isnull=True)) &
                Q(invoicestatusv2__finance_status='completed')  
            )
            .select_related('container_number')
            .prefetch_related('container_number__orders')
        )
        result_list = []
        for invoice in invoices_without_amount:
            container = invoice.container_number
            if container:
                # 获取订单类型
                order_type = ""
                if hasattr(container, 'orders'):
                    # 获取第一个订单的类型
                    orders = await sync_to_async(list)(container.orders.all())
                    if orders:
                        order_type = orders[0].order_type if orders[0].order_type else ""
                
                result_list.append({
                    'container_number': container.container_number if container.container_number else "",
                    'order_type': order_type,
                    'receivable_total_amount': invoice.receivable_total_amount or 0,
                    'remain_offset': invoice.remain_offset or 0,
                    'invoice_number': invoice.invoice_number or ""
                })

        context={'abnormal_receivable_fee':result_list}
        return self.template_post_port_status, context
    
    async def handle_update_receivable_total_fee(self, request):
        '''invoicev2的应收总费用是0的赋值'''
        invoices_without_amount = await sync_to_async(list)(
            Invoicev2.objects.filter(
                (Q(receivable_total_amount__isnull=True) | Q(receivable_total_amount=0) | Q(remain_offset=0) | Q(remain_offset__isnull=True)) &
                Q(invoicestatusv2__finance_status='completed')  
            )
            .select_related('container_number')
            .prefetch_related('container_number__orders')
        )
        result_list = []
        updated_count = 0

        @transaction.atomic
        def update_invoices():
            nonlocal updated_count
            with transaction.atomic():
                for invoice in invoices_without_amount:
                    try:
                        container = invoice.container_number
                        if not container:
                            continue
                        
                        # 1. 获取订单类型（用于结果展示）
                        order_type = ""
                        if hasattr(container, 'orders'):
                            orders = list(container.orders.all())
                            if orders:
                                order_type = orders[0].order_type or ""
                        # 2. 查询 InvoiceItemv2 记录并计算总额
                        invoice_items = list(
                            InvoiceItemv2.objects.filter(
                                container_number=container,
                                invoice_number=invoice,
                                invoice_type="receivable"
                            )
                        )
                        
                        # 计算总金额
                        total_amount = sum(item.amount or 0 for item in invoice_items)
                        
                        # 3. 更新 Invoicev2 记录
                        update_needed = False
                        
                        # 如果 receivable_total_amount 为空或0，则更新
                        if invoice.receivable_total_amount in (None, 0):
                            invoice.receivable_total_amount = total_amount
                            update_needed = True
                        
                        # 如果 remain_offset 为空或0，也更新为相同的值
                        if invoice.remain_offset in (None, 0):
                            invoice.remain_offset = total_amount
                            update_needed = True
                        
                        # 如果有更新，保存记录
                        if update_needed and total_amount > 0:
                            invoice.save()
                            updated_count += 1
                        
                        # 4. 添加到结果列表（无论是否更新）
                        result_list.append({
                            'container_number': container.container_number if container.container_number else "",
                            'order_type': order_type,
                            'receivable_total_amount': invoice.receivable_total_amount or 0,
                            'remain_offset': invoice.remain_offset or 0,
                            'invoice_number': invoice.invoice_number or "",
                            'calculated_amount': total_amount,  # 计算出的金额
                            'is_updated': update_needed and total_amount > 0,  # 是否已更新
                            'invoice_item_count': len(invoice_items)  # 关联的费用项数量
                        })
                        
                    except Exception as e:
                        # 记录错误但继续处理其他发票
                        result_list.append({
                            'container_number': container.container_number if container and container.container_number else "",
                            'order_type': order_type,
                            'error': str(e),
                            'invoice_number': invoice.invoice_number or ""
                        })
                        continue

        # 执行更新
        await sync_to_async(update_invoices, thread_sensitive=True)()

        context = {
            'success_messages': f'成功更新了{updated_count}条!',
            'abnormal_receivable_fee': result_list
        }

        return self.template_post_port_status, context

    async def handle_update_shipment_type_to_fleet_type(self, request):
        fleets_without_type = await sync_to_async(list)(
            Fleet.objects.filter(
                fleet_type__isnull=True
            ) | Fleet.objects.filter(fleet_type='')
        )
        
        results = {
            'success': 0,
            'errors': 0,
            'error_details': []
        }
        
        for fleet in fleets_without_type:
            # 获取关联的shipment
            shipments_list = []
            async for shipment in Shipment.objects.filter(fleet_number=fleet):
                shipments_list.append(shipment)
            
            if not shipments_list:
                continue
            
            # 获取所有shipment_type并去重
            shipment_types = set()
            for shipment in shipments_list:
                if shipment.shipment_type:
                    shipment_types.add(shipment.shipment_type)
            
            if not shipment_types:
                continue
            
            if len(shipment_types) == 1:
                fleet_type = list(shipment_types)[0]
                fleet.fleet_type = fleet_type
                await fleet.asave()
                results['success'] += 1
            else:
                raise ValueError(shipment_types)
                    
        
        return await self.handle_search_shipment(request)

    async def handle_search_outbound_pos(self, request):
        """查询未绑定的出库记录"""
        context = {}
        search_date_str = request.POST.get('search_date')
        search_date_lower_str = request.POST.get('search_date_lower')

        search_date = datetime.fromisoformat(search_date_str)      
        search_date_lower = datetime.fromisoformat(search_date_lower_str)   
        warehouse = request.POST.get('name')
        # 查询条件：指定时间之前，shipment_batch_number为空
        base_criteria = Q(
            shipment_batch_number__isnull=True,
            container_number__orders__offload_id__offload_at__gte=search_date_lower,
            container_number__orders__offload_id__offload_at__lte=search_date,
            container_number__orders__retrieval_id__retrieval_destination_precise=warehouse
        )
        
        # 查询 Pallet 数据
        pal_list = await self.get_pallet_data(base_criteria)
        
        # 查询 PackingList 数据
        pl_list = await self.get_packinglist_data(base_criteria)
        
        context.update({
            'pallet_data': pal_list,
            'packinglist_data': pl_list,
            'search_date': search_date_str,
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "warehouse": warehouse,
        })
        
        return self.template_temporary_function, context

    async def get_pallet_data(self, base_criteria):
        """获取Pallet数据"""
        pal_list = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__orders",
                "container_number__orders__warehouse",
                "shipment_batch_number",
                "container_number__orders__offload_id",
                "container_number__orders__customer_name",
                "container_number__orders__retrieval_id",
            )
            .filter(base_criteria)
            .annotate(
                str_id=Cast("id", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__orders__customer_name__zem_name",
                "destination",
                "delivery_method",
                "container_number__orders__offload_id__offload_at",
                "PO_ID",
            )
            .annotate(
                custom_delivery_method=F("delivery_method"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("container_number__orders__offload_id__offload_at")
        )
        return pal_list

    async def get_packinglist_data(self, base_criteria):
        """获取PackingList数据"""
        pl_list = await sync_to_async(list)(
            PackingList.objects.prefetch_related(
                "container_number",
                "container_number__orders",
                "container_number__orders__warehouse",
                "shipment_batch_number",
                "container_number__orders__offload_id",
                "container_number__orders__customer_name",
                "container_number__orders__retrieval_id",
            )
            .filter(base_criteria)
            .annotate(
                str_id=Cast("id", CharField()),
                calculated_pallets=Case(
                    When(
                        cbm__isnull=False,
                        then=Round(F("cbm") / 1.8)
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            )
            .values(
                "container_number__container_number",
                "container_number__orders__customer_name__zem_name",
                "destination",
                "delivery_method",
                "container_number__orders__offload_id__offload_at",
                "PO_ID",
                "calculated_pallets",
            )
            .annotate(
                custom_delivery_method=F("delivery_method"),
                fba_ids=F("fba_id"),
                ref_ids=F("ref_id"),
                shipping_marks=F("shipping_mark"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
            )
            .order_by("container_number__orders__offload_id__offload_at")
        )
        return pl_list
    
    async def handle_process_excel(self, request):
        """处理上传的Excel文件"""
        excel_file = request.FILES['excel_file']
        processing_logs = []
        
        df = pd.read_excel(excel_file)
        
        # 验证列名
        required_columns = ['柜号', '仓点', '时间', 'ISA']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            await sync_to_async(messages.error)(request, f'Excel文件缺少必要的列: {", ".join(missing_columns)}')
            return self.template_temporary_function, {}
        
        total_rows = len(df)
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        # 逐行处理数据
        for index, row in df.iterrows():
            row_number = index + 2  # Excel行号（包含标题行）
            
            #try:
            # 提取数据
            container_number = str(row['柜号']).strip()
            destination = str(row['仓点']).strip()
            time_value = row['时间']
            isa_value = str(row['ISA']).strip()
            
            # 记录开始处理
            processing_logs.append({
                'row': row_number,
                'type': 'info',
                'message': f'开始处理: 柜号={container_number}, 仓点={destination}, 批次号={isa_value}'
            })
            
            # 处理ISA值（去空格取整数）
            # try:
            #     isa_int = int(isa_value.strip())
            # except (ValueError, AttributeError):
            #     processing_logs.append({
            #         'row': row_number,
            #         'type': 'error',
            #         'message': f'ISA值格式错误: {isa_value}'
            #     })
            #     error_count += 1
            #     continue
            
            # 查找shipment记录
            try:
                shipment = await self.get_shipment_by_appointment_id(isa_value) #传的是批次号
                if not shipment:
                    processing_logs.append({
                        'row': row_number,
                        'type': 'warning',
                        'message': f'未找到对应的shipment记录: appointment_id={isa_value}'
                    })
                    skipped_count += 1
                    continue
                elif shipment.is_canceled:
                    processing_logs.append({
                        'row': row_number,
                        'type': 'warning',
                        'message': f'约已被取消: appointment_id={isa_value}'
                    })
                    skipped_count += 1
                    continue
                else:
                    processing_logs.append({
                        'row': row_number,
                        'type': 'info',
                        'message': f'找到shipment记录: ID={shipment.id}, appointment_id={shipment.appointment_id}'
                    })
            except Exception as e:
                processing_logs.append({
                    'row': row_number,
                    'type': 'error',
                    'message': f'查询shipment失败: {str(e)}'
                })
                error_count += 1
                continue
            
            # 查找并更新pallet记录
            pallet_result = await self.update_pallet_records(
                container_number, destination, shipment, processing_logs, row_number
            )
            
            # 查找并更新packinglist记录  
            packinglist_result = await self.update_packinglist_records(
                container_number, destination, shipment, processing_logs, row_number
            )
            
            # 分别记录每个表的更新结果
            if pallet_result['updated'] > 0 and packinglist_result['updated'] > 0:
                success_count += 1
                processing_logs.append({
                    'row': row_number,
                    'type': 'success', 
                    'message': f'处理成功! Pallet更新: {pallet_result["updated"]}条, PackingList更新: {packinglist_result["updated"]}条'
                })
            elif pallet_result['updated'] > 0 and packinglist_result['updated'] == 0:
                success_count += 1
                processing_logs.append({
                    'row': row_number,
                    'type': 'success',
                    'message': f'Pallet更新成功: {pallet_result["updated"]}条, PackingList: {packinglist_result["message"]}'
                })
            elif pallet_result['updated'] == 0 and packinglist_result['updated'] > 0:
                success_count += 1
                processing_logs.append({
                    'row': row_number,
                    'type': 'success',
                    'message': f'PackingList更新成功: {packinglist_result["updated"]}条, Pallet: {pallet_result["message"]}'
                })
            else:
                skipped_count += 1
                processing_logs.append({
                    'row': row_number,
                    'type': 'warning',
                    'message': f'两个表都未更新: Pallet-{pallet_result["message"]}, PackingList-{packinglist_result["message"]}'
                })
 
        context = {
            'processing_result': {
                'total_rows': total_rows,
                'success_count': success_count,
                'error_count': error_count,
                'skipped_count': skipped_count,
                'logs': processing_logs
            }
        }
        
        # 添加汇总消息
        if success_count > 0:
            messages.success(request, f'成功处理 {success_count} 条记录')
        if error_count > 0:
            messages.error(request, f'处理失败 {error_count} 条记录')
        if skipped_count > 0:
            messages.warning(request, f'跳过 {skipped_count} 条记录')
                    
        return self.template_temporary_function, context

    async def get_shipment_by_appointment_id(self, appointment_id):
        """根据appointment_id查找shipment记录"""
        try:          
            shipment = await Shipment.objects.aget(
                shipment_batch_number=appointment_id,
            )
            return shipment
        except Shipment.DoesNotExist:
            return None
        except Exception as e:
            logger.error(f"查询shipment失败: {str(e)}")
            raise
    
    async def handle_query_pallet_packinglist(self, request: HttpRequest):
        warehouse = (
            request.POST.get("name")
            if request.POST.get("name")
            else request.POST.get("warehouse")
        )
        query_type = request.POST.get("query_type", "pallet")
        container_number = request.POST.get("container_number", "").strip()
        start_date_str = request.POST.get("start_date", "")
        end_date_str = request.POST.get("end_date", "")
        month_filter = request.POST.get("month_filter", "")
        destination = request.POST.get("destination", "")
        show_complete = request.POST.get("show_complete", "") == "on"  # 新增：是否显示完整记录

        today = now().date()
        default_start = today - timedelta(days=30)
        default_end = today
        
        # 构建查询条件
        filters = Q(container_number__orders__cancel_notification=False)
        if query_type == "pallet":
            filters &= Q(location=warehouse)
        else:
            filters &= Q(container_number__orders__retrieval_id__retrieval_destination_area=warehouse)

        # 定义日期变量
        start_date = None
        end_date = None

        # 如果没有提供任何搜索条件，默认查询前两个月
        if not container_number and not start_date_str and not end_date_str and not month_filter and not destination:
            two_months_ago = today - timedelta(days=60)
            default_start = two_months_ago
            start_date = make_aware(datetime.combine(default_start, datetime.min.time()))
            end_date = make_aware(datetime.combine(default_end, datetime.max.time()))
            filters &= Q(container_number__orders__offload_id__offload_at__gte=start_date) 
            filters &= Q(container_number__orders__offload_id__offload_at__lte=end_date)
        else:
            if start_date_str:
                start_date = make_aware(datetime.strptime(start_date_str, "%Y-%m-%d"))
                filters &= Q(container_number__orders__offload_id__offload_at__gte=start_date) 

            if end_date_str:
                end_date = make_aware(datetime.strptime(end_date_str, "%Y-%m-%d"))
                filters &= Q(container_number__orders__offload_id__offload_at__lte=end_date)

            # 月份筛选 - 修改为按照 offload_at 时间筛选
            if month_filter:
                month_date = datetime.strptime(month_filter, "%Y-%m")
                month_start = make_aware(datetime(month_date.year, month_date.month, 1))
                next_month = month_date.replace(day=28) + timedelta(days=4)
                month_end = make_aware(datetime(next_month.year, next_month.month, 1) - timedelta(days=1))
                filters &= Q(container_number__orders__offload_id__offload_at__range=(month_start, month_end))

        if container_number:
            filters &= Q(container_number__container_number__icontains=container_number)
        if destination:
            destination_clean = destination.strip() 
            if ' ' in destination_clean:
                destination_list = destination_clean.split()
                filters &= Q(destination__in=destination_list)
            else:
                filters &= Q(destination=destination_clean)

        # 查询
        results = []
        if query_type == "pallet":
            pallets = await sync_to_async(
                lambda: list(
                    Pallet.objects.filter(filters)
                    .select_related(
                        "shipment_batch_number", 
                        "container_number", 
                        "transfer_batch_number"
                    )
                    .prefetch_related('shipment_batch_number')
                    .order_by("shipment_batch_number__shipment_appointment")
                )
            )()
            
            container_numbers = [p.container_number for p in pallets if p.container_number]
            
            orders = await sync_to_async(
                lambda: list(
                    Order.objects.filter(container_number__in=container_numbers)
                    .select_related('offload_id', 'retrieval_id')
                )
            )()
            
            order_map = {}
            for order in orders:
                order_map[order.container_number_id] = order
            
            for p in pallets:
                offload_time = None
                actual_retrieval_timestamp = None
                empty_returned_at = None
                
                if p.container_number:
                    order = order_map.get(p.container_number.id)
                    if order:
                        offload_time = order.offload_id.offload_at if order.offload_id else None
                        actual_retrieval_timestamp = order.retrieval_id.actual_retrieval_timestamp if order.retrieval_id else None
                        empty_returned_at = order.retrieval_id.empty_returned_at if order.retrieval_id else None
                results.append({
                    "container_number": p.container_number.container_number if p.container_number else "-",
                    "PO_ID": p.PO_ID,
                    "shipment_batch_number": p.shipment_batch_number.shipment_batch_number if p.shipment_batch_number else "-",
                    "pallet_id": p.pallet_id,
                    "pcs": p.pcs,
                    "cbm": p.cbm,
                    "destination": p.destination,
                    "weight": p.weight_lbs,
                    "delivery_window_start": p.delivery_window_start,
                    "delivery_window_end": p.delivery_window_end,
                    "actual_retrieval_timestamp": actual_retrieval_timestamp,
                    "offload_time": offload_time,
                    "empty_returned_at": empty_returned_at,
                    "shipment_appointment": getattr(p.shipment_batch_number, "shipment_appointment", None),
                    "shipped_at": getattr(p.shipment_batch_number, "shipped_at", None),
                    "arrived_at": getattr(p.shipment_batch_number, "arrived_at", None),
                    "pod_uploaded_at": getattr(p.shipment_batch_number, "pod_uploaded_at", None),
                    "record_type": "pallet"
                })
        else:
            packinglists = await sync_to_async(
                lambda: list(
                    PackingList.objects.filter(filters)
                    .select_related(
                        "shipment_batch_number", 
                        "container_number"
                    )
                    .prefetch_related('shipment_batch_number')
                    .order_by("shipment_batch_number__shipment_appointment")
                )
            )()

            container_ids = [pl.container_number.id for pl in packinglists if pl.container_number]

            orders = await sync_to_async(
                lambda: list(
                    Order.objects.filter(container_number_id__in=container_ids)
                    .select_related('offload_id', 'retrieval_id')
                )
            )()

            order_map = {order.container_number_id: order for order in orders}

            for pl in packinglists:
                container_id = pl.container_number.id if pl.container_number else None
                order = order_map.get(container_id) if container_id else None
                
                offload_time = order.offload_id.offload_at if order and order.offload_id else None
                actual_retrieval_timestamp = order.retrieval_id.actual_retrieval_timestamp if order and order.retrieval_id else None
                empty_returned_at = order.retrieval_id.empty_returned_at if order and order.retrieval_id else None
                results.append({
                    "container_number": pl.container_number.container_number if pl.container_number else "-",
                    "PO_ID": pl.PO_ID,
                    "shipment_batch_number": pl.shipment_batch_number.shipment_batch_number if pl.shipment_batch_number else "-",
                    "pcs": pl.pcs,
                    "cbm": pl.cbm,
                    "destination": pl.destination,
                    "weight": pl.total_weight_lbs,
                    "delivery_window_start": pl.delivery_window_start,
                    "delivery_window_end": pl.delivery_window_end,
                    "actual_retrieval_timestamp": actual_retrieval_timestamp,
                    "offload_time": offload_time,
                    "empty_returned_at": empty_returned_at,
                    "shipment_appointment": getattr(pl.shipment_batch_number, "shipment_appointment", None),
                    "shipped_at": getattr(pl.shipment_batch_number, "shipped_at", None),
                    "arrived_at": getattr(pl.shipment_batch_number, "arrived_at", None),
                    "pod_uploaded_at": getattr(pl.shipment_batch_number, "pod_uploaded_at", None),
                    "record_type": "packinglist"
                })

        # 分组统计
        grouped_results = []
        group_dict = {}
        
        for r in results:
            key = (r["PO_ID"], r["shipment_batch_number"], r["container_number"])
            if key not in group_dict:
                group_dict[key] = {
                    "container_number": r["container_number"],
                    "PO_ID": r["PO_ID"],
                    "shipment_batch_number": r["shipment_batch_number"],
                    "total_pcs": 0,
                    "total_cbm": 0,
                    "pallet_count": 0,
                    "destination": r["destination"],
                    "actual_retrieval_timestamp": r["actual_retrieval_timestamp"],
                    "offload_time": r["offload_time"],
                    "empty_returned_at": r["empty_returned_at"],
                    # 确保这些 shipment 相关字段正确传递
                    "shipment_appointment": r["shipment_appointment"],
                    "shipped_at": r["shipped_at"], 
                    "arrived_at": r["arrived_at"],
                    "pod_uploaded_at": r["pod_uploaded_at"],
                    "delivery_window_start": r["delivery_window_start"],
                    "delivery_window_end": r["delivery_window_end"],
                }
            
            group_dict[key]["total_pcs"] += r.get("pcs") or 0
            group_dict[key]["total_cbm"] += r.get("cbm") or 0
            group_dict[key]["pallet_count"] += 1
        # 转换为列表
        grouped_results = list(group_dict.values())

        # 计算记录完整度并排序
        for group in grouped_results:
            # 计算时间字段完整度
            time_fields = [
                group['actual_retrieval_timestamp'],
                group['offload_time'], 
                group['empty_returned_at'],
                group['shipment_appointment'],
                group['shipped_at'],
                group['arrived_at'],
                group['pod_uploaded_at']
            ]
            completed_fields = sum(1 for field in time_fields if field is not None)
            group['completed_score'] = completed_fields
            
            # 标记是否所有时间字段都完整
            group['is_complete'] = completed_fields == len(time_fields)

        # 根据是否显示完整记录进行筛选
        if not show_complete:
            grouped_results = [group for group in grouped_results if not group['is_complete']]

        # 排序：按完整度降序，然后按入仓时间升序
        grouped_results.sort(key=lambda x: (
            -x['completed_score'],  # 完整度高的在前
            x['offload_time'] or datetime.max  # 入仓时间早的在前
        ))

        # 获取可用的月份用于筛选 - 去重处理
        available_months = await sync_to_async(
            lambda: list(
                Order.objects.filter(offload_id__isnull=False)
                .annotate(month=TruncMonth('offload_id__offload_at'))
                .values('month')
                .distinct()
                .order_by('-month')
            )
        )()
        
        # 转换为日期对象列表
        available_months = [item['month'] for item in available_months]

        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        context = {
            "query_type": query_type,
            "container_number": container_number,
            "start_date": start_date,
            "end_date": end_date,
            "month_filter": month_filter,
            "grouped_results": grouped_results,
            "available_months": available_months,
            "warehouse_form": warehouse_form,
            "destination": destination,
            "show_complete": show_complete,  # 新增
        }
        return self.template_query_pallet_packinglist, context


    async def update_packinglist_records(self,container_number, destination, shipment, processing_logs, row_number):
        """更新packinglist记录"""
        #try:
        result = {'updated': 0, 'message': ''}
        
        # 1. 先查找已经有约的记录
        existing_packinglists = PackingList.objects.filter(
            container_number__container_number=container_number,
            destination=destination,
            shipment_batch_number__isnull=False
        ).select_related('shipment_batch_number', 'master_shipment_batch_number')
        
        existing_count = 0
        master_updated_count = 0
        async for packinglist in existing_packinglists:
            existing_count += 1
            # 记录已有约的信息
            existing_appointment_id = packinglist.shipment_batch_number.appointment_id if packinglist.shipment_batch_number else '未知'
            processing_logs.append({
                'row': row_number,
                'type': 'info',
                'message': f'PackingList已有约: ID={packinglist.id}, 当前约={existing_appointment_id}, 目标约={shipment.appointment_id}'
            })
            
            # 如果master_shipment_batch_number为空，就更新它
            if not packinglist.master_shipment_batch_number:
                try:
                    packinglist.master_shipment_batch_number = shipment
                    await packinglist.asave()
                    master_updated_count += 1
                    processing_logs.append({
                        'row': row_number,
                        'type': 'info',
                        'message': f'PackingList更新master_shipment: ID={packinglist.id}'
                    })
                except Exception as e:
                    processing_logs.append({
                        'row': row_number,
                        'type': 'error',
                        'message': f'更新PackingList master_shipment失败: ID={packinglist.id}, 错误: {str(e)}'
                    })
        
        if existing_count > 0:
            result['message'] += f'已有约{existing_count}条(更新master{master_updated_count}条) '
        
        # 2. 查找需要更新的空记录
        empty_packinglists = PackingList.objects.filter(
            container_number__container_number=container_number,
            destination=destination,
            shipment_batch_number__isnull=True
        ).select_related('shipment_batch_number', 'master_shipment_batch_number')
        
        updated_count = 0
        empty_count = 0
        
        # 批量更新
        packinglists_to_update = []
        async for packinglist in empty_packinglists:
            empty_count += 1
            packinglist.shipment_batch_number = shipment
            packinglist.master_shipment_batch_number = shipment
            packinglists_to_update.append(packinglist)
        
        if packinglists_to_update:
            #try:
                # 批量保存
                await PackingList.objects.abulk_update(
                    packinglists_to_update, 
                    ['shipment_batch_number', 'master_shipment_batch_number']
                )
                updated_count = len(packinglists_to_update)
                processing_logs.append({
                    'row': row_number,
                    'type': 'info',
                    'message': f'PackingList批量更新: 找到{empty_count}条空记录, 成功更新{updated_count}条'
                })
            # except Exception as e:
            #     processing_logs.append({
            #         'row': row_number,
            #         'type': 'error',
            #         'message': f'PackingList批量更新失败: {str(e)}'
            #     })
            
        result['updated'] = updated_count
        
        if empty_count == 0 and existing_count == 0:
            result['message'] += '未找到任何记录'
        elif empty_count == 0 and existing_count > 0:
            result['message'] += '无空记录可更新'
        else:
            result['message'] += f'更新空记录{updated_count}条'
            
        return result
            
        # except Exception as e:
        #     processing_logs.append({
        #         'row': row_number,
        #         'type': 'error',
        #         'message': f'查询packinglist记录失败: {str(e)}'
        #     })
        return {'updated': 0, 'message': f'查询失败: {str(e)}'}
    
    async def update_pallet_records(self, container_number, destination, shipment, processing_logs, row_number):
        """更新pallet记录"""
        #try:
        result = {'updated': 0, 'message': ''}
        
        # 使用sync_to_async包装同步查询
        # 1. 先查找已经有约的记录
        existing_pallets_query = Pallet.objects.filter(
            container_number__container_number=container_number,
            destination=destination,
            shipment_batch_number__isnull=False
        ).select_related('shipment_batch_number', 'master_shipment_batch_number')
        
        existing_pallets = await sync_to_async(list)(existing_pallets_query)
        
        existing_count = 0
        master_updated_count = 0
        for pallet in existing_pallets:
            existing_count += 1
            # 记录已有约的信息
            existing_appointment_id = pallet.shipment_batch_number.appointment_id if pallet.shipment_batch_number else '未知'
            processing_logs.append({
                'row': row_number,
                'type': 'info',
                'message': f'Pallet已有约: ID={pallet.id}, 当前约={existing_appointment_id}, 目标约={shipment.appointment_id}'
            })
            
            # 如果master_shipment_batch_number为空，就更新它
            if not pallet.master_shipment_batch_number:
                try:
                    pallet.master_shipment_batch_number = shipment
                    await sync_to_async(pallet.save)()
                    master_updated_count += 1
                    processing_logs.append({
                        'row': row_number,
                        'type': 'info',
                        'message': f'Pallet更新master_shipment: ID={pallet.id}'
                    })
                except Exception as e:
                    processing_logs.append({
                        'row': row_number,
                        'type': 'error',
                        'message': f'更新Pallet master_shipment失败: ID={pallet.id}, 错误: {str(e)}'
                    })
        
        if existing_count > 0:
            result['message'] += f'已有约{existing_count}条(更新master{master_updated_count}条) '
        
        # 2. 查找需要更新的空记录
        empty_pallets_query = Pallet.objects.filter(
            container_number__container_number=container_number,
            destination=destination,
            shipment_batch_number__isnull=True
        ).select_related('shipment_batch_number', 'master_shipment_batch_number')
        
        empty_pallets = await sync_to_async(list)(empty_pallets_query)
        
        updated_count = 0
        empty_count = len(empty_pallets)
        
        if empty_pallets:
            try:
                # 批量更新
                for pallet in empty_pallets:
                    pallet.shipment_batch_number = shipment
                    pallet.master_shipment_batch_number = shipment
                
                # 使用sync_to_async包装批量保存
                def bulk_update_pallets(pallets):
                    with transaction.atomic():
                        Pallet.objects.bulk_update(
                            pallets, 
                            ['shipment_batch_number', 'master_shipment_batch_number']
                        )
                
                await sync_to_async(bulk_update_pallets)(empty_pallets)
                updated_count = len(empty_pallets)
                processing_logs.append({
                    'row': row_number,
                    'type': 'info',
                    'message': f'Pallet批量更新: 找到{empty_count}条空记录, 成功更新{updated_count}条'
                })
            except Exception as e:
                processing_logs.append({
                    'row': row_number,
                    'type': 'error',
                    'message': f'Pallet批量更新失败: {str(e)}'
                })
        
        result['updated'] = updated_count
        
        if empty_count == 0 and existing_count == 0:
            result['message'] += '已有约和没有约都未找到任何记录'
        elif empty_count == 0 and existing_count > 0:
            result['message'] += '都有约了，不更新了'
        else:
            result['message'] += f'更新空记录{updated_count}条'
            
        return result
    
    async def handle_search_data(self, request: HttpRequest):
        context = {}
        
        table_name = request.POST.get("table_name")
        search_field = request.POST.get("search_field") 
        search_value = request.POST.get("search_value")
        
        if not all([table_name, search_field, search_value]):
            await sync_to_async(messages.error)(request, "请填写完整的查询条件")
            context['available_fields'] = await self.get_available_fields(None)
            return self.template_find_all_table, context
        
        # 设置上下文
        context.update({
            'table_name': table_name,
            'search_field': search_field,
            'search_value': search_value,
            'default_search_field': search_field,
        })
        
        # 根据表名执行查询 - 返回所有记录
        records_data = await self.query_table_data_all(table_name, search_field, search_value)
        
        if records_data:
            record_count = len(records_data)
            context.update({
                'record_data': records_data[0] if record_count > 0 else None,  # 第一条记录用于详细显示
                'records_list': records_data,  # 所有记录
                'record_count': record_count,
                'search_info': True,
            })
            if record_count > 1:
                await sync_to_async(messages.success)(request, f"找到 {record_count} 条记录")
            else:
                await sync_to_async(messages.success)(request, "查询成功")
        else:
            context.update({
                'record_data': None,
                'records_list': [],
                'record_count': 0,
                'search_info': True,
            })
            await sync_to_async(messages.warning)(request, "未找到匹配的记录")
        
        # 设置可用的查询字段
        context['available_fields'] = await self.get_available_fields(table_name)
        
        return self.template_find_all_table, context

    async def get_model_by_name(self, table_name: str):
        """根据表名获取对应的Django模型"""
        model_map = {
            'Container': Container,
            'Customer': Customer,
            'FeeDetail': FeeDetail,
            'FleetShipmentPallet': FleetShipmentPallet,
            'Fleet': Fleet,
            'InvoicePreport': InvoicePreport,
            'InvoiceWarehouse': InvoiceWarehouse,
            'InvoiceDelivery': InvoiceDelivery,
            'Invoice': Invoice,
            'InvoiceStatus': InvoiceStatus,
            'AbnormalOffloadStatus': AbnormalOffloadStatus,
            'Order': Order,
            'PackingList': PackingList,
            'PalletDestroyed': PalletDestroyed,
            'Pallet': Pallet,
            'Historicalpallet': Pallet.history,
            'Historicalpackinglist': PackingList.history,
            'PoCheckEtaSeven': PoCheckEtaSeven,
            'QuotationMaster': QuotationMaster,
            'TransferLocation': TransferLocation,
            'Vessel': Vessel,
        }
        
        return model_map.get(table_name)

    async def query_table_data_all(self, table_name: str, search_field: str, search_value: str):
        """查询表数据 - 返回所有匹配的记录（支持普通模型和历史表）"""
        try:
            # 根据表名获取模型/历史管理器
            model = await self.get_model_by_name(table_name)
            if not model:
                logger.error(f"未找到表名对应的模型：{table_name}")
                return None

            # 定义同步查询函数（内部区分普通模型和历史表）
            def get_records():
                # 1. 区分：普通模型（有 _meta） vs 历史表管理器（HistoryManager，无 _meta）
                if isinstance(model, HistoryManager):
                    # 历史表：字段信息从主表获取（历史表字段与主表完全一致）
                    # 主表 = 历史管理器对应的主模型（如 Pallet.history 对应的主表是 Pallet）
                    main_model = model.model
                    # 检查查询字段是否存在于主表
                    if search_field not in [f.name for f in main_model._meta.fields]:
                        logger.error(f"历史表对应主表 {main_model.__name__} 中不存在字段：{search_field}")
                        return []
                    # 历史表直接用 model.filter()（无需 .objects）
                    queryset = model
                else:
                    # 普通模型：直接使用 model._meta
                    if search_field not in [f.name for f in model._meta.fields]:
                        logger.error(f"模型 {model.__name__} 中不存在字段：{search_field}")
                        return []
                    # 普通模型用 model.objects.filter()
                    queryset = model.objects

                # 2. 构建查询条件（外键处理逻辑不变，仅调整字段来源）
                # 确定字段所属的模型（普通模型/历史表主表）
                field_owner = main_model if isinstance(model, HistoryManager) else model
                model_field = field_owner._meta.get_field(search_field)

                filter_key = ""
                if model_field.is_relation:
                    # 外键字段：拼接关联模型的匹配字段
                    related_model = model_field.related_model
                    # 候选关联字段（按优先级排序，匹配到第一个就用）
                    candidate_fields = ["container_number", "name", "number", "fleet_number", "shipment_batch_number",
                                        "id"]
                    target_field = None
                    for c in candidate_fields:
                        if c in [f.name for f in related_model._meta.fields]:
                            target_field = c
                            break
                    # 构建外键查询条件（如：container_number__container_number__icontains）
                    filter_key = f"{search_field}__{target_field}__icontains"
                else:
                    # 普通字段：直接用 icontains 模糊查询
                    filter_key = f"{search_field}__icontains"

                # 3. 执行查询（历史表和普通模型的 queryset 都支持 filter）
                filter_kwargs = {filter_key: search_value}
                records = list(queryset.filter(**filter_kwargs).values())
                logger.info(f"查询 {table_name} 成功，匹配条件：{filter_key}={search_value}，结果数：{len(records)}")
                return records

            # 异步执行查询
            records = await sync_to_async(get_records)()
            return records

        except Exception as e:
            logger.error(f"查询表数据失败: 表名={table_name}, 字段={search_field}, 值={search_value}, 错误={str(e)}",
                         exc_info=True)
            return None

    async def query_table_data(self, table_name, search_field, search_value):
        """异步查询表数据"""
        model_map = {
            'Container': Container,
            'Customer': Customer,
            'FeeDetail': FeeDetail,
            'FleetShipmentPallet': FleetShipmentPallet,
            'Fleet': Fleet,
            'InvoicePreport': InvoicePreport,
            'InvoiceWarehouse': InvoiceWarehouse,
            'InvoiceDelivery': InvoiceDelivery,
            'Invoice': Invoice,
            'InvoiceStatus': InvoiceStatus,
            'AbnormalOffloadStatus': AbnormalOffloadStatus,
            'Order': Order,
            'PackingList': PackingList,
            'Historicalpackinglist': PackingList.history,
            'PalletDestroyed': PalletDestroyed,
            'Pallet': Pallet,
            'Historicalpallet': Pallet.history,
            'PoCheckEtaSeven': PoCheckEtaSeven,
            'QuotationMaster': QuotationMaster,
            'TransferLocation': TransferLocation,
            'Vessel': Vessel,
        }
        
        if table_name not in model_map:
            raise ValueError(f"未知的表名: {table_name}")
        
        model = model_map[table_name]
        
        # 构建查询条件
        if search_field == 'id':
            # ID查询
            try:
                obj = await model.objects.aget(id=int(search_value))
            except (ValueError, model.DoesNotExist):
                return None
        else:
            # 其他字段查询
            if search_field in ['container_number', 'fleet_number', 'shipment_batch_number', 
                        'quotation_id', 'invoice_number']:
                # 这些是外键字段，需要特殊处理
                if search_field == 'container_number':
                    if table_name == 'Container':
                        # 在Container表本身搜索container_number字段
                        obj = await model.objects.filter(
                            container_number__icontains=search_value
                        ).afirst()
                    elif hasattr(model, 'container_number'):
                        # 在其他表通过外键搜索关联的container_number
                        obj = await model.objects.filter(
                            container_number__container_number__icontains=search_value
                        ).afirst()
                    else:
                        obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
                        
                elif search_field == 'fleet_number':
                    if table_name == 'Fleet':
                        # 在Fleet表本身搜索fleet_number字段
                        obj = await model.objects.filter(
                            fleet_number__icontains=search_value
                        ).afirst()
                    elif hasattr(model, 'fleet_number'):
                        # 在其他表通过外键搜索关联的fleet_number
                        obj = await model.objects.filter(
                            fleet_number__fleet_number__icontains=search_value
                        ).afirst()
                    else:
                        obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
                        
                elif search_field == 'shipment_batch_number':
                    if table_name == 'Shipment':  # 注意：Shipment表不在model_map中，需要调整
                        obj = await model.objects.filter(
                            shipment_batch_number__icontains=search_value
                        ).afirst()
                    elif hasattr(model, 'shipment_batch_number'):
                        obj = await model.objects.filter(
                            shipment_batch_number__shipment_batch_number__icontains=search_value
                        ).afirst()
                    else:
                        obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
                        
                elif search_field == 'quotation_id':
                    if table_name == 'QuotationMaster':
                        obj = await model.objects.filter(
                            quotation_id__icontains=search_value
                        ).afirst()
                    elif hasattr(model, 'quotation_id'):
                        obj = await model.objects.filter(
                            quotation_id__quotation_id__icontains=search_value
                        ).afirst()
                    else:
                        obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
                        
                elif search_field == 'invoice_number':
                    if table_name == 'Invoice':
                        obj = await model.objects.filter(
                            invoice_number__icontains=search_value
                        ).afirst()
                    elif hasattr(model, 'invoice_number'):
                        obj = await model.objects.filter(
                            invoice_number__invoice_number__icontains=search_value
                        ).afirst()
                    else:
                        obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
                else:
                    obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
            else:
                # 普通字段查询
                obj = await model.objects.filter(**{f"{search_field}__icontains": search_value}).afirst()
        
        if not obj:
            return None
        
        # 使用 sync_to_async 包装对象属性访问
        record_data = await sync_to_async(self._convert_obj_to_dict)(obj)
        return record_data

    def _convert_obj_to_dict(self, obj):
        """同步方法：将对象转换为字典"""
        record_data = {}
        for field in obj._meta.fields:
            field_name = field.name
            field_value = getattr(obj, field_name)
            
            # 处理外键字段
            if field.is_relation and field_value is not None:
                try:
                    # 获取外键对象的字符串表示
                    related_obj = getattr(obj, field_name)
                    if hasattr(related_obj, 'id'):
                        related_str = f"{related_obj} (ID: {related_obj.id})"
                        record_data[field_name] = related_str
                    else:
                        record_data[field_name] = str(related_obj)
                except Exception as e:
                    record_data[field_name] = f"Error: {str(e)}"
            else:
                # 处理JSONField
                if hasattr(field_value, 'items'):  # 如果是字典类型的JSONField
                    try:
                        record_data[field_name] = json.dumps(field_value, ensure_ascii=False, indent=2)
                    except:
                        record_data[field_name] = str(field_value)
                elif isinstance(field_value, (list, tuple)):
                    try:
                        record_data[field_name] = json.dumps(field_value, ensure_ascii=False, indent=2)
                    except:
                        record_data[field_name] = str(field_value)
                else:
                    record_data[field_name] = field_value
        
        return record_data

    async def get_available_fields(self, table_name):
        """异步获取可用字段"""
        field_map = {
            'Container': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'Customer': [
                ('id', 'ID'),
                ('zem_name', 'zem_name')
            ],
            'FeeDetail': [
                ('id', 'ID'),
                ('quotation_id', 'quotation_id')
            ],
            'FleetShipmentPallet': [
                ('id', 'ID'),
                ('fleet_number', 'fleet_number'),
                ('pickup_number', 'pickup_number'),
                ('shipment_batch_number', 'shipment_batch_number')
            ],
            'Fleet': [
                ('id', 'ID'),
                ('fleet_number', 'fleet_number'),
                ('pickup_number', 'pickup_number')
            ],
            'InvoicePreport': [
                ('id', 'ID'),
                ('invoice_number', 'invoice_number')
            ],
            'InvoiceWarehouse': [
                ('id', 'ID'),
                ('invoice_number', 'invoice_number')
            ],
            'InvoiceDelivery': [
                ('id', 'ID'),
                ('invoice_number', 'invoice_number')
            ],
            'Invoice': [
                ('id', 'ID'),
                ('invoice_number', 'invoice_number'),
                ('container_number', 'container_number')
            ],
            'InvoiceStatus': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'AbnormalOffloadStatus': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'Order': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'PackingList': [
                ('id', 'ID'),
                ('container_number', 'container_number'),
                ('PO_ID', 'PO_ID')
            ],
            'Historicalpackinglist': [
                ('id', 'ID'),
                ('container_number', 'container_number'),
                ('PO_ID', 'PO_ID')
            ],
            'PalletDestroyed': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'Pallet': [
                ('id', 'ID'),
                ('container_number', 'container_number'),
                ('PO_ID', 'PO_ID'),
                ('slot', 'slot')
            ],
            'Historicalpallet': [
                ('id', 'ID'),
                ('container_number', 'container_number'),
                ('PO_ID', 'PO_ID'),
                ('slot', 'slot')
            ],
            'PoCheckEtaSeven': [
                ('id', 'ID'),
                ('container_number', 'container_number')
            ],
            'QuotationMaster': [
                ('id', 'ID'),
                ('exclusive_user', 'exclusive_user'),
                ('quote_type', 'quote_type')
            ],
            'TransferLocation': [
                ('id', 'ID'),
                ('fleet_number', 'fleet_number'),
                ('container_number', 'container_number')
            ],
            'Vessel': [
                ('id', 'ID'),
                ('vessel', 'vessel')
            ]
        }
        
        return field_map.get(table_name, [('id', 'ID')])
    async def handle_search_container(self, request: HttpRequest):
        """处理查询柜号请求"""
        context = {}
        container_number = request.POST.get('container_number', '').strip()
        
        if not container_number:
            messages.error(request, "请输入柜号")
            return self.template_container_pallet, context
        
        try:
            # 查询柜号
            container = await sync_to_async(
                lambda: Container.objects.filter(container_number=container_number).first()
            )()
            
            if not container:
                messages.error(request, f"未找到柜号 '{container_number}' 的相关数据")
                return self.template_container_pallet, context
            
            # 查询该柜号下的所有pallet记录，并关联shipment表
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(
                    container_number=container
                ).select_related(
                    'shipment_batch_number', 
                    'master_shipment_batch_number'
                ).order_by('PO_ID'))
            )()
            
            if not pallets:
                messages.error(request, f"柜号 '{container_number}' 下没有找到任何托盘记录")
                return self.template_container_pallet, context
            
            # 按照PO_ID、shipment_batch_number、master_shipment_batch_number分组
            grouped_pallets = await self.group_pallets_by_shipment(pallets)
            
            context['container'] = container
            context['pallets'] = pallets
            context['grouped_pallets'] = grouped_pallets
            context['container_number'] = container_number
                
        except Exception as e:
            messages.error(request, f"查询失败: {str(e)}")
        
        return self.template_container_pallet, context

    async def query_history(self, request: HttpRequest):
        # 1. 获取并处理请求参数
        pallet_ids = request.POST.get('pallet_ids', '').strip()  # 获取用户输入的Pallet ID
        context = {
            'pallet_ids': pallet_ids,  # 回显用户输入的ID
            'history_records': [],  # 存储查询到的历史记录（默认空）
            'has_result': False  # 标记是否有查询结果
        }

        # 2. 验证输入，若有有效ID则查询历史表
        if pallet_ids:
            # 解析ID（空格分隔，去重，过滤空值）
            pallet_id_list = [id.strip() for id in pallet_ids.split() if id.strip().isdigit()]
            if not pallet_id_list:
                context['error_msg'] = '请输入有效的数字类型Pallet ID'
                return TemplateResponse(request, self.template_restore_sorted_data_get, context)

            try:
                # 显示加载状态（前端通过has_result和loading标记控制，这里先查询）
                # 获取历史表模型（通过你之前的get_model_by_name方法）
                history_model = await self.get_model_by_name('Historicalpallet')
                if not history_model:
                    context['error_msg'] = '未找到历史表模型'
                    return self.template_restore_sorted_data_get, context

                # 3. 异步查询历史记录（主表Pallet.id在历史表中对应id字段）
                def get_history_records():
                    # 历史表查询：过滤主表ID在列表中，按操作时间倒序
                    return list(
                        history_model.filter(id__in=pallet_id_list)
                        .order_by('-history_date')  # 最新操作在前
                    )

                # 执行查询并处理结果
                history_records = await sync_to_async(get_history_records)()
                if history_records:
                    context['history_records'] = history_records
                    context['has_result'] = True
                else:
                    context['error_msg'] = '未查询到相关历史记录'

            except Exception as e:
                logger.error(f"查询Pallet历史记录失败：{str(e)}", exc_info=True)
                context['error_msg'] = f'查询失败：{str(e)}'

        return self.template_restore_sorted_data_get, context

    async def query_history_packinglist(self, request: HttpRequest):
        """查询packinglist记录"""
        # 1. 获取并处理请求参数
        packinglist_ids = request.POST.get('packinglist_ids', '').strip()  # 获取用户输入的Pallet ID
        context = {
            'packinglist_ids': packinglist_ids,  # 回显用户输入的ID
            'history_records': [],  # 存储查询到的历史记录（默认空）
            'has_result': False  # 标记是否有查询结果
        }

        # 2. 验证输入，若有有效ID则查询历史表
        if packinglist_ids:
            # 解析ID（空格分隔，去重，过滤空值）
            packinglist_id_list = [id.strip() for id in packinglist_ids.split() if id.strip().isdigit()]
            if not packinglist_id_list:
                context['error_msg'] = '请输入有效的数字类型Pallet ID'
                return TemplateResponse(request, self.template_restore_sorted_data_get_packinglist, context)

            try:
                # 显示加载状态（前端通过has_result和loading标记控制，这里先查询）
                # 获取历史表模型（通过你之前的get_model_by_name方法）
                history_model = await self.get_model_by_name('Historicalpackinglist')
                if not history_model:
                    context['error_msg'] = '未找到历史表模型'
                    return self.template_restore_sorted_data_get_packinglist, context

                # 3. 异步查询历史记录（主表Pallet.id在历史表中对应id字段）
                def get_history_records():
                    # 历史表查询：过滤主表ID在列表中，按操作时间倒序
                    return list(
                        history_model.filter(id__in=packinglist_id_list)
                        .order_by('-history_date')  # 最新操作在前
                    )

                # 执行查询并处理结果
                history_records = await sync_to_async(get_history_records)()
                if history_records:
                    context['history_records'] = history_records
                    context['has_result'] = True
                else:
                    context['error_msg'] = '未查询到相关历史记录'

            except Exception as e:
                logger.error(f"查询Pallet历史记录失败：{str(e)}", exc_info=True)
                context['error_msg'] = f'查询失败：{str(e)}'

        return self.template_restore_sorted_data_get_packinglist, context

    async def single_restore(self, request: HttpRequest):
        """单行恢复：根据历史记录ID恢复Pallet主表数据（完整字段匹配）"""
        history_id = request.POST.get('history_id')
        pallet_id = request.POST.get('pallet_id')
        if not history_id:
            # 无ID则返回查询页面（确保返回TemplateResponse）
            context = await self._get_query_context(request)
            return self.template_restore_sorted_data_get, context

        try:
            # 获取历史表模型和记录
            history_model = await self.get_model_by_name('Historicalpallet')
            history_record = await sync_to_async(history_model.get)(history_id=history_id)

            # 构建主表数据（与批量恢复完全一致）
            pallet_data = {
                'packing_list_id': getattr(history_record, 'packing_list_id', None),
                'container_number_id': getattr(history_record, 'container_number_id', None),
                'shipment_batch_number_id': getattr(history_record, 'shipment_batch_number_id', None),
                'master_shipment_batch_number_id': getattr(history_record, 'master_shipment_batch_number_id', None),
                'transfer_batch_number_id': getattr(history_record, 'transfer_batch_number_id', None),
                'invoice_delivery_id': getattr(history_record, 'invoice_delivery_id', None),
                'destination': getattr(history_record, 'destination', ''),
                'address': getattr(history_record, 'address', ''),
                'zipcode': getattr(history_record, 'zipcode', ''),
                'delivery_method': getattr(history_record, 'delivery_method', ''),
                'delivery_type': getattr(history_record, 'delivery_type', ''),
                'pallet_id': getattr(history_record, 'pallet_id', ''),
                'PO_ID': getattr(history_record, 'PO_ID', ''),
                'slot': getattr(history_record, 'slot', ''),
                'shipping_mark': getattr(history_record, 'shipping_mark', ''),
                'fba_id': getattr(history_record, 'fba_id', ''),
                'ref_id': getattr(history_record, 'ref_id', ''),
                'sequence_number': getattr(history_record, 'sequence_number', ''),
                'note': getattr(history_record, 'note', ''),
                'note_sp': getattr(history_record, 'note_sp', ''),
                'priority': getattr(history_record, 'priority', ''),
                'location': getattr(history_record, 'location', ''),
                'contact_name': getattr(history_record, 'contact_name', ''),
                'pcs': getattr(history_record, 'pcs', 0),
                'length': getattr(history_record, 'length', 0.0),
                'width': getattr(history_record, 'width', 0.0),
                'height': getattr(history_record, 'height', 0.0),
                'cbm': getattr(history_record, 'cbm', 0.0),
                'weight_lbs': getattr(history_record, 'weight_lbs', 0.0),
                'is_dropped_pallet': getattr(history_record, 'is_dropped_pallet', False),
                'abnormal_palletization': getattr(history_record, 'abnormal_palletization', False),
                'po_expired': getattr(history_record, 'po_expired', False),
                'delivery_window_start': getattr(history_record, 'delivery_window_start', None),
                'delivery_window_end': getattr(history_record, 'delivery_window_end', None),
            }

            # 恢复到主表：存在则更新，不存在则创建（按主表ID匹配）
            await sync_to_async(Pallet.objects.update_or_create)(
                id=pallet_id,  # 历史记录的id字段 = 主表Pallet.id
                defaults=pallet_data
            )

            # 传递成功消息，返回查询页面
            context = await self._get_query_context(request)
            context['message'] = '恢复成功！'
            context['message_type'] = 'success'
            return self.template_restore_sorted_data_get, context

        except Exception as e:
            logger.error(f"单行恢复失败（历史ID：{history_id}）：{str(e)}", exc_info=True)
            context = await self._get_query_context(request)
            context['error_msg'] = f'恢复失败：{str(e)}'
            return self.template_restore_sorted_data_get, context

    async def single_restore_packinglist(self, request: HttpRequest):
        """单行恢复：根据历史记录ID恢复Pallet主表数据（完整字段匹配）"""
        history_id = request.POST.get('history_id')
        packinglist_id = request.POST.get('packinglist_id')
        if not packinglist_id:
            # 无ID则返回查询页面（确保返回TemplateResponse）
            context = await self._get_query_context_packinglist(request)
            return self.template_restore_sorted_data_get_packinglist, context

        try:
            # 获取历史表模型和记录
            history_model = await self.get_model_by_name('Historicalpackinglist')
            history_record = await sync_to_async(history_model.get)(history_id=history_id)

            # 构建主表数据（与批量恢复完全一致）
            packinglist_data = {
                # 主键&核心外键字段
                "id": getattr(history_record, "id", None),
                "container_number_id": getattr(history_record, "container_number_id", None),
                "shipment_batch_number_id": getattr(history_record, "shipment_batch_number_id", None),
                "master_shipment_batch_number_id": getattr(history_record, "master_shipment_batch_number_id",
                                                           None),
                "quote_id_id": getattr(history_record, "quote_id_id", None),
                # 配送基础信息
                "destination": getattr(history_record, "destination", ""),
                "address": getattr(history_record, "address", ""),
                "zipcode": getattr(history_record, "zipcode", ""),
                "delivery_method": getattr(history_record, "delivery_method", ""),
                "delivery_type": getattr(history_record, "delivery_type", ""),
                # 标识字段
                "PO_ID": getattr(history_record, "PO_ID", ""),
                "shipping_mark": getattr(history_record, "shipping_mark", ""),
                "fba_id": getattr(history_record, "fba_id", ""),
                "ref_id": getattr(history_record, "ref_id", ""),
                # 计量核心字段
                "pcs": getattr(history_record, "pcs", 0),
                "unit_weight_lbs": getattr(history_record, "unit_weight_lbs", 0.0),
                "total_weight_lbs": getattr(history_record, "total_weight_lbs", 0.0),
                "total_weight_kg": getattr(history_record, "total_weight_kg", 0.0),
                "cbm": getattr(history_record, "cbm", 0.0),
                "n_pallet": getattr(history_record, "n_pallet", 0),
                # 尺寸字段（表中字段为long/width/height，对应长度/宽度/高度）
                "long": getattr(history_record, "long", 0.0),
                "width": getattr(history_record, "width", 0.0),
                "height": getattr(history_record, "height", 0.0),
                # 备注&产品信息
                "product_name": getattr(history_record, "product_name", ""),
                "note": getattr(history_record, "note", ""),
                "note_sp": getattr(history_record, "note_sp", ""),
                # 联系人信息
                "contact_name": getattr(history_record, "contact_name", ""),
                "contact_method": getattr(history_record, "contact_method", ""),
                # 物流相关字段
                "express_number": getattr(history_record, "express_number", ""),
                "carrier_company": getattr(history_record, "carrier_company", ""),
                "PickupAddr": getattr(history_record, "PickupAddr", ""),
                "est_pickup_time": getattr(history_record, "est_pickup_time", None),
                # 配送时间窗口
                "delivery_window_start": getattr(history_record, "delivery_window_start", None),
                "delivery_window_end": getattr(history_record, "delivery_window_end", None),
                # LTL相关字段（含布尔必段）
                "ltl_verify": getattr(history_record, "ltl_verify", False),
                "ltl_bol_num": getattr(history_record, "ltl_bol_num", ""),
                "ltl_pro_num": getattr(history_record, "ltl_pro_num", ""),
                "ltl_follow_status": getattr(history_record, "ltl_follow_status", ""),
                "ltl_contact_method": getattr(history_record, "ltl_contact_method", ""),
                "ltl_release_command": getattr(history_record, "ltl_release_command", "")
            }

            # 恢复主表数据
            await sync_to_async(PackingList.objects.update_or_create)(
                id=packinglist_id,
                defaults=packinglist_data,
            )


            # 传递成功消息，返回查询页面
            context = await self._get_query_context_packinglist(request)
            context['message'] = '恢复成功！'
            context['message_type'] = 'success'
            return self.template_restore_sorted_data_get_packinglist, context

        except Exception as e:
            logger.error(f"单行恢复失败（历史ID：{history_id}）：{str(e)}", exc_info=True)
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = f'恢复失败：{str(e)}'
            return self.template_restore_sorted_data_get_packinglist, context

    async def batch_restore(self, request: HttpRequest):
        """批量恢复：根据选中的历史记录ID列表恢复（完整字段匹配）"""
        selected_ids_str = request.POST.get('selected_history_ids', '').strip()
        selected_pallet_ids = request.POST.get('selected_pallet_ids', '').strip()
        if not selected_ids_str:
            context = await self._get_query_context(request)
            context['error_msg'] = '请选中要恢复的历史记录！'
            return self.template_restore_sorted_data_get, context

        selected_ids = [id.strip() for id in selected_ids_str.split(',') if id.strip()]
        pallet_ids = [id.strip() for id in selected_pallet_ids.split(',') if id.strip()]
        success_count = 0
        failed_ids = []  # 记录恢复失败的历史ID

        try:
            history_model = await self.get_model_by_name('Historicalpallet')
            for history_id, pallet_id in zip(selected_ids, pallet_ids):
                try:
                    # 查询单个历史记录
                    history_record = await sync_to_async(history_model.get)(history_id=history_id)

                    # 构建主表数据（与单行恢复完全一致）
                    pallet_data = {
                        'packing_list_id': getattr(history_record, 'packing_list_id', None),
                        'container_number_id': getattr(history_record, 'container_number_id', None),
                        'shipment_batch_number_id': getattr(history_record, 'shipment_batch_number_id', None),
                        'master_shipment_batch_number_id': getattr(history_record, 'master_shipment_batch_number_id',
                                                                   None),
                        'transfer_batch_number_id': getattr(history_record, 'transfer_batch_number_id', None),
                        'invoice_delivery_id': getattr(history_record, 'invoice_delivery_id', None),
                        'destination': getattr(history_record, 'destination', ''),
                        'address': getattr(history_record, 'address', ''),
                        'zipcode': getattr(history_record, 'zipcode', ''),
                        'delivery_method': getattr(history_record, 'delivery_method', ''),
                        'delivery_type': getattr(history_record, 'delivery_type', ''),
                        'pallet_id': getattr(history_record, 'pallet_id', ''),
                        'PO_ID': getattr(history_record, 'PO_ID', ''),
                        'slot': getattr(history_record, 'slot', ''),
                        'shipping_mark': getattr(history_record, 'shipping_mark', ''),
                        'fba_id': getattr(history_record, 'fba_id', ''),
                        'ref_id': getattr(history_record, 'ref_id', ''),
                        'sequence_number': getattr(history_record, 'sequence_number', ''),
                        'note': getattr(history_record, 'note', ''),
                        'note_sp': getattr(history_record, 'note_sp', ''),
                        'priority': getattr(history_record, 'priority', ''),
                        'location': getattr(history_record, 'location', ''),
                        'contact_name': getattr(history_record, 'contact_name', ''),
                        'pcs': getattr(history_record, 'pcs', 0),
                        'length': getattr(history_record, 'length', 0.0),
                        'width': getattr(history_record, 'width', 0.0),
                        'height': getattr(history_record, 'height', 0.0),
                        'cbm': getattr(history_record, 'cbm', 0.0),
                        'weight_lbs': getattr(history_record, 'weight_lbs', 0.0),
                        'is_dropped_pallet': getattr(history_record, 'is_dropped_pallet', False),
                        'abnormal_palletization': getattr(history_record, 'abnormal_palletization', False),
                        'po_expired': getattr(history_record, 'po_expired', False),
                        'delivery_window_start': getattr(history_record, 'delivery_window_start', None),
                        'delivery_window_end': getattr(history_record, 'delivery_window_end', None),
                    }

                    # 恢复主表数据
                    await sync_to_async(Pallet.objects.update_or_create)(
                        id=pallet_id,
                        defaults=pallet_data
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"批量恢复单条失败（历史ID：{history_id}）：{str(e)}", exc_info=True)
                    failed_ids.append(history_id)

            # 构建结果消息
            context = await self._get_query_context(request)
            if success_count > 0 and len(failed_ids) == 0:
                context['message'] = f'批量恢复成功！共恢复 {success_count} 条记录'
                context['message_type'] = 'success'
            elif success_count > 0 and len(failed_ids) > 0:
                context[
                    'message'] = f'部分恢复成功！共恢复 {success_count} 条，失败 {len(failed_ids)} 条（失败ID：{",".join(failed_ids)}）'
                context['message_type'] = 'warning'
            else:
                context['error_msg'] = f'批量恢复失败！所有选中记录均未恢复（失败ID：{",".join(failed_ids)}）'

            # 返回TemplateResponse
            return self.template_restore_sorted_data_get, context

        except Exception as e:
            logger.error(f"批量恢复总览失败：{str(e)}", exc_info=True)
            context = await self._get_query_context(request)
            context['error_msg'] = f'批量恢复失败：{str(e)}'
            # 返回TemplateResponse
            return self.template_restore_sorted_data_get, context

    async def batch_restore_packinglist(self, request: HttpRequest):
        """批量恢复：根据选中的历史记录ID列表恢复（完整字段匹配）"""
        selected_ids_str = request.POST.get('selected_history_ids', '').strip()
        selected_packinglist_ids = request.POST.get('selected_packinglist_ids', '').strip()
        if not selected_ids_str:
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = '请选中要恢复的历史记录！'
            return self.template_restore_sorted_data_get_packinglist, context

        selected_ids = [id.strip() for id in selected_ids_str.split(',') if id.strip()]
        packinglist_ids = [id.strip() for id in selected_packinglist_ids.split(',') if id.strip()]
        success_count = 0
        failed_ids = []  # 记录恢复失败的历史ID

        try:
            history_model = await self.get_model_by_name('Historicalpackinglist')
            for history_id, packinglist_id in zip(selected_ids, packinglist_ids):
                try:
                    # 查询单个历史记录
                    history_record = await sync_to_async(history_model.get)(history_id=history_id)

                    # 构建主表数据（与单行恢复完全一致）
                    packinglist_data = {
                        # 主键&核心外键字段
                        "id": getattr(history_record, "id", None),
                        "container_number_id": getattr(history_record, "container_number_id", None),
                        "shipment_batch_number_id": getattr(history_record, "shipment_batch_number_id", None),
                        "master_shipment_batch_number_id": getattr(history_record, "master_shipment_batch_number_id",
                                                                   None),
                        "quote_id_id": getattr(history_record, "quote_id_id", None),
                        # 配送基础信息
                        "destination": getattr(history_record, "destination", ""),
                        "address": getattr(history_record, "address", ""),
                        "zipcode": getattr(history_record, "zipcode", ""),
                        "delivery_method": getattr(history_record, "delivery_method", ""),
                        "delivery_type": getattr(history_record, "delivery_type", ""),
                        # 标识字段
                        "PO_ID": getattr(history_record, "PO_ID", ""),
                        "shipping_mark": getattr(history_record, "shipping_mark", ""),
                        "fba_id": getattr(history_record, "fba_id", ""),
                        "ref_id": getattr(history_record, "ref_id", ""),
                        # 计量核心字段
                        "pcs": getattr(history_record, "pcs", 0),
                        "unit_weight_lbs": getattr(history_record, "unit_weight_lbs", 0.0),
                        "total_weight_lbs": getattr(history_record, "total_weight_lbs", 0.0),
                        "total_weight_kg": getattr(history_record, "total_weight_kg", 0.0),
                        "cbm": getattr(history_record, "cbm", 0.0),
                        "n_pallet": getattr(history_record, "n_pallet", 0),
                        # 尺寸字段（表中字段为long/width/height，对应长度/宽度/高度）
                        "long": getattr(history_record, "long", 0.0),
                        "width": getattr(history_record, "width", 0.0),
                        "height": getattr(history_record, "height", 0.0),
                        # 备注&产品信息
                        "product_name": getattr(history_record, "product_name", ""),
                        "note": getattr(history_record, "note", ""),
                        "note_sp": getattr(history_record, "note_sp", ""),
                        # 联系人信息
                        "contact_name": getattr(history_record, "contact_name", ""),
                        "contact_method": getattr(history_record, "contact_method", ""),
                        # 物流相关字段
                        "express_number": getattr(history_record, "express_number", ""),
                        "carrier_company": getattr(history_record, "carrier_company", ""),
                        "PickupAddr": getattr(history_record, "PickupAddr", ""),
                        "est_pickup_time": getattr(history_record, "est_pickup_time", None),
                        # 配送时间窗口
                        "delivery_window_start": getattr(history_record, "delivery_window_start", None),
                        "delivery_window_end": getattr(history_record, "delivery_window_end", None),
                        # LTL相关字段（含布尔必段）
                        "ltl_verify": getattr(history_record, "ltl_verify", False),
                        "ltl_bol_num": getattr(history_record, "ltl_bol_num", ""),
                        "ltl_pro_num": getattr(history_record, "ltl_pro_num", ""),
                        "ltl_follow_status": getattr(history_record, "ltl_follow_status", ""),
                        "ltl_contact_method": getattr(history_record, "ltl_contact_method", ""),
                        "ltl_release_command": getattr(history_record, "ltl_release_command", "")
                    }

                    # 恢复主表数据
                    await sync_to_async(PackingList.objects.update_or_create)(
                        id=packinglist_id,
                        defaults=packinglist_data,
                    )
                    success_count += 1
                except Exception as e:
                    logger.error(f"批量恢复单条失败（历史ID：{history_id}）：{str(e)}", exc_info=True)
                    failed_ids.append(history_id)

            # 构建结果消息
            context = await self._get_query_context_packinglist(request)
            if success_count > 0 and len(failed_ids) == 0:
                context['message'] = f'批量恢复成功！共恢复 {success_count} 条记录'
                context['message_type'] = 'success'
            elif success_count > 0 and len(failed_ids) > 0:
                context[
                    'message'] = f'部分恢复成功！共恢复 {success_count} 条，失败 {len(failed_ids)} 条（失败ID：{",".join(failed_ids)}）'
                context['message_type'] = 'warning'
            else:
                context['error_msg'] = f'批量恢复失败！所有选中记录均未恢复（失败ID：{",".join(failed_ids)}）'

            # 返回TemplateResponse
            return self.template_restore_sorted_data_get_packinglist, context

        except Exception as e:
            logger.error(f"批量恢复总览失败：{str(e)}", exc_info=True)
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = f'批量恢复失败：{str(e)}'
            # 返回TemplateResponse
            return self.template_restore_sorted_data_get_packinglist, context

    async def _get_query_context(self, request):
        """复用查询页面的context（补充所有需显示的历史字段）- 返回纯字典"""
        pallet_ids = request.POST.get('pallet_ids', '').strip()
        # 初始化context为字典（而非元组）
        context = {
            'pallet_ids': pallet_ids,
            'history_records': [],
            'has_result': False,
            'error_msg': '',  # 初始化错误消息字段
            'message': '',  # 初始化成功消息字段
            'message_type': ''  # 初始化消息类型字段
        }
        if pallet_ids:
            # 解析有效ID（仅保留数字）
            pallet_id_list = [id.strip() for id in pallet_ids.split() if id.strip().isdigit()]
            if pallet_id_list:
                history_model = await self.get_model_by_name('Historicalpallet')
                try:
                    # 查询时包含所有需显示的字段（与前端表格列对应）
                    history_records = await sync_to_async(list)(
                        history_model.filter(id__in=pallet_id_list)
                        .order_by('-history_date')  # 最新操作在前
                        .values(
                            # 历史表特有字段
                            'history_id', 'history_date', 'history_type',
                            'history_user__username', 'history_change_reason',
                            # Pallet主表关联字段（与前端表格列对应）
                            'id', 'container_number_id', 'destination', 'fba_id', 'ref_id',
                            'pcs', 'cbm', 'weight_lbs', 'is_dropped_pallet', 'delivery_method',
                            'delivery_type', 'PO_ID', 'shipping_mark', 'note', 'location'
                        )
                    )
                    context['history_records'] = history_records
                    context['has_result'] = len(history_records) > 0
                except Exception as e:
                    logger.error(f"查询历史记录失败：{str(e)}", exc_info=True)
                    context['error_msg'] = f'查询历史记录失败：{str(e)}'
        return context

    async def _get_query_context_packinglist(self, request):
        """复用查询页面的context（补充所有需显示的历史字段）- 返回纯字典"""
        packinglist_ids = request.POST.get('packinglist_ids', '').strip()
        # 初始化context为字典（而非元组）
        context = {
            'packinglist_ids': packinglist_ids,
            'history_records': [],
            'has_result': False,
            'error_msg': '',  # 初始化错误消息字段
            'message': '',  # 初始化成功消息字段
            'message_type': ''  # 初始化消息类型字段
        }
        if packinglist_ids:
            # 解析有效ID（仅保留数字）
            packinglist_id_list = [id.strip() for id in packinglist_ids.split() if id.strip().isdigit()]
            if packinglist_id_list:
                history_model = await self.get_model_by_name('Historicalpackinglist')
                try:
                    # 查询时包含所有需显示的字段（与前端表格列对应）
                    history_records = await sync_to_async(list)(
                        history_model.filter(id__in=packinglist_id_list)
                        .order_by('-history_date')  # 最新操作在前
                        .values(
                            # 历史表特有字段
                            'history_id', 'history_date', 'history_type',
                            'history_user_id', 'history_change_reason',
                            # Pallet主表关联字段（与前端表格列对应）
                            'id', 'container_number_id', 'destination', 'fba_id', 'ref_id',
                            'pcs', 'cbm', 'total_weight_lbs', 'delivery_method',
                            'delivery_type', 'PO_ID', 'shipping_mark', 'note'
                        )
                    )
                    context['history_records'] = history_records
                    context['has_result'] = len(history_records) > 0
                except Exception as e:
                    logger.error(f"查询历史记录失败：{str(e)}", exc_info=True)
                    context['error_msg'] = f'查询历史记录失败：{str(e)}'
        return context

    async def single_delete(self, request: HttpRequest):
        """单行删除：通过历史记录关联的主表ID删除Pallet记录"""
        pallet_id = request.POST.get('pallet_id')
        if not pallet_id:
            context = await self._get_query_context(request)
            context['error_msg'] = '未获取到要删除的Pallet ID！'
            return self.template_restore_sorted_data_get, context

        try:
            # 执行主表删除（物理删除，若需逻辑删除可修改为更新状态字段）
            delete_count, _ = await sync_to_async(Pallet.objects.filter(id=pallet_id).delete)()

            if delete_count > 0:
                context = await self._get_query_context(request)
                context['message'] = f'成功删除Pallet ID为 {pallet_id} 的主表记录！'
                context['message_type'] = 'success'
            else:
                context = await self._get_query_context(request)
                context['error_msg'] = f'未找到Pallet ID为 {pallet_id} 的主表记录（可能已删除）！'

            return self.template_restore_sorted_data_get, context

        except Exception as e:
            logger.error(f"单行删除失败（Pallet ID：{pallet_id}）：{str(e)}", exc_info=True)
            context = await self._get_query_context(request)
            context['error_msg'] = f'删除失败：{str(e)}'
            return self.template_restore_sorted_data_get, context

    async def single_delete_packinglist(self, request: HttpRequest):
        """单行删除：通过历史记录关联的主表ID删除Pallet记录"""
        packinglist_id = request.POST.get('packinglist_id')
        if not packinglist_id:
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = '未获取到要删除的Packinglist ID！'
            return self.template_restore_sorted_data_get_packinglist, context

        try:
            # 执行主表删除（物理删除，若需逻辑删除可修改为更新状态字段）
            delete_count, _ = await sync_to_async(PackingList.objects.filter(id=packinglist_id).delete)()

            if delete_count > 0:
                context = await self._get_query_context_packinglist(request)
                context['message'] = f'成功删除PackingList ID为 {packinglist_id} 的主表记录！'
                context['message_type'] = 'success'
            else:
                context = await self._get_query_context_packinglist(request)
                context['error_msg'] = f'未找到PackingList ID为 {packinglist_id} 的主表记录（可能已删除）！'

            return self.template_restore_sorted_data_get_packinglist, context

        except Exception as e:
            logger.error(f"单行删除失败（Pallet ID：{packinglist_id}）：{str(e)}", exc_info=True)
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = f'删除失败：{str(e)}'
            return self.template_restore_sorted_data_get_packinglist, context

    async def batch_delete(self, request: HttpRequest):
        """批量删除：通过选中的历史记录关联的主表ID列表删除Pallet记录"""
        selected_pallet_pallets_str = request.POST.get('selected_pallet_ids', '').strip()
        if not selected_pallet_pallets_str:
            context = await self._get_query_context(request)
            context['error_msg'] = '请选中要删除的历史记录！'
            return self.template_restore_sorted_data_get, context

        selected_pallet_ids = [id.strip() for id in selected_pallet_pallets_str.split(',') if id.strip().isdigit()]
        if not selected_pallet_ids:
            context = await self._get_query_context(request)
            context['error_msg'] = '未获取到有效的Pallet ID！'
            return self.template_restore_sorted_data_get, context

        success_count = 0
        failed_ids = []

        try:
            for pallet_id in selected_pallet_ids:
                try:
                    # 执行单条删除
                    delete_count, _ = await sync_to_async(Pallet.objects.filter(id=pallet_id).delete)()
                    if delete_count > 0:
                        success_count += 1
                    else:
                        failed_ids.append(pallet_id)
                except Exception as e:
                    logger.error(f"批量删除单条失败（Pallet ID：{pallet_id}）：{str(e)}", exc_info=True)
                    failed_ids.append(pallet_id)

            # 构建结果消息
            context = await self._get_query_context(request)
            if success_count > 0 and len(failed_ids) == 0:
                context['message'] = f'批量删除成功！共删除 {success_count} 条Pallet主表记录'
                context['message_type'] = 'success'
            elif success_count > 0 and len(failed_ids) > 0:
                context[
                    'message'] = f'部分删除成功！共删除 {success_count} 条，失败 {len(failed_ids)} 条（失败ID：{",".join(failed_ids)}）'
                context['message_type'] = 'warning'
            else:
                context['error_msg'] = f'批量删除失败！所有选中记录均未删除（失败ID：{",".join(failed_ids)}）'

            return self.template_restore_sorted_data_get, context

        except Exception as e:
            logger.error(f"批量删除总览失败：{str(e)}", exc_info=True)
            context = await self._get_query_context(request)
            context['error_msg'] = f'批量删除失败：{str(e)}'
            return self.template_restore_sorted_data_get, context

    async def batch_delete_packinglist(self, request: HttpRequest):
        """批量删除：通过选中的历史记录关联的主表ID列表删除Pallet记录"""
        selected_packinglist_packinglists_str = request.POST.get('selected_packinglist_ids', '').strip()
        if not selected_packinglist_packinglists_str:
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = '请选中要删除的历史记录！'
            return self.template_restore_sorted_data_get_packinglist, context

        selected_packinglist_ids = [id.strip() for id in selected_packinglist_packinglists_str.split(',') if id.strip().isdigit()]
        if not selected_packinglist_ids:
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = '未获取到有效的Pallet ID！'
            return self.template_restore_sorted_data_get_packinglist, context

        success_count = 0
        failed_ids = []

        try:
            for packinglist_id in selected_packinglist_ids:
                try:
                    # 执行单条删除
                    delete_count, _ = await sync_to_async(PackingList.objects.filter(id=packinglist_id).delete)()
                    if delete_count > 0:
                        success_count += 1
                    else:
                        failed_ids.append(packinglist_id)
                except Exception as e:
                    logger.error(f"批量删除单条失败（Packinglist ID：{packinglist_id}）：{str(e)}", exc_info=True)
                    failed_ids.append(packinglist_id)

            # 构建结果消息
            context = await self._get_query_context_packinglist(request)
            if success_count > 0 and len(failed_ids) == 0:
                context['message'] = f'批量删除成功！共删除 {success_count} 条Packinglist主表记录'
                context['message_type'] = 'success'
            elif success_count > 0 and len(failed_ids) > 0:
                context[
                    'message'] = f'部分删除成功！共删除 {success_count} 条，失败 {len(failed_ids)} 条（失败ID：{",".join(failed_ids)}）'
                context['message_type'] = 'warning'
            else:
                context['error_msg'] = f'批量删除失败！所有选中记录均未删除（失败ID：{",".join(failed_ids)}）'

            return self.template_restore_sorted_data_get_packinglist, context

        except Exception as e:
            logger.error(f"批量删除总览失败：{str(e)}", exc_info=True)
            context = await self._get_query_context_packinglist(request)
            context['error_msg'] = f'批量删除失败：{str(e)}'
            return self.template_restore_sorted_data_get_packinglist, context

    async def group_pallets_by_shipment(self, pallets):
        """按照PO_ID、shipment_batch_number、master_shipment_batch_number分组pallet记录"""
        groups = {}
        
        for pallet in pallets:
            # 构建分组键
            po_id = pallet.PO_ID or '未分类'
            shipment_key = pallet.shipment_batch_number_id or '未绑定'
            master_shipment_key = pallet.master_shipment_batch_number_id or '未绑定'
            
            group_key = f"{po_id}|{shipment_key}|{master_shipment_key}"
            
            if group_key not in groups:
                groups[group_key] = {
                    'PO_ID': po_id,
                    'destination': pallet.destination,
                    'delivery_method': pallet.delivery_method,
                    'shipment_batch_number_id': pallet.shipment_batch_number_id,
                    'master_shipment_batch_number_id': pallet.master_shipment_batch_number_id,
                    'shipment_batch_number_display': await self.get_shipment_display(pallet.shipment_batch_number),
                    'master_shipment_batch_number_display': await self.get_shipment_display(pallet.master_shipment_batch_number),
                    'pallets': [],
                    'pallet_count': 0,
                    'total_cbm': 0,
                    'total_weight': 0,
                    'total_pcs': 0
                }
            
            # 添加pallet到组
            groups[group_key]['pallets'].append(pallet)
            groups[group_key]['pallet_count'] += 1
            groups[group_key]['total_cbm'] += (pallet.cbm or 0)
            groups[group_key]['total_weight'] += (pallet.weight_lbs or 0)
            groups[group_key]['total_pcs'] += (pallet.pcs or 0)
        
        return list(groups.values())
    
    async def get_shipment_display(self, shipment):
        """获取shipment的显示字符串"""
        if not shipment:
            return "未绑定"
        
        display_info = await sync_to_async(
            lambda: f"{shipment.shipment_batch_number} ({shipment.destination or '无目的地'})"
        )()
        return display_info
    
    async def handle_update_pallet_master_shipment(self, request: HttpRequest):
        """处理更新pallet的主约批次号绑定"""
        context = {}
        try:
            container_number = request.POST.get('container_number')
            po_id = request.POST.get('po_id')
            current_shipment_id = request.POST.get('current_shipment_id')
            current_master_shipment_id = request.POST.get('current_master_shipment_id')
            new_master_shipment_batch = request.POST.get('new_master_shipment_batch', '').strip()
            
            if not container_number:
                messages.error(request, "缺少柜号信息")
                return self.template_container_pallet, context
            
            if not new_master_shipment_batch:
                messages.error(request, "请输入新的主约批次号")
                return self.template_container_pallet, context
            
            # 查询柜号
            container = await sync_to_async(
                lambda: Container.objects.filter(container_number=container_number).first()
            )()
            
            if not container:
                messages.error(request, f"未找到柜号 '{container_number}'")
                return self.template_container_pallet, context
            
            # 查找新的主约shipment记录
            try:
                new_master_shipment = await sync_to_async(
                    lambda: Shipment.objects.get(shipment_batch_number=new_master_shipment_batch)
                )()
            except ObjectDoesNotExist:
                messages.error(request, f"未找到主约批次号 '{new_master_shipment_batch}'")
                return self.template_container_pallet, context
            except MultipleObjectsReturned:
                messages.error(request, f"找到多个主约批次号 '{new_master_shipment_batch}'，请核实")
                return self.template_container_pallet, context
            
            # 构建查询条件
            query_filters = {'container_number': container}
            if po_id and po_id != '未分类':
                query_filters['PO_ID'] = po_id
            if current_shipment_id and current_shipment_id != '未绑定':
                query_filters['shipment_batch_number_id'] = current_shipment_id
            if current_master_shipment_id and current_master_shipment_id != '未绑定':
                query_filters['master_shipment_batch_number_id'] = current_master_shipment_id
            
            # 更新符合条件的pallet记录的主约批次号
            updated_count = await sync_to_async(
                lambda: Pallet.objects.filter(**query_filters).update(
                    master_shipment_batch_number=new_master_shipment
                )
            )()
            
            messages.success(request, f"成功更新 {updated_count} 条托盘记录的主约批次号绑定")
            
            # 重新查询数据以更新显示
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(
                    container_number=container
                ).select_related(
                    'shipment_batch_number', 
                    'master_shipment_batch_number'
                ).order_by('PO_ID'))
            )()
            
            grouped_pallets = await self.group_pallets_by_shipment(pallets)
            
            context['container'] = container
            context['pallets'] = pallets
            context['grouped_pallets'] = grouped_pallets
            context['container_number'] = container_number
            
        except Exception as e:
            messages.error(request, f"更新失败: {str(e)}")
        
        return self.template_container_pallet, context
    
    async def handle_find_table_id_get(self,request):
        context = {}
        return self.template_delivery_invoice, context
    
    async def handle_search_shipment(self, request: HttpRequest):
        """处理查询shipment请求"""
        context = {
            'warehouse_options':self.warehouse_options,
            'shipment_type_options': self.shipment_type_options,
        }
        
        search_value = request.POST.get('search_value', '').strip()
        search_type = request.POST.get('search_type')
        if not search_value:
            messages.error(request, "请输入查询内容")
            return self.template_post_port_status, context
        
        try:
            # 根据查询类型构建查询条件
            if search_type == 'batch':
                # 按批次号查询
                shipment = await sync_to_async(
                    lambda: Shipment.objects.select_related('fleet_number').get(shipment_batch_number=search_value)
                )()
                context['search_type'] = 'batch'
                context['search_value'] = search_value
            elif search_type == 'shipment_id':
                shipment = await sync_to_async(
                    lambda: Shipment.objects.select_related('fleet_number').get(id=search_value)
                )()
                context['search_type'] = 'shipment_id'
                context['search_value'] = search_value
            elif search_type == 'appointment':
                # 按预约号查询
                shipment = await sync_to_async(
                    lambda: Shipment.objects.select_related('fleet_number').get(appointment_id=search_value)
                )()
                context['search_type'] = 'appointment'
                context['search_value'] = search_value
            elif search_type == 'fleet':
                if 'ZEM' in search_value:
                    fleets = await sync_to_async(lambda: Fleet.objects.get(pickup_number=search_value))()
                else:
                    fleets = await sync_to_async(lambda: Fleet.objects.get(fleet_number=search_value))()
                fleet_sp = await sync_to_async(
                    lambda: list(Shipment.objects.filter(fleet_number=fleets))
                )()
                context['search_type'] = 'fleet'
                context['search_value'] = search_value
                context['fleets'] = [fleets]
                context['fleet_sp'] = fleet_sp
            elif search_type == 'invoicestatus':
                invoicestatus_object = await sync_to_async(
                    lambda: list(InvoiceStatusv2.objects.filter(container_number__container_number=search_value))
                )()
                for s in invoicestatus_object:
                    s.preport_choices = s._meta.get_field("preport_status").choices
                    s.warehouse_public_choices = s._meta.get_field("warehouse_public_status").choices
                    s.warehouse_other_choices = s._meta.get_field("warehouse_other_status").choices
                    s.delivery_public_choices = s._meta.get_field("delivery_public_status").choices
                    s.delivery_other_choices = s._meta.get_field("delivery_other_status").choices
                    s.finance_choices = s._meta.get_field("finance_status").choices
                context['search_type'] = 'invoicestatus'
                context['search_value'] = search_value
                context['invoicestatus_object'] = invoicestatus_object
                
            if search_type != 'fleet' and search_type != 'invoicestatus':
                # 将单个shipment对象放入列表中，保持前端模板的一致性
                context['shipments'] = [shipment]
                context['search_batch_number'] = search_value
                
                # 计算状态和可用操作
                shipment.current_status = await self.get_shipment_status(shipment)
                shipment.status_display = await self.get_status_display_name(shipment.current_status)
                shipment.available_operations = await self.get_available_operations(shipment.current_status)
                
        except MultipleObjectsReturned:
            messages.error(request, f"找到多个匹配的记录，请核实查询条件：{search_value}")
        except ObjectDoesNotExist:
            if search_type == 'batch':
                messages.error(request, f"未找到批次号 '{search_value}' 的相关数据")
            elif search_type == 'shipment_id':
                messages.error(request, f"未找到id为 '{search_value}' 的预约批次数据")
            else:
                messages.error(request, f"未找到预约号 '{search_value}' 的相关数据")
        
        return self.template_post_port_status, context
    
    async def handle_update_shipment_is_canceled(self, request: HttpRequest):
        shipment_id = request.POST.get('shipment_id')
        is_canceled_value = request.POST.get('is_canceled')
        is_canceled_bool = is_canceled_value.lower() == 'true' if is_canceled_value else False
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        if shipment:
            # 更新in_use字段
            shipment.is_canceled = is_canceled_bool
            await sync_to_async(shipment.save)()
            
            # 添加成功消息
            messages.success(request, f"成功更新 Shipment ID {shipment_id} 的取消状态为: {'是' if is_canceled_bool else '否'}")
        else:
            messages.error(request, f"未找到 ID 为 {shipment_id} 的 Shipment")
        return await self.handle_search_shipment(request)
    
    async def handle_update_shipment_type(self, request: HttpRequest):
        shipment_id = request.POST.get('shipment_id')
        shipment_type = request.POST.get('shipment_type')
        
        shipment = await sync_to_async(
            lambda: Shipment.objects.filter(id=shipment_id).first()
        )()
        
        if shipment:
            shipment.shipment_type = shipment_type
            await sync_to_async(shipment.save)()
            
            messages.success(request, f"成功更新预约批次 {shipment.shipment_batch_number} 的类型为: {shipment_type}")
        else:
            messages.error(request, f"未找到 ID 为 {shipment_id} 的车次")
        return await self.handle_search_shipment(request)
    
    async def handle_update_fleet_type(self, request: HttpRequest):
        fleet_id = request.POST.get('fleet_id')
        fleet_type = request.POST.get('fleet_type')
        
        fleet = await sync_to_async(
            lambda: Fleet.objects.filter(id=fleet_id).first()
        )()
        
        if fleet:
            fleet.fleet_type = fleet_type
            await sync_to_async(fleet.save)()
            
            messages.success(request, f"成功更新车次 {fleet.fleet_number} 的类型为: {fleet_type}")
        else:
            messages.error(request, f"未找到 ID 为 {fleet_id} 的车次")
        return await self.handle_search_shipment(request)

    async def handle_update_fleet_origin(self, request: HttpRequest):
        fleet_id = request.POST.get('fleet_id')
        origin = request.POST.get('origin')
        
        fleet = await sync_to_async(
            lambda: Fleet.objects.filter(id=fleet_id).first()
        )()
        
        if fleet:
            fleet.origin = origin
            await sync_to_async(fleet.save)()
            
            messages.success(request, f"成功更新车次 {fleet.fleet_number} 的仓库为: {origin}")
        else:
            messages.error(request, f"未找到 ID 为 {fleet_id} 的车次")
        return await self.handle_search_shipment(request)

    async def handle_update_fleet_is_canceled(self, request: HttpRequest):
        fleet_id = request.POST.get('fleet_id')
        is_canceled_value = request.POST.get('is_canceled')
        is_canceled_bool = is_canceled_value.lower() == 'true' if is_canceled_value else False
        
        fleet = await sync_to_async(
            lambda: Fleet.objects.filter(id=fleet_id).first()
        )()
        
        if fleet:
            fleet.is_canceled = is_canceled_bool
            await sync_to_async(fleet.save)()
            
            messages.success(request, f"成功更新车次 {fleet.fleet_number} 的取消状态为: {'是' if is_canceled_bool else '否'}")
        else:
            messages.error(request, f"未找到 ID 为 {fleet_id} 的车次")
        return await self.handle_search_shipment(request)
    
    async def handle_update_shipment_in_use(self, request: HttpRequest):
        shipment_id = request.POST.get('shipment_id')
        in_use_value = request.POST.get('in_use')
        in_use_bool = in_use_value.lower() == 'true' if in_use_value else False
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        if shipment:
            # 更新in_use字段
            shipment.in_use = in_use_bool
            await sync_to_async(shipment.save)()
            
            # 添加成功消息
            messages.success(request, f"成功更新 Shipment ID {shipment_id} 的使用状态为: {'是' if in_use_bool else '否'}")
        else:
            messages.error(request, f"未找到 ID 为 {shipment_id} 的 Shipment")
        return await self.handle_search_shipment(request)

    async def handle_delete_shipment_batch_number(self, request: HttpRequest):
        """删除约，非必要不删除"""
        context = {}
        shipment_id = request.POST.get('shipment_id')
        search_batch_number = request.POST.get('search_batch_number')
        search_type = request.POST.get('search_type', 'batch')
        if not shipment_id:
            messages.error(request, "缺少必要参数")
            return self.template_post_port_status, context
        # 异步获取shipment对象
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        if not shipment:
            messages.error(request, "未找到要删除的记录")
        else:
            has_related_data = await sync_to_async(
                lambda: (
                    PackingList.objects.filter(shipment_batch_number=shipment).exists() or
                    Pallet.objects.filter(shipment_batch_number=shipment).exists()
                )
            )()
            del_able = True
            if has_related_data:
                messages.error(request, "存在po绑定在这条约上，不能直接删除批次号！")
                del_able = False
            if shipment.fleet_number:
                messages.error(request, "这条约已排车，不能直接删除批次号！")
                del_able = False
            if del_able:
                try:
                    shipment.shipment_batch_number = None
                    await sync_to_async(shipment.save)()
                    messages.success(request, "批次号删除成功")
                except Exception as e:
                    messages.error(request, f"批次号删除失败: {str(e)}")
        return self.template_post_port_status, context
    
    async def handle_delete_fleet(self, request: HttpRequest):
        '''删除车次'''
        context = {}
        fleet_id = request.POST.get('fleet_id')
        if not fleet_id:
            messages.error(request, "缺少必要参数")
            return self.template_post_port_status, context
        # 异步获取shipment对象
        try:
            fleet = await sync_to_async(
                lambda: Fleet.objects.filter(id=fleet_id).first()
            )()
            if fleet:
                await sync_to_async(fleet.delete)()
                messages.success(request, "记录删除成功")
            else:
                messages.error(request, "未找到要删除的记录")
        except Exception as e:
            messages.error(request, f"记录删除失败: {str(e)}")
        return self.template_post_port_status, context
    
    async def handle_delete_shipment(self, request: HttpRequest):
        """删除约，非必要不删除"""
        context = {}
        shipment_id = request.POST.get('shipment_id')
        search_batch_number = request.POST.get('search_batch_number')
        search_type = request.POST.get('search_type', 'batch')
        if not shipment_id:
            messages.error(request, "缺少必要参数")
            return self.template_post_port_status, context
        # 异步获取shipment对象
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        if not shipment:
            messages.error(request, "未找到要删除的记录")
        else:
            has_related_data = await sync_to_async(
                lambda: (
                    PackingList.objects.filter(shipment_batch_number=shipment).exists() or
                    Pallet.objects.filter(shipment_batch_number=shipment).exists()
                )
            )()
            del_able = True
            if has_related_data:
                messages.error(request, "存在po绑定在这条约上，不能直接删除！")
                del_able = False
            if shipment.fleet_number:
                messages.error(request, "这条约已排车，不能直接删除！")
                del_able = False

            if del_able:
                try:
                    await sync_to_async(shipment.delete)()
                    messages.success(request, "记录删除成功")
                except Exception as e:
                    messages.error(request, f"记录删除失败: {str(e)}")
        return self.template_post_port_status, context

    async def handle_update_shipment_status(self, request: HttpRequest):
        """处理更新shipment状态请求"""
        context = {}
        
        shipment_id = request.POST.get('shipment_id')
        target_status = request.POST.get('target_status')
        search_batch_number = request.POST.get('search_batch_number')
        search_type = request.POST.get('search_type', 'batch')
        
        if not shipment_id or not target_status:
            messages.error(request, "缺少必要参数")
            return self.template_post_port_status, context
        
        # 异步获取shipment对象
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        
        if not shipment:
            messages.error(request, "未找到对应的shipment记录")
            return self.template_post_port_status, context
        fleet = shipment.fleet_number if shipment else None
        # 根据目标状态更新相应字段
        await self.update_shipment_status_fields(shipment, target_status, fleet)
        
        status_name = await self.get_status_display_name(target_status.replace('cancel_', ''))
        messages.success(request, f"状态已更新为: {status_name}")
        
        # 重新查询数据以更新显示
        if search_batch_number:
            try:
                if search_type == 'batch':
                    # 按批次号重新查询
                    shipment = await sync_to_async(
                        lambda: Shipment.objects.get(shipment_batch_number=search_batch_number)
                    )()
                else:
                    # 按预约号重新查询
                    shipment = await sync_to_async(
                        lambda: Shipment.objects.get(appointment_id=search_batch_number)
                    )()
                
                # 将单个shipment对象放入列表中
                context['shipments'] = [shipment]
                context['search_batch_number'] = search_batch_number
                context['search_type'] = search_type
                context['search_value'] = search_batch_number
                
                # 重新计算状态和可用操作
                shipment.current_status = await self.get_shipment_status(shipment)
                shipment.status_display = await self.get_status_display_name(shipment.current_status)
                shipment.available_operations = await self.get_available_operations(shipment.current_status)
                
            except MultipleObjectsReturned:
                messages.error(request, f"找到多个匹配的记录，请核实：{search_batch_number}")
            except ObjectDoesNotExist:
                if search_type == 'batch':
                    messages.error(request, f"未找到批次号 '{search_batch_number}' 的相关数据")
                else:
                    messages.error(request, f"未找到预约号 '{search_batch_number}' 的相关数据")
        
        return self.template_post_port_status, context
    
    async def update_shipment_status_fields(self, shipment, target_status, fleet):
        """根据目标状态更新shipment的各个字段"""
        #target_status就是想改为的状态        
        if target_status == 'cancel_shipped': 
            await self.cancel_shipped_status(shipment, fleet)
        elif target_status == 'cancel_arrived':
            await self.cancel_arrived_status(shipment, fleet)
        elif target_status == 'cancel_pod_uploaded':
            await self.cancel_pod_uploaded_status(shipment, fleet)

    async def cancel_shipped_status(self, shipment, fleet):
        """取消发货状态"""
        # 更新shipment状态
        await sync_to_async(self._cancel_shipped_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_shipped_fleet)(fleet)

    def _cancel_shipped_shipment(self, shipment):
        """取消发货状态 - shipment部分"""
        shipment.is_shipped = False
        shipment.shipped_at = None
        shipment.shipped_at_utc = None
        # 未送达
        shipment.is_arrived = False
        shipment.arrived_at = None
        shipment.arrived_at_utc = None
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_shipped_fleet(self, fleet):
        """取消发货状态 - fleet部分"""
        fleet.departured_at = None
        fleet.arrived_at = None
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()

    async def cancel_arrived_status(self, shipment, fleet):
        """取消送达状态（回退到已发货）"""
        # 更新shipment状态
        await sync_to_async(self._cancel_arrived_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_arrived_fleet)(fleet)

    def _cancel_arrived_shipment(self, shipment):
        """取消送达状态 - shipment部分"""
        shipment.is_arrived = False
        shipment.arrived_at = None
        shipment.arrived_at_utc = None
        # 同时取消POD状态
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_arrived_fleet(self, fleet):
        """取消送达状态 - fleet部分"""
        fleet.arrived_at = None
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()

    async def cancel_pod_uploaded_status(self, shipment, fleet):
        """取消POD状态（回退到已送达）"""
        # 更新shipment状态
        await sync_to_async(self._cancel_pod_uploaded_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_pod_uploaded_fleet)(fleet)

    def _cancel_pod_uploaded_shipment(self, shipment):
        """取消POD状态 - shipment部分"""
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_pod_uploaded_fleet(self, fleet):
        """取消POD状态 - fleet部分"""
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()
    
    async def get_shipment_status(self, shipment):
        """
        获取shipment的当前状态
        按照优先级判断：POD上传 > 已送达 > 已发货 > 已预约
        """
        if shipment.pod_uploaded_at:
            return 'pod_uploaded'
        elif shipment.is_arrived:
            return 'arrived'
        elif shipment.is_shipped:
            return 'shipped'
        elif shipment.is_shipment_schduled:
            return 'scheduled'
        else:
            return 'unknown'
    
    async def get_status_display_name(self, status):
        """获取状态显示名称"""
        status_names = {
            'scheduled': '已预约',
            'shipped': '已发货', 
            'arrived': '已送达',
            'pod_uploaded': '已上传POD',
            'unknown': '未知'
        }
        return status_names.get(status, status)
    
    async def get_available_operations(self, current_status):
        """
        根据当前状态获取可用的操作
        """
        operations = []
        
        if current_status == 'pod_uploaded':
            # 已上传POD状态可以回退到已送达，或者取消POD状态
            operations.append(('cancel_pod_uploaded', '取消POD', 'btn-danger'))
            operations.append(('cancel_arrived', '取消送达', 'btn-secondary'))
            operations.append(('cancel_shipped', '取消发货', 'btn-secondary'))
        
        elif current_status == 'arrived':
            # 已送达状态可以回退到已发货，或者取消送达状态
            operations.append(('cancel_arrived', '取消送达', 'btn-danger'))
            operations.append(('cancel_shipped', '取消发货', 'btn-secondary'))
        
        elif current_status == 'shipped':
            # 已发货状态可以回退到已预约，或者取消发货状态
            operations.append(('cancel_shipped', '取消发货', 'btn-danger'))
        
        return operations
    
    async def handle_search_invoice_delivery(self, request: HttpRequest):
        """查询 Invoice Delivery 记录"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        if not search_type or not search_value:
            messages.error(request, "请选择查询类型并输入查询值")
            return self.template_delivery_invoice, {"search_performed": False}

        context = {"search_type": search_type, "search_value": search_value}
        search_results = []

        # === 柜号查询 ===
        if search_type == "container":
            container = await sync_to_async(
                lambda: Container.objects.filter(container_number=search_value).first()
            )()
            if container:
                context["container_info"] = {"id": container.id, "number": container.container_number}
                pallets = await sync_to_async(list)(
                    Pallet.objects.filter(container_number=container)
                    .select_related("invoice_delivery","shipment_batch_number", "shipment_batch_number__fleet_number")
                    .values(
                        "id", "container_number__container_number", "shipment_batch_number", "shipment_batch_number__shipment_batch_number", 
                        "shipment_batch_number__fleet_number__fleet_number","destination", "delivery_method",
                        "cbm", "weight_lbs", "pcs", "location", "note",
                        "fba_id", "ref_id", "invoice_delivery_id"
                    )
                )
                search_results = pallets
            else:
                messages.warning(request, f"未找到柜号: {search_value}")

        # === 发票查询 ===
        elif search_type == "invoice":
            invoice = None
            # 优先尝试查 invoice_number
            invoice = await sync_to_async(
                lambda: Invoice.objects.filter(invoice_number=search_value).first()
            )()
            # 如果没找到，尝试按 container_number 查
            if not invoice:
                container = await sync_to_async(
                    lambda: Container.objects.filter(container_number=search_value).first()
                )()
                if container:
                    invoice = await sync_to_async(
                        lambda: Invoice.objects.filter(container_number=container).first()
                    )()

            if invoice:
                context["invoice_info"] = {"id": invoice.id, "number": invoice.invoice_number}

                # 查该 Invoice 下的 InvoiceDelivery
                invoice_deliveries = await sync_to_async(list)(
                    InvoiceDelivery.objects.filter(invoice_number=invoice)
                    .values(
                        "id", "invoice_delivery", "invoice_type", "delivery_type",
                        "destination", "zipcode", "total_pallet", "total_cbm",
                        "total_weight_lbs", "total_cost", "expense", "note"
                    )
                )
                context["invoice_deliveries"] = invoice_deliveries
            else:
                messages.warning(request, f"未找到 Invoice 或 Container: {search_value}")

        elif search_type == "invoicev2":
            invoice = None
            # 优先尝试查 invoice_number
            invoice = await sync_to_async(
                lambda: Invoicev2.objects.filter(invoice_number=search_value).first()
            )()
            if not invoice:
                container = await sync_to_async(
                    lambda: Container.objects.filter(container_number=search_value).first()
                )()
                print('container',container)
                invoice = await sync_to_async(
                    lambda: Invoicev2.objects.filter(container_number=container).first()
                )()               
            else:
                container = await sync_to_async(
                    lambda: Container.objects.filter(container_number=invoice.container_number).first()
                )()
            # 如果没找到，尝试按 container_number 查
            if invoice:
                context["invoice_info"] = {"id": invoice.id, "number": invoice.invoice_number}
            my_checkbox = request.POST.get("my_checkbox")
            is_checked = my_checkbox == "true"
            if is_checked:
                # 只看组合柜类型的地址和所属区
                invoice_items = await sync_to_async(list)(
                    InvoiceItemv2.objects.filter(invoice_number=invoice,invoice_type="receivable",delivery_type="combine")
                    .values(
                        "warehouse_code","region"
                    )
                )
                context["region_items"] = invoice_items
                context['my_checkbox'] = 'true'
            else:
                if invoice:
                    # 查该 Invoice 下的 InvoiceDelivery
                    invoice_items = await sync_to_async(list)(
                        InvoiceItemv2.objects.filter(invoice_number=invoice,invoice_type="receivable")
                        .values(
                            "id", "container_number_id","invoice_type", "item_category", "cbm",
                            "cbm_ratio", "weight", "description", "qty","note",
                            "rate", "amount", "PO_ID", "delivery_type","warehouse_code",
                            "region","regionPrice","surcharges","registered_user"
                        )
                    )
                    receivable_total_fee = sum(item['amount'] for item in invoice_items if item['amount'] is not None)
                    context["invoice_items"] = invoice_items
                    context["receivable_total_fee"] = receivable_total_fee
                    context["container_number"] = container.container_number
                else:
                    messages.warning(request, f"未找到 Invoice 或 Container: {search_value}")
        # === Pallet 查询 ===
        elif search_type == "pallet":
            try:
                if "," in search_value:
                    pallet_ids = [
                        int(i.strip()) for i in search_value.split(",") if i.strip().isdigit()
                    ]
                    if not pallet_ids:
                        messages.error(request, "请输入有效的 Pallet ID 列表")
                    else:
                        pallets = await sync_to_async(
                            lambda: list(
                                Pallet.objects.filter(id__in=pallet_ids)
                                .select_related("invoice_delivery", "container_number","shipment_batch_number", "shipment_batch_number__fleet_number")
                                .values(
                                    "id", "container_number__container_number", "shipment_batch_number", "shipment_batch_number__shipment_batch_number", 
                                    "shipment_batch_number__fleet_number__fleet_number", "destination", "delivery_method",
                                    "cbm", "weight_lbs", "pcs", "location", "note",
                                    "fba_id", "ref_id", "invoice_delivery_id"
                                )
                            )
                        )()
                        if pallets:
                            search_results = pallets
                        else:
                            messages.warning(request, f"未找到指定的 Pallet ID: {search_value}")
                else:
                    pallet_id = int(search_value)
                    pallet = await sync_to_async(
                        lambda: Pallet.objects.filter(id=pallet_id)
                        .select_related("invoice_delivery", "container_number","shipment_batch_number", "shipment_batch_number__fleet_number")
                        .values(
                            "id", "container_number__container_number","shipment_batch_number", "shipment_batch_number__shipment_batch_number", 
                            "shipment_batch_number__fleet_number__fleet_number", "destination", "delivery_method",
                            "cbm", "weight_lbs", "pcs", "location", "note",
                            "fba_id", "ref_id", "invoice_delivery_id"
                        ).first()
                    )()
                    if pallet:
                        search_results = [pallet]
                    else:
                        messages.warning(request, f"未找到 Pallet ID: {search_value}")
            except ValueError:
                messages.error(request, "Pallet ID 必须是数字")

        context.update({
            "search_results": search_results,
            "search_performed": True
        })
        return self.template_delivery_invoice, context

    async def delete_all_invoice_items_public(self, request: HttpRequest):
        """删除全部公仓派送"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()
        invoicev2 = await sync_to_async(lambda: Invoicev2.objects.filter(invoice_number=search_value).first())()
        try:
            # 删除 InvoiceDelivery
            await sync_to_async(
                lambda: InvoiceItemv2.objects.filter(
                    invoice_number=invoicev2,
                    delivery_type__in=["walmart", "amazon"],
                ).delete()
            )()
            messages.success(request, f"成功删除 所有公仓的")
        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")
        await sync_to_async(
            lambda: InvoiceStatusv2.objects.filter(
                invoice__invoice_number=search_value, 
                invoice_type="receivable"
            ).update(finance_status="tobeconfirmed", delivery_public_status="unstarted")
        )()
        # 重新计算账单总费用
        await self._async_update_invoice_amount(invoicev2,invoicev2.container_number)

        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    

    async def handle_delete_all_invoice_items(self, request: HttpRequest):
        """删除单条 InvoiceDelivery"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()
        invoice_number = request.POST.get("invoice_number", "").strip()

        invoicev2 = await sync_to_async(lambda: Invoicev2.objects.filter(invoice_number=invoice_number).first())()
        try:
            # 删除 InvoiceDelivery
            await sync_to_async(
                lambda: InvoiceItemv2.objects.filter(invoice_number=invoicev2,delivery_type="combine").delete()
            )()
            messages.success(request, f"成功删除 所有组合柜的")
        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")
        await sync_to_async(
            lambda: InvoiceStatusv2.objects.filter(
                invoice__invoice_number=invoice_number, 
                invoice_type="receivable"
            ).update(finance_status="tobeconfirmed", delivery_public_status="unstarted")
        )()
        messages.success(request, f"已更新财务状态为待确认，公仓派送状态为未录入")

        # 重新计算账单总费用
        inv = await sync_to_async(Invoicev2.objects.get)(invoice_number=invoice_number)
        await self._async_update_invoice_amount(inv,inv.container_number)
        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    

    async def handle_delete_invoice_item(self, request: HttpRequest):
        """删除单条 InvoiceDelivery"""
        invoice_item_id = request.POST.get("invoice_item_id")
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()


        try:
            # 删除 InvoiceDelivery
            await sync_to_async(
                lambda: InvoiceItemv2.objects.filter(id=invoice_item_id).delete()
            )()
            messages.success(request, f"成功删除 InvoiceDelivery ID {invoice_item_id}")
        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        inv = await sync_to_async(Invoicev2.objects.get)(id=invoice_item_id)
        container = inv.container_number
        # 重新计算账单总费用
        await self._async_update_invoice_amount(inv,container)
        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    

    async def handle_delete_invoice_delivery(self, request: HttpRequest):
        """删除单条 InvoiceDelivery"""
        invoice_delivery_id = request.POST.get("invoice_delivery_id")
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 先把对应 Pallet 的外键置空
            await sync_to_async(
                lambda: Pallet.objects.filter(invoice_delivery_id=invoice_delivery_id)
                .update(invoice_delivery=None)
            )()
            # 删除 InvoiceDelivery
            await sync_to_async(
                lambda: InvoiceDelivery.objects.filter(id=invoice_delivery_id).delete()
            )()
            messages.success(request, f"成功删除 InvoiceDelivery ID {invoice_delivery_id}")
        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)

    

    async def handle_delete_all_invoice_delivery(self, request: HttpRequest):
        """删除 Invoice/Container 下所有 InvoiceDelivery"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            invoice = None
            if search_type == "invoice":
                invoice = await sync_to_async(lambda: Invoice.objects.filter(invoice_number=search_value).first())()
                if not invoice:
                    container = await sync_to_async(lambda: Container.objects.filter(container_number=search_value).first())()
                    if container:
                        invoice = await sync_to_async(lambda: Invoice.objects.filter(container_number=container).first())()

            if invoice:
                # 清空对应 Pallet 外键
                await sync_to_async(
                    lambda: Pallet.objects.filter(
                        invoice_delivery__invoice_number=invoice
                    ).exclude(delivery_type='other')
                    .update(invoice_delivery=None)
                )()
                # 删除 InvoiceDelivery
                await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(invoice_number=invoice)
                    .exclude(delivery_type='other')
                    .delete()
                )()
                messages.success(request, f"成功删除 Invoice ID {invoice.id} 的所有 InvoiceDelivery")
            else:
                messages.warning(request, f"未找到对应的 Invoice 或 Container: {search_value}")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    
    async def handle_delete_pallet_invoice_delivery(self, request: HttpRequest):
        """删除 Pallet 的 Invoice Delivery 外键，并删除对应的 InvoiceDelivery 记录"""
        pallet_id = request.POST.get("pallet_id")
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 先获取 pallet 对应的 invoice_delivery_id
            pallet = await sync_to_async(
                lambda: Pallet.objects.filter(id=pallet_id).first()
            )()

            if pallet and pallet.invoice_delivery_id:
                invoice_delivery_id = pallet.invoice_delivery_id

                # 先删除 Pallet 的外键
                await sync_to_async(
                    lambda: Pallet.objects.filter(id=pallet_id).update(invoice_delivery=None)
                )()

                # 再删除对应的 InvoiceDelivery 记录
                deleted_count = await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(id=invoice_delivery_id).delete()
                )()

                messages.success(request, f"成功删除 Pallet ID {pallet_id} 的 Invoice Delivery 及对应记录")
            else:
                messages.error(request, "Pallet 没有对应的 Invoice Delivery 记录")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 删除后重新执行查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    
    async def handle_delete_container_invoice_deliveries(self, request: HttpRequest):
        """删除整个柜子下的所有 Pallet 的 Invoice Delivery 外键，并删除对应的 InvoiceDelivery 记录"""
        container_id = request.POST.get("container_id")
        search_type = request.POST.get("search_type", "")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 获取这个柜子下所有 Pallet
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(container_number_id=container_id))
            )()

            if not pallets:
                messages.error(request, f"未找到 Container ID {container_id} 下的任何 Pallet")
            else:
                # 收集所有相关的 invoice_delivery_id
                invoice_delivery_ids = [p.invoice_delivery_id for p in pallets if p.invoice_delivery_id]

                # 将所有 Pallet 的外键置空
                await sync_to_async(
                    lambda: Pallet.objects.filter(container_number_id=container_id).update(invoice_delivery=None)
                )()

                # 删除对应的 InvoiceDelivery 记录
                if invoice_delivery_ids:
                    await sync_to_async(
                        lambda: InvoiceDelivery.objects.filter(id__in=invoice_delivery_ids).delete()
                    )()
                    messages.success(request, f"成功删除 Container ID {container_id} 下所有 Pallet 的 Invoice Delivery 及对应记录")
                else:
                    messages.info(request, "这个柜子下的 Pallet 没有任何 Invoice Delivery 记录")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 删除后重新执行查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    

    async def handle_modify_direct_status(self, request):
        logg = []
        try:
            # 1. 获取直送订单的集装箱ID列表
            container_ids = [
                cid async for cid in 
                Order.objects.filter(
                    order_type='直送',
                    container_number__isnull=False
                ).values_list('container_number_id', flat=True).distinct()
            ]
            
            logg.append(f"找到 {len(container_ids)} 个直送集装箱")
            
            if container_ids:
                # 2. 更新InvoiceStatus
                invoice_status_count = await InvoiceStatus.objects.filter(
                    container_number_id__in=container_ids,
                    preport_status='completed'
                ).aupdate(
                    warehouse_public_status='completed',
                    warehouse_other_status='completed',
                    delivery_public_status='completed',
                    delivery_other_status='completed'
                )
                logg.append(f"更新了 {invoice_status_count} 条 InvoiceStatus 记录")
                
                # 3. 更新InvoiceStatusv2
                invoice_statusv2_count = await InvoiceStatusv2.objects.filter(
                    container_number_id__in=container_ids,
                    preport_status='completed'
                ).aupdate(
                    warehouse_public_status='completed',
                    warehouse_other_status='completed',
                    delivery_public_status='completed',
                    delivery_other_status='completed'
                )
                logg.append(f"更新了 {invoice_statusv2_count} 条 InvoiceStatusv2 记录")
            else:
                logg.append("没有找到需要更新的直送订单")
            context = {'logg':logg}
            return self.template_receivable_status_migrate, context
            
        except Exception as e:
            logg.append(f"更新过程中发生错误: {str(e)}")
            import traceback
            logg.append(traceback.format_exc())
            context = {'logg':logg}
            return self.template_receivable_status_migrate, context

    async def handle_receivale_status_search(self, request):
        old_status = await sync_to_async(list)(
            InvoiceStatus.objects.filter(
                stage='tobeconfirmed'
            ).select_related(
                'container_number'
            ).prefetch_related(
                'container_number'
            )
        )

        # 第二组：所有状态字段都是completed的
        now_status = await sync_to_async(list)(
            InvoiceStatus.objects.filter(
                preport_status='completed',
                warehouse_public_status='completed', 
                warehouse_other_status='completed',
                delivery_public_status='completed',
                delivery_other_status='completed'
            ).exclude(finance_status="completed")
            .select_related(
                'container_number'
            ).prefetch_related(
                'container_number'
            )
        )

        # 处理旧状态数据
        old_status_data = []
        for status in old_status:
            old_status_data.append({
                'invoice_status_id': status.id,
                'container_number': status.container_number.container_number,
                'container_type': status.container_number.container_type,
                'invoice_type': status.invoice_type,
                'stage': status.stage,
                'finance_status': status.finance_status,
                'preport_status': status.preport_status,
                'warehouse_public_status': status.warehouse_public_status,
                'warehouse_other_status': status.warehouse_other_status,
                'delivery_public_status': status.delivery_public_status,
                'delivery_other_status': status.delivery_other_status,
            })

        # 处理新状态数据
        now_status_data = []
        for status in now_status:
            now_status_data.append({
                'invoice_status_id': status.id,
                'container_number': status.container_number.container_number,
                'container_type': status.container_number.container_type,
                'invoice_type': status.invoice_type,
                'stage': status.stage,
                'finance_status': status.finance_status,
                'preport_status': status.preport_status,
                'warehouse_public_status': status.warehouse_public_status,
                'warehouse_other_status': status.warehouse_other_status,
                'delivery_public_status': status.delivery_public_status,
                'delivery_other_status': status.delivery_other_status,
            })

        # 创建差异对比数据
        diff_status_data = []
        
        # 1. 找出在 now_status 但不在 old_status 的记录（新增的记录）
        old_container_numbers = {item['container_number'] for item in old_status_data}
        now_container_numbers = {item['container_number'] for item in now_status_data}
        
        # 新增的记录
        new_added = [item for item in now_status_data if item['container_number'] not in old_container_numbers]
        
        # 2. 找出在 old_status 但不在 now_status 的记录（删除的记录）
        removed = [item for item in old_status_data if item['container_number'] not in now_container_numbers]
        
        # 3. 找出在两个数据集中都存在但有状态变化的记录
        old_dict = {item['container_number']: item for item in old_status_data}
        now_dict = {item['container_number']: item for item in now_status_data}
        
        changed = []
        for container_num in old_container_numbers.intersection(now_container_numbers):
            old_item = old_dict[container_num]
            now_item = now_dict[container_num]
            
            # 检查是否有任何状态字段发生变化
            status_fields = ['stage', 'finance_status', 'preport_status', 'warehouse_public_status', 
                            'warehouse_other_status', 'delivery_public_status', 'delivery_other_status']
            
            has_changes = any(old_item[field] != now_item[field] for field in status_fields)
            
            if has_changes:
                changed.append({
                    'container_number': container_num,
                    'old_data': old_item,
                    'now_data': now_item,
                    'changes': {field: {'old': old_item[field], 'new': now_item[field]} 
                            for field in status_fields if old_item[field] != now_item[field]}
                })

        # 构建差异数据表格（格式与 now_status_data、old_status_data 一致）
        diff_data = []
        
        # 添加新增的记录
        for item in new_added:
            diff_data.append({
                'container_number': item['container_number'],
                'change_type': '新增',
                'stage': item['stage'],
                'finance_status': item['finance_status'],
                'preport_status': item['preport_status'],
                'warehouse_public_status': item['warehouse_public_status'],
                'warehouse_other_status': item['warehouse_other_status'],
                'delivery_public_status': item['delivery_public_status'],
                'delivery_other_status': item['delivery_other_status'],
                'invoice_type': item['invoice_type'],
                'container_type': item['container_type']
            })
        
        # 添加删除的记录
        for item in removed:
            diff_data.append({
                'container_number': item['container_number'],
                'change_type': '删除', 
                'stage': item['stage'],
                'finance_status': item['finance_status'],
                'preport_status': item['preport_status'],
                'warehouse_public_status': item['warehouse_public_status'],
                'warehouse_other_status': item['warehouse_other_status'],
                'delivery_public_status': item['delivery_public_status'],
                'delivery_other_status': item['delivery_other_status'],
                'invoice_type': item['invoice_type'],
                'container_type': item['container_type']
            })
        
        # 添加变化的记录（显示新的状态值）
        for item in changed:
            diff_data.append({
                'container_number': item['container_number'],
                'change_type': '修改',
                'stage': item['now_data']['stage'],
                'finance_status': item['now_data']['finance_status'],
                'preport_status': item['now_data']['preport_status'],
                'warehouse_public_status': item['now_data']['warehouse_public_status'],
                'warehouse_other_status': item['now_data']['warehouse_other_status'],
                'delivery_public_status': item['now_data']['delivery_public_status'],
                'delivery_other_status': item['now_data']['delivery_other_status'],
                'invoice_type': item['now_data']['invoice_type'],
                'container_type': item['now_data']['container_type'],
                'changes': item['changes']  # 保存具体的变化信息
            })

        # 统计数量
        old_status_count = await sync_to_async(
            InvoiceStatus.objects.filter(stage='tobeconfirmed').count
        )()
        now_status_count = await sync_to_async(
            InvoiceStatus.objects.filter(
                preport_status='completed',
                warehouse_public_status='completed', 
                warehouse_other_status='completed',
                delivery_public_status='completed',
                delivery_other_status='completed'
            ).count
        )()
        
        context = {
            'old_status_count': old_status_count,
            'now_status_count': now_status_count,
            'now_status_data': now_status_data,
            'old_status_data': old_status_data,
            'diff_status_data': diff_data,  # 添加差异数据
            'diff_count': len(diff_data),   # 差异记录总数
            'new_added_count': len(new_added),
            'removed_count': len(removed),
            'changed_count': len(changed)
        }
        return self.template_receivable_status_migrate, context

    async def handle_search_wrong_fee(self,request):
        """
        查询有没有状态迁移错误的账单
        """
        start_index = int(request.POST.get("start_index", 0))
        end_index = int(request.POST.get("end_index", 0))
        migration_log = []
        containers = await sync_to_async(list)(
            Container.objects.filter(
                id__gte=start_index,
                id__lte=end_index
            ).order_by('id')
        )
        inconsistent_count = 0
        for container in containers:
            old_invoice = await sync_to_async(
                lambda c: Invoice.objects.filter(container_number=c).first()
            )(container)
            
            new_invoice = await sync_to_async(
                lambda c: Invoicev2.objects.filter(container_number=c).first()
            )(container)
            
            # 只有当两个账单都存在时才比较
            if old_invoice and new_invoice:
                # 计算Invoicev2的合并金额
                # new_warehouse_total = (new_invoice.payable_wh_public_amount or 0) + (new_invoice.payable_wh_other_amount or 0)
                # new_delivery_total = (new_invoice.payable_delivery_public_amount or 0) + (new_invoice.payable_delivery_other_amount or 0)
                
                # 定义要比较的字段和对应的值
                comparisons = [
                    ('payable_total_amount', '应付总额', 
                     old_invoice.payable_total_amount or 0, 
                     new_invoice.payable_total_amount or 0),
                    ('payable_preport_amount', '港前金额', 
                     old_invoice.payable_preport_amount or 0, 
                     new_invoice.payable_preport_amount or 0),
                    ('payable_warehouse_amount', '仓库金额', 
                     old_invoice.payable_warehouse_amount or 0,
                     new_invoice.payable_warehouse_amount or 0
                     ),
                    ('payable_delivery_amount', '派送金额', 
                     old_invoice.payable_delivery_amount or 0,
                     new_invoice.payable_delivery_amount or 0
                     ),
                    # ('payable_direct_amount', '直送金额',
                    #  old_invoice.payable_direct_amount or 0,
                    #  new_invoice.payable_direct_amount or 0)
                ]
                
                differences = []
                old_data = {}
                new_data = {}
                
                for field, field_name, old_value, new_value in comparisons:
                    old_data[field] = old_value
                    new_data[field] = new_value
                    
                    if round(old_value, 2) != round(new_value, 2):
                        differences.append({
                            'field': field,
                            'field_name': field_name,
                            'old_value': old_value,
                            'new_value': new_value,
                            'diff': new_value - old_value
                        })
                
                # 如果有差异，记录到日志
                if differences:
                    inconsistent_count += 1
                    migration_log.append({
                        'container_number': container.container_number,
                        'container_id': container.id,
                        'old_data': old_data,
                        'new_data': new_data,
                        'differences': differences,
                        'actions': f'❌ 金额不一致: {len(differences)} 个字段不一致',
                        'old_invoice_number': old_invoice.invoice_number,
                        'new_invoice_number': new_invoice.invoice_number
                    })
        
        context = {
            'inconsistent_count': inconsistent_count,
            'success': True,
            'start_index': start_index,
            'end_index': end_index,
            'migration_log': migration_log,
        }
        
        return self.template_receivable_status_migrate, context

    async def handle_overweight_single_save(self, request):
        '''历史组合柜费用计算之超重费'''
        invoice_number_str = request.POST.get('invoice_number', '').strip()
        container_number_str = request.POST.get('container_number', '').strip()
        overcharge_amount_str = request.POST.get('overcharge_amount', '0').strip()

        container = Container.objects.get(container_number=container_number_str)
        invoice = Invoicev2.objects.get(invoice_number=invoice_number_str)
        InvoiceItemv2.objects.filter(
            container_number=container,
            invoice_number=invoice,
            invoice_type='receivable'
        ).filter(
            Q(description__contains="超重费") | Q(note__contains="超重费")
        ).delete()
        InvoiceItemv2.objects.create(
            item_category="combina_extra_fee",
            container_number=container,
            invoice_number=invoice,
            invoice_type="receivable",
            description="超重费",
            qty=1,
            rate=float(overcharge_amount_str),
            amount=float(overcharge_amount_str),
            weight=float(overcharge_amount_str),
        )

        await self._async_update_invoice_amount(invoice,container)
        log_entry = {
            "container_number": container_number_str,
            "invoice_number": invoice_number_str,
            "actions": "记录超重费成功",
            "new_data": None, # 用于前端显示状态
            "updated_count": 1,
            "filename":"",
            "is_overweight": True,
        }
        context = {
            'migration_log': [log_entry],
        }
        return self.template_recaculate_combine, context


    def _sync_update_invoice_amount(self, invoice, container):
        """更新账单总费用"""
        
        # 1. 重新计算 'delivery_public' (公仓派送) 的总额
        total_del_pub = InvoiceItemv2.objects.filter(
            invoice_number=invoice,
            container_number=container,
            invoice_type='receivable',
            item_category='delivery_public'
        ).aggregate(total=Sum('amount'))['total'] or 0

        # 2. 重新计算 'combina_extra_fee' (组合费额外费用) 的总额
        total_extra = InvoiceItemv2.objects.filter(
            invoice_number=invoice,
            container_number=container,
            invoice_type='receivable',
            item_category='combina_extra_fee'
        ).aggregate(total=Sum('amount'))['total'] or 0

        # 3. 更新 Invoicev2 实例的 delivery_public 字段
        invoice.receivable_delivery_public_amount = total_del_pub

        # 4. 准备计算总金额所需的分项数据 (转为 Decimal 以避免浮点数精度丢失)
        # 如果字段为 None，则默认为 0
        preport = Decimal(str(invoice.receivable_preport_amount or 0))
        wh_pub = Decimal(str(invoice.receivable_wh_public_amount or 0))
        wh_oth = Decimal(str(invoice.receivable_wh_other_amount or 0))
        del_oth = Decimal(str(invoice.receivable_delivery_other_amount or 0))
        
        # 当前计算出的值也要转 Decimal
        current_del_pub = Decimal(str(total_del_pub))
        current_extra = Decimal(str(total_extra))

        # 5. 计算最终总金额
        # 公式：港前 + 公仓 + 私仓 + 公仓派送(刚算的) + 私仓派送 + 额外费用(刚算的)
        final_total = preport + wh_pub + wh_oth + current_del_pub + del_oth + current_extra

        # 6. 赋值并保存
        invoice.receivable_total_amount = float(final_total)
        invoice.remain_offset = float(final_total)
        invoice.save()

    async def _async_update_invoice_amount(self, invoice, container):
        """更新账单总费用"""
        
        # 1. 重新计算 'delivery_public' (公仓派送) 的总额
        total_del_pub = await sync_to_async(
            lambda: InvoiceItemv2.objects.filter(
                invoice_number=invoice,
                container_number=container,
                invoice_type='receivable',
                item_category='delivery_public'
            ).aggregate(total=Sum('amount'))['total'] or 0
        )()
        
        # 2. 重新计算 'combina_extra_fee' (组合费额外费用) 的总额
        total_extra = await sync_to_async(
            lambda: InvoiceItemv2.objects.filter(
                invoice_number=invoice,
                container_number=container,
                invoice_type='receivable',
                item_category='combina_extra_fee'
            ).aggregate(total=Sum('amount'))['total'] or 0
        )()
        
        # 3. 更新 Invoicev2 实例的 delivery_public 字段
        invoice.receivable_delivery_public_amount = total_del_pub
        
        # 4. 准备计算总金额所需的分项数据 (转为 Decimal 以避免浮点数精度丢失)
        # 如果字段为 None，则默认为 0
        preport = Decimal(str(invoice.receivable_preport_amount or 0))
        wh_pub = Decimal(str(invoice.receivable_wh_public_amount or 0))
        wh_oth = Decimal(str(invoice.receivable_wh_other_amount or 0))
        del_oth = Decimal(str(invoice.receivable_delivery_other_amount or 0))
        
        # 当前计算出的值也要转 Decimal
        current_del_pub = Decimal(str(total_del_pub))
        current_extra = Decimal(str(total_extra))
        
        # 5. 计算最终总金额
        # 公式：港前 + 公仓 + 私仓 + 公仓派送(刚算的) + 私仓派送 + 额外费用(刚算的)
        final_total = preport + wh_pub + wh_oth + current_del_pub + del_oth + current_extra
        
        # 6. 赋值并保存
        invoice.receivable_total_amount = float(final_total)
        invoice.remain_offset = float(final_total)
        
        # 使用 sync_to_async 包装 save 操作
        await sync_to_async(invoice.save)()
 
    async def handle_recalculate_by_containers(self, request):
        """
        异步处理：根据输入的柜号列表重新计算
        """
        container_raw = request.POST.get('container_list', '').strip()
        if not container_raw:
            return []

        # 1. 处理输入字符串：支持空格、逗号、换行符分隔
        # 使用正则 \s+ 可以匹配任何空白字符（空格、制表符、换行）
        container_numbers = re.split(r'[\s,]+', container_raw)
        container_numbers = [c.strip() for c in container_numbers if c.strip()]

        if not container_numbers:
            return []

        # 2. 异步获取这些柜号对应的已完成账单
        # 注意：一个柜子可能有多个账单(Invoicev2)，所以用 __in 查询
        invoices = await sync_to_async(list)(
            Invoicev2.objects.filter(
                container_number__container_number__in=container_numbers,
            ).select_related('container_number').order_by('container_number__container_number')
        )

        migration_log = []

        # 3. 调用你已有的核心处理函数 (复用逻辑)
        for inv in invoices:
            log_entry = await self._async_process_single_invoice(inv, request.user)
            migration_log.append(log_entry)    

        context = {
            'migration_log': migration_log,
            'container_list': container_raw,
        }
        return self.template_recaculate_combine, context


    async def handle_recalculate_combine(self,request):
        """
        异步处理：历史数据组合柜重新计算
        """
        start_index = int(request.POST.get('start_index', 0))
        end_index = int(request.POST.get('end_index', 100))
        
        # 1. 异步获取范围内的 Invoicev2 列表
        # 注意：这里根据 id 范围筛选，你也可以根据 start_index:end_index 切片
        invoices = await sync_to_async(list)(
            Invoicev2.objects.filter(
                id__gte=start_index,
                id__lte=end_index,
                invoicestatusv2__finance_status='completed' 
            ).select_related('container_number').order_by('id')
        )
        
        migration_log = []

        for inv in invoices:
            # 2. 调用内部同步处理函数执行复杂的 DB 事务
            # 我们对每一个账单单独运行一个事务，防止一个报错导致全部回滚
            log_entry = await self._async_process_single_invoice(inv, request.user)
            migration_log.append(log_entry)

        context = {
            'migration_log': migration_log,
            'start_index': start_index,
            'end_index': end_index,
        }
        return self.template_recaculate_combine, context  # 返回给 context 用于前端渲染

    @sync_to_async
    def _async_process_single_invoice(self, inv, username):
        """
        同步包装：处理单个账单的 DB 事务操作
        """
        container_number = inv.container_number.container_number
        log_entry = {
            "container_number": container_number,
            "invoice_number": inv.invoice_number,
            "actions": "跳过",
            "new_data": None, # 用于前端显示状态
            "updated_count": 0,
            "filename":"",
            "is_overweight": False,
        }

        rece_a = ReceivableAccounting()
        if 1:
            with transaction.atomic():
                # -1. 检查账单是否已确认
                try:
                    status_obj = InvoiceStatusv2.objects.get(
                        invoice=inv,
                        invoice_type='receivable'
                    )
                    
                    if status_obj.finance_status != 'completed':
                        log_entry["actions"] = "[跳过]：该柜子账单尚未被财务确认"
                        return log_entry                     
                except InvoiceStatusv2.DoesNotExist:
                    log_entry["actions"] = "[跳过]：该柜子尚未录过任何账单"
                    return log_entry  
                
                # 0. 检查柜子是否满足组合柜
                order = Order.objects.get(container_number__container_number=container_number)
                if not order:
                    log_entry["actions"] = "[跳过]：找不到对应订单"
                    return log_entry
                if not rece_a._determine_is_combina(order):
                    log_entry["actions"] = "[跳过]：该柜子不是组合柜"
                    return log_entry
                
                container = inv.container_number
                warehouse = order.retrieval_id.retrieval_destination_area
                # 1. 检查是否有需要计算的 combine 项 (比率为 0 或 Null)
                all_combine_items = InvoiceItemv2.objects.filter(
                    invoice_number=inv,
                    invoice_type='receivable',
                    delivery_type='combine'
                )
                
                # --- 拆分判断逻辑 ---               
                # A. 判断是否存在组合柜类型的账单项
                if all_combine_items:
                    # B. 在存在组合柜项的前提下，判断是否需要更新（即是否存在比例为空或为0的项）
                    needs_update = all_combine_items.filter(
                        Q(cbm_ratio__isnull=True) | Q(cbm_ratio=0)
                    ).exists()

                    if not needs_update:
                        log_entry["actions"] = "跳过：该账单组合柜项的比例(cbm_ratio)均已存在，为新版本录过的数据，不需要重新计算"
                        return log_entry
                    all_combine_items.delete() 

                # 2. 计算整柜总 CBM
                total_cbm_res = PackingList.objects.filter(
                    container_number=inv.container_number
                ).aggregate(total=Sum('cbm'))
                total_container_cbm = float(total_cbm_res['total'] or 0)

                if total_container_cbm <= 0:
                    log_entry["actions"] = "错误：柜子总CBM为0"
                    return log_entry

                # 5. 重新计算组合柜item
                # 5.1 获取报价表规则
                quotations = rece_a._get_fee_details(order, order.retrieval_id.retrieval_destination_area,order.customer_name.zem_name)
                if isinstance(quotations, dict) and quotations.get("error_messages"):
                    log_entry["actions"] = f"获取报价表规则错误:{quotations['error_messages']}"
                    return log_entry

                quotation_info = quotations['quotation']
                log_entry["filename"] = quotation_info.filename
                
                fee_details = quotations['fees']
                combina_key = f"{warehouse}_COMBINA"
                if combina_key not in fee_details:
                    log_entry["actions"] = f"获取报价表规则错误:combina_key，仓库为{warehouse}"
                    return log_entry
                
                rules = fee_details.get(combina_key).details
                
                destinations = list(Pallet.objects.filter(container_number=container)
                                .values_list("destination", flat=True).distinct())
                
                # 重新计算组合柜数据
                new_items = []
                # 5.1 公仓数据
                for d_type in ['public', 'other']:
                    pallet_groups,err = self._recaculate_pallet_groups(container_number,inv.invoice_number,d_type)      
                    if err:
                        log_entry["actions"] = f"[错误]：计算组合项异常 - {err}"
                        return log_entry
                      
                    items, err = self._recaculate_combine_items(pallet_groups, container, inv, order, rules, warehouse, total_container_cbm, username, destinations)
                    if err:
                        log_entry["actions"] = f"[错误]：计算组合项异常 - {err}"
                        return log_entry
                    new_items.extend(items)

                # 6. 批量创建
                if new_items:
                    InvoiceItemv2.objects.bulk_create([InvoiceItemv2(**item) for item in new_items])
                    log_entry["updated_count"] = len(new_items)

                matching_quotation, quotation_error = rece_a._get_quotation_for_order(order, 'receivable')
                stipulate = FeeDetail.objects.get(
                    quotation_id=matching_quotation.id, fee_type="COMBINA_STIPULATE"
                ).details
                # 7. 计算超板费用
                self._caculate_overpallet_fee(container_number, inv, fee_details, stipulate, warehouse, container, destinations, username)
                
                # 8. 计算超区提拆费
                res_overregion = self._caculate_overregion_fee(container_number, inv, total_container_cbm, container, stipulate, warehouse, username)
                if isinstance(res_overregion, str): # 如果返回字符串则代表错误
                    log_entry["actions"] = f"[错误]：超区费计算失败 - {res_overregion}"
                    return log_entry
                
                # 9. 超重费，需要手动输入
                limit_weight = stipulate.get("global_rules", {}).get("weight_limit", {}).get("default", 0)
                actual_weight_res = PackingList.objects.filter(
                    container_number__container_number=container.container_number
                ).aggregate(total=Sum('total_weight_lbs'))
                actual_weight = actual_weight_res['total'] or 0
                log_entry["is_overweight"] = actual_weight > limit_weight
                
                # 7. 更新 Invoicev2 的金额汇总
                # 重新计算 delivery_public 总额
                self._sync_update_invoice_amount(inv,container)

                log_entry["actions"] = "[成功]：数据已重算并保存"
                
                # 为了让前端展示状态列，模拟 new_data
                log_entry["new_data"] = {
                    "finance_status": "completed",
                    "delivery_public_status": "completed"
                }

        # except Exception as e:
        #     log_entry["actions"] = f"[错误]：系统异常 - {str(e)}"
        
        return log_entry
    
    def _caculate_overregion_fee(self, container_number, inv, total_combina_cbm, container, stipulate, warehouse, username):
        '''计算超区的提拆费费用'''
        # 1.统计组合柜的仓点
        base_queryset = InvoiceItemv2.objects.filter(
            invoice_type="receivable",
            container_number=container,
            invoice_number=inv,
            delivery_type='combine',
        )
        warehouse_code_list = list(base_queryset.values_list('warehouse_code', flat=True).distinct())

        # 2.获取所有仓点
        non_combina_res = PackingList.objects.filter(
            container_number=container
        ).exclude(
            destination__in=warehouse_code_list
        ).aggregate(total=Sum('cbm'))
        non_combina_cbm = non_combina_res['total'] or 0
        if non_combina_cbm <= 0: return "总CBM为0"

        non_combina_cbm_ratio = round(non_combina_cbm / total_combina_cbm, 4)       
        match = re.match(r"\d+", container.container_type)
        if not match: return f"无法识别柜型 {container.container_type}"

        pick_subkey = match.group()
        # 这个提拆费是从组合柜规则的warehouse_pricing的nonmix_40ft 45ft取
        c_type = f"nonmix_{pick_subkey}ft"
        try:
            pickup_fee = stipulate["warehouse_pricing"][warehouse][c_type]
        except KeyError as e:
            return f"报价单缺少 {warehouse} - {c_type} 的提拆费配置"
        
        overregion_pickup_fee = round(non_combina_cbm_ratio * pickup_fee, 3)

        InvoiceItemv2.objects.create(
            item_category="combina_extra_fee",
            container_number=container,
            invoice_number=inv,
            invoice_type="receivable",
            description="超区提拆费",
            qty=non_combina_cbm_ratio,
            rate=pickup_fee,
            amount=overregion_pickup_fee,
            note="提拆费",
            registered_user=username
        )
        return overregion_pickup_fee
        


    def _caculate_overpallet_fee(self, container_number, inv, fee_details, stipulate, warehouse, container, destinations, username):
        '''计算超板费用'''
        rece_a = ReceivableAccounting()

        total_pallets = Pallet.objects.filter(container_number=container).count()  
        # 4.2、规定的最大板数
        max_pallets = rece_a._get_max_pallets(stipulate, warehouse, container.container_type)
        # 4.3、超出板数
        over_count = max(0, total_pallets - max_pallets)
        if over_count <= 0: return 0

        # 4.4、计算超板费用
        plts_by_destination = list(Pallet.objects.filter(container_number=container).values("destination").annotate(total_cbm=Sum("cbm")))
        
        plts_costs = rece_a._calculate_delivery_fee_cost(
            fee_details, warehouse, plts_by_destination, destinations, over_count
        )
        max_price = 0
        max_single_price = 0
        for plt_d in plts_costs:
            if plt_d["is_fixed_price"]:  # 一口价的不用乘板数
                max_price = max(float(plt_d["price"]), max_price)
                max_single_price = max(max_price, max_single_price)
            else:
                max_price = max(float(plt_d["price"]) * over_count, max_price)
                max_single_price = max(float(plt_d["price"]), max_single_price)

        InvoiceItemv2.objects.create(
            item_category="combina_extra_fee",
            container_number=container,
            invoice_number=inv,
            invoice_type="receivable",
            description="超板费",
            qty=over_count,
            rate=round(max_price / over_count, 2) if over_count > 0 else 0,
            amount=max_price,
            note=f"超出 {over_count} 板",
            registered_user=username
        )
        return max_price
    
    def _recaculate_combine_items(self, pallet_groups, container, inv, order, rules, warehouse, total_container_cbm, username, destinations):
        '''重新计算历史组合柜数据的账单项'''
        container_type_temp = 0 if "40" in container.container_type else 1
        
        # 2. 筛选出属于组合区域的pallet_groups
        combina_items = []
        all_combina_destinations = set()
        for group in pallet_groups:
            
            destination_str = group.get("destination", "")

            #改前和改后的
            _, destination = self._process_destination(destination_str)
            if destination.upper() == "UPS":
                continue
            
            # 检查是否属于组合区域
            is_match = False
            matched_region, matched_price = None, 0
            for region_name, region_data in rules.items():
                for item in region_data:
                    normalized_locations = [loc.strip() for loc in item["location"] if loc]
                    if destination in normalized_locations:
                        is_match = True
                        matched_region = region_name
                        matched_price = item['prices'][container_type_temp]
                        break
                if is_match:
                    break
            
            
            if is_match:
                all_combina_destinations.add(destination_str)
                cbm = group.get('total_cbm', 0)
                cbm_ratio = round(cbm / total_container_cbm, 4) if total_container_cbm > 0 else 0
                item_data = {
                    'container_number': container, # 外键对象
                    'invoice_number': inv,
                    'invoice_type': 'receivable',
                    'item_category': 'delivery_public',
                    'delivery_type': 'combine',
                    'PO_ID': group.get('PO_ID'),
                    'warehouse_code': destination_str,  # 对应表的 warehouse_code
                    'region': matched_region,     # 对应表的 region
                    'note': matched_region,       # 对应表的 note (根据要求对应region)
                    'rate': matched_price,             # 对应表的 rate
                    'regionPrice': matched_price,      # 对应表的 regionPrice
                    'qty': len(group.get('pallet_ids', [])), # group的id数量
                    'cbm': cbm,
                    'cbm_ratio': cbm_ratio,
                    'weight': round(group.get('total_weight_lbs', 0), 2),
                    'shipping_marks': group.get('shipping_marks', ''),
                    'description': '组合柜费用',
                    'amount': 0, 
                    'registered_user': username,
                }
                
                
                combina_items.append(item_data)
        if not combina_items:
            return [], None
        
        # 查看是不是整个柜子都是组合柜仓点，都是的话，组合柜cbm_ratio要归一
        is_all_combina = set(destinations).issubset(all_combina_destinations) if destinations else False

        error_msg = None
        if is_all_combina:
            current_ratio_sum = sum(item['cbm_ratio'] for item in combina_items)
            if any(item['cbm_ratio'] < 0 for item in combina_items):
                error_msg = f"账单计算异常: 发现负数占比。整柜CBM: {total_container_cbm}"
            if round(current_ratio_sum, 4) != 1.0000:
                diff = round(1.0 - current_ratio_sum, 4)
                # 找到 cbm 最大的项
                max_item = max(combina_items, key=lambda x: x['cbm'])
                max_item['cbm_ratio'] = round(max_item['cbm_ratio'] + diff, 4)

        for item in combina_items:
            item['amount'] = round(item['rate'] * item['cbm_ratio'], 2)

        return combina_items, error_msg

    def _process_destination(self, destination_origin):
        """处理目的地字符串"""
        def _clean_destination(destination):
            """清理目的地字符串：如果包含'-'且不是UPS开头，取后面的部分"""
            if not destination:
                return destination
            destination = str(destination)
            if destination and '-' in destination:
                # 如果是UPS开头，保留原样
                if destination.upper().startswith("UPS-"):
                    return destination
                parts = destination.split('-')
                if len(parts) > 1:
                    return parts[1]
            return destination
        destination_origin = str(destination_origin)

        # 匹配模式：按"改"或"送"分割，分割符放在第一组的末尾
        if "改" in destination_origin or "送" in destination_origin:
            # 找到第一个"改"或"送"的位置
            first_change_pos = min(
                (destination_origin.find(char) for char in ["改", "送"] 
                if destination_origin.find(char) != -1),
                default=-1
            )
            
            if first_change_pos != -1:
                # 第一部分：到第一个"改"或"送"（包含分隔符）
                first_part = destination_origin[:first_change_pos + 1]
                # 第二部分：剩下的部分
                second_part = destination_origin[first_change_pos + 1:]
                
                # 处理第一部分：按"-"分割取后面的部分
                if "-" in first_part:
                    if first_part.upper().startswith("UPS-"):
                        first_result = first_part
                    else:
                        first_result = first_part.split("-", 1)[1]
                else:
                    first_result = first_part
                
                # 处理第二部分：按"-"分割取后面的部分
                if "-" in second_part:
                    if second_part.upper().startswith("UPS-"):
                        second_result = second_part
                    else:
                        second_result = second_part.split("-", 1)[1]
                else:
                    second_result = second_part
                
                return _clean_destination(first_result), _clean_destination(second_result)
            else:
                return None, _clean_destination(second_result)
        
        # 如果不包含"改"或"送"或者没有找到
        # 只处理第二部分（假设第一部分为空）
        if "-" in destination_origin:
            if destination_origin.upper().startswith("UPS-"):
                second_result = destination_origin
            else:
                second_result = destination_origin.split("-", 1)[1]
            
        else:
            second_result = destination_origin
        
        return None, _clean_destination(second_result)
    
    def _process_destination_wlm(self,destination):
        """处理目的地字段"""
        if destination and '-' in destination:
            parts = destination.split('-')
            if len(parts) > 1:
                return parts[1]
        return destination
    
    def _recaculate_pallet_groups(self,container_number,invoice_number,d_type):
        '''重新计算组合柜数据查找板子数据'''
        base_query = Pallet.objects.filter(
            container_number__container_number=container_number,
            delivery_type=d_type,
        )

        group_fields = [
            "PO_ID",
            "destination",
            "zipcode",
            "delivery_method",
            "location",
            "delivery_type",
        ]

        if d_type == "other":
            group_fields.append("shipping_mark")
        pallet_groups = list(
            base_query.values(*group_fields)
            .annotate(
                total_pallets=models.Count("pallet_id"),
                total_cbm=models.Sum("cbm"),
                total_weight_lbs=models.Sum("weight_lbs"),
                pallet_ids=ArrayAgg("pallet_id"),
                shipping_marks=StringAgg("shipping_mark", delimiter=", ", distinct=True),
            ).order_by("PO_ID")
        )
        for group in pallet_groups:
            po_id = group.get("PO_ID")
            shipping_marks = group.get("shipping_marks")

            if not po_id:
                return [], f"数据异常：柜号 {container_number} 的板子数据中存在缺失 PO_ID 的情况"
            try:
                aggregated = None
                if d_type == "other":
                    try:
                        aggregated = PackingList.objects.filter(PO_ID=po_id,shipping_mark=shipping_marks).aggregate(
                            total_cbm=Sum('cbm'),
                            total_weight_lbs=Sum('total_weight_lbs')
                        )                      
                        
                        if aggregated['total_cbm'] is not None:
                            group['total_cbm'] = aggregated['total_cbm']
                        if aggregated['total_weight_lbs'] is not None:
                            group['total_weight_lbs'] = aggregated['total_weight_lbs']
                        
                    except Exception as e:
                        # 如果查询出错，不修改值
                        continue
                else:
                    if '_' in po_id:
                        continue
                    try:
                        aggregated = PackingList.objects.filter(PO_ID=po_id).aggregate(
                            total_cbm=Sum('cbm'),
                            total_weight_lbs=Sum('total_weight_lbs')
                        )
                    
                        if aggregated['total_cbm'] is not None:
                            group['total_cbm'] = aggregated['total_cbm']
                        if aggregated['total_weight_lbs'] is not None:
                            group['total_weight_lbs'] = aggregated['total_weight_lbs']
                        
                    except Exception as e:
                        # 如果查询出错，不修改值
                        continue
                    
            except Exception as e:
                return [], f"查询 PackingList 异常：{str(e)}"
        return pallet_groups, ""
    
    async def handle_search_wrong_status(self,request):
        """
        查询有没有状态迁移错误的账单
        """
        start_index = int(request.POST.get("start_index", 0))
        end_index = int(request.POST.get("end_index", 0))
        migration_log = []
        containers = await sync_to_async(list)(
            Container.objects.filter(
                id__gte=start_index,
                id__lte=end_index
            ).order_by('id')
        )
        inconsistent_count = 0
        containers_list = []

        for container in containers:
            containers_list.append(container.container_number)
            old_status = await sync_to_async(
                lambda c: InvoiceStatus.objects.filter(
                    container_number=c,
                    invoice_type="payable"
                ).first()
            )(container)
                
            # 直接异步查询新状态表记录
            new_status = await sync_to_async(
                lambda c: InvoiceStatusv2.objects.filter(
                    container_number=c,
                    invoice_type="payable"
                ).first()
            )(container)
            
            # 如果两个状态都存在，进行比较
            if old_status and new_status:
                # 构建旧数据
                old_data = {
                    'preport_status': old_status.preport_status,
                    'warehouse_public_status': old_status.warehouse_public_status,
                    'warehouse_other_status': old_status.warehouse_other_status,
                    'delivery_public_status': old_status.delivery_public_status,
                    'delivery_other_status': old_status.delivery_other_status,
                    'finance_status': old_status.finance_status
                }
                
                # 构建新数据
                new_data = {
                    'preport_status': new_status.preport_status,
                    'warehouse_public_status': new_status.warehouse_public_status,
                    'warehouse_other_status': new_status.warehouse_other_status,
                    'delivery_public_status': new_status.delivery_public_status,
                    'delivery_other_status': new_status.delivery_other_status,
                    'finance_status': new_status.finance_status
                }
                
                # 比较各个状态字段
                status_fields = [
                    ('preport_status', '港前状态'),
                    ('warehouse_public_status', '公仓仓库状态'),
                    ('warehouse_other_status', '私仓仓库状态'),
                    ('delivery_public_status', '公仓派送状态'),
                    ('delivery_other_status', '私仓派送状态'),
                    ('finance_status', '财务状态')
                ]
                
                differences = []
                
                for field, field_name in status_fields:
                    old_value = old_data[field]
                    new_value = new_data[field]
                    
                    if old_value != new_value:
                        differences.append(f"{field_name}: 旧值={old_value}, 新值={new_value}")
                
                # 如果有差异，记录到日志
                if differences:
                    inconsistent_count += 1
                    
                    error_log = {
                        'container_number': container.container_number,
                        'container_id': container.id,
                        'error_type': 'status_inconsistent',
                        'old_data': old_data,
                        'new_data': new_data,
                        'differences': differences,
                        'actions': f'❌ 状态不一致: {container.container_number} 有 {len(differences)} 个状态字段不一致',
                        'old_status_id': old_status.id,
                        'new_status_id': new_status.id
                    }
                    migration_log.append(error_log)
            
            elif old_status and not new_status:
                # 只有旧状态，没有新状态
                # 构建旧数据
                old_data = {
                    'preport_status': old_status.preport_status,
                    'warehouse_public_status': old_status.warehouse_public_status,
                    'warehouse_other_status': old_status.warehouse_other_status,
                    'delivery_public_status': old_status.delivery_public_status,
                    'delivery_other_status': old_status.delivery_other_status,
                    'finance_status': old_status.finance_status
                }
                
                migration_log.append({
                    'container_number': container.container_number,
                    'container_id': container.id,
                    'error_type': 'missing_new_status',
                    'old_data': old_data,
                    'new_data': None,
                    'actions': f'⚠️ 只有旧状态: {container.container_number} 没有新状态'
                })
        context = {
            'message': f'查询到{len(containers)} 条柜子',
            'success': True,
            'start_index': start_index,
            'end_index': end_index,
            'migration_log': migration_log,
            'inconsistent_count': inconsistent_count,
            'containers_list': containers_list,
        }
        return self.template_receivable_status_migrate,context       
            

    async def handle_search_extra_invoice(self,request):
        """
        查询有没有重复迁移的账单
        """
        start_index = int(request.POST.get("start_index", 0))
        end_index = int(request.POST.get("end_index", 0))
        migration_log = []
        containers = await sync_to_async(list)(
            Container.objects.filter(
                id__gte=start_index,
                id__lte=end_index
            ).order_by('id')
        )
        for container in containers:
            # 异步查询这个container在Invoicev2表中的记录数量
            invoicev2_count = await sync_to_async(
                lambda c: Invoicev2.objects.filter(container_number=c).count()
            )(container)
            
            if invoicev2_count > 1:
                # 获取详细信息
                invoicev2_records = await sync_to_async(list)(
                    Invoicev2.objects.filter(container_number=container)
                    .values('id', 'invoice_number', 'invoice_date', 'created_at')
                )
                
                error_log = {
                    'container_number': container.container_number,
                    'container_id': container.id,
                    'invoicev2_count': invoicev2_count,
                    'invoicev2_details': invoicev2_records,
                    'error_type': 'duplicate_invoicev2',
                    'actions': f'❌ 错误： {container.container_number} 在Invoicev2表中有 {invoicev2_count} 条重复记录'
                }
                migration_log.append(error_log)           
            
        context = {
            'message': f'查询到{len(containers)} 条柜子',
            'success': True,
            'start_index': start_index,
            'end_index': end_index,
            'migration_log': migration_log,
        }
        return self.template_receivable_status_migrate,context
    
    async def handle_receivale_status_migrate_delete_old(self,request):
        migration_log = []
        search_input = request.POST.get("search_index", "").strip()

        container_ids = []
        for part in search_input.replace(',', ' ').split():
            if part.isdigit():
                container_ids.append(int(part))

        containers = await sync_to_async(list)(
            Container.objects.filter(id__in=container_ids)
        )
        for container in containers:
            await sync_to_async(lambda c: InvoiceItemv2.objects.filter(container_number=c).delete())(container)
            await sync_to_async(lambda c: InvoiceStatusv2.objects.filter(container_number=c).delete())(container)
            await sync_to_async(lambda c: Invoicev2.objects.filter(container_number=c).delete())(container)
            migration_log.append({
                'container_number': f'{container.container_number}(ID:{container.id})',
                'result': f'删除成功'
            })
        context = {
            'success': True,
            'migration_log': migration_log,
            'search_index': search_input
        }
        return self.template_receivable_status_migrate,context
        
    async def handle_receivale_item_migrate(self,request):
        """迁移InvoiceItem"""
        item_start_index = int(request.POST.get("item_start_index", 0))
        item_end_index = int(request.POST.get("item_end_index", 0))
        migration_log = []
        invoice_items = await sync_to_async(list)(
            InvoiceItem.objects.filter(
                id__gte=item_start_index, 
                id__lte=item_end_index
            ).select_related('invoice_number')
        )
        # 建立映射关系
        container_invoice_items = {}
        for item in invoice_items:
            invoice = item.invoice_number
            if not invoice or not invoice.container_number:
                continue
            
            container = invoice.container_number
            if container.id not in container_invoice_items:
                continue
            
            if invoice.id not in container_invoice_items[container.id]['invoice_items']:
                container_invoice_items[container.id]['invoice_items'][invoice.id] = {
                    'invoice': invoice,
                    'items': []
                }
            
            container_invoice_items[container.id]['invoice_items'][invoice.id]['items'].append(item)
        
        # 3. 遍历每个容器
        total_migrated = 0
        
        for container_id, data in container_invoice_items.items():
            container = data['container']
            
            for invoice_id, invoice_data in data['invoice_items'].items():
                invoice = invoice_data['invoice']
                items = invoice_data['items']
                
                # 查找Invoicev2
                invoicev2 = await sync_to_async(
                    lambda: Invoicev2.objects.filter(
                        invoice_number=invoice.invoice_number,
                        container_number=container
                    ).first()
                )()
                
                if not invoicev2:
                    msg = {
                        'container_number': container.container_number,
                        'actions':f"跳过 {invoice.invoice_number} - 无Invoicev2"
                    }
                    migration_log.append(msg)
                    continue
                
                # 删除该发票的旧记录
                await sync_to_async(
                    InvoiceItemv2.objects.filter(
                        container_number=container,
                        invoice_number=invoicev2
                    ).delete
                )()
                
                # 批量创建新记录
                new_items = []
                for item in items:
                    item_category=self._get_item_category(item.description)
                    new_items.append(InvoiceItemv2(
                        container_number=container,
                        invoice_number=invoicev2,
                        invoice_type="payable",
                        item_category=item_category,
                        description=item.description,
                        qty=item.qty,
                        rate=item.rate,
                        amount=item.amount,
                        cbm=item.cbm,
                        weight=item.weight,
                        warehouse_code=item.warehouse_code,
                        note=item.note
                    ))
                
                if new_items:
                    await sync_to_async(InvoiceItemv2.objects.bulk_create)(new_items)
                    total_migrated += len(new_items)
                    msg = {
                        'container_number': container.container_number,
                        'actions':f"✅ {container.container_number}: {len(new_items)}条"
                    }
                    migration_log.append(msg)
        
        context = {
            'migration_log': migration_log,
            'total_migrated': total_migrated,
            'success': True,
            'message': f'迁移完成: {total_migrated}条记录'
        }
        return self.template_receivable_status_migrate,context

    def _get_item_category(self, description):
        """根据InvoiceItem确定分类的辅助函数"""
        # 这里根据您的业务逻辑实现
        # 例如根据description、warehouse_code或其他字段判断
        if description:
            desc_lower = description.lower()
            if 'ALL' in desc_lower or '提拆' in desc_lower or '打托' in desc_lower:
                return "preport"
            elif '派送' in desc_lower or '送货' in desc_lower:
                return "delivery_public"  # 需要更多逻辑区分公仓/私仓
            elif '仓库' in desc_lower:
                return "warehouse_public"  # 需要更多逻辑区分公仓/私仓
        
        return "preport"  # 默认值

    async def handle_receivale_status_migrate(self,request):
        """
        迁移Invoice和InvoiceStatus数据到新表结构 - 修改版
        """
        start_index = int(request.POST.get("start_index", 0))
        end_index = int(request.POST.get("end_index", 0))
        migration_log = []

        # 查询旧数据数量
        #total_invoices = await sync_to_async(lambda: Invoice.objects.count())()
        
        # 验证范围
        if start_index < 0:
            start_index = 0
        # if end_index <= 0 or end_index > total_invoices:
        #     end_index = total_invoices
        
        # 统一创建日期
        FIXED_CREATED_DATE = date(2025, 12, 9)
        
        # 分批处理
        batch_size = 50
        
        missed = 0
        # for batch_start in range(start_index, end_index, batch_size):
        #     batch_end = min(batch_start + batch_size, end_index)
            
            # 查询当前批次的旧Invoice数据
        old_invoices = await sync_to_async(
            lambda: list(
                Invoice.objects.select_related('customer', 'container_number')
                .filter(id__gte=start_index, id__lte=end_index) 
                .values(
                    'id',
                    'invoice_number',
                    'invoice_date',
                    'invoice_link',
                    'payable_preport_amount',
                    'payable_total_amount',
                    # 'payable_direct_amount',
                    'payable_total_amount',
                    'payable_preport_amount',
                    'payable_warehouse_amount',
                    'payable_delivery_amount',
                    'is_invoice_delivered',
                    'remain_offset',
                    'received_amount',
                    'customer_id',
                    'container_number_id',
                    'container_number__container_number',
                    'statement_id',
                )
                .order_by('id')
            )
        )()
        old_invoices_text = old_invoices

        # 处理每个旧发票
        tasks = []
        for old_invoice in old_invoices:
            task_result = await self.migrate_missed_invoice(old_invoice, FIXED_CREATED_DATE)
            if task_result and task_result.get('miss'):
                missed += 1
            tasks.append(task_result)
            error_log = {
                        'container_number': task_result.get('container_number'),
                        'old_invoice_id': task_result.get('old_invoice_id'),
                        'old_invoice_number': task_result.get('old_invoice_number'),
                        'actions': task_result.get('actions'),
                    }
            migration_log.append(error_log)
        context = {
            'migration_log': migration_log,
            'total_migrated': len(migration_log),
            'message': f'成功迁移 {len(migration_log)} 条账单记录,{missed}条之前未迁移',
            'success': True,
            'start_index': start_index,
            'end_index': end_index,
            'old_invoices_text': old_invoices_text,
        }
        return self.template_receivable_status_migrate, context

    
    async def migrate_missed_invoice(self, old_invoice_dict, fixed_date):
        '''迁移查漏补缺'''
        container_id = old_invoice_dict['container_number_id']
        container_number = old_invoice_dict['container_number__container_number']

        migration_log = {
            'container_number': container_number,
            'old_invoice_id': old_invoice_dict['id'],
            'old_invoice_number': old_invoice_dict['invoice_number'],
            'container_id': container_id,
            'actions': [],
            'miss': False,
        }
        
        try:
            # 1. 检查Container是否存在
            container_exists_func = sync_to_async(
                lambda: Container.objects.filter(id=container_id).exists()
            )
            container_exists = await container_exists_func()
            
            if not container_exists:
                migration_log['actions'].append(f"⚠️ 柜子不存在: ID={container_id}")
                return migration_log
            
            # 2. 按照container_number_id查询旧Invoice
            old_invoice_exists = await sync_to_async(
                lambda: Invoice.objects.filter(container_number_id=container_id).exists()  # 使用container_number_id字段
            )()
            
            migration_log['actions'].append(f"旧Invoice存在: {old_invoice_exists}")
            
            # 3. 按照container_number_id查询新Invoicev2
            new_invoice_exists = await sync_to_async(
                lambda: InvoiceStatusv2.objects.filter(
                    ~models.Q(preport_status='unstarted') |
                    ~models.Q(warehouse_public_status='unstarted') |
                    ~models.Q(warehouse_other_status='unstarted') |
                    ~models.Q(delivery_public_status='unstarted') |
                    ~models.Q(delivery_other_status='unstarted'),
                    preport_status__isnull=False,
                    warehouse_public_status__isnull=False,
                    warehouse_other_status__isnull=False,
                    delivery_public_status__isnull=False,
                    delivery_other_status__isnull=False,
                    container_number_id=container_id
                ).exists()
            )()
            
            migration_log['actions'].append(f"新Invoicev2存在: {new_invoice_exists}")
            
            # 4. 检查是否需要迁移账单
            if old_invoice_exists and not new_invoice_exists:
                
                migration_log['miss'] = True
                migration_log['actions'].append(f"❌ 账单未迁移 - 柜子ID: {container_id}")
                migration_result = await self.migrate_single_invoice(old_invoice_dict, fixed_date)
                migration_log['actions'].extend(migration_result.get('actions', []))
                    
        except Exception as e:
            migration_log['actions'].append(f"❌ 错误: {str(e)}")
            import traceback
            migration_log['actions'].append(traceback.format_exc())
        
        return migration_log
    
    async def migrate_single_invoice(self, old_invoice_dict, fixed_date):
        """
        迁移单个Invoice及其相关数据
        """
        try:   
           
            migration_log = {
                'old_invoice_id': old_invoice_dict['id'],
                'old_invoice_number': old_invoice_dict['invoice_number'],
                'container_number': old_invoice_dict['container_number__container_number'],
                'actions': []
            }

            try:
                # 1. 获取关联对象
                customer = await sync_to_async(Customer.objects.get)(id=old_invoice_dict['customer_id'])
            except Customer.DoesNotExist:
                migration_log['actions'].append(f"找不到Customer ID: {old_invoice_dict['customer_id']}")
                # 创建错误日志并返回
                migration_log['old_data'] = {'stage': 'error', 'stage_public': 'error', 'stage_other': 'error'}
                migration_log['new_data'] = {
                    'preport_status': 'error',
                    'warehouse_public_status': 'error',
                    'warehouse_other_status': 'error',
                    'delivery_public_status': 'error',
                    'delivery_other_status': 'error',
                    'finance_status': 'error'
                }
                return migration_log

            try:
                container = await sync_to_async(Container.objects.get)(id=old_invoice_dict['container_number_id'])
            except Container.DoesNotExist:
                migration_log['actions'].append(f"找不到Container ID: {old_invoice_dict['container_number_id']}")
                # 尝试通过柜号查找
                container_number_str = old_invoice_dict['container_number__container_number']
                if container_number_str:
                    container = await sync_to_async(
                        lambda: Container.objects.filter(container_number=container_number_str).first()
                    )()
                    if container:
                        migration_log['actions'].append(f"通过柜号找到Container: {container.id}")
                    else:
                        migration_log['actions'].append(f"柜号 {container_number_str} 也不存在")
                        migration_log['old_data'] = {'stage': 'error', 'stage_public': 'error', 'stage_other': 'error'}
                        migration_log['new_data'] = {
                            'preport_status': 'error',
                            'warehouse_public_status': 'error',
                            'warehouse_other_status': 'error',
                            'delivery_public_status': 'error',
                            'delivery_other_status': 'error',
                            'finance_status': 'error'
                        }
                        return migration_log
                else:
                    migration_log['actions'].append("没有柜号信息")
                    migration_log['old_data'] = {'stage': 'error', 'stage_public': 'error', 'stage_other': 'error'}
                    migration_log['new_data'] = {
                        'preport_status': 'error',
                        'warehouse_public_status': 'error',
                        'warehouse_other_status': 'error',
                        'delivery_public_status': 'error',
                        'delivery_other_status': 'error',
                        'finance_status': 'error'
                    }
                    return migration_log

            # 检查是否已存在相同invoice_number的Invoicev2
            existing_invoicev2 = await sync_to_async(
                lambda: InvoiceStatusv2.objects.filter(
                    ~models.Q(preport_status='unstarted') |
                    ~models.Q(warehouse_public_status='unstarted') |
                    ~models.Q(warehouse_other_status='unstarted') |
                    ~models.Q(delivery_public_status='unstarted') |
                    ~models.Q(delivery_other_status='unstarted'),
                    invoice=new_invoice,
                    invoice_type="payable",
                ).first()
            )()
           
            if existing_invoicev2:
                migration_log['actions'].append(f"Invoicev2已存在，跳过创建: {existing_invoicev2.id}")
                new_invoice = existing_invoicev2
                is_invoicev2_existing = True 
            else:
                is_invoicev2_existing = False
                public_wh_amount = await sync_to_async(
                    lambda: InvoiceWarehouse.objects.filter(
                        invoice_number__invoice_number=old_invoice_dict['invoice_number'],
                        invoice_type="payable",
                        delivery_type="public"
                    ).aggregate(total_amount=Sum('amount'))['total_amount'] or 0
                )()

                other_wh_amount = await sync_to_async(
                    lambda: InvoiceWarehouse.objects.filter(
                        invoice_number__invoice_number=old_invoice_dict['invoice_number'],
                        invoice_type="payable",
                        delivery_type="other"
                    ).aggregate(total_amount=Sum('amount'))['total_amount'] or 0
                )()

                public_dl_amount = await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(
                        invoice_number__invoice_number=old_invoice_dict['invoice_number'],
                        invoice_type="payable",
                        delivery_type="public"
                    ).aggregate(total_amount=Sum('total_cost'))['total_amount'] or 0
                )()

                other_dl_amount = await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(
                        invoice_number__invoice_number=old_invoice_dict['invoice_number'],
                        invoice_type="payable",
                        delivery_type="other"
                    ).aggregate(total_amount=Sum('total_cost'))['total_amount'] or 0
                )()
                
                statement_object = None
                statement_id_value = old_invoice_dict.get('statement_id')
                if statement_id_value:
                    
                    try:
                        statement_object = await sync_to_async(InvoiceStatement.objects.get)(
                            id=statement_id_value
                        )
                        migration_log['actions'].append(f"找到关联的Statement: {statement_object.invoice_statement_id}")
                    except InvoiceStatement.DoesNotExist:
                        migration_log['actions'].append(f"警告：找不到Statement ID: {statement_id_value}")
                        statement_object = None

                # 1. 创建新的Invoicev2
                new_invoice = Invoicev2(
                    invoice_number=old_invoice_dict['invoice_number'],
                    invoice_date=old_invoice_dict['invoice_date'],
                    created_at=fixed_date,  # 固定为2025年12月9号
                    invoice_link=old_invoice_dict['invoice_link'],
                    customer=customer,
                    container_number=container,
                    statement_id=statement_object,
                    
                    # 应付金额字段
                    payable_total_amount=old_invoice_dict['payable_total_amount'] or 0,
                    payable_preport_amount=old_invoice_dict['payable_preport_amount'] or 0,
                    # 根据说明：两个新字段都等于旧表的payable_warehouse_amount
                    # payable_wh_public_amount=public_wh_amount,
                    # payable_wh_other_amount=other_wh_amount,
                    # 根据说明：两个新字段都等于旧表的payable_delivery_amount
                    # payable_delivery_public_amount=public_dl_amount,
                    # payable_delivery_other_amount=other_dl_amount,
                    # payable_direct_amount=old_invoice_dict['payable_direct_amount'] or 0,
                    payable_is_locked=False,  # 默认未锁定
                    
                    # 应付金额字段
                    payable_warehouse_amount=old_invoice_dict['payable_warehouse_amount'] or 0,
                    payable_delivery_amount=old_invoice_dict['payable_delivery_amount'] or 0,
                    
                    # 其他字段
                    is_invoice_delivered=old_invoice_dict['is_invoice_delivered'],
                    received_amount=old_invoice_dict['received_amount'] or 0,
                    remain_offset=old_invoice_dict['remain_offset'] or 0,
                    
                )
                
                await sync_to_async(new_invoice.save)()
                migration_log['actions'].append(f"创建Invoicev2: {new_invoice.id}")
                if statement_object:
                    migration_log['actions'].append(f"链接迁移成功")
            
            # 2. 迁移InvoiceStatus数据
            # 获取旧Invoice的所有状态记录
            try:
                try:
                    old_status = await sync_to_async(InvoiceStatus.objects.get)(
                        container_number=container,
                        invoice_type="payable"
                    )
                except InvoiceStatus.DoesNotExist:
                    migration_log['actions'].append("没有应付状态表")
                    migration_log['old_data'] = {'stage': 'error', 'stage_public': 'error', 'stage_other': 'error'}
                    migration_log['new_data'] = {
                        'preport_status': 'error',
                        'warehouse_public_status': 'error',
                        'warehouse_other_status': 'error',
                        'delivery_public_status': 'error',
                        'delivery_other_status': 'error',
                        'finance_status': 'error'
                    }
                    return migration_log
                old_data = {
                    'stage': getattr(old_status, 'stage', 'unstarted'),
                    'stage_public': getattr(old_status, 'stage_public', 'pending'),
                    'stage_other': getattr(old_status, 'stage_other', 'pending')
                }
            

                # 检查是否已存在相同invoice和invoice_type的InvoiceStatusv2
                existing_status = await sync_to_async(
                    lambda: InvoiceStatusv2.objects.filter(
                        ~models.Q(preport_status='unstarted') |
                        ~models.Q(warehouse_public_status='unstarted') |
                        ~models.Q(warehouse_other_status='unstarted') |
                        ~models.Q(delivery_public_status='unstarted') |
                        ~models.Q(delivery_other_status='unstarted'),
                        invoice=new_invoice,
                        invoice_type="payable",
                    ).first()
                )()
                                
                if not existing_status:
                    is_status_existing = False
                    # 创建新的InvoiceStatusv2
                    new_status = InvoiceStatusv2(
                        container_number=container,
                        invoice=new_invoice,  # 关联到新创建的Invoicev2
                        invoice_type="payable",
                        
                        # 状态字段
                        preport_status=old_status.preport_status,
                        warehouse_public_status=old_status.warehouse_public_status,
                        warehouse_other_status=old_status.warehouse_other_status,
                        delivery_public_status=old_status.delivery_public_status,
                        delivery_other_status=old_status.delivery_other_status,
                        finance_status=old_status.finance_status,
                        
                        # 新增的驳回原因字段（都为空）
                        preport_reason='',
                        warehouse_public_reason='',
                        warehouse_self_reason='',
                        delivery_public_reason='',
                        delivery_other_reason='',
                        
                        # payable字段
                        payable_status=old_status.payable_status,
                        payable_date=old_status.payable_date,
                    )
                    
                    await sync_to_async(new_status.save)()
                    migration_log['actions'].append(f"创建InvoiceStatusv2: {new_status.id}")

                    new_data = {
                        'preport_status': new_status.preport_status,
                        'warehouse_public_status': new_status.warehouse_public_status,
                        'warehouse_other_status': new_status.warehouse_other_status,
                        'delivery_public_status': new_status.delivery_public_status,
                        'delivery_other_status': new_status.delivery_other_status,
                        'finance_status': new_status.finance_status
                    }
                else:
                    is_status_existing = True
                    migration_log['actions'].append(f"InvoiceStatusv2已存在: {existing_status.id}")
                    # 记录已存在的新状态
                    new_data = {
                        'preport_status': existing_status.preport_status,
                        'warehouse_public_status': existing_status.warehouse_public_status,
                        'warehouse_other_status': existing_status.warehouse_other_status,
                        'delivery_public_status': existing_status.delivery_public_status,
                        'delivery_other_status': existing_status.delivery_other_status,
                        'finance_status': existing_status.finance_status
                    }
                migration_log['old_data'] = old_data
                migration_log['new_data'] = new_data
                
            except InvoiceStatus.DoesNotExist:
                # 查不到就跳过
                migration_log['actions'].append(f"没有找到柜号 {container.container_number} 的InvoiceStatus记录")
                # 创建空的old_data和new_data用于前端显示
                migration_log['old_data'] = {
                    'stage': 'unstarted',
                    'stage_public': 'pending',
                    'stage_other': 'pending'
                }
                migration_log['new_data'] = {
                    'preport_status': 'unstarted',
                    'warehouse_public_status': 'unstarted',
                    'warehouse_other_status': 'unstarted',
                    'delivery_public_status': 'unstarted',
                    'delivery_other_status': 'unstarted',
                    'finance_status': 'unstarted'
                }
            
            if is_invoicev2_existing and is_status_existing:
                migration_log['actions'].append(f"已经迁移过了，不再迁移明细表")
            else:
                # 3. 迁移InvoiceItem数据
                # 这里需要根据你的业务逻辑来创建InvoiceItemv2
                # 可以根据Invoicev2的金额字段创建对应的明细记录
                old_invoice_obj = await sync_to_async(Invoice.objects.get)(id=old_invoice_dict['id'])
                migration_log = await self.create_invoice_items_from_invoice(new_invoice, old_invoice_obj, migration_log)
            
            return migration_log
            
        except Exception as e:
            migration_log['actions'].append(f"迁移失败: {str(e)}")
            # 添加错误状态数据
            migration_log['old_data'] = {
                'stage': 'error',
                'stage_public': 'error',
                'stage_other': 'error'
            }
            migration_log['new_data'] = {
                'preport_status': 'error',
                'warehouse_public_status': 'error',
                'warehouse_other_status': 'error',
                'delivery_public_status': 'error',
                'delivery_other_status': 'error',
                'finance_status': 'error'
            }
            return migration_log

    async def create_invoice_items_from_invoice(self, new_invoice, old_invoice_obj, migration_log):
        """
        根据Invoicev2的金额字段创建InvoiceItemv2明细
        """
        # 获取容器编号
        if 1:
            container_number = new_invoice.container_number
            
            # 1. 迁移InvoicePreport数据
            preport_count = await self._migrate_preport_items(new_invoice, old_invoice_obj, container_number)
            migration_log['actions'].append(f"迁移港前表成功: {preport_count}条记录")
            
            # 2. 迁移InvoiceWarehouse数据
            warehouse_count = await self._migrate_warehouse_items(new_invoice, old_invoice_obj, container_number)
            migration_log['actions'].append(f"迁移库内表成功: {warehouse_count}条记录")
            
            # 3. 迁移InvoiceDelivery数据
            delivery_count = await self._migrate_delivery_items(new_invoice, old_invoice_obj, container_number)
            migration_log['actions'].append(f"迁移派送表成功: {delivery_count}条记录")
            
        # except Exception as e:
        #     migration_log['actions'].append(f"创建InvoiceItem明细失败: {str(e)}")
            
        return migration_log

    async def _migrate_preport_items(self, new_invoice, old_invoice, container_number):
        """迁移InvoicePreport数据到InvoiceItemv2"""
        created_count = 0
        if 1:
            # 获取InvoicePreport记录
            invoice_preports = await sync_to_async(list)(
                InvoicePreport.objects.filter(
                    invoice_number__invoice_number=old_invoice.invoice_number,
                    invoice_type="payable",
                )
            )
            
            for preport in invoice_preports:
                # 获取surcharges和surcharge_notes字典
                surcharges_dict = preport.surcharges if isinstance(preport.surcharges, dict) else {}
                surcharge_notes_dict = preport.surcharge_notes if isinstance(preport.surcharge_notes, dict) else {}
                
                # 定义字段映射：字段名 -> (verbose_name, 值)
                field_mapping = {
                    'pickup': ("提拆/打托缠膜", preport.pickup),
                    'chassis': ("托架费", preport.chassis),
                    'chassis_split': ("托架提取费", preport.chassis_split),
                    'prepull': ("预提费", preport.prepull),
                    'yard_storage': ("货柜放置费", preport.yard_storage),
                    'handling_fee': ("操作处理费", preport.handling_fee),
                    'pier_pass': ("码头", preport.pier_pass),
                    'congestion_fee': ("港口拥堵费", preport.congestion_fee),
                    'hanging_crane': ("吊柜费", preport.hanging_crane),
                    'dry_run': ("空跑费", preport.dry_run),
                    'exam_fee': ("查验费", preport.exam_fee),
                    'hazmat': ("危险品", preport.hazmat),
                    'over_weight': ("超重费", preport.over_weight),
                    'urgent_fee': ("加急费", preport.urgent_fee),
                    'other_serive': ("其他服务", preport.other_serive),
                    'demurrage': ("港内滞期费", preport.demurrage),
                    'per_diem': ("港外滞期费", preport.per_diem),
                    'second_pickup': ("二次提货", preport.second_pickup),
                }
                
                # 处理固定费用字段
                for field_name, (description, value) in field_mapping.items():
                    if value and float(value) != 0:
                        # 获取该字段对应的附加费和备注
                        surcharge_amount = surcharges_dict.get(field_name, 0)
                        surcharge_note = surcharge_notes_dict.get(field_name, "")
                        
                        invoice_item = InvoiceItemv2(
                            container_number=container_number,
                            invoice_number=new_invoice,
                            invoice_type="payable",
                            item_category="preport",
                            description=description,
                            qty=1,
                            rate=float(value),
                            amount=float(value),
                            surcharges=float(surcharge_amount) if surcharge_amount else None,
                            note=str(surcharge_note) if surcharge_note else "",
                        )
                        await invoice_item.asave()
                        created_count += 1
                
                # 处理other_fees（额外费用） - 这些是独立的不在固定字段列表中的费用
                if preport.other_fees and isinstance(preport.other_fees, dict):
                    for fee_name, fee_amount in preport.other_fees.items():
                        if fee_amount and float(fee_amount) != 0:
                            invoice_item = InvoiceItemv2(
                                container_number=container_number,
                                invoice_number=new_invoice,
                                invoice_type="payable",
                                item_category="preport",
                                description=str(fee_name),
                                qty=1,
                                rate=float(fee_amount),
                                amount=float(fee_amount),
                            )
                            await invoice_item.asave()
                            created_count += 1
                         
        # except Exception as e:
        #     logger.error(f"迁移港前表错误: {str(e)}")
        #     raise
        return created_count

    async def _migrate_warehouse_items(self, new_invoice, old_invoice, container_number):
        """迁移InvoiceWarehouse数据到InvoiceItemv2"""
        created_count = 0
        if 1:
            # 获取InvoiceWarehouse记录
            invoice_warehouses = await sync_to_async(list)(
                InvoiceWarehouse.objects.filter(
                    invoice_number__invoice_number=old_invoice.invoice_number,
                    invoice_type="payable",
                )
            )
            
            for warehouse in invoice_warehouses:
                # 根据delivery_type确定item_category
                if warehouse.delivery_type == "public":
                    item_category = "warehouse_public"
                else:
                    item_category = "warehouse_other"
                
                # 获取surcharges和surcharge_notes字典
                surcharges_dict = warehouse.surcharges if isinstance(warehouse.surcharges, dict) else {}
                surcharge_notes_dict = warehouse.surcharge_notes if isinstance(warehouse.surcharge_notes, dict) else {}
                
                # 定义字段映射：字段名 -> (verbose_name, 值)
                field_mapping = {
                    'sorting': ("分拣费", warehouse.sorting),
                    'intercept': ("拦截费", warehouse.intercept),
                    'po_activation': ("亚马逊PO激活", warehouse.po_activation),
                    'self_pickup': ("客户自提", warehouse.self_pickup),
                    'split_delivery': ("拆柜交付快递", warehouse.split_delivery),
                    're_pallet': ("重新打板", warehouse.re_pallet),
                    'handling': ("操作费", warehouse.handling),
                    'counting': ("货品清点费", warehouse.counting),
                    'warehouse_rent': ("仓租", warehouse.warehouse_rent),
                    'specified_labeling': ("指定贴标", warehouse.specified_labeling),
                    'inner_outer_box': ("内外箱", warehouse.inner_outer_box),
                    'inner_outer_box_label': ("内外箱标签", warehouse.inner_outer_box_label),
                    'pallet_label': ("托盘标签", warehouse.pallet_label),
                    'open_close_box': ("开封箱", warehouse.open_close_box),
                    'destroy': ("销毁", warehouse.destroy),
                    'take_photo': ("拍照", warehouse.take_photo),
                    'take_video': ("拍视频", warehouse.take_video),
                    'repeated_operation_fee': ("重复操作费", warehouse.repeated_operation_fee),
                    'palletization_fee': ("应付拆柜费", warehouse.palletization_fee),
                    'arrive_fee': ("应付入库费", warehouse.arrive_fee),
                }
                
                # 处理固定费用字段
                for field_name, (description, value) in field_mapping.items():
                    if value and float(value) != 0:
                        # 获取该字段对应的附加费和备注
                        surcharge_amount = surcharges_dict.get(field_name, 0)
                        surcharge_note = surcharge_notes_dict.get(field_name, "")
                        
                        invoice_item = InvoiceItemv2(
                            container_number=container_number,
                            invoice_number=new_invoice,
                            invoice_type="payable",
                            item_category=item_category,
                            description=description,
                            qty=1,
                            rate=float(value),
                            amount=float(value),
                            surcharges=float(surcharge_amount) if surcharge_amount and float(surcharge_amount) != 0 else None,
                            note=str(surcharge_note) if surcharge_note else ""
                        )
                        await invoice_item.asave()
                        created_count += 1
                
                # 处理other_fees（额外费用） - 这些是独立的不在固定字段列表中的费用
                if warehouse.other_fees and isinstance(warehouse.other_fees, dict):
                    for fee_name, fee_amount in warehouse.other_fees.items():
                        if fee_amount and float(fee_amount) != 0:
                            invoice_item = InvoiceItemv2(
                                container_number=container_number,
                                invoice_number=new_invoice,
                                invoice_type="payable",
                                item_category=item_category,
                                description=str(fee_name),
                                qty=1,
                                rate=float(fee_amount),
                                amount=float(fee_amount),
                            )
                            await invoice_item.asave()
                            created_count += 1
                                  
        # except Exception as e:
        #     logger.error(f"迁移库内表错误: {str(e)}")
        #     raise
        return created_count
    
    async def _migrate_delivery_items(self, new_invoice, old_invoice, container_number):
        """迁移InvoiceDelivery数据到InvoiceItemv2"""
        created_count = 0
        if 1:
            # 获取InvoiceDelivery记录
            invoice_deliveries = await sync_to_async(list)(
                InvoiceDelivery.objects.filter(
                    invoice_number__invoice_number=old_invoice.invoice_number,
                    invoice_type="payable",
                )
            )
            
            for delivery in invoice_deliveries:
                # 根据delivery_type确定item_category
                if delivery.delivery_type == "public":
                    item_category = "delivery_public"
                else:
                    item_category = "delivery_other"
                
                # 创建InvoiceItemv2记录（一条InvoiceDelivery对应一条InvoiceItemv2）
                invoice_item = InvoiceItemv2(
                    container_number=container_number,
                    invoice_number=new_invoice,
                    invoice_type="payable",
                    item_category=item_category,
                    delivery_type=delivery.type,  # type对应delivery_type
                    warehouse_code=delivery.destination,  # destination对应warehouse_code
                    qty=delivery.total_pallet,  # total_pallet对应qty
                    rate=delivery.cost,  # cost对应rate
                    cbm=delivery.total_cbm,  # total_cbm对应cbm
                    weight=delivery.total_weight_lbs,  # total_weight_lbs对应weight
                    amount=delivery.total_cost,  # total_cost对应amount
                    note=delivery.note,  # note对应note
                    description="派送费", 
                )
                await invoice_item.asave()
                created_count += 1
           
        # except Exception as e:
        #     logger.error(f"迁移派送费出错: {str(e)}")
        #     raise
        return created_count


    async def map_public_other_status_async(self, old_status):
        """映射公仓/私仓状态到新状态"""
        mapping = {
            "pending": "unstarted",
            "warehouse_completed": "completed",
            "delivery_completed": "completed", 
            "warehouse_rejected": "rejected",  # 保留原有的驳回状态
            "delivery_rejected": "rejected",   # 保留原有的驳回状态
        }
        return mapping.get(old_status, "unstarted")

    async def apply_rejection_status(self, old_stage, base_status):
        """根据原阶段应用驳回状态"""
        if old_stage == "preport":
            # 港前被驳回
            base_status['preport_status'] = "rejected"
        elif old_stage == "warehouse":
            # 仓库被驳回 - 根据具体的公仓/私仓状态判断
            if base_status['warehouse_public_status'] != "unstarted":
                base_status['warehouse_public_status'] = "rejected"
            if base_status['warehouse_other_status'] != "unstarted":
                base_status['warehouse_other_status'] = "rejected"
        elif old_stage == "delivery":
            # 派送被驳回 - 根据具体的公仓/私仓状态判断
            if base_status['delivery_public_status'] != "unstarted":
                base_status['delivery_public_status'] = "rejected"
            if base_status['delivery_other_status'] != "unstarted":
                base_status['delivery_other_status'] = "rejected"
        return base_status

    async def _validate_user_exception_handling(self, user: User) -> bool:
        if user.is_staff:
            return True
        
        return await sync_to_async(
            lambda: user.groups.filter(name="exception_handling").exists()
        )()
    
    async def _validate_super_user(self, user: User) -> bool:
        if user.is_staff:
            return True
        else:
            return False