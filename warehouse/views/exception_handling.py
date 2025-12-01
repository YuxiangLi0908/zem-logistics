from typing import Any, Coroutine
from django.db.models.functions import Trim
from django.db.models.functions import TruncMonth
from collections import Counter
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.db import models
from django.core.exceptions import FieldDoesNotExist
from django.views import View
import pandas as pd
import re, json
import asyncio
from django.db.models import Q
from django.utils.timezone import make_aware, now
from datetime import datetime, timedelta
from django.contrib import messages
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
from warehouse.models.invoice_details import InvoiceDelivery
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.fleet_shipment_pallet import FleetShipmentPallet
from warehouse.models.invoice_details import InvoicePreport
from warehouse.models.invoice_details import InvoiceWarehouse
from warehouse.models.invoice import InvoiceStatus
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
class ExceptionHandling(View):
    template_container_pallet = "exception_handling/shipment_actual.html"
    template_post_port_status = "exception_handling/post_port_status.html"
    template_delivery_invoice = "exception_handling/delivery_invoice.html"
    template_excel_formula_tool = "exception_handling/excel_formula_tool.html"
    template_find_all_table = "exception_handling/find_all_table_id.html"   
    template_query_pallet_packinglist = "exception_handling/query_pallet_packinglist.html"
    template_temporary_function = "exception_handling/temporary_function.html"
    template_receivable_status_migrate = "exception_handling/receivable_status_migrate.html"
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
        
    async def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
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
        elif step == "receivale_status_search":
            template, context = await self.handle_receivale_status_search(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "update_shipment_type_to_fleet_type":
            template, context = await self.handle_update_shipment_type_to_fleet_type(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_shipment":
            template, context = await self.handle_delete_shipment(request)
            return await sync_to_async(render)(request, template, context)
        else:
            return await sync_to_async(T49Webhook().post)(request)
    
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
            container_number__order__offload_id__offload_at__gte=search_date_lower,
            container_number__order__offload_id__offload_at__lte=search_date,
            container_number__order__retrieval_id__retrieval_destination_precise=warehouse
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
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
                "container_number__order__offload_id",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
            )
            .filter(base_criteria)
            .annotate(
                str_id=Cast("id", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__order__customer_name__zem_name",
                "destination",
                "delivery_method",
                "container_number__order__offload_id__offload_at",
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
            .order_by("container_number__order__offload_id__offload_at")
        )
        return pal_list

    async def get_packinglist_data(self, base_criteria):
        """获取PackingList数据"""
        pl_list = await sync_to_async(list)(
            PackingList.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
                "container_number__order__offload_id",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
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
                "container_number__order__customer_name__zem_name",
                "destination",
                "delivery_method",
                "container_number__order__offload_id__offload_at",
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
            .order_by("container_number__order__offload_id__offload_at")
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
        filters = Q(container_number__order__cancel_notification=False)
        if query_type == "pallet":
            filters &= Q(location=warehouse)
        else:
            filters &= Q(container_number__order__retrieval_id__retrieval_destination_area=warehouse)

        # 定义日期变量
        start_date = None
        end_date = None

        # 如果没有提供任何搜索条件，默认查询前两个月
        if not container_number and not start_date_str and not end_date_str and not month_filter and not destination:
            two_months_ago = today - timedelta(days=60)
            default_start = two_months_ago
            start_date = make_aware(datetime.combine(default_start, datetime.min.time()))
            end_date = make_aware(datetime.combine(default_end, datetime.max.time()))
            filters &= Q(container_number__order__offload_id__offload_at__gte=start_date) 
            filters &= Q(container_number__order__offload_id__offload_at__lte=end_date)
        else:
            if start_date_str:
                start_date = make_aware(datetime.strptime(start_date_str, "%Y-%m-%d"))
                filters &= Q(container_number__order__offload_id__offload_at__gte=start_date) 

            if end_date_str:
                end_date = make_aware(datetime.strptime(end_date_str, "%Y-%m-%d"))
                filters &= Q(container_number__order__offload_id__offload_at__lte=end_date)

            # 月份筛选 - 修改为按照 offload_at 时间筛选
            if month_filter:
                month_date = datetime.strptime(month_filter, "%Y-%m")
                month_start = make_aware(datetime(month_date.year, month_date.month, 1))
                next_month = month_date.replace(day=28) + timedelta(days=4)
                month_end = make_aware(datetime(next_month.year, next_month.month, 1) - timedelta(days=1))
                filters &= Q(container_number__order__offload_id__offload_at__range=(month_start, month_end))

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
            'PoCheckEtaSeven': PoCheckEtaSeven,
            'QuotationMaster': QuotationMaster,
            'TransferLocation': TransferLocation,
            'Vessel': Vessel,
        }
        
        return model_map.get(table_name)
    
    async def query_table_data_all(self, table_name: str, search_field: str, search_value: str):
        """查询表数据 - 返回所有匹配的记录"""
        try:
            # 根据表名获取模型
            model = await self.get_model_by_name(table_name)
            if not model:
                return None
            
            # 构建查询条件
            filter_kwargs = {f"{search_field}__icontains": search_value}
            
            # 使用 sync_to_async 执行查询
            def get_records():
                # 检查字段是否是外键
                model_field = model._meta.get_field(search_field)
                if model_field.is_relation:
                    # 是外键，自动拼接常用字段名
                    related_model = model_field.related_model

                    # 假设外键关联的模型里有一个明显的标识字段，比如 number、name 或 container_number
                    candidate_fields = ["container_number", "name", "number", "fleet_number", "shipment_batch_number"]
                    target_field = None
                    for c in candidate_fields:
                        if any(f.name == c for f in related_model._meta.fields):
                            target_field = c
                            break

                    if target_field:
                        filter_key = f"{search_field}__{target_field}__icontains"
                    else:
                        # 实在找不到就退回 id 匹配
                        filter_key = f"{search_field}__id__icontains"
                else:
                    # 普通字段
                    filter_key = f"{search_field}__icontains"

                filter_kwargs = {filter_key: search_value}

                return list(model.objects.filter(**filter_kwargs).values())
            
            records = await sync_to_async(get_records)()
            return records
            
        except Exception as e:
            logger.error(f"查询表数据失败: {str(e)}")
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
            'PalletDestroyed': PalletDestroyed,
            'Pallet': Pallet,
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
                
            if search_type != 'fleet':
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
            else:
                messages.error(request, f"未找到预约号 '{search_value}' 的相关数据")
        except Exception as e:
            messages.error(request, f"查询失败: {str(e)}")
        
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
            if has_related_data:
                messages.error(request, "存在po绑定在这条约上，不能直接删除！")
            if shipment.fleet_number:
                messages.error(request, "这条约已排车，不能直接删除！")
            else:
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

        try:
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

        except Exception as e:
            messages.error(request, f"查询过程中发生错误: {str(e)}")
            return self.template_delivery_invoice, {"search_performed": True}

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

    async def handle_receivale_status_migrate(self,request):
        """
        将旧的 InvoiceStatus的stage 迁移到新的结构 - 应付状态
        """
        start_index = int(request.POST.get("start_index", 0))
        end_index = int(request.POST.get("end_index", 0))
        migration_log = []
        
        # 查一下数量
        total_orders = await sync_to_async(Order.objects.count)()
        
        # 验证范围
        if start_index < 0:
            start_index = 0
        if end_index <= 0 or end_index > total_orders:
            end_index = total_orders
        
        total_to_process = end_index - start_index
        
        # 分批处理
        batch_size = 100
        
        # 计算需要处理的批次范围
        start_batch = start_index // batch_size
        end_batch = (end_index - 1) // batch_size
        
        for batch_index in range(start_batch, end_batch + 1):
            batch_start = batch_index * batch_size
            batch_end = min((batch_index + 1) * batch_size, total_orders)
            
            # 如果当前批次不在目标范围内，跳过
            if batch_end <= start_index or batch_start >= end_index:
                continue
                
            # 计算当前批次在目标范围内的实际起止位置
            current_batch_start = max(batch_start, start_index)
            current_batch_end = min(batch_end, end_index)
            
            # 计算在批次内的偏移量
            batch_offset = current_batch_start - batch_start
            batch_limit = current_batch_end - current_batch_start
            
            # 异步查询当前批次的订单
            orders = await sync_to_async(list)(
                Order.objects.select_related('container_number', 'receivable_status')
                .all()[current_batch_start:current_batch_end]
            )
            
            # 并发处理每个订单
            tasks = []
            for order in orders:
                # 只处理应付状态
                if order.receivable_status:
                    task = self.migrate_single_status(order, order.receivable_status, "receivable")
                    tasks.append(task)
            
            # 等待所有任务完成
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 收集成功的结果
            for result in batch_results:
                if result and not isinstance(result, Exception):
                    migration_log.append(result)
    
        context = {
            'migration_log': migration_log,
            'total_migrated': len(migration_log),
            'message': f'成功迁移 {len(migration_log)} 条应付状态记录',
            'success': True,
            'start_index': start_index,
            'end_index': end_index,
        }
        return self.template_receivable_status_migrate, context


    async def migrate_single_status(self, order, old_status, invoice_type):
        """
        迁移单个 stage 
        """
        try:
            old_stage = old_status.stage
            old_stage_public = old_status.stage_public
            old_stage_other = old_status.stage_other
            is_rejected = old_status.is_rejected
            
            # 构建迁移日志
            log_entry = {
                'container_number': order.container_number.container_number if order.container_number else '未知柜号',
                'old_data': {
                    'stage': old_stage,
                    'stage_public': old_stage_public,
                    'stage_other': old_stage_other,
                },
                'new_data': {},
            }
            if old_stage == "confirmed":
                # 根据旧状态映射到新状态
                new_status_mapping = await self.calculate_new_status(old_stage, old_stage_public, old_stage_other, is_rejected)
                
                # 异步更新 InvoiceStatus 对象
                await self.update_invoice_status(old_status, new_status_mapping)
                
                # 记录新数据
                log_entry['new_data'] = new_status_mapping
                
                return log_entry
            
        except Exception as e:
            print(f"迁移失败 - Order ID: {order.id}, Error: {str(e)}")
            return None
    
    async def update_invoice_status(self, old_status, new_status_mapping):
        """异步更新 InvoiceStatus 对象"""
        old_status.preport_status = new_status_mapping['preport_status']
        old_status.warehouse_public_status = new_status_mapping['warehouse_public_status']
        old_status.warehouse_other_status = new_status_mapping['warehouse_other_status']
        old_status.delivery_public_status = new_status_mapping['delivery_public_status']
        old_status.delivery_other_status = new_status_mapping['delivery_other_status']
        old_status.finance_status = new_status_mapping['finance_status']
        
        # 异步保存
        await sync_to_async(old_status.save)()

    async def calculate_new_status(self, old_stage, old_stage_public, old_stage_other, is_rejected):
        """异步计算新状态映射 - 处理驳回状态"""
        # 基础状态映射
        # if old_stage == "unstarted":
        #     base_status = {
        #         'preport_status': "unstarted",
        #         'warehouse_public_status': "unstarted",
        #         'warehouse_other_status': "unstarted",
        #         'delivery_public_status': "unstarted",
        #         'delivery_other_status': "unstarted",
        #         'finance_status': "unstarted",
        #     }
        # elif old_stage == "preport":
        #     base_status = {
        #         'preport_status': "pending_review",
        #         'warehouse_public_status': "unstarted",
        #         'warehouse_other_status': "unstarted",
        #         'delivery_public_status': "unstarted",
        #         'delivery_other_status': "unstarted",
        #         'finance_status': "unstarted",
        #     }
        # elif old_stage == "warehouse":
        #     base_status = {
        #         'preport_status': "completed",
        #         'warehouse_public_status': await self.map_public_other_status_async(old_stage_public),
        #         'warehouse_other_status': await self.map_public_other_status_async(old_stage_other),
        #         'delivery_public_status': await self.map_public_other_status_async(old_stage_public),
        #         'delivery_other_status': await self.map_public_other_status_async(old_stage_other),
        #         'finance_status': "unstarted",
        #     }
        # elif old_stage == "delivery":
        #     base_status = {
        #         'preport_status': "completed",
        #         'warehouse_public_status': await self.map_public_other_status_async(old_stage_public),
        #         'warehouse_other_status': await self.map_public_other_status_async(old_stage_public),
        #         'delivery_public_status': await self.map_public_other_status_async(old_stage_public),
        #         'delivery_other_status': await self.map_public_other_status_async(old_stage_other),
        #         'finance_status': "unstarted",
        #     }
        # elif old_stage == "tobeconfirmed":
        #     base_status = {
        #         'preport_status': "completed",
        #         'warehouse_public_status': "completed",
        #         'warehouse_other_status': "completed",
        #         'delivery_public_status': "completed",
        #         'delivery_other_status': "completed",
        #         'finance_status': "tobeconfirmed",
        #     }
        if old_stage == "confirmed":
            base_status = {
                'preport_status': "completed",
                'warehouse_public_status': "completed",
                'warehouse_other_status': "completed",
                'delivery_public_status': "completed",
                'delivery_other_status': "completed",
                'finance_status': "completed",
            }
        # else:
        #     base_status = {
        #         'preport_status': "unstarted",
        #         'warehouse_public_status': "unstarted",
        #         'warehouse_other_status': "unstarted",
        #         'delivery_public_status': "unstarted",
        #         'delivery_other_status': "unstarted",
        #         'finance_status': "unstarted",
        #     }
        
        # 如果有驳回，根据当前阶段设置驳回状态
        if is_rejected:
            base_status = await self.apply_rejection_status(old_stage, base_status)
        
        return base_status

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