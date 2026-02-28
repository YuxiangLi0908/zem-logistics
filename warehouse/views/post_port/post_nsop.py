from datetime import datetime, timedelta, time
from typing import Any, Dict, List, Tuple
from django.db.models import Prefetch, F, Subquery, OuterRef
from collections import OrderedDict, defaultdict
import pandas as pd
import json
import uuid
import pytz
import os
import platform
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from django.db import transaction
import re
import base64
import io
import zipfile
import barcode
from PIL import Image
from django.template.loader import get_template
from PyPDF2 import PdfMerger, PdfReader
from xhtml2pdf import pisa
from barcode.writer import ImageWriter
from django.db.models.functions import Ceil
from django.utils.safestring import mark_safe
from django.utils.html import escape
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Round, Cast, Coalesce
from django.core.exceptions import ObjectDoesNotExist
from simple_history.utils import bulk_update_with_history
from django.db import models
from django.db.models.expressions import ExpressionWrapper
import math  
from django.db.models import (
    Case,
    CharField,
    BooleanField,
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
import asyncio
import aiohttp
from django.http import JsonResponse
from io import BytesIO
from django.core.exceptions import MultipleObjectsReturned
from django.db.models.functions import Cast, Concat
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from django.utils import timezone
from datetime import timedelta
from dateutil.parser import parse
from django.core.serializers.json import DjangoJSONEncoder
from django.contrib.auth.models import User
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.fleet import Fleet
from warehouse.models.order import Order
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.shipment import Shipment
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.container import Container
from warehouse.models.invoicev2 import (
    Invoicev2,
    InvoiceItemv2,
    InvoiceStatusv2,
)
from django.contrib import messages
from warehouse.models.transfer_location import TransferLocation
from warehouse.views.post_port.shipment.fleet_management import FleetManagement
from warehouse.views.post_port.shipment.shipping_management import ShippingManagement
from warehouse.views.receivable_accounting import ReceivableAccounting
from warehouse.views.po import PO
from warehouse.views.export_file import link_callback
from warehouse.utils.constants import (
    LOAD_TYPE_OPTIONS,
    amazon_fba_locations,
    NJ_DES,SAV_DES,LA_DES,
    DELIVERY_METHOD_OPTIONS
)
FOUR_MAJOR_WAREHOUSES = ["ONT8", "LAX9", "LGB8", "SBD1"]


class PostNsop(View):
    template_main_dash = "post_port/new_sop/01_appointment/01_appointment_management.html"
    template_td_shipment = "post_port/new_sop/02_shipment/02_td_shipment.html"
    template_td_unshipment = "post_port/new_sop/02_1_shipment/unscheduled_section.html"
    template_fleet_schedule = "post_port/new_sop/03_fleet_schedule/03_fleet_schedule.html"
    template_unscheduled_pos_all = "post_port/new_sop/01_unscheduled_pos_all/01_unscheduled_main.html"
    template_ltl_pos_all = "post_port/new_sop/05_ltl_pos_all/05_ltl_main.html"
    template_ltl_history_pos = "post_port/new_sop/06_ltl_history_pos/06_ltl_main.html"
    template_history_shipment = "post_port/new_sop/04_history_shipment/04_history_shipment_main.html"
    template_bol = "export_file/bol_base_template.html"
    template_bol_pickup = "export_file/bol_template.html"
    template_la_bol_pickup = "export_file/LA_bol_template.html"
    template_ltl_label = "export_file/ltl_label.html"
    template_ltl_bol = "export_file/ltl_bol.html"
    template_ltl_bol_multi = "export_file/ltl_bol_multi.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX", "LA": "LA"}
    warehouse_options = {"":"", "NJ-07001": "NJ-07001", "SAV-31326": "SAV-31326", "LA-91761": "LA-91761", "LA-91748": "LA-91748", "LA-91766": "LA-91766",}
    load_type_options = {
        "卡板": "卡板",
        "地板": "地板",
    }
    account_options = {
        "": "",
        "Carrier Central1": "Carrier Central1",
        "Carrier Central2": "Carrier Central2",
        "ZEM-AMF": "ZEM-AMF",
        "ARM-AMF": "ARM-AMF",
        "walmart": "walmart",
    }
    arm_account_options = {
        "": "",
        "Carrier Central ADWG": "Carrier Central ADWG",
        "Carrier Central1": "Carrier Central1",
        "Carrier Central2": "Carrier Central2",
        "ZEM-AMF": "ZEM-AMF",
        "ARM-AMF": "ARM-AMF",
        "walmart": "walmart",
    }
    abnormal_fleet_options = {
        "": "",
        "司机未按时提货": "司机未按时提货",
        "送仓被拒收": "送仓被拒收",
        "未送达": "未送达",
        "其它": "其它",
    }
    carrier_options = {
        "": "",
        "Arm-AMF": "Arm-AMF",
        "Zem-AMF": "Zem-AMF",
        "ASH": "ASH",
        "Arm": "Arm",
        "ZEM": "ZEM",
        "LiFeng": "LiFeng",
        "LLC": "LLC",
    }
    shipment_type_options = {
        "FTL": "FTL",
        "外配": "外配",
        # "LTL": "LTL",     
        # "快递": "快递",
        # "客户自提": "客户自提",
    }
    RE_PUBLIC_WH = re.compile(
        r"^[A-Z]{4}$|"               # 4位纯字母
        r"^[A-Z]{3}\d{1,2}$|"         # 3字母 + 1或2位数字 (最常见)
        r"^[A-Z]{3}\d[A-Z]$|"         # 3字母 + 1数字 + 1字母
        r"^[A-Z]{2}\d{2}$"            # 2字母 + 2数字
    )
    PUBLIC_KEYWORDS = {"WALMART", "沃尔玛", "AMAZON", "亚马逊"}
    
    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "appointment_management":
            template, context = await self.handle_appointment_management_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "schedule_shipment":
            template, context = await self.handle_td_shipment_get(request)
            return render(request, template, context)
        elif step == "schedule_unshipment":
            template, context = await self.handle_td_unshipment_get(request)
            return render(request, template, context)
        elif step == "fleet_management":
            template, context = await self.handle_fleet_management_get(request)
            return render(request, template, context)
        elif step == "unscheduled_pos_all":
            template, context = await self.handle_unscheduled_pos_all_get(request)
            return render(request, template, context)
        elif step == "LTL_pallets":
            context = {
                "warehouse_options": self.warehouse_options
            }
            return render(request, self.template_ltl_pos_all, context)
        elif step == "LTL_history_po":
            context = {
                "warehouse_options": self.warehouse_options
            }
            return render(request, self.template_ltl_history_pos, context)
        elif step == "history_shipment":
            context = {"warehouse_options": self.warehouse_options}
            return render(request, self.template_history_shipment, context)
        else:
            raise ValueError('输入错误')

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        print('step',step)
        if step == "appointment_management_warehouse":
            template, context = await self.handle_appointment_management_post(request)
            return render(request, template, context)
        elif step == "unscheduled_pos_warehouse":
            template, context = await self.handle_unscheduled_pos_post(request)
            return render(request, template, context)
        elif step == "ltl_warehouse":
            template, context = await self.handle_ltl_unscheduled_pos_post(request)
            return render(request, template, context)
        elif step == "ltl_history_warehouse":
            template, context = await self.handle_ltl_history_pos_post(request)
            return render(request, template, context)
        elif step == "td_shipment_warehouse":
            template, context = await self.handle_td_shipment_post(request)
            return render(request, template, context)
        elif step == "td_unshipment_warehouse":
            template, context = await self.handle_td_unshipment_post(request)
            return render(request, template, context)
        elif step == "fleet_schedule_warehouse":
            template, context = await self.handle_fleet_schedule_post(request)
            return render(request, template, context)
        elif step == "history_shipment_warehouse":
            template, context = await self.handle_history_shipment_post(request)
            return render(request, template, context)
        elif step == "export_pos":
            return await self.handle_export_pos(request)
        elif step == "appointment_time_modify":
            template, context = await self.handle_appointment_time(request)
            return render(request, template, context)
        elif step == "update_fleet":
            fm = FleetManagement()
            context = await fm.handle_update_fleet_post(request,'post_nsop')
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request)
            else:
                template, context = await self.handle_td_shipment_post(request)
            context.update({"success_messages": "更新出库车次成功!"}) 
            return render(request, template, context)
        elif step == "fleet_confirmation":
            template, context = await self.handle_fleet_confirmation_post(request)
            return render(request, template, context) 
        elif step == "cancel_fleet":
            fm = FleetManagement()
            context = await fm.handle_cancel_fleet_post(request,'post_nsop')
            
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request,context)
            elif page == "ltl_readyShip":
                template, context = await self.handle_ltl_unscheduled_pos_post(request)
            else:
                template, context = await self.handle_td_shipment_post(request)
            context.update({"success_messages": '取消批次成功!'})  
            return render(request, template, context)
        elif step == "confirm_delivery":
            fm = FleetManagement()
            context = await fm.handle_confirm_delivery_post(request,'post_nsop')
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request)
            elif page == "ltl_delivery_section":
                template, context = await self.handle_ltl_unscheduled_pos_post(request)
            else:
                template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '确认送达成功!'})  
            return render(request, template, context)
        elif step == "abnormal_fleet":
            fm = FleetManagement()
            page = request.POST.get("page")
            context = await fm.handle_abnormal_fleet_post(request,'post_nsop')
            
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request)
            elif page == "ltl_pos_all":
                # 把约直接取消
                sm = ShippingManagement()
                info = await sm.handle_cancel_abnormal_appointment_post(request,'post_nsop')     
                template, context = await self.handle_ltl_unscheduled_pos_post(request)
            else:
                template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '异常处理成功!'})  
            return render(request, template, context)
        elif step == "pod_upload":
            fm = FleetManagement()
            context = await fm.handle_pod_upload_post(request,'post_nsop')
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request)
            elif page == "ltl_pod_section":
                template, context = await self.handle_ltl_unscheduled_pos_post(request)
            else:
                template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": 'POD上传成功!'})           
            return render(request, template, context)
        elif step == "get_maersk_quote":
            return await self.handle_get_maersk_quote(request)
        elif step == 'maersk_schedule_post':
            return await self.handle_maersk_schedule_post(request)
        elif step == "bind_group_shipment":
            template, context = await self.handle_appointment_post(request)
            return render(request, template, context) 
        elif step =="ltl_bind_group_shipment":
            template, context = await self.handle_ltl_bind_group_shipment(request)
            return render(request, template, context) 
        elif step == "unassign_shipment":
            template, context = await self.handle_cancel_appointment_post(request)
            return render(request, template, context) 
        elif step == "one_fleet_departure":
            template, context = await self.handle_one_fleet_departure_post(request)
            return render(request, template, context)
        elif step == "add_pallet":
            template, context = await self.handle_add_pallet_post(request)
            return render(request, template, context)      
        elif step == "fleet_add_pallet":
            template, context = await self.handle_fleet_add_pallet_post(request)
            return render(request, template, context)
        elif step == "sp_notified_customer":
            template, context = await self.handle_sp_notified_customer_post(request)
            return render(request, template, context)       
        elif step == "fl_notified_customer":
            template, context = await self.handle_fl_notified_customer_post(request)
            return render(request, template, context)
        elif step == "search_addable_po":
            template, context = await self.handle_search_addable_po_post(request)
            return render(request, template, context)
        elif step == "shipment_add_pallet":
            template, context = await self.handle_shipment_add_pallet_post(request)
            return render(request, template, context)
        elif step == "modify_intelligent_po":
            template, context = await self.handle_modify_intelligent_po_post(request)
            return render(request, template, context)
        elif step == "upload_check_po":
            po_cl = PO()
            request.POST = request.POST.copy()
            request.POST['time_code'] = 'eta' 
            info = await po_cl.handle_upload_check_po_post(request,'post_nsop')
            context = {'success_messages':'校验结果上传成功！'}
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request,context)
            else:
                template, context = await self.handle_appointment_management_post(request,context)
            return render(request, template, context)    
        elif step == "create_empty_appointment":
            sm = ShippingManagement()
            info = await sm.handle_create_empty_appointment_post(request,'post_nsop') 
            context = {'success_messages':'备约登记成功！'}
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request,context)
            else:
                template, context = await self.handle_appointment_management_post(request,context)
            return render(request, template, context)   
        elif step == "download_empty_appointment_template":
            sm = ShippingManagement()
            return await sm.handle_download_empty_appointment_template_post()  
        elif step == "upload_and_create_empty_appointment":
            sm = ShippingManagement()
            info = await sm.handle_upload_and_create_empty_appointment_post(request,'post_nsop') 
            context = {'success_messages':'备约批量登记成功！'}
            page = request.POST.get("page")
            if page == "arm_appointment":
                template, context = await self.handle_unscheduled_pos_post(request,context)
            else:
                template, context = await self.handle_appointment_management_post(request,context)
            return render(request, template, context)   
        elif step == "edit_appointment":
            template, context = await self.handle_edit_appointment_post(request)
            return render(request, template, context) 
        elif step == "edit_note_sp":
            template, context = await self.handle_edit_note_sp_post(request)
            return render(request, template, context) 
        elif step == "export_virtual_fleet_pos":
            return await self.handle_export_virtual_fleet_pos_post(request)
        elif step == "multi_group_booking":
            template, context = await self.handle_multi_group_booking(request)
            return render(request, template, context) 
        elif step == "update_fleet_info":
            template, context = await self.handle_update_fleet_info(request)
            return render(request, template, context)      
        elif step == "fix_shipment_exceptions":
            solution = request.POST.get("solution")
            if solution != "keep_old":
                template, context = await self.handle_edit_appointment_post(request,"fleet_departure")
                return render(request, template, context) 
            sm = ShippingManagement()
            info = await sm.handle_fix_shipment_exceptions_post(request,'post_nsop')  
            shipment_batch_number = request.POST.get("shipment_batch_number")
            
            if solution == "keep_old":
                context = {"success_messages": f"{shipment_batch_number}已改为正常状态！"}
            else:
                context = {"success_messages": f"{shipment_batch_number}约已修改，可以正常使用！"}
            template, context = await self.handle_fleet_schedule_post(request,context)
            return render(request, template, context) 
        elif step == "cancel_abnormal_appointment":
            shipment_batch_number = request.POST.get("batch_number")
            sm = ShippingManagement()
            info = await sm.handle_cancel_abnormal_appointment_post(request,'post_nsop')     
            context = {"success_messages": f"{shipment_batch_number}约已取消不可用，所有po已解绑！"}
            template, context = await self.handle_fleet_schedule_post(request,context)
            return render(request, template, context) 
        elif step == "export_bol":
            return await self.handle_bol_post(request)
        elif step =="export_bol_fleet":
            return await self.handle_bol_fleet_post(request)
        elif step == "verify_ltl_cargo":
            template, context = await self.handle_verify_ltl_cargo(request)
            return render(request, template, context)  
        elif step == "export_ltl_unscheduled":
            return await self.export_ltl_unscheduled(request)
        elif step == "save_fleet_cost":
            template, context = await self.handle_save_fleet_cost(request)
            return render(request, template, context)  
        elif step =="save_selfpick_cargo":
            template, context = await self.handle_save_selfpick_cargo(request)
            return render(request, template, context) 
        elif step == "save_selfdel_cargo":
            template, context = await self.handle_save_selfdel_cargo(request)
            return render(request, template, context) 
        elif step == "export_ltl_label":
            return await self.export_ltl_label(request)
        elif step == "export_ltl_bol":
            return await self.export_ltl_bol(request)
        elif step == "upload_self_pickup_file":
            return await self.handle_bol_upload_post(request)
        elif step == "save_shipping_tracking":
            template, context = await self.handle_save_shipping_tracking(request)
            return render(request, template, context) 
        elif step == "update_pod_status":
            template, context = await self.handle_update_pod_status(request)
            return render(request, template, context) 
        elif step == "save_releaseCommand":
            template, context = await self.handle_save_releaseCommand(request)
            return render(request, template, context) 
        elif step == "query_quotation":
            template, context = await self.handle_query_quotation(request)
            return render(request, template, context) 
        elif step == "po_invalid_save":
            template, context = await self.handle_po_invalid_save(request)
            return render(request, template, context) 
        elif step == "batch_update_delivery_method":
            template, context = await self.handle_batch_update_delivery_method(request)
            return render(request, template, context)
        elif step == "batch_one_pick_multi_drop":
            template, context = await self.handle_batch_one_pick_multi_drop(request)
            return render(request, template, context)
        else:
            raise ValueError('输入错误',step)
    
    async def handle_batch_update_delivery_method(self, request: HttpRequest):
        '''批量保存派送方式'''
        context = {}
        batch_data_json = request.POST.get('batch_methods')
        try:
            batch_data = json.loads(batch_data_json)

            for entry in batch_data:
                raw_id = entry.get('cargo_id', '')
                new_method = entry.get('delivery_method', '')

                if not raw_id:
                    continue

                if raw_id.startswith('plt_'):
                    # --- 处理打板数据 (Pallet) ---
                    # 移除前缀并分割成 ID 列表
                    pallet_ids = raw_id.replace('plt_', '').split(',')
                    # 批量更新对应的托盘
                    await Pallet.objects.filter(id__in=pallet_ids).aupdate(delivery_method=new_method)
                else:
                    # --- 处理未打板数据 (PackingList) ---
                    # 通常是普通货物的 ID 列表
                    cargo_ids = raw_id.split(',')
                    await PackingList.objects.filter(id__in=cargo_ids).aupdate(delivery_method=new_method)
            context.update({'success_messages':f"成功批量修改 {len(batch_data)} 项派送方式"})
        
        except Exception as e:
            context.update({'error_messages':f"修改 {len(batch_data)} 项派送方式失败，原因是{e}"})

        # 重定向回原页面并保持当前的仓库和标签页
        return await self.handle_ltl_unscheduled_pos_post(request, context)

    async def handle_batch_one_pick_multi_drop(self, request: HttpRequest):
        '''LTL 一提多卸标记'''
        context = {}
        batch_json = request.POST.get('batch_correlation_data', '[]')
        try:
            correlation_list = json.loads(batch_json)
        except (json.JSONDecodeError, TypeError):
            correlation_list = []

        if correlation_list:
            # 1. 生成 Seed: 取所有选中项唛头的前5位和后5位拼接
            # 例如: ZSDMI5120800011 -> ZSDMI0011
            seed_parts = []
            for item in correlation_list:
                mark = str(item.get('shipping_mark', '')).strip()
                if len(mark) > 10:
                    seed_parts.append(f"{mark[:5]}{mark[-5:]}")
                else:
                    seed_parts.append(mark)
            
            seed_str = "|".join(seed_parts)
            
            # 2. 通过 Seed 生成唯一的 UUID (uuid5 基于命名空间和字符串生成)
            # 这样如果用户在不同时间点选了同一组唛头，生成的关联ID理论上是一致的
            correlation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, seed_str))

            # 3. 异步循环更新
            for entry in correlation_list:
                raw_id = entry.get('cargo_id', '')
                if not raw_id:
                    continue

                if raw_id.startswith('plt_'):
                    # --- 处理打板数据 (Pallet) ---
                    pallet_ids = raw_id.replace('plt_', '').split(',')

                    existing_records = await sync_to_async(list)(
                        Pallet.objects.filter(id__in=pallet_ids).values('id', 'ltl_correlation_id')
                    )
                    if not existing_records:
                        continue
                        
                    # 检查是否需要取消关联（如果所有选中的记录都有ltl_correlation_id值）
                    has_correlation = any(record['ltl_correlation_id'] for record in existing_records)
                    
                    # 只取消已有关联的记录
                    if has_correlation:
                        # 找出有值的记录ID
                        await Pallet.objects.filter(id__in=pallet_ids).aupdate(ltl_correlation_id='')
                        context.update({'success_messages': f"成功取消一提多卸关联"})
                    else:
                        # 使用异步 aupdate 批量更新
                        await Pallet.objects.filter(id__in=pallet_ids).aupdate(ltl_correlation_id=correlation_id)
                        context.update({'success_messages':f"成功关联一提多卸"})
                else:
                    # --- 处理未打板数据 (PackingList) ---
                    cargo_ids = raw_id.split(',')

                    existing_records = await sync_to_async(list)(
                        PackingList.objects.filter(id__in=cargo_ids).values('id', 'ltl_correlation_id')
                    )
                    if not existing_records:
                        continue
                    has_correlation = any(record['ltl_correlation_id'] for record in existing_records)
                    
                    # 如果原本有值，则设置为空（取消操作）
                    if has_correlation:
                        await PackingList.objects.filter(id__in=cargo_ids).aupdate(ltl_correlation_id='')
                        context.update({'success_messages': f"成功取消一提多卸关联"})
                    else:
                        # 使用异步 aupdate 批量更新
                        await PackingList.objects.filter(id__in=cargo_ids).aupdate(ltl_correlation_id=correlation_id)
                        context.update({'success_messages':f"成功关联一提多卸"})
        return await self.handle_ltl_unscheduled_pos_post(request, context)
    
    async def handle_maersk_schedule_post(self, request: HttpRequest) -> JsonResponse:
        """处理Maersk预约下单"""
        try:
            from warehouse.utils.config import app_config
            
            # 1. 提取参数
            cargo_ids = request.POST.get('cargo_ids')
            plt_ids = request.POST.get('plt_ids')
            quote_id = request.POST.get('quote_id')
            service_code = request.POST.get('service_code')
            consignee_json = request.POST.get('consignee_json')
            line_items_json = request.POST.get('line_items_json')
            schedule_json = request.POST.get('schedule_json')
            warehouse = request.session.get('warehouse') or request.POST.get('warehouse')

            if not all([quote_id, service_code, consignee_json, line_items_json, schedule_json]):
                return JsonResponse({'success': False, 'message': '必要参数缺失'}, status=400)

            consignee = json.loads(consignee_json)
            line_items_raw = json.loads(line_items_json)
            schedule = json.loads(schedule_json)

            # 2. 构造 Shipper 信息 (符合 ShipmentAddress 定义)
            shipper_config = app_config.WAREHOUSE_ADDRESS.get(warehouse, {})
            if not shipper_config:
                 # 默认使用 LA-91761 如果找不到
                 shipper_config = app_config.WAREHOUSE_ADDRESS.get('LA-91761', {})
            
            shipper_address = {
                "name": shipper_config.get('name'),
                "address1": shipper_config.get('address1'),
                "city": shipper_config.get('city'),
                "regionCode": shipper_config.get('regionCode'),
                "postalCode": shipper_config.get('postalCode'),
                "countryCode": shipper_config.get('countryCode', 'US'),
                "phone": "909-320-8774" # 默认或从配置获取
            }

            # 3. 构造 LineItems (格式化)
            line_items = []
            for item in line_items_raw:
                line_items.append({
                    "description": item.get('description') or 'Pallet',
                    "pieces": int(item.get('pieces') or item.get('Pieces') or 1),
                    "length": int(item.get('length') or item.get('Length') or 0),
                    "width": int(item.get('width') or item.get('Width') or 0),
                    "height": int(item.get('height') or item.get('Height') or 0),
                    "weight": int(float(item.get('weight') or item.get('Weight') or 0)),
                    # 补充必填字段，如果有默认值的话
                    "packaging": "Pallet", # 假设默认为 Pallet
                    "weightUnit": "lb",
                    "dimensionalUnit": "in"
                })

            # 4. 调用 Maersk 下单 API
            # 构造 Consignee Address (符合 ShipmentAddress 定义)
            consignee_address = {
                "name": consignee.get('company') or consignee.get('name'), # 公司名为 name
                "contact": consignee.get('name'), # 联系人为 contact
                "address1": consignee.get('address1'),
                "city": consignee.get('city'),
                "regionCode": consignee.get('state'),
                "postalCode": consignee.get('zipcode'),
                "countryCode": "US",
                "phone": consignee.get('phone'),
                "email": consignee.get('email')
            }
            
            # 处理 Consignee References (目的地处理)
            warehouse_type = request.POST.get('warehouse_type', 'public')
            destination = schedule.get('destination', '')
            destination_name = destination
            
            # 生成 batch_number 用于作为 references
            batch_number_ref = ""
            if warehouse_type == 'private' and len(destination) > 8:
                destination_name = destination[:8]
            else:
                destination_name = destination
                
            batch_number_ref = await self.generate_unique_batch_number(destination_name)
            if not batch_number_ref:
                return JsonResponse({'success': False, 'message': '批次号生成失败'}, status=400)

            payload = {
                "quoteId": int(quote_id),
                "serviceCode": service_code,
                "shipper": {
                    "address": shipper_address,
                    "references": [schedule.get('bol')] if schedule.get('bol') else []
                },
                "consignee": {
                    "address": consignee_address,
                    "references": [batch_number_ref]
                },
                "lineItems": line_items,
                "shipDate": schedule.get('pickup_time'), # 必填
                "shipReadyTime": "09:00:00", # 默认值
                "shipCloseTime": "17:00:00", # 默认值
                "accessorials": [] 
            }

            api_url = "https://zem-maersk-gateway.kindmoss-a5050a64.eastus.azurecontainerapps.io/shipment"
            #api_key = '2Tdtqrj4dqnooXIJi4ReCVrMGW3ehJnC'
            api_key = os.environ.get("MAERSK_API_KEY")
            headers = {
                "Content-Type": "application/json",
                "x-api-key": api_key
            }

            pro_number = None
            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        res_data = await response.json()
                        pro_number = res_data.get('ProNumber')
                    else:
                        text = await response.text()
                        return JsonResponse({'success': False, 'message': f'Maersk API下单失败: {response.status} - {text}'}, status=response.status)

            if not pro_number:
                return JsonResponse({'success': False, 'message': '下单成功但未获取到PRO号'}, status=500)

            # 5. 根据仓库类型分发处理
            warehouse_type = request.POST.get('warehouse_type', 'public')
            
            if warehouse_type == 'private':  
                # --- 私仓预约出库逻辑 (调用 handle_ltl_bind_group_shipment) ---
                post_data = request.POST.copy()
                post_data['destination'] = schedule.get('destination')
                post_data['address'] = f"{consignee.get('address1')}, {consignee.get('city')}, {consignee.get('state')} {consignee.get('zipcode')}"
                post_data['carrier'] = 'Maersk'
                post_data['shipment_appointment'] = schedule.get('pickup_time')
                post_data['arm_bol'] = schedule.get('bol', '')
                post_data['arm_pro'] = pro_number
                post_data['shipment_type'] = schedule.get('shipment_type', 'LTL')
                # 私仓 auto_fleet 接收 'true'/'false' 字符串
                post_data['auto_fleet'] = 'true' if schedule.get('auto_schedule') == '是' else 'false'
                post_data['fleet_cost'] = schedule.get('cost', 0)
                # 传递已生成的 batch_number 
                post_data['maersk_batch_number'] = batch_number_ref
                # 传递备注
                post_data['note'] = schedule.get('note', '')
                
                # 确保 cargo_ids 和 plt_ids 存在 (已在 request.POST 中)
                
                request.POST = post_data
                
                # 调用私仓绑定逻辑
                template_name, context = await self.handle_ltl_bind_group_shipment(request)
                
                if 'error_messages' in context:
                    return JsonResponse({'success': False, 'message': f"私仓预约失败: {context['error_messages']}"}, status=400)
                
                return JsonResponse({
                    'success': True, 
                    'pro_number': pro_number, 
                    'batch_number': '', # 私仓逻辑暂不返回 batch_number 给前端弹窗显示
                    'message': '下单及私仓预约成功'
                })
                
            else:
                # --- 公仓逻辑 (调用 handle_appointment_post) ---
                # 构造 request.POST
                # 生成唯一的 appointment_id
                appointment_id = f"M-{pro_number}"
                
                # 准备参数
                post_data = request.POST.copy()
                post_data['shipment_type'] = schedule.get('shipment_type', 'LTL')
                post_data['appointment_id'] = appointment_id
                post_data['destination'] = schedule.get('destination')
                post_data['address'] = f"{consignee.get('address1')}, {consignee.get('city')}, {consignee.get('state')} {consignee.get('zipcode')}"
                post_data['pickup_time'] = schedule.get('pickup_time') # 提货时间
                post_data['shipment_appointment'] = schedule.get('pickup_time') # 也是预约时间
                post_data['note'] = schedule.get('note', '')
                post_data['maersk_batch_number'] = batch_number_ref
                # cargo_ids 和 plt_ids 已经存在于 post_data 中
                
                # 替换 request.POST
                request.POST = post_data
                
                # 调用 handle_appointment_post
                template_name, context = await self.handle_appointment_post(request)
                
                if 'error_messages' in context:
                    return JsonResponse({'success': False, 'message': f"预约保存失败: {context['error_messages']}"}, status=400)
                
                success_msg = context.get('success_messages', '')
                # 提取生成的 batch_number (通常在 success_messages 中 "绑定成功，批次号是XXX")
                batch_number = ''
                if '批次号是' in success_msg:
                    batch_number = success_msg.split('批次号是')[1].split(',')[0].strip()

                # 6. 更新 PRO 号到 Pallet
                # 根据 batch_number 或 appointment_id 查找 Shipment
                try:
                    shipment = await sync_to_async(Shipment.objects.get)(appointment_id=appointment_id)
                    
                    # 更新 Pallet 的 ltl_pro_num
                    # 注意：Pallet 关联的是 shipment_batch_number (Shipment 对象)
                    await sync_to_async(Pallet.objects.filter(shipment_batch_number=shipment).update)(
                        ltl_pro_num=pro_number
                    )
                    
                    # 7. 自动排车 (如果需要)
                    if schedule.get('auto_schedule') == '是':
                        # 调用 _add_appointments_to_fleet
                        fleet_number = await self._add_appointments_to_fleet([appointment_id])
                        
                        # 更新 Fleet 信息 (比如 carrier, pro)
                        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
                        fleet.carrier = 'Maersk'
                        await sync_to_async(fleet.save)()
                        
                except Exception as e:
                    # 记录警告但不中断流程，因为下单已成功
                    print(f"更新PRO或排车失败: {e}")

                return JsonResponse({
                    'success': True, 
                    'pro_number': pro_number, 
                    'batch_number': batch_number,
                    'message': '下单及预约成功'
                })

        except Exception as e:
            import traceback
            traceback.print_exc()
            return JsonResponse({'success': False, 'message': f'系统异常: {str(e)}'}, status=500)

    async def handle_get_maersk_quote(self, request: HttpRequest) -> JsonResponse:
        """调用Maersk API获取报价"""
        try:
            origin_zip = request.POST.get('origin_zip')
            dest_zip = request.POST.get('dest_zip')
            ship_date = request.POST.get('ship_date')
            
            # 新增参数：结构化的货物明细
            line_items_json = request.POST.get('line_items_json')
            
            # 旧参数（保留兼容）
            cargo_details = request.POST.get('cargo_details')
            total_weight = request.POST.get('total_weight')

            if not all([origin_zip, dest_zip, ship_date]):
                return JsonResponse({'success': False, 'message': '基础参数不完整'}, status=400)

            line_items = []
            
            if line_items_json:
                try:
                    parsed_items = json.loads(line_items_json)
                    for item in parsed_items:
                        line_items.append({
                            "description": item.get('description') or 'Pallet',
                            "pieces": int(item.get('pieces', 1)),
                            "length": int(item.get('length', 0)),
                            "width": int(item.get('width', 0)),
                            "height": int(item.get('height', 0)),
                            "weight": int(float(item.get('weight', 0)))
                        })
                except json.JSONDecodeError:
                    return JsonResponse({'success': False, 'message': '货物明细数据格式错误'}, status=400)
            elif cargo_details:
                # 解析货物详情 (旧逻辑)
                # 格式: 32*35*60*3板 或 32*35*60
                
                # 如果有多个尺寸行，按换行符分割
                detail_lines = [line.strip() for line in cargo_details.split('\n') if line.strip()]
                
                # 计算总板数用于分摊重量
                total_pallets = 0
                parsed_details = []
                
                for line in detail_lines:
                    # 匹配 L*W*H*Count 或 L*W*H
                    match = re.match(r'(\d+)\*(\d+)\*(\d+)(?:\*(\d+))?', line)
                    if match:
                        l, w, h, count = match.groups()
                        count = int(count) if count else 1
                        total_pallets += count
                        parsed_details.append({'l': int(l), 'w': int(w), 'h': int(h), 'count': count})
                
                if total_pallets == 0:
                    return JsonResponse({'success': False, 'message': '无法解析托盘尺寸信息'}, status=400)

                # 分摊重量
                try:
                    weight_val = float(total_weight) if total_weight else 0
                except ValueError:
                    weight_val = 0
                    
                weight_per_pallet = int(weight_val / total_pallets) if total_pallets > 0 else 0
                
                for item in parsed_details:
                    # 对于每一种尺寸，生成对应数量的LineItem
                    for _ in range(item['count']):
                        line_items.append({
                            "description": "Pallet",
                            "pieces": 1,
                            "length": item['l'],
                            "width": item['w'],
                            "height": item['h'],
                            "weight": weight_per_pallet
                        })
            else:
                 return JsonResponse({'success': False, 'message': '缺少货物明细'}, status=400)

            payload = {
                "shipDate": ship_date,
                "origin_zip": origin_zip,
                "dest_zip": dest_zip,
                "lineItems": line_items,
                "declaredValue": None,
                "insuranceValue": None,
                "debrisRemoval": None
            }

            api_url = "https://zem-maersk-gateway.kindmoss-a5050a64.eastus.azurecontainerapps.io/rating"
            api_key = os.environ.get("MAERSK_API_KEY")
            #api_key = '2Tdtqrj4dqnooXIJi4ReCVrMGW3ehJnC'
            
            if not api_key:
                 return JsonResponse({'success': False, 'message': '未配置API Key'}, status=500)

            headers = {
                "Content-Type": "application/json",
                "x-api-key": api_key
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        # 将请求参数中的 lineItems 注入到响应数据中，以便前端使用
                        data['lineItems'] = line_items
                        return JsonResponse({'success': True, 'data': data})
                    else:
                        text = await response.text()
                        return JsonResponse({'success': False, 'message': f'API调用失败: {response.status} - {text}'}, status=response.status)

        except Exception as e:
            return JsonResponse({'success': False, 'message': str(e)}, status=500)

    async def handle_bol_upload_post(self, request: HttpRequest) -> HttpResponse:
        '''客户自提的BOL文件下载'''
        fleet_number = request.POST.get("fleet_number")
        customerInfo = request.POST.get("arm_pickup_data")
        notes = ""
        pickup_number = ""
        shipment_batch_number = ""

        # 如果在界面输入了，就用界面添加后的值
        if customerInfo and customerInfo != "[]":
            customer_info = json.loads(customerInfo)
            arm_pickup = [
                [
                    "container",
                    "destination",
                    "mark",
                    "pallet",
                    "pcs",
                    "carrier",
                    "pickup",
                ]
            ]
            for row in customer_info:
                # 把提货时间修改格式
                pickup_time = row.get('appointment_datetime', '').strip()
                s_time = pickup_time.split("T")[0]
                dt = datetime.strptime(s_time, "%Y-%m-%d")
                new_string = dt.strftime("%m-%d")

                destination_raw = row.get('zipcode', '').strip()
                destination = re.sub(r"[\u4e00-\u9fff]", " ", destination_raw)
                if not shipment_batch_number:
                    shipment_batch_number = (
                        row.get("shipment_batch_number__shipment_batch_number")
                        or row.get("shipment_batch_number")
                        or ""
                    ).strip()
                arm_pickup.append(
                    [
                        row.get('container_number', '').strip(),
                        destination,
                        row.get('shipping_mark', '').strip(),
                        row.get('total_pallet', '').strip(),
                        row.get('total_pcs', '').strip(),
                        row.get('carrier', '').strip(),
                        s_time,
                    ]
                )

        else:  # 没有就从数据库查
            arm_pickup = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number__container_number",
                    "shipment_batch_number__fleet_number",
                )
                .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
                .values(
                    "container_number__container_number",
                    "destination",
                    "shipping_mark",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_type",
                    "shipment_batch_number__fleet_number__carrier",
                    "shipment_batch_number__fleet_number__appointment_datetime",
                    "shipment_batch_number__fleet_number__pickup_number",  # 提取pickup_number
                    "shipment_batch_number__note",
                )
                .annotate(
                    total_pcs=Count("pcs", distinct=True),
                    total_pallet=Count("pallet_id", distinct=True),
                )
            )
            if arm_pickup:
                new_list = []
                for p in arm_pickup:
                    # 保存pickup_number（从数据库提取）
                    pickup_number = p["shipment_batch_number__fleet_number__pickup_number"] or ""
                    if not shipment_batch_number:
                        shipment_batch_number = (
                            p.get("shipment_batch_number__shipment_batch_number") or ""
                        )
                    p_time = p["shipment_batch_number__fleet_number__appointment_datetime"]

                    # 提取年、月、日
                    year = p_time.year
                    month = p_time.month
                    day = p_time.day
                    p_time = f"{year}-{month}-{day}"
                    destination = re.sub(r"[\u4e00-\u9fff]", " ", p["destination"])
                    new_list.append(
                        [
                            p["container_number__container_number"],
                            destination,
                            p["shipping_mark"],
                            p["total_pallet"],
                            p["total_pcs"],
                            p["shipment_batch_number__fleet_number__carrier"],
                            p_time,
                        ]
                    )
                    notes += p["shipment_batch_number__note"] or ""  # 拼接备注
                arm_pickup = [
                                 [
                                     "container",
                                     "destination",
                                     "mark",
                                     "pallet",
                                     "pcs",
                                     "carrier",
                                     "pickup",
                                 ]
                             ] + new_list
            else:
                raise ValueError("柜子未拆柜，请核实")
            s_time = arm_pickup[1][-1]
            dt = datetime.strptime(s_time, "%Y-%m-%d")
            new_string = dt.strftime("%m-%d")

        # BOL需要在后面加一个拣货单
        df = pd.DataFrame(arm_pickup[1:], columns=arm_pickup[0])

        # 添加换行函数
        def wrap_text(text, max_length=11):
            """将文本按最大长度换行"""
            if not isinstance(text, str):
                text = str(text)

            if len(text) <= max_length:
                return text

            # 按最大长度分割文本
            wrapped_lines = []
            for i in range(0, len(text), max_length):
                wrapped_lines.append(text[i:i + max_length])
            return '\n'.join(wrapped_lines)

        # 对DataFrame应用换行处理
        df_wrapped = df.applymap(wrap_text)

        files = request.FILES.getlist("files")
        if files:
            system_name = platform.system()
            zh_font_path = None

            # ✅ 按系统类型设置默认路径
            if system_name == "Windows":
                zh_font_path = "C:/Windows/Fonts/msyh.ttc"  # 微软雅黑
            else:  # Linux
                # Linux 通常用 Noto 或思源黑体字体
                possible_fonts = [
                    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
                    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                    "/usr/share/fonts/truetype/arphic/uming.ttc",  # 备用
                    "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",  # 文泉驿微米黑
                ]
                for path in possible_fonts:
                    if os.path.exists(path):
                        zh_font_path = path
                        break

            # ✅ 检查字体文件是否存在，否则退回默认英文字体
            if zh_font_path and os.path.exists(zh_font_path):
                zh_font = fm.FontProperties(fname=zh_font_path)
                plt.rcParams["font.family"] = zh_font.get_name()
            else:
                plt.rcParams["font.family"] = "DejaVu Sans"

            plt.rcParams["axes.unicode_minus"] = False  # 防止负号乱码

            for file in files:
                # 设置通用字体避免警告
                # plt.rcParams['font.family'] = ['sans-serif']
                # plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']

                # 保持原来的A4尺寸
                fig, ax = plt.subplots(figsize=(10.4, 8.5))
                #ax.axis("tight")
                ax.axis("off")
                # 稍微减小顶部边距，为标题留出一点空间
                fig.subplots_adjust(top=1.45)  # 从1.5微调到1.45

                # 在表格上方添加标题
                ax.text(
                    0.5,
                    0.93,
                    "Pickup List",
                    fontsize=14,
                    fontweight="bold",
                    ha="center",
                    va="top",
                    transform=ax.transAxes,
                )

                # 在标题下方添加Pickup Number
                ax.text(
                    0.5,
                    0.89,
                    f"Pickup Number: {pickup_number}",
                    fontsize=11,
                    ha="center",
                    va="top",
                    transform=ax.transAxes,
                )
                def get_line_count(text):
                    return str(text).count("\n") + 1

                row_line_counts = [
                    max(get_line_count(cell) for cell in row)
                    for row in df_wrapped.values
                ]
                max_line_count = max(row_line_counts) if row_line_counts else 1
                EXTRA_PADDING = 0.003 * max_line_count
                BASE_ROW_HEIGHT = 0.028
                HEADER_HEIGHT = 0.05
                
                # 数据行总高度
                data_height = sum(
                    BASE_ROW_HEIGHT * line_count + EXTRA_PADDING
                    for line_count in row_line_counts
                )

                # 表头高度
                total_table_height = HEADER_HEIGHT + data_height
                # 创建表格 - 保持原来的位置和设置
                TABLE_TOP_Y = 0.85  # 表格顶部固定在标题下方
                table_y = TABLE_TOP_Y - total_table_height

                the_table = ax.table(
                    cellText=df_wrapped.values,
                    colLabels=df_wrapped.columns,
                    cellLoc="center",
                    bbox=[0.1, table_y, 0.8, total_table_height],
                )

                

                # 设置表格样式 - 保持原来的设置，只增加行高
                for (row, col), cell in the_table.get_celld().items():
                    cell.set_fontsize(10)
                    cell.set_text_props(wrap=True)

                    if row == 0:
                        cell.set_height(HEADER_HEIGHT)
                    else:
                        line_count = row_line_counts[row - 1]
                        cell.set_height(
                            BASE_ROW_HEIGHT * line_count + EXTRA_PADDING
                        )

                    # 列宽保持你原来的逻辑
                    if col in (0, 1, 2):
                        cell.set_width(0.15)
                    elif col in (3, 4):
                        cell.set_width(0.06)
                    else:
                        cell.set_width(0.12)
                
                # ========= 8️⃣ 计算表格底部位置 =========
                renderer = fig.canvas.get_renderer()
                table_bbox = the_table.get_window_extent(renderer=renderer)
                table_bbox = table_bbox.transformed(ax.transAxes.inverted())
                table_bottom = table_bbox.y0
                
                # ========= 9️⃣ Notes =========
                notes_y = table_y - 0.04
                ax.text(
                    0.05,
                    notes_y,
                    f"Notes: {notes}",
                    fontsize=10,
                    ha="left",
                    va="top",
                    transform=ax.transAxes,
                )

                pickup_y = notes_y - 0.03
                ax.text(
                    0.05,
                    pickup_y,
                    f"pickup_number: {pickup_number}",
                    fontsize=10,
                    ha="left",
                    va="top",
                    transform=ax.transAxes,
                )

                # ========= 🔟 保存表格 PDF =========
                buf_table = io.BytesIO()
                fig.savefig(buf_table, format="pdf", bbox_inches="tight")
                plt.close(fig)
                buf_table.seek(0)

                # ========= 1️⃣1️⃣ 合并原 PDF =========
                merger = PdfMerger()
                merger.append(PdfReader(io.BytesIO(file.read())))
                merger.append(PdfReader(buf_table))

                output_buf = io.BytesIO()
                merger.write(output_buf)
                output_buf.seek(0)

                file_name = file.name

        response = HttpResponse(output_buf.getvalue(), content_type="application/octet-stream")
        def sanitize_filename_component(value: str) -> str:
            value_str = str(value or "").strip()
            value_str = re.sub(r"\s+", "", value_str)
            value_str = re.sub(r'[\\/:*?"<>|]+', "_", value_str)
            value_str = re.sub(r"\++", "+", value_str)
            value_str = re.sub(r"[^A-Za-z0-9._+\-]+", "_", value_str)
            return value_str

        container_no = ""
        destination_name = ""
        shipping_mark = ""
        if isinstance(arm_pickup, list) and len(arm_pickup) > 1 and isinstance(arm_pickup[1], list):
            container_no = arm_pickup[1][0] if len(arm_pickup[1]) > 0 else ""
            destination_name = arm_pickup[1][1] if len(arm_pickup[1]) > 1 else ""
            shipping_mark = arm_pickup[1][2] if len(arm_pickup[1]) > 2 else ""

        filename_parts = [
            sanitize_filename_component(new_string),
            sanitize_filename_component(container_no),
            sanitize_filename_component(destination_name),
            sanitize_filename_component(shipping_mark),
            "BOL",
            sanitize_filename_component(shipment_batch_number),
            "LTL",
        ]
        filename_parts = [p for p in filename_parts if p]
        output_filename = "+".join(filename_parts) + ".pdf"
        response["Content-Disposition"] = (
            f'attachment; filename="{output_filename}"'
        )
        response["X-Content-Type-Options"] = "nosniff"
        return response
    
    async def export_ltl_bol(self, request: HttpRequest) -> HttpResponse:
        '''导出LTL BOL'''
        fleet_number = request.POST.get("fleet_number")
        arm_pickup_data = request.POST.get("arm_pickup_data")
        warehouse = request.POST.get("warehouse")
        contact_flag = False  # 表示地址栏空出来，客服手动P上去
        contact = {}
        arm_pickup_groups = []

        def safe_int(value, default: int = 0) -> int:
            if value is None:
                return default
            if isinstance(value, int):
                return value
            value_str = str(value).strip()
            if value_str == "" or value_str.lower() == "none":
                return default
            try:
                return int(float(value_str))
            except Exception:
                return default

        def parse_contact_from_address(address: str) -> dict:
            address = re.sub("[\u4e00-\u9fff]", " ", address)
            address = re.sub(r"\uFF0C", ",", address)
            parts = [p.strip() for p in address.split(";")]
            return {
                "company": parts[0] if len(parts) > 0 else "",
                "Road": parts[1] if len(parts) > 1 else "",
                "city": parts[2] if len(parts) > 2 else "",
                "name": parts[3] if len(parts) > 3 else "",
                "phone": parts[4] if len(parts) > 4 else "",
            }

        def format_two_per_line(values: List[str]) -> str:
            cleaned = []
            seen = set()
            for v in values:
                v_str = str(v).strip()
                if not v_str or v_str.lower() == "none":
                    continue
                if v_str in seen:
                    continue
                seen.add(v_str)
                cleaned.append(escape(v_str))

            lines = []
            for i in range(0, len(cleaned), 2):
                lines.append(", ".join(cleaned[i : i + 2]))
            return mark_safe("<br>".join(lines)) if lines else ""

        def extract_pickup_attachments(rows: List[dict]) -> List[dict]:
            attachments = []
            seen = set()
            for r in rows:
                image_value = r.get("pickup_image", "")
                if isinstance(image_value, str):
                    image_value_str = image_value.strip()
                    if (
                        image_value_str
                        and image_value_str.startswith("data:image/")
                        and ";base64," in image_value_str
                        and len(image_value_str) <= 3_000_000
                        and image_value_str not in seen
                    ):
                        seen.add(image_value_str)
                        attachments.append({"kind": "image", "src": image_value_str})

                file_value = r.get("pickup_file_content", "")
                if isinstance(file_value, str):
                    file_value_str = file_value.strip()
                    if file_value_str:
                        if file_value_str.startswith("data:application/pdf;base64,"):
                            base64_part = file_value_str.split(",", 1)[1] if "," in file_value_str else ""
                            if base64_part and len(base64_part) <= 4_500_000:
                                try:
                                    pdf_bytes = base64.b64decode(base64_part, validate=False)
                                    reader = PdfReader(io.BytesIO(pdf_bytes))
                                    extracted = []
                                    for page in reader.pages:
                                        page_text = page.extract_text() or ""
                                        if page_text:
                                            extracted.append(page_text)
                                    extracted_text = "\n\n".join(extracted).strip()
                                    if extracted_text:
                                        file_value_str = extracted_text
                                except Exception:
                                    file_value_str = "[PDF parse failed]"
                        if len(file_value_str) > 30_000:
                            file_value_str = file_value_str[:30_000] + "\n...[truncated]"
                        safe_html = escape(file_value_str).replace("\r\n", "\n").replace(
                            "\n", "<br>"
                        )
                        dedupe_key = f"FILE::{safe_html}"
                        if dedupe_key in seen:
                            continue
                        seen.add(dedupe_key)
                        attachments.append(
                            {
                                "kind": "file",
                                "html": mark_safe(safe_html),
                            }
                        )
            return attachments

        if arm_pickup_data and arm_pickup_data != "[]":
            loaded = json.loads(arm_pickup_data)
            raw_groups = []
            if isinstance(loaded, dict) and isinstance(loaded.get("data"), list):
                raw_groups = [loaded.get("data", [])]
            elif isinstance(loaded, list):
                if loaded and all(isinstance(x, list) for x in loaded):
                    raw_groups = loaded
                elif loaded and all(isinstance(x, dict) for x in loaded) and any(
                    isinstance(x.get("data"), list) for x in loaded
                ):
                    raw_groups = [
                        x.get("data", []) if isinstance(x, dict) else [] for x in loaded
                    ]
                else:
                    raw_groups = [loaded]

            arm_pickup = []
            for group in raw_groups:
                if not isinstance(group, list):
                    continue
                group_contact_flag = False
                group_contact = {}
                group_rows = []
                for row in group:
                    if not isinstance(row, dict):
                        continue
                    address = row.get("address", "")
                    row_contact_flag = False
                    row_contact = {}
                    if address:
                        row_contact_flag = True
                        row_contact = parse_contact_from_address(address)
                        group_contact_flag = True
                        group_contact = row_contact
                        contact_flag = True
                        contact = group_contact
                    row_dict = {
                        "container_number__container_number": str(
                            row.get("container_number__container_number", "")
                        ).strip(),
                        "destination": str(row.get("destination", "")).strip(),
                        "shipping_mark": str(row.get("shipping_mark", "")).strip(),
                        "shipment_batch_number__ARM_PRO": str(
                            row.get("shipment_batch_number__ARM_PRO", "")
                        ).strip(),
                        "total_pallet": safe_int(row.get("total_pallet", 0)),
                        "total_pcs": safe_int(row.get("total_pcs", 0)),
                        "shipment_batch_number__fleet_number__carrier": str(
                            row.get("shipment_batch_number__fleet_number__carrier", "")
                        ).strip(),
                        "shipment_batch_number__note": str(
                            row.get("shipment_batch_number__note", "")
                        ).strip(),
                        "slot": str(row.get("slot", "")).strip(),
                        "pickup_image": str(row.get("pickup_image", "")).strip(),
                        "pickup_image_orientation": str(
                            row.get("pickup_image_orientation", "horizontal")
                        ).strip(),
                        "pickup_file_content": str(
                            row.get("pickup_file_content", "")
                        ),
                    }
                    for possible_key in (
                        "arm_pickup_group",
                        "pickup_group",
                        "group_id",
                        "group",
                        "pickup_number",
                        "bol_group",
                    ):
                        possible_value = row.get(possible_key)
                        if possible_value is None:
                            continue
                        possible_value_str = str(possible_value).strip()
                        if possible_value_str:
                            row_dict[possible_key] = possible_value_str
                    if row_contact_flag:
                        row_dict["__contact_flag"] = True
                        row_dict["__contact"] = row_contact
                    group_rows.append(row_dict)
                    arm_pickup.append(row_dict)
                if group_rows:
                    if len(raw_groups) == 1:
                        grouped = {}
                        grouped_order = []
                        for r in group_rows:
                            group_id = ""
                            for possible_key in (
                                "arm_pickup_group",
                                "pickup_group",
                                "group_id",
                                "group",
                                "pickup_number",
                                "bol_group",
                            ):
                                if r.get(possible_key):
                                    group_id = str(r.get(possible_key)).strip()
                                    break
                            arm_pro_value = r.get("shipment_batch_number__ARM_PRO", "")
                            if group_id:
                                key = f"GROUP::{group_id}"
                            elif arm_pro_value and arm_pro_value != "None":
                                key = f"ARM_PRO::{arm_pro_value}"
                            else:
                                key = f"CN_DEST::{r.get('container_number__container_number','')}|{r.get('destination','')}"
                            if key not in grouped:
                                grouped[key] = []
                                grouped_order.append(key)
                            grouped[key].append(r)

                        if len(grouped_order) > 1:
                            for key in grouped_order:
                                rows = grouped[key]
                                first = rows[0] if rows else {}
                                arm_pickup_groups.append(
                                    {
                                        "rows": rows,
                                        "contact_flag": bool(
                                            first.get("__contact_flag", False)
                                        ),
                                        "contact": first.get("__contact", {}),
                                    }
                                )
                        else:
                            arm_pickup_groups.append(
                                {
                                    "rows": group_rows,
                                    "contact_flag": group_contact_flag,
                                    "contact": group_contact,
                                }
                            )
                    else:
                        arm_pickup_groups.append(
                            {
                                "rows": group_rows,
                                "contact_flag": group_contact_flag,
                                "contact": group_contact,
                            }
                        )
        else:  # 没有就从数据库查
            arm_pickup = await sync_to_async(list)(
                Pallet.objects.select_related(
                    "container_number__container_number",
                    "shipment_batch_number__fleet_number",
                )
                .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
                .values(
                    "container_number__container_number",
                    "shipment_batch_number__shipment_appointment",
                    "shipment_batch_number__ARM_PRO",
                    "shipment_batch_number__fleet_number__carrier",
                    "shipment_batch_number__fleet_number__appointment_datetime",
                    "shipment_batch_number__fleet_number__fleet_type",
                    "destination",
                    "shipping_mark",
                    "shipment_batch_number__note",
                    "slot",
                )
                .annotate(
                    total_pcs=Sum("pcs"),
                    total_pallet=Count("pallet_id", distinct=True),
                    total_weight=Sum("weight_lbs"),
                    total_cbm=Sum("cbm"),
                )
            )
            if arm_pickup:
                arm_pickup_groups = [
                    {"rows": arm_pickup, "contact_flag": contact_flag, "contact": contact}
                ]

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        pickup_time_str = fleet.appointment_datetime
        pickup_time = pickup_time_str.strftime("%Y-%m-%d")
        pallet = 0
        pcs = 0
        shipping_mark = ""
        notes = set()
        arm_pro = ""
        carrier = ""
        container_number = ""
        destination = ""
        for arm in arm_pickup:
            arm_pro = arm.get("shipment_batch_number__ARM_PRO", "")
            carrier = arm.get("shipment_batch_number__fleet_number__carrier", "")
            pallet += safe_int(arm.get("total_pallet", 0))
            pcs += safe_int(arm.get("total_pcs", 0))
            container_number = arm.get("container_number__container_number", "")
            destination = arm.get("destination", "")
            shipping_mark += arm.get("shipping_mark", "")
            notes.add(arm.get("shipment_batch_number__note", ""))
            marks = arm.get("shipping_mark", "")
            new_marks = ""
            if marks:
                array = marks.split(",")
                if len(array) > 1:
                    parts = []
                    for i in range(0, len(array)):
                        part = ",".join(array[i : i + 1])
                        parts.append(part)
                    new_marks = "\n".join(parts)
                else:
                    new_marks = marks
            arm["shipping_mark"] = new_marks
        group_container_number = format_two_per_line(
            [a.get("container_number__container_number", "") for a in arm_pickup]
        )
        all_marks = []
        for a in arm_pickup:
            marks_value = a.get("shipping_mark", "")
            for part in re.split(r"[\n,]+", str(marks_value)):
                part_str = part.strip()
                if part_str:
                    all_marks.append(part_str)
        group_shipping_mark = format_two_per_line(all_marks)
        pickup_attachments = extract_pickup_attachments(arm_pickup)
        notes_str = "<br>".join(filter(None, notes))
        barcode_type = "code128"
        barcode_class = barcode.get_barcode_class(barcode_type)

        def generate_barcode_base64(content: str) -> str:
            my_barcode = barcode_class(content, writer=ImageWriter())
            buffer = io.BytesIO()
            my_barcode.write(buffer, options={"dpi": 600})
            buffer.seek(0)
            image = Image.open(buffer)
            width, height = image.size
            new_height = int(height * 0.7)
            cropped_image = image.crop((0, 0, width, new_height))
            new_buffer = io.BytesIO()
            cropped_image.save(new_buffer, format="PNG")
            return base64.b64encode(new_buffer.getvalue()).decode("utf-8")

        is_multi_arm_pickup = len(arm_pickup_groups) > 1
        if is_multi_arm_pickup:
            bol_pages = []
            for group in arm_pickup_groups:
                group_rows = group.get("rows", [])
                if not group_rows:
                    continue
                group_arm_pro = ""
                group_carrier = ""
                group_container_number = ""
                group_destination = ""
                group_pallet = 0
                group_pcs = 0
                group_container_numbers = []
                group_marks = []
                for row in group_rows:
                    if not group_arm_pro:
                        group_arm_pro = row.get("shipment_batch_number__ARM_PRO", "")
                    if not group_carrier:
                        group_carrier = row.get(
                            "shipment_batch_number__fleet_number__carrier", ""
                        )
                    if not group_container_number:
                        group_container_number = row.get(
                            "container_number__container_number", ""
                        )
                    if not group_destination:
                        group_destination = row.get("destination", "")
                    group_pallet += safe_int(row.get("total_pallet", 0))
                    group_pcs += safe_int(row.get("total_pcs", 0))
                    group_container_numbers.append(
                        row.get("container_number__container_number", "")
                    )
                    row_marks_value = row.get("shipping_mark", "")
                    for part in re.split(r"[\n,]+", str(row_marks_value)):
                        part_str = part.strip()
                        if part_str:
                            group_marks.append(part_str)

                if not group_arm_pro or group_arm_pro == "None":
                    if '自提' in group_destination:
                        group_destination = 'client pickup'
                    barcode_content = f"{group_container_number}|{group_destination}"
                else:
                    barcode_content = f"{group_arm_pro}"
                if not group_carrier or group_carrier == "None" or group_carrier == "":
                    group_carrier = "nocarrier"
                bol_pages.append(
                    {
                        "warehouse": warehouse,
                        "arm_pro": group_arm_pro,
                        "carrier": group_carrier,
                        "pallet": group_pallet,
                        "pcs": group_pcs,
                        "container_number": format_two_per_line(group_container_numbers),
                        "shipping_mark": format_two_per_line(group_marks),
                        "pickup_attachments": extract_pickup_attachments(group_rows),
                        "barcode": generate_barcode_base64(barcode_content),
                        "contact": group.get("contact", {}),
                        "contact_flag": group.get("contact_flag", False),
                        "pickup_time": pickup_time,
                    }
                )

            context = {
                "warehouse": warehouse,
                "bol_pages": bol_pages,
                "arm_pickup": arm_pickup,
                "notes": notes_str,
            }
            template = get_template(self.template_ltl_bol_multi)
            html = template.render(context)
        else:
            if arm_pro == "" or arm_pro == "None" or arm_pro is None:
                if '自提' in destination:
                    destination = 'client pickup'
                barcode_content = f"{container_number}|{destination}"
            else:
                barcode_content = f"{arm_pro}"
            barcode_base64 = generate_barcode_base64(barcode_content)

            if not carrier or carrier == "None" or carrier == "":
                carrier = "nocarrier"
            context = {
                "warehouse": warehouse,
                "arm_pro": arm_pro,
                "carrier": carrier,
                "pallet": pallet,
                "pcs": pcs,
                "container_number": group_container_number,
                "shipping_mark": group_shipping_mark,
                "barcode": barcode_base64,
                "pickup_attachments": pickup_attachments,
                "arm_pickup": arm_pickup,
                "contact": contact,
                "contact_flag": contact_flag,
                "pickup_time": pickup_time,
                "notes": notes_str,
            }
            template = get_template(self.template_ltl_bol)
            html = template.render(context)

        response = HttpResponse(content_type="application/pdf")
        def sanitize_filename(value: str) -> str:
            """清理文件名中的换行和非法字符"""
            if not value:
                return ""
            # 去掉换行符
            value = value.replace("\n", "_").replace("\r", "_")
            # 替换 Windows 不允许的字符
            value = re.sub(r'[\\/:*?"<>|]+', "_", value)
            # 防止文件名太长
            return value[:100]
        safe_shipping_mark = sanitize_filename(shipping_mark)
        safe_destination = sanitize_filename(destination)
        if is_multi_arm_pickup:
            segments = []
            for group in arm_pickup_groups:
                rows = group.get("rows", [])
                if not rows:
                    continue
                containers = []
                container_seen = set()
                marks = []
                mark_seen = set()
                for r in rows:
                    cn = str(r.get("container_number__container_number", "")).strip()
                    if cn and cn.lower() != "none" and cn not in container_seen:
                        container_seen.add(cn)
                        containers.append(cn)
                    sm = r.get("shipping_mark", "")
                    for part in re.split(r"[\n,]+", str(sm)):
                        part_str = part.strip()
                        if (
                            part_str
                            and part_str.lower() != "none"
                            and part_str not in mark_seen
                        ):
                            mark_seen.add(part_str)
                            marks.append(part_str)
                container_part = sanitize_filename(containers[0]) if containers else "NA"
                mark_part = sanitize_filename(marks[0]) if marks else "NA"
                segments.append(f"{container_part}-{mark_part}")
            filename_base = "+".join(filter(None, segments))
            filename_base = sanitize_filename(filename_base)
            if not filename_base:
                safe_fleet_number = sanitize_filename(fleet_number)
                filename_base = f"{safe_fleet_number}+MULTI"
            response["Content-Disposition"] = (
                f'attachment; filename="{filename_base}+BOL.pdf"'
            )
        else:
            response["Content-Disposition"] = (
                f'attachment; filename="{container_number}+{safe_destination}+{safe_shipping_mark}+BOL.pdf"'
            )
        pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response
    
    async def export_ltl_label(self, request: HttpRequest) -> HttpResponse:
        '''新功能LTL的LABEL文件下载'''
        fleet_number = request.POST.get("fleet_number")
        arm_pickup_data = request.POST.get("arm_pickup_data")
        contact_flag = False  # 表示地址栏空出来，客服手动P上去
        contact = ""
        if arm_pickup_data and arm_pickup_data != "[]":
            arm_pickup_data = json.loads(arm_pickup_data)
            for row in arm_pickup_data:
                address = row.get("address", "")
                if address:
                    contact_flag = True
                    address = re.sub("[\u4e00-\u9fff]", " ", address)
                    address = re.sub(r"\uFF0C", ",", address)
                    parts = [p.strip() for p in address.split(";")]
                    contact = {
                        "company": parts[0] if len(parts) > 0 else "",
                        "Road":    parts[1] if len(parts) > 1 else "",
                        "city":    parts[2] if len(parts) > 2 else "",
                        "name":    parts[3] if len(parts) > 3 else "",
                        "phone":   parts[4] if len(parts) > 4 else "",
                    }
        
        arm_pickup = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number__container_number",
                "shipment_batch_number__fleet_number",
            )
            .filter(shipment_batch_number__fleet_number__fleet_number=fleet_number)
            .values(
                "container_number__container_number",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__ARM_PRO",
                "shipment_batch_number__fleet_number__carrier",
                "shipment_batch_number__fleet_number__appointment_datetime",
                "destination",
                "shipping_mark",
            )
            .annotate(
                total_pcs=Sum("pcs"),
                total_pallet=Count("pallet_id", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
            )
        )
        if not arm_pickup:
            raise ValueError('该车次下未查到板子记录！')
        pallets = 0
        for arm in arm_pickup:
            if not arm["shipment_batch_number__shipment_appointment"]:
                raise ValueError(f'{fleet_number}的约没有时间!')
            arm_pro = arm["shipment_batch_number__ARM_PRO"]
            carrier = arm["shipment_batch_number__fleet_number__carrier"]
            pickup_time = arm["shipment_batch_number__shipment_appointment"]
            container_number = arm["container_number__container_number"]
            destination = arm["destination"]
            shipping_mark = arm["shipping_mark"]
            pallets += arm["total_pallet"]
        pickup_time_str = str(pickup_time)
        date_str = datetime.strptime(pickup_time_str[:19], "%Y-%m-%d %H:%M:%S")
        pickup_time = date_str.strftime("%Y-%m-%d")

        # 生成条形码
        barcode_type = "code128"
        barcode_class = barcode.get_barcode_class(barcode_type)
        if arm_pro == "" or arm_pro == "None" or arm_pro == None:
            barcode_content = f"{container_number}|{shipping_mark}"
        else:
            barcode_content = f"{arm_pro}"
        my_barcode = barcode_class(
            barcode_content, writer=ImageWriter()
        )  # 将条形码转换为图像形式
        buffer = io.BytesIO()  # 创建缓冲区
        my_barcode.write(buffer, options={"dpi": 600})  # 缓冲区存储图像
        buffer.seek(0)
        image = Image.open(buffer)
        width, height = image.size
        new_height = int(height * 0.7)
        cropped_image = image.crop((0, 0, width, new_height))
        new_buffer = io.BytesIO()
        cropped_image.save(new_buffer, format="PNG")

        barcode_base64 = base64.b64encode(new_buffer.getvalue()).decode("utf-8")
        data = [
            {
                "warehouse": request.POST.get("warehouse"),
                "arm_pro": arm_pro,
                "barcode": barcode_base64,
                "carrier": carrier,
                "contact": contact,
                "contact_flag": contact_flag,
                "fraction": f"{i + 1}/{pallets}",
            }
            for i in range(pallets)
        ]
        context = {"data": data}
        template = get_template(self.template_ltl_label)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response["Content-Disposition"] = (
            f'attachment; filename="{pickup_time}+{container_number}+{destination}+{shipping_mark}+LABEL.pdf"'
        )
        pisa_status = pisa.CreatePDF(html, dest=response, link_callback=link_callback)
        if pisa_status.err:
            raise ValueError(
                "Error during PDF generation: %s" % pisa_status.err,
                content_type="text/plain",
            )
        return response

    
    async def handle_bol_fleet_post(self, request: HttpRequest) -> HttpResponse:
        #准备参数
        mutable_post = request.POST.copy()
        fleet_number = request.POST.get("fleet_number")
        mutable_post["customerInfo"] = None
        mutable_post["pickupList"] = None

        request.POST = mutable_post
        fm = FleetManagement()
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
            
        #if fleet.fleet_type == 'FTL': 
        shipment = await sync_to_async(list)(Shipment.objects.filter(fleet_number=fleet))
        if len(shipment) > 1:
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for s in shipment:
                    s_number = s.shipment_batch_number
                    mutable_post["shipment_batch_number"] = s_number
                    pdf_response = await fm.handle_export_bol_post(request)
                    zip_file.writestr(f"BOL_{s_number}.pdf", pdf_response.content)
            response = HttpResponse(zip_buffer.getvalue(), content_type="application/zip")
            response["Content-Disposition"] = 'attachment; filename="orders.zip"'
            zip_buffer.close()
            return response
        else:
            mutable_post["shipment_batch_number"] = shipment[0].shipment_batch_number
        return await fm.handle_export_bol_post(request)
        # else:
        #     raise ValueError('出库类型异常！')
        
    async def handle_bol_post(self, request: HttpRequest) -> Any:
        fm = FleetManagement()
        mutable_post = request.POST.copy()
        mutable_post["customerInfo"] = None
        mutable_post["pickupList"] = None
        warehouse = mutable_post["warehouse"]
        context = {}
        for key, code in self.warehouse_options.items():
            if key in warehouse:
                mutable_post["warehouse"] = code

        appointment_id = request.POST.get("appointment_id")
        mutable_post["appointment_id"] = appointment_id
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related("fleet_number").get(
                appointment_id=appointment_id
            )
        )()
        
        if shipment.shipment_type == '客户自提':
            raise ValueError("该预约批次预约类型为客户自提，不支持客提的BOL下载！")
        if shipment.fleet_number:
            mutable_post["fleet_number"] = shipment.fleet_number
        else:
            raise ValueError("该预约批次尚未排车")
        request.POST = mutable_post

        shipment_batch_number = shipment.shipment_batch_number
        fleet_number = shipment.fleet_number.fleet_number

        mutable_post = request.POST.copy()
        mutable_post['shipment_batch_number'] = shipment_batch_number
        mutable_post['fleet_number'] = fleet_number
        request.POST = mutable_post
        return await fm.handle_export_bol_post(request)
    
    async def generate_unique_batch_number(self,destination):
        """生成唯一的shipment_batch_number"""
        current_time = datetime.now()

        for i in range(10):
            batch_id = (
                destination
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            batch_number = batch_id.replace(" ", "").replace("/", "-").upper()      
            exists = await sync_to_async(
                Shipment.objects.filter(shipment_batch_number=batch_number).exists
            )()

            if not exists:
                return batch_number
        raise ValueError('批次号始终重复')
    
    async def handle_modify_intelligent_po_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        all_suggestions_raw = request.POST.get('all_suggestions')
        if all_suggestions_raw:
            all_suggestions = json.loads(all_suggestions_raw)

        suggestion_data_raw = request.POST.get('suggestion_data')
        if suggestion_data_raw:
            suggestion_data = json.loads(suggestion_data_raw)
            suggestion_id = suggestion_data['suggestion_id']
            cargos = suggestion_data.get('cargos', [])
            intelligent_cargos = suggestion_data.get('intelligent_cargos', [])

        selected_ids_raw = request.POST.get("selected_ids")
        selected_ids = json.loads(selected_ids_raw) if selected_ids_raw else []
        
        selected_cargos = [c for c in intelligent_cargos if c['unique_id'] in selected_ids]
        cargos.extend(selected_cargos)

         # === 更新 primary_group 的统计数据 ===
        total_pallets = sum(c.get('total_n_pallet_act', 0) or c.get('total_n_pallet_est', 0) for c in cargos)
        total_cbm = sum(c.get('total_cbm', 0) for c in cargos)

        primary_group = suggestion_data.get('primary_group', {})
        if primary_group:
            # 更新主组的板数和CBM
            primary_group['total_pallets'] = total_pallets
            primary_group['total_cbm'] = total_cbm
        new_intelligent_cargos = [c for c in intelligent_cargos if c['unique_id'] not in selected_ids]

        suggestion_data['cargos'] = cargos
        suggestion_data['intelligent_cargos'] = new_intelligent_cargos

        # 替换掉 all_suggestions 中对应项
        for i, s in enumerate(all_suggestions):
            if s['suggestion_id'] == suggestion_id:
                all_suggestions[i] = suggestion_data
                break
        return await self.handle_td_unshipment_post(request,{},all_suggestions)

    async def handle_fleet_add_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {}
        appointment_id = request.POST.get("add_pallet_ISA")
        plt_ids = request.POST.getlist("plt_ids")
        actual_shipped_pallet = request.POST.getlist("actual_shipped_pallet")
        shipped_pallet_ids = []
        
        for plt_id, p_shipped in zip(plt_ids, actual_shipped_pallet):
            # 清理数据：移除空字符串和None
            if not plt_id or not p_shipped:
                continue

            p_shipped_int = int(float(p_shipped))
            
            # 分割plt_ids并清理空值
            plt_id_list = [pid.strip() for pid in plt_id.split(',') if pid.strip()]
            
            if not plt_id_list:
                continue
                
            # 取前p_shipped_int个元素
            shipped_count = min(p_shipped_int, len(plt_id_list))  # 防止索引越界
            shipped_pallet_ids += plt_id_list[:shipped_count]
        pallets = await sync_to_async(list)(
            Pallet.objects.select_related("container_number").filter(
                id__in=shipped_pallet_ids
            )
        )
        total_weight, total_cbm, total_pcs = 0.0, 0.0, 0
        for plt in pallets:
            total_weight += plt.weight_lbs
            total_pcs += plt.pcs
            total_cbm += plt.cbm
        # 查找该出库批次,将重量等信息加到出库批次上
        try:
            shipment = await Shipment.objects.select_related("fleet_number").aget(appointment_id=appointment_id)
        except ObjectDoesNotExist:
            context.update({"error_messages": f"{appointment_id}预约号找不到"})
            return await self.handle_fleet_schedule_post(request,context)
        fleet = shipment.fleet_number
        fleet.total_weight += total_weight
        fleet.total_pcs += total_pcs
        fleet.total_cbm += total_cbm
        fleet.total_pallet += len(shipped_pallet_ids)
        await sync_to_async(fleet.save)()
        # 查找该出库批次下的约，把加塞的柜子板数加到同一个目的地的约
        
        plt_to_update = []
        for pallet in pallets:
            pallet.shipment_batch_number = shipment
            plt_to_update.append(pallet)
        if plt_to_update:
            result = await sync_to_async(bulk_update_with_history)(
                plt_to_update,
                Pallet,
                fields=["shipment_batch_number"],
            )
        if 'error_messages' not in context:
            context.update({"success_messages": f"{appointment_id}加塞成功！"})
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_fleet_schedule_post(request,context)
    
    async def handle_fl_notified_customer_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        page = request.POST.get("page")
        fleet_number = request.POST.get("fleet_number")
        context = {}
        if not bool(fleet_number) or not fleet_number or 'None' in fleet_number:
            context.update({
                'error_messages':'fleet_number为空！',
            })
            if page == "arm_appointment":
                return await self.handle_unscheduled_pos_post(request,context)
            else:
                return await self.handle_td_shipment_post(request, context)
        try:
            fleet = await sync_to_async(Fleet.objects.get)(
                fleet_number=fleet_number
            )
            shipments = await sync_to_async(list)(
                Shipment.objects.filter(fleet_number=fleet)
            )
            if not shipments:
                context.update({
                    'error_messages': f"找不到车次号为 {fleet_number} 相关联的预约批次记录",
                })
            else:
                for shipment in shipments:
                    shipment.is_notified_customer = True
                    await sync_to_async(shipment.save)()
                context = {'success_messages': f"{fleet_number}通知客户成功！"}
        except Fleet.DoesNotExist:
            context.update({
                'error_messages': f"找不到车次号为 {fleet_number} 的记录",
            })
        except Exception as e:
            context.update({
                'error_messages': f"通知客户失败: {str(e)}",
            })
        
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request, context)
        
    async def handle_sp_notified_customer_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointment_id = request.POST.get("appointment_id")
        context = {}
        if not bool(appointment_id) or not appointment_id or 'None' in appointment_id:
            context.update({
                'error_messages':'ISA为空！',
            })
            return await self.handle_td_shipment_post(request, context)
        try:
            shipment = await sync_to_async(Shipment.objects.get)(
                appointment_id=appointment_id
            )
            shipment.is_notified_customer = True
            await sync_to_async(shipment.save)()
            context = {'success_messages': f"{appointment_id}通知客户成功！"}
        except Shipment.DoesNotExist:
            context.update({
                'error_messages': f"找不到预约号为 {appointment_id} 的记录",
            })
        except Exception as e:
            context.update({
                'error_messages': f"通知客户失败: {str(e)}",
            })
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request, context)
    
    async def handle_search_addable_po_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        destination = request.POST.get("destination")
        appointment_id = request.POST.get("appointment_id")
        context = {}
        if not bool(appointment_id) or not appointment_id or 'None' in appointment_id:
            context.update({
                'error_messages':'ISA为空！',
                "show_add_po_inventory_modal": False,
            })
        criteria_p = models.Q(
            (
                models.Q(container_number__orders__order_type="转运")
                | models.Q(container_number__orders__order_type="转运组合")
            ),
            shipment_batch_number__isnull=True,
            container_number__orders__created_at__gte="2024-09-01",
        )
        pl_criteria = criteria_p & models.Q(
            container_number__orders__offload_id__offload_at__isnull=True,
            container_number__orders__retrieval_id__retrieval_destination_precise=warehouse,
            #destination=destination,
            delivery_type='public',
        )
        plt_criteria = criteria_p & models.Q(
            container_number__orders__offload_id__offload_at__isnull=False,
            location__startswith=warehouse,
            #destination=destination,
            delivery_type='public',
        )
        packing_list_not_scheduled = await self._get_packing_list(
            request.user,pl_criteria, plt_criteria
        )
        # 过滤掉每组中 is_pass=False 的内容
        filtered_packing_list = []
        for po in packing_list_not_scheduled:
            if getattr(po, 'is_pass', True):  # 如果 is_pass 为 True 或者不存在该属性，则保留
                filtered_packing_list.append(po)

        packing_list_not_scheduled = filtered_packing_list
        
        context.update({
            "warehouse": warehouse,
            "destination": destination,
            "appointment_id": request.POST.get("appointment_id"),
            "packing_list_not_scheduled": packing_list_not_scheduled,
            "active_tab": request.POST.get("active_tab"),       
        })
        if 'show_add_po_inventory_modal' not in context:
            context.update({"show_add_po_inventory_modal": True})# ← 控制是否直接弹出“添加PO”弹窗
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request, context)
    
    async def handle_add_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        destination = request.POST.get("destination")
        active_tab = request.POST.get("active_tab")
        step = request.POST.get("step")
        criteria_plt = models.Q(
            models.Q(
                shipment_batch_number__fleet_number__fleet_number__isnull=True
            ) | models.Q(
                shipment_batch_number__fleet_number__fleet_number__isnull=False,
                shipment_batch_number__fleet_number__departured_at__isnull=True
            ),
            location=warehouse,
            destination=destination,
            container_number__orders__offload_id__offload_at__isnull=False,
        )
        plt_unshipped = await self._get_packing_list(
            request.user,
            models.Q(
                container_number__orders__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
            criteria_plt
        )
        context = {
            "warehouse": warehouse,
            "destination": destination,
            "plt_unshipped": plt_unshipped,
            "step": step,  # ← 前端靠这个判断要不要弹窗
            "active_tab": active_tab,          # ← 用来控制前端打开哪个标签页
            "show_add_po_modal": True,   # ← 控制是否直接弹出“添加PO”弹窗
            "add_po_title": "加塞",
            "add_pallet_ISA": request.POST.get('add_pallet_ISA')
        }
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_fleet_schedule_post(request, context)

    async def handle_one_fleet_departure_post(
        self, request: HttpRequest, context: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        if not context:
            context = {}
        request.POST = request.POST.copy()
        if "plt_ids" in request.POST:
            try:
                raw_plt = request.POST.get("plt_ids")
                plt_list = json.loads(raw_plt)  # => [["5022,5023,5024,5025,5026"], ["3343,3344"]]
                flattened_plt = [",".join(inner) if isinstance(inner, list) else str(inner) for inner in plt_list]
                request.POST.setlist("plt_ids", flattened_plt)
            except Exception as e:
                raise ValueError("⚠️ plt_ids 解析错误:", e)

        for key in ["scheduled_pallet", "actual_shipped_pallet"]:
            if key in request.POST:
                raw_value = request.POST.get(key)  # 例如 '5,2'
                parts = [v.strip() for v in raw_value.split(",") if v.strip()]
                request.POST.setlist(key, parts)
        if "batch_number" in request.POST:
            raw_value = request.POST.get("batch_number")  # 例如 '23487532,43324296'
            try:
                parts = [int(v.strip()) for v in raw_value.split(",") if v.strip()]
                parts = list(set(parts))
                shipments = await sync_to_async(Shipment.objects.filter)(
                    appointment_id__in=parts
                )
                batch_numbers = await sync_to_async(list)(
                    shipments.values_list('shipment_batch_number', flat=True)
                )
            except ValueError:
                raise ValueError("⚠️ batch_number 转换为 int 出错，原始值:", raw_value)
        else:
            #LTL那边确认出库时，没有batch_number
            raw_plt_ids = request.POST.get("plt_ids")
            plt_ids_int = [int(x) for x in raw_plt_ids.split(",") if x.strip()]
            batch_numbers = await sync_to_async(
                lambda: list(
                    Pallet.objects
                    .filter(id__in=plt_ids_int, shipment_batch_number__isnull=False)
                    .values_list('shipment_batch_number__shipment_batch_number', flat=True)
                    .distinct()
                )
            )()

        request.POST = request.POST.copy()
        request.POST.setlist('batch_number', batch_numbers)
        fm = FleetManagement()
        context_new = await fm.handle_fleet_departure_post(request,'post_nsop')
        context.update(context_new)
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        elif page == "ltl_readyShip":
            return await self.handle_ltl_unscheduled_pos_post(request)       
        else:
            return await self.handle_fleet_schedule_post(request,context)         
    
    async def handle_export_virtual_fleet_pos_post(
        self, request: HttpRequest
    ) ->  HttpResponse:
        cargo_ids_str = request.POST.get("cargo_ids", "").strip()
        pl_ids = [
            int(i.strip()) for i in cargo_ids_str.split(",") if i.strip().isdigit()
        ]
        
        plt_ids_str = request.POST.get("plt_ids", "").strip()
        plt_ids = [
            int(i.strip()) for i in plt_ids_str.split(",") if i.strip().isdigit()
        ]
        if not pl_ids and not plt_ids:
            raise ValueError("没有获取到任何 ID")
        
        all_data = []

        if plt_ids:
            pallet_data = await sync_to_async(
                lambda: list(
                    Pallet.objects.select_related("container_number")
                    .filter(id__in=plt_ids,is_dropped_pallet=False)
                    .values(
                        "id",
                        "shipping_mark",
                        "fba_id",
                        "ref_id",
                        "address",
                        "zipcode",
                        "container_number__container_number",
                        "destination",
                        "cbm",
                        "pcs",
                        "PO_ID"
                    )
                    .annotate(
                        total_pcs=Sum("pcs"),
                        total_n_pallet_act=Count("id"),
                        total_cbm=Sum("cbm", output_field=FloatField()),
                        label=Value('ACT')
                    )
                    .distinct()
                    .order_by("destination", "container_number__container_number")
                )
            )()
            
            additional_packinglist_ids, unmatched_pallet_data = await self.find_packinglist_ids_by_pallet_data(pallet_data)
            # 合并packinglist IDs
            all_packinglist_ids = list(set(pl_ids + additional_packinglist_ids))
            # 处理未匹配的pallet数据（添加标记）
            if unmatched_pallet_data:
                for item in unmatched_pallet_data:
                    item = dict(item)
                    ref_ids = str(item["ref_id"]).split(",") if item["ref_id"] else [""]
                    
                    for ref_id in ref_ids:
                        new_row = item.copy()
                        new_row["ref_id"] = ref_id.strip()
                        new_row["check_id"] = "未找到对应PO记录，请手动处理"
                        new_row["is_unmatched"] = True
                        all_data.append(new_row)
        else:
            all_packinglist_ids = pl_ids
        
        
        packinglist_data = await sync_to_async(
            lambda: list(
                PackingList.objects.select_related("container_number")
                .filter(id__in=all_packinglist_ids)
                .values(
                    "id",
                    "shipping_mark",
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "container_number__container_number",
                    "destination",
                    "cbm"
                )
                .annotate(
                    total_pcs=Sum("pcs"),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    label=Value("EST"),
                )
                .distinct()
                .order_by("destination", "container_number__container_number")
            )
        )()
        pl_ids_list = [pl["id"] for pl in packinglist_data]
        check_map = await sync_to_async(
            lambda: {
                p.packing_list_id: p.id
                for p in PoCheckEtaSeven.objects.filter(packing_list_id__in=pl_ids_list)
            }
        )()

        # 展开数据：将ref_id按逗号分割成多行
        for item in packinglist_data:
            check_id = check_map.get(item["id"])
            if check_id:
                item["check_id"] = check_id  # 匹配到就显示ID
            else:
                item["check_id"] = "未找到校验记录"  # 匹配不到就显示提示
            
        
        # 合并数据
        all_data += packinglist_data
        df = pd.DataFrame.from_records(all_data)

        df["total_n_pallet_est"] = df["total_cbm"] / 2
        def get_est_pallet(n):
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)

        df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
        df["is_valid"] = None
        df["is_est"] = df["label"] == "EST"
        df["Pallet Count"] = df.apply(
            lambda row: row["total_n_pallet_est"] if row["is_est"] else max(1, row.get("total_n_pallet_act", 1)),
            axis=1
        ).astype(int)

        keep = [
            "shipping_mark",
            "container_number__container_number",
            "fba_id",
            "ref_id",
            "total_pcs",
            "Pallet Count",
            "label",
            "is_valid",
            "check_id",
            "total_cbm",
            "destination",
        ]
        df = df[keep].rename(
            {
                "fba_id": "PRO",
                "container_number__container_number": "BOL",
                "ref_id": "PO List (use , as separator) *",
                "total_pcs": "Carton Count",
                "total_cbm": "Total CBM",
                "destination": "Destination",
                "check_id": "Check Result",
            },
            axis=1,
        )
        
        # 导出 CSV

        if len(df) == 0:
            raise ValueError('没有数据',len(df))
        # 如果只有一个 Destination，保持原来返回单 CSV
        grouped_by_dest = {}
        for _, row in df.iterrows():
            dest = row["Destination"]
            if dest not in grouped_by_dest:
                grouped_by_dest[dest] = []
            grouped_by_dest[dest].append(row.to_dict())
        
        # 如果只有一个 Destination，返回单 CSV
        if len(grouped_by_dest) == 1:
            dest_name = list(grouped_by_dest.keys())[0]
            df_single = pd.DataFrame(grouped_by_dest[dest_name])
            response = HttpResponse(content_type="text/csv")
            response["Content-Disposition"] = f"attachment; filename=PO_{dest_name}.csv"
            df_single.to_csv(path_or_buf=response, index=False)
            return response
        
        # 多个 Destination 打包 zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for dest, rows in grouped_by_dest.items():
                df_dest = pd.DataFrame.from_records(rows)
                
                csv_buffer = io.BytesIO()
                df_dest.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                csv_buffer.seek(0)
            
                safe_dest = "".join(c for c in dest if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_dest}.csv" if safe_dest else f"destination_{hash(dest)}.csv"
                
                zf.writestr(filename, csv_buffer.getvalue())

        zip_buffer.seek(0)
        zip_bytes = zip_buffer.getvalue()

        response = HttpResponse(zip_bytes, content_type="application/zip")
        response["Content-Disposition"] = "attachment; filename=PO_virtual_fleet.zip"
        response["Content-Length"] = len(zip_bytes)
        return response
    
    async def handle_update_fleet_info(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        """更新 fleet 基础信息"""
        context = {}

        # 获取请求中的字段
        fleet_number = request.POST.get("fleet_number", "").strip()
        warehouse = request.POST.get("warehouse", "").strip()
        carrier = request.POST.get("carrier", "").strip()
        third_party_address = request.POST.get("third_party_address", "").strip()
        pickup_number = request.POST.get("pickup_number", "").strip()
        license_plate = request.POST.get("license_plate", "").strip()
        motor_carrier_number = request.POST.get("motor_carrier_number", "").strip()
        dot_number = request.POST.get("dot_number", "").strip()
        appointment_datetime_str = request.POST.get("appointment_datetime", "").strip()
        note = request.POST.get("note", "").strip()

        # 查找 Fleet
        fleet = await sync_to_async(lambda: Fleet.objects.filter(fleet_number=fleet_number).first())()
        if not fleet:
            context["error_messages"] = f"Fleet {fleet_number} 不存在"
            return await self.handle_td_shipment_post(request, context)

        # 解析时间字符串
        appointment_datetime = None
        if appointment_datetime_str:
            try:
                # 格式例如 2025-10-11T16:09
                appointment_datetime = datetime.strptime(appointment_datetime_str, "%Y-%m-%dT%H:%M")
            except Exception as e:
                context["error_messages"] = f"时间格式错误: {appointment_datetime_str} ({e})"
                return await self.handle_td_shipment_post(request, context)

        fleet.origin = warehouse or fleet.origin
        fleet.carrier = carrier or fleet.carrier
        fleet.third_party_address = third_party_address or fleet.third_party_address
        fleet.pickup_number = pickup_number or fleet.pickup_number
        fleet.license_plate = license_plate or fleet.license_plate
        fleet.motor_carrier_number = motor_carrier_number or fleet.motor_carrier_number
        fleet.dot_number = dot_number or fleet.dot_number
        fleet.note = note or fleet.note
        fleet.is_virtual = False
        if appointment_datetime:
            fleet.appointment_datetime = appointment_datetime

        await sync_to_async(fleet.save)()

        context["message"] = f"Fleet {fleet_number} 信息已成功更新。"
        context["fleet_number"] = fleet_number
        return await self.handle_td_shipment_post(request, context)


    async def handle_multi_group_booking(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        """处理多组预约出库"""
        booking_data_str = request.POST.get('booking_data')
        warehouse = request.POST.get('warehouse')
        context = {}
        
        if not booking_data_str:
            context.update({"error_messages": "没有收到预约数据"})
            return await self.handle_td_shipment_post(request, context)
        
        try:
            booking_data = json.loads(booking_data_str)
        except json.JSONDecodeError:
            context.update({"error_messages": "预约数据格式错误"})
            return await self.handle_td_shipment_post(request, context)
        
        if not isinstance(booking_data, list) or len(booking_data) == 0:
            context.update({"error_messages": "预约数据为空或格式不正确"})
            return await self.handle_td_shipment_post(request, context)
        
        # 验证每组的id，如果有问题，直接报错给前端
        appointment_ids = [group.get('appointment_id', '') for group in booking_data if group.get('appointment_id')]
        
        # 批量验证所有预约号
        error_messages = await self._batch_validate_appointments(appointment_ids, booking_data)
        
        # 如果有验证错误，直接返回
        if error_messages:
            context.update({"error_messages": mark_safe("<br>".join(error_messages))})
            return await self.handle_td_shipment_post(request, context)
        
        # 存储处理结果
        success_groups = []
        failed_groups = []
        success_appointment_ids = []
        # 为每个大组分别处理预约
        for group_index, group_data in enumerate(booking_data, 1):
            # 准备调用 handle_appointment_post 所需的参数
            cargo_ids = group_data.get('cargo_ids_checked', '')
            plt_ids = group_data.get('plt_ids_checked', '')
            destination = group_data.get('destination', '')
            appointment_id = group_data.get('appointment_id', '')
            shipment_cargo_id = group_data.get('shipment_cargo_id', '')
            shipment_type = group_data.get('shipment_type', '')
            shipment_account = group_data.get('shipment_account', '')
            shipment_appointment = group_data.get('shipment_appointment', '')
            load_type = group_data.get('load_type', '')
            origin = group_data.get('origin', '')
            if not origin:
                origin = warehouse
            note = group_data.get('note', '')
            suggestion_id = group_data.get('suggestion_id')
            pickup_time = group_data.get('pickup_time')
            pickup_number = group_data.get('pickupNumber')
            result = await self._process_single_group_booking(request, suggestion_id, cargo_ids, plt_ids, destination, appointment_id,shipment_cargo_id,shipment_type,
                                                              shipment_account,shipment_appointment,load_type,origin,note,pickup_time,pickup_number)
            
            if result['success']:
                success_groups.append({
                    'suggestion_id': result.get('suggestion_id'),
                    'appointment_id': result.get('appointment_id'),
                    'batch_number': result.get('shipment_batch_number')
                })
                appointment_id = int(str(appointment_id).strip())
                success_appointment_ids.append(appointment_id)
            else:
                failed_groups.append({
                    'suggestion_id': result.get('suggestion_id'),
                    'appointment_id': result.get('appointment_id'),
                    'batch_number': result.get('shipment_batch_number'),
                    'error': result.get('error', '未知错误')
                })
                       
        # 构建返回消息
        messages = []
        if success_groups:
            success_msg = mark_safe(f"成功预约 {len(success_groups)} 个大组: <br>")
            success_msg += ", ".join([f"(批次号：{group['batch_number']},预约号:{group['appointment_id']})" for group in success_groups])
            messages.append(mark_safe(success_msg + "<br>"))
            
        if failed_groups:
            failed_msg = mark_safe(f"预约失败 {len(failed_groups)} 个大组: <br>")
            failed_details = []
            for group in failed_groups:
                detail = f"(批次号：{group['batch_number']},预约号:{group['appointment_id']}) - {group['error']}"
                failed_details.append(detail)
            failed_msg += "; ".join(failed_details)
            if not success_msg:
                success_msg = None
            messages.append(mark_safe(success_msg + "<br>"))
        
        # 存储成功创建的shipment IDs，方便后续约车使用
        if success_appointment_ids:
            # 排车
            fleet_number = await self._add_appointments_to_fleet(success_appointment_ids)
            success_msg = f"成功排车，车次号是 {fleet_number}"
            messages.append(success_msg)
        if messages:
            context.update({"success_messages": mark_safe("<br>".join(messages))})
            
        template_name = request.POST.get('template_name')
        if template_name and template_name == "unshipment":
            return await self.handle_td_unshipment_post(request,context)
        return await self.handle_td_shipment_post(request, context)

    async def _batch_validate_appointments(self, appointment_ids: list, booking_data: list) -> list[str]:
        """批量验证所有预约号"""
        error_messages = []
        
        if not appointment_ids:
            return error_messages
        
        try:
            # 批量查询所有预约号
            existed_appointments = await sync_to_async(list)(
                Shipment.objects.filter(appointment_id__in=appointment_ids)
            )
            
            # 创建映射字典便于查找
            appointment_dict = {appt.appointment_id: appt for appt in existed_appointments}
            
            # 验证每个预约号
            for appointment_id in appointment_ids:
                existed_appointment = appointment_dict.get(appointment_id)
                if not existed_appointment:
                    continue  # 预约号不存在，验证通过
                
                # 找到对应的组数据
                group_data = next((group for group in booking_data if group.get('appointment_id') == appointment_id), None)
                if not group_data:
                    continue
                    
                destination = group_data.get('destination', '')
                
                # 验证预约状态
                if existed_appointment.in_use:
                    error_messages.append(f"ISA {appointment_id} 已经被使用!")
                elif existed_appointment.is_canceled:
                    error_messages.append(f"ISA {appointment_id} 已经取消!")
                elif (existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC) < timezone.now()):
                    error_messages.append(f"ISA {appointment_id} 预约时间小于当前时间，已过期!")
                elif (existed_appointment.destination.replace("Walmart", "").replace("WALMART", "").replace("-", "").upper() != 
                    destination.replace("Walmart", "").replace("WALMART", "").replace("-", "").upper()):
                    error_messages.append(f"ISA {appointment_id} 登记的目的地是 {existed_appointment.destination}，此次登记的目的地是 {destination}!")
                    
        except Exception as e:
            error_messages.append(f"验证预约号时出错: {str(e)}")
        
        return error_messages
    
    async def _add_appointments_to_fleet(self,appointment_ids):
        current_time = datetime.now()
        fleet_number = (
            "F"
            + current_time.strftime("%m%d%H%M%S")
            + str(uuid.uuid4())[:2].upper()
        )
        shipment_info = await sync_to_async(list)(
            Shipment.objects.filter(appointment_id__in=appointment_ids)
            .values('id', 'shipment_type', 'origin')
            .distinct()
        )
        shipment_ids = [item['id'] for item in shipment_info]
        shipment_types = list(set(item['shipment_type'] for item in shipment_info))
        origins = list(set(item['origin'] for item in shipment_info))

        total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
        #记录总数
        if shipment_ids:
            # 获取所有Pallet记录
            pallet_records = await sync_to_async(list)(
                Pallet.objects.filter(
                    shipment_batch_number_id__in=shipment_ids,
                    container_number__orders__offload_id__offload_at__isnull=False
                )
            )
            
            # 获取所有PackingList记录  
            packinglist_records = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number_id__in=shipment_ids,
                    container_number__orders__offload_id__offload_at__isnull=True
                )
            )
            
            # 汇总Pallet数据
            for p in pallet_records:
                total_weight += p.weight_lbs or 0
                total_cbm += p.cbm or 0
                total_pcs += p.pcs or 0
                total_pallet += 1
            
            # 汇总PackingList数据
            for pl in packinglist_records:
                total_weight += pl.total_weight_lbs or 0
                total_cbm += pl.cbm or 0
                total_pcs += pl.pcs or 0
                total_pallet += round(pl.cbm /1.8)
        
        fleet_data = {
            "fleet_number": fleet_number,
            "fleet_type": shipment_types[0] if shipment_types else None,
            "origin": origins[0] if origins else None,
            "total_weight": total_weight,
            "total_cbm": total_cbm,
            "total_pallet": total_pallet,
            "total_pcs": total_pcs,
            "is_virtual": True,
        }
        fleet = Fleet(**fleet_data)
        await sync_to_async(fleet.save)()

        if shipment_ids:
            await sync_to_async(
                Shipment.objects.filter(id__in=shipment_ids).update
            )(fleet_number=fleet)
        return fleet_number

    async def _process_single_group_booking(self, request: HttpRequest, suggestion_id, cargo_ids, plt_ids,destination, appointment_id,shipment_cargo_id,shipment_type,
                                                              shipment_account,shipment_appointment,load_type,origin,note,pickup_time,pickup_number) -> dict:
        """处理单个大组的预约出库"""       
        new_post = {}
        cargo_id_list = []
        if cargo_ids and cargo_ids.strip():
            cargo_id_list = [int(id.strip()) for id in cargo_ids.split(',') if id.strip()]
        
        plt_id_list = []
        if plt_ids and plt_ids.strip():
            plt_id_list = [int(id.strip()) for id in plt_ids.split(',') if id.strip()]
        # 设置货物ID参数（与handle_appointment_post保持一致）
        
        
        total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
        pallet_records = await sync_to_async(list)(
            Pallet.objects.filter(
                id__in=plt_id_list,
            )
        )
       
        # 获取所有PackingList记录  
        packinglist_records = await sync_to_async(list)(
            PackingList.objects.filter(
                id__in=cargo_id_list,
            )
        )
        # 汇总Pallet数据
        for p in pallet_records:
            total_weight += p.weight_lbs or 0
            total_cbm += p.cbm or 0
            total_pcs += p.pcs or 0
            total_pallet += 1
        
        # 汇总PackingList数据
        for pl in packinglist_records:
            total_weight += pl.total_weight_lbs or 0
            total_cbm += pl.cbm or 0
            total_pcs += pl.pcs or 0
            total_pallet += round(pl.cbm /1.8)
        # 设置预约信息参数
        shipment_batch_number = await self.generate_unique_batch_number(destination)
        address = await self.get_address(destination)
        shipment_data = {
            'shipment_batch_number': shipment_batch_number,
            'destination': destination,
            'total_weight': total_weight,
            'total_cbm': total_cbm,
            'total_pallet': total_pallet,
            'total_pcs': total_pcs,
            'total_pallet': total_pallet,
            'shipment_type': shipment_type,
            'shipment_account': shipment_account,
            'appointment_id': appointment_id,
            'shipment_cargo_id': shipment_cargo_id,
            'shipment_appointment': shipment_appointment,
            'load_type': load_type,
            'origin': origin,
            'note': note,
            'address': address,
            'pickup_time': pickup_time,
            'pickup_number': pickup_number,
        }
        new_post = {**new_post, **shipment_data}
        new_post['shipment_data'] = str(shipment_data)
        new_post['pl_ids'] = cargo_id_list
        new_post['plt_ids'] = plt_id_list
        new_post['type'] = 'td' 
        new_post['batch_number'] = shipment_batch_number  

        # 创建新的HttpRequest对象
        new_request = HttpRequest()
        new_request.method = 'POST'     
        new_request.POST = new_post     
        
        try:
            # 直接调用 sm.handle_appointment_post_tuple
            sm = ShippingManagement()
            info = await sm.handle_appointment_post(new_request, 'post_nsop')
            appointment_id = appointment_id
            return {
                'success': True,
                'appointment_id': appointment_id,
                'suggestion_id': suggestion_id,
                'shipment_batch_number': shipment_batch_number,
            }
            
        except Exception as e:
            return {
                'success': False,
                'appointment_id': appointment_id,
                'suggestion_id': suggestion_id,
                'shipment_batch_number': shipment_batch_number,
                'error': f"预约失败: {str(e)}"
            }
    
    async def handle_query_quotation(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''查询多个仓点的报价'''
        cargo_ids = request.POST.get("cargo_ids", "")
        plt_ids = request.POST.get("plt_ids", "")
        cargo_id_list = [int(i) for i in cargo_ids.split(",") if i]
        plt_id_list = [int(i) for i in plt_ids.split(",") if i]
        if not cargo_ids and not plt_ids:
            context.update({'error_messages': "未提供ID，无法查询报价"})
            return await self.handle_td_shipment_post(request, context)
        
        pls = await sync_to_async(
            lambda: list(
                PackingList.objects
                .filter(id__in=cargo_id_list)
                .select_related('container_number')
                .values('PO_ID','container_number__container_number','destination')
                .annotate(
                    total_cbm=Round(Sum('cbm'), 2, output_field=FloatField()),
                    total_pallets=Ceil(
                        Round(Sum('cbm'), 2, output_field=FloatField()) / 1.8
                    ),
                    source=Value('packinglist')
                )
                .order_by('PO_ID')
            )
        )()
        plts = await sync_to_async(
            lambda: list(
                Pallet.objects
                .filter(id__in=plt_id_list)
                .select_related('container_number')
                .values('PO_ID','container_number__container_number','destination','location')
                .annotate(
                    total_cbm=Round(Sum('cbm'), 2, output_field=FloatField()),
                    total_pallets=Count('id'), 
                    source=Value('pallet')
                )
                .order_by('PO_ID')
            )
        )()

        combined_list = pls + plts
        quotation_table_data = []
        quote_total = 0.0
        # 查找报价
        if not combined_list:
            context = {"error_messages": f'{combined_list}是空的'}
            return await self.handle_td_shipment_post(request, context)
        for po in combined_list:
            container_number = po['container_number__container_number']
            destination = po['destination']
            order = await sync_to_async(
                lambda cn=container_number: Order.objects.select_related(
                    'retrieval_id',  # 预加载retrieval_id
                    'vessel_id',
                    'customer_name'   # 预加载customer_name
                ).filter(
                    container_number__container_number=cn
                ).first()
            )()
            if po['source'] == 'packinglist':
                warehouse = order.retrieval_id.retrieval_destination_area
            else:
                warehouse = po['location'].split('-')[0]
                if not warehouse:
                    context = {"error_messages": f'{container_number}的板子缺少实际仓库位置！'}
                    return await self.handle_td_shipment_post(request, context)

            customer_name = order.customer_name.zem_name if order.customer_name else None
            #查找报价表
            quotations = await self._get_fee_details(order, warehouse, customer_name)
            if isinstance(quotations, dict) and quotations.get("error_messages"):
                context = {"error_messages": quotations["error_messages"]}
                return await self.handle_td_shipment_post(request, context)
            fee_details = quotations['fees']
            
            is_combina = False
            if order.order_type == "转运组合":
                container = await sync_to_async(
                    lambda: Container.objects.get(container_number=container_number)
                )()
                if container.manually_order_type == "转运组合":
                    is_combina = True
                elif container.manually_order_type == "转运":
                    is_combina = False
                else:
                    is_combina = self._is_combina(container, order, warehouse)
            
            non_combina_table = True
            if is_combina:
                #组合柜计算
                combina_key = f"{warehouse}_COMBINA"
                if combina_key not in fee_details:
                    context = {"error_messages": f'{warehouse}_COMBINA-{container_number}未找到组合柜报价表！'}
                    return await self.handle_td_shipment_post(request, context)
                
                rules = fee_details.get(combina_key).details
                
                container_type_temp = 0 if "40" in container.container_type else 1
                total_container_cbm = await sync_to_async(
                    lambda: PackingList.objects
                        .filter(container_number=container)
                        .aggregate(
                            total_cbm_sum=Round(
                                Sum('cbm'), 
                                2, 
                                output_field=FloatField()
                            )
                        )['total_cbm_sum'] or 0.0
                )()
                cbm = float(po['total_cbm'])
                temp_table = await self._process_combina_quote(po, cbm, float(total_container_cbm), rules, container_type_temp, warehouse, quotations.get("filename"))
                if temp_table:
                    non_combina_table = False
                    quotation_table_data.append(temp_table)
                
                rules = fee_details.get(combina_key).details
            
            if non_combina_table:
                # 不管符不符合转运，没按组合柜计到费，就按转运方式计算
                
                public_key = f"{warehouse}_PUBLIC"
                if public_key not in fee_details:
                    context = {"error_messages": f'{warehouse}_PUBLIC-{container_number}未找到亚马逊沃尔玛报价表！'}
                    return await self.handle_td_shipment_post(request, context)
                
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
                rate_found = False
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
                
                if rate_found:
                    # 找到报价
                    rate = float(rate) if rate else 0.0
                    quotation_table_data.append({
                        'container_number': po['container_number__container_number'],
                        'destination': destination,                         
                        'cbm': po['total_cbm'],
                        'total_pallets': po['total_pallets'], 
                        'rate': rate, 
                        'amount': rate * po['total_pallets'],
                        'type': delivery_category,
                        'region': None,
                        'warehouse': warehouse, 
                        'is_niche_warehouse': is_niche_warehouse,  
                        'quotation_name': quotations['filename'],  
                    })
                    quote_total += rate * po['total_pallets']
                else:            
                    quotation_table_data.append({
                        'container_number': po['container_number__container_number'],
                        'destination': destination,                         
                        'cbm': po['total_cbm'],
                        'total_pallets': po['total_pallets'], 
                        'rate': None, 
                        'amount': None,
                        'type': delivery_category,
                        'region': None,
                        'warehouse': warehouse, 
                        'is_niche_warehouse': None,  
                        'quotation_name': quotations.get("filename"),  
                    })
            
            # 按照type排序
            quotation_table_data = sorted(
                quotation_table_data, 
                key=lambda x: x['type'] or ''
            )
            context = {'quotation_table_data':quotation_table_data}
        return await self.handle_td_shipment_post(request, context)
    
    async def _process_combina_quote(self, po, cbm, total_cbm, rules, container_type_temp, warehouse, filename):
        """按组合柜方式查找仓点报价 """

        #改前和改后的
        destination_origin, destination = self._process_destination(po['destination'])
        
        # 检查是否属于组合区域
        price = 0
        is_combina_region = False
        region = None
        for region, region_data in rules.items():
            for item in region_data:
                rule_locations = item.get("location", [])
                if isinstance(rule_locations, str):
                    rule_locations = [rule_locations] # 统一转成列表处理

                if any(destination == loc.replace(" ", "").upper() for loc in rule_locations):
                    is_combina_region = True
                    price = item["prices"][container_type_temp]
                    region = region
                    break
            if is_combina_region:
                break
        if destination == "UPS":
            is_combina_region = False
        
        if is_combina_region:
            '''按组合柜计费'''
            return ({
                'container_number': po['container_number__container_number'],
                'destination': po['destination'],                         
                'cbm': cbm,
                'total_pallets': po['total_pallets'], 
                'rate': price, 
                'amount': round(price * cbm / total_cbm,2),
                'type': "组合柜",
                'region': region,
                'warehouse': warehouse, 
                'is_niche_warehouse': None,  
                'quotation_name': filename,  
            })
        else:
            return None
        
    def _process_destination(self, destination_origin):
        """处理目的地字符串"""
        def clean_all_spaces(s):
            if not s:  # 处理None/空字符串
                return ""
            # 匹配所有空格类型：
            # \xa0 非中断空格 | \u3000 中文全角空格 | \s 普通空格/制表符/换行等
            import re
            cleaned = re.sub(r'[\xa0\u3000\s]+', '', str(s))
            return cleaned
        
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
                
                second_result = second_result.replace(" ", "").upper()
                return clean_all_spaces(first_result), clean_all_spaces(second_result)
            else:
                raise ValueError(first_change_pos)
        
        # 如果不包含"改"或"送"或者没有找到
        # 只处理第二部分（假设第一部分为空）
        if "-" in destination_origin:
            if destination_origin.upper().startswith("UPS-"):
                second_result = destination_origin
            else:
                second_result = destination_origin.split("-", 1)[1]
            
        else:
            second_result = destination_origin
        
        second_result = second_result.replace(" ", "").upper()
        return None, clean_all_spaces(second_result)
    
    async def _is_combina(self, container: Container, order: Order, warehouse) -> Any:
        context = {}
        
        customer_name = order.customer_name.zem_name
        vessel_etd = order.vessel_id.vessel_etd

        container_type = container.container_type
        #  基础数据统计
        plts = await sync_to_async(
            lambda: Pallet.objects.filter(
                container_number=container
            ).aggregate(
                unique_destinations=Count("destination", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
                total_pallets=Count("id"),
            )
        )()
        plts["total_cbm"] = round(plts["total_cbm"], 2)
        plts["total_weight"] = round(plts["total_weight"], 2)
        # 获取匹配的报价表
        matching_quotation = await sync_to_async(
            lambda: QuotationMaster.objects.filter(
                effective_date__lte=vessel_etd,
                is_user_exclusive=True,
                exclusive_user=customer_name,
                quote_type='receivable',
            )
            .order_by("-effective_date")
            .first()
        )()
        if not matching_quotation:
            matching_quotation = await sync_to_async(
                lambda: QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=False,
                    quote_type='receivable',
                )
                .order_by("-effective_date")
                .first()
            )()
        if not matching_quotation:
            context.update({"error_messages": f"找不到{container.container_number}可用的报价表！"})
            return context, None, None
        # 获取组合柜规则
        try:
            stipulate_fee_detail = await sync_to_async(
                lambda: FeeDetail.objects.get(
                    quotation_id=matching_quotation.id, 
                    fee_type="COMBINA_STIPULATE"
                )
            )()
            stipulate = stipulate_fee_detail.details
        except FeeDetail.DoesNotExist:
            context.update({
                "error_messages": f"报价表《{matching_quotation.filename}》-{matching_quotation.id}中找不到<报价表规则>分表，请截此图给技术员！"
            })
            return context, None, None
        
        combina_fee_detail = await sync_to_async(
            lambda: FeeDetail.objects.get(
                quotation_id=matching_quotation.id, 
                fee_type=f"{warehouse}_COMBINA"
            )
        )()
        combina_fee = combina_fee_detail.details
        if isinstance(combina_fee, str):
            combina_fee = json.loads(combina_fee)

        # 看是否超出组合柜限定仓点,NJ/SAV是14个
        warehouse_specific_key = f'{warehouse}_max_mixed'
        if warehouse_specific_key in stipulate.get("global_rules", {}):
            combina_threshold = stipulate["global_rules"][warehouse_specific_key]["default"]
        else:
            combina_threshold = stipulate["global_rules"]["max_mixed"]["default"]

        warehouse_specific_key1 = f'{warehouse}_bulk_threshold'
        if warehouse_specific_key1 in stipulate.get("global_rules", {}):
            uncombina_threshold = stipulate["global_rules"][warehouse_specific_key1]["default"]
        else:
            uncombina_threshold = stipulate["global_rules"]["bulk_threshold"]["default"]

        if plts["unique_destinations"] > uncombina_threshold:
            container.account_order_type = "转运"
            container.non_combina_reason = (
                f"总仓点超过{uncombina_threshold}个"
            )
            container.save()
            return context, False, f"总仓点超过{uncombina_threshold}个" # 不是组合柜

        # 按区域统计
        destinations = await sync_to_async(
            lambda: list(Pallet.objects.filter(container_number=container)
                        .values_list("destination", flat=True)
                        .distinct())
        )()
        plts_by_destination = await sync_to_async(
            lambda: list(Pallet.objects.filter(container_number=container)
                        .values("destination")
                        .annotate(total_cbm=Sum("cbm")))
        )()
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
        sum_region_count = non_combina_region_count + combina_region_count
        if sum_region_count > uncombina_threshold:
            # 当非组合柜的区域数量超出时，不能按转运组合
            container.account_order_type = "转运"
            container.non_combina_reason = f"总区数量为{sum_region_count},要求是{uncombina_threshold}"
            container.save()
            return context, False,f"总区数量为{sum_region_count},要求是{uncombina_threshold}"
        container.non_combina_reason = None
        container.account_order_type = "转运组合"
        container.save()
        return context, True, None
    
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
            clean_dest = destination.replace("沃尔玛", "").strip()

            if clean_dest.upper().startswith("UPS-"):
                dest = clean_dest
            else:
                dest = clean_dest.split("-")[-1].strip()

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
    
    def _filter_ups_destinations(self, destinations):
        """过滤掉包含UPS的目的地，支持列表和QuerySet"""
        if hasattr(destinations, '__iter__') and not isinstance(destinations, (str, dict)):
            destinations_list = list(destinations)
        else:
            destinations_list = destinations
        filtered_destinations = [
            dest.strip() for dest in destinations_list 
            if dest is not None 
            and 'UPS' not in str(dest).upper() 
            and 'FEDEX' not in str(dest).upper()
        ]
        return list(dict.fromkeys(filtered_destinations))
    
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
    
    async def _get_fee_details(self, order: Order, warehouse, customer_name) -> dict:
        context = {}
        quotation, quotation_error = await self._get_quotation_for_order(order, customer_name, 'receivable')
        if quotation_error:
            context.update({"error_messages": quotation_error})
            return context
        id = quotation.id

        fee_types = {
            "NJ": ["NJ_LOCAL", "NJ_PUBLIC", "NJ_COMBINA"],
            "SAV": ["SAV_PUBLIC", "SAV_COMBINA"],
            "LA": ["LA_PUBLIC", "LA_COMBINA"],
        }.get(warehouse, [])

        fees_list = await sync_to_async(
            lambda qid=id, ft=fee_types: list(
                FeeDetail.objects.filter(
                    quotation_id=qid, 
                    fee_type__in=ft
                )
            )
        )()
        fees_dict = {fee.fee_type: fee for fee in fees_list}
        return {
            "quotation": quotation,
            "fees": fees_dict,
            "filename": quotation.filename,
        }
    
    async def _get_quotation_for_order(self, order: Order, customer_name, quote_type: str = 'receivable') :
        """获取订单对应的报价表"""
        vessel_etd = order.vessel_id.vessel_etd
        
        # 先查找用户专属报价表
        quotation = await sync_to_async(
            lambda: QuotationMaster.objects.filter(
                effective_date__lte=vessel_etd,
                is_user_exclusive=True,
                exclusive_user=customer_name,
                quote_type=quote_type,
            )
            .order_by("-effective_date")
            .first()
        )()
        
        if not quotation:
            # 查找通用报价表
            quotation = await sync_to_async(
                lambda: QuotationMaster.objects.filter(
                    effective_date__lte=vessel_etd,
                    is_user_exclusive=False,
                    quote_type=quote_type,
                )
                .order_by("-effective_date")
                .first()
            )()
        
        if quotation:
            return quotation, None
        else:
            error_msg = f"找不到生效日期在{vessel_etd}之前的{quote_type}报价表"
            return None, error_msg
        
    async def handle_edit_note_sp_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        cargo_ids = request.POST.get("cargo_ids", "")
        plt_ids = request.POST.get("plt_ids", "")
        note_sp = request.POST.get("note_sp", "").strip()
        context = {}

        cargo_id_list = [int(i) for i in cargo_ids.split(",") if i]
        plt_id_list = [int(i) for i in plt_ids.split(",") if i]
        if not cargo_ids and not plt_ids:
            context.update({'error_messages': "未提供任何记录ID，无法更新备注"})
            return await self.handle_td_shipment_post(request, context)
        # 更新 PackingList
        if cargo_id_list:
            updated_count = await sync_to_async(
                lambda: PackingList.objects.filter(id__in=cargo_id_list).update(note_sp=note_sp)
            )()
            if updated_count == 0:
                context.update({'error_messages': "更新备注失败！"})
                return await self.handle_td_shipment_post(request,context)

        # 更新 Pallet
        if plt_id_list:
            updated_count = await sync_to_async(
                lambda: Pallet.objects.filter(id__in=plt_id_list).update(note_sp=note_sp)
            )()
            if updated_count == 0:
                context.update({'error_messages': "更新备注失败！"})
                return await self.handle_td_shipment_post(request,context)
        context.update({'success_messages':"更新备注成功！"}) 
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request,context)


    async def handle_edit_appointment_post(
        self, request: HttpRequest, name: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        context = {}
        if name == "fleet_departure":
            shipment_batch_number = request.POST.get('shipment_batch_number', '').strip()
            old_shipments = await sync_to_async(list)(
                Shipment.objects.filter(shipment_batch_number=shipment_batch_number)
            ) 
            appointment_id_new = request.POST.get('appointment_id', '').strip()
        else:
            appointment_id_old = request.POST.get('appointment_id', '').strip()
            old_shipments = await sync_to_async(list)(
                Shipment.objects.filter(appointment_id=appointment_id_old)
            )      
            appointment_id_new = request.POST.get('appointment_id_input', '').strip()
        if not old_shipments:
            context.update({'error_messages':f"未找到 ISA={appointment_id_old}!"})     
        if len(old_shipments) > 1:
            context.update({'error_messages':f"找到多条相同 ISA={appointment_id_old}的记录，请检查数据!"})   
        
        old_shipment = old_shipments[0]
        if name == "fleet_departure":
            appointment_id_old = old_shipments[0].appointment_id
    
        shipment_appointment = request.POST.get('shipment_appointment')
        pickup_time = request.POST.get('pickup_time')
        pickup_number = request.POST.get('pickup_number')
        destination = request.POST.get('destination').strip()
        load_type = request.POST.get('load_type')
        origin = request.POST.get('origin')
        shipment_cargo_id = request.POST.get('shipment_cargo_id')
        page = request.POST.get("page")
        if appointment_id_new == appointment_id_old:
            old_shipment.shipment_appointment = shipment_appointment if shipment_appointment else None
            old_shipment.destination = destination
            old_shipment.load_type = load_type
            old_shipment.origin = origin
            old_shipment.pickup_time = pickup_time if pickup_time else None
            old_shipment.pickup_number = pickup_number
            old_shipment.shipment_cargo_id = shipment_cargo_id           
            old_shipment.is_canceled = False
            old_shipment.status = ""
            if page != "01_appointment" and page != "arm_appointment":
                old_shipment.in_use = True
            await sync_to_async(old_shipment.save)()         
            context.update( {'success_messages':f"{appointment_id_old}预约信息修改成功！"})
            if name == "fleet_departure":
                return await self.handle_fleet_schedule_post(request,context)
            
            if page == "arm_appointment":
                return await self.handle_unscheduled_pos_post(request,context)
            elif page == "01_appointment":
                return await self.handle_appointment_management_post(request,context)
            return await self.handle_td_shipment_post(request,context)
        else:
            context = await self._check_ISA_is_repetition(appointment_id_new,destination)
            if context.get('success_messages'):
                old_shipment.appointment_id = appointment_id_new
                old_shipment.destination = destination
                old_shipment.load_type = load_type
                old_shipment.origin = origin
                old_shipment.pickup_time = pickup_time if pickup_time else None
                old_shipment.pickup_number = pickup_number
                old_shipment.shipment_cargo_id = shipment_cargo_id
                old_shipment.is_canceled = False
                old_shipment.status = ""
                old_shipment.shipment_appointment = shipment_appointment if shipment_appointment else None
                if page != "01_appointment" and page != "arm_appointment":
                    old_shipment.in_use = True
                await sync_to_async(old_shipment.save)()
            if name == "fleet_departure":
                return await self.handle_fleet_schedule_post(request,context)
            page = request.POST.get("page")
            if page == "arm_appointment":
                return await self.handle_unscheduled_pos_post(request,context)
            elif page == "01_appointment":
                return await self.handle_appointment_management_post(request,context)
            return await self.handle_td_shipment_post(request,context)
    
    async def _check_ISA_is_repetition(self,appointment_id,destination):
        context = {}
        try:
            existed_appointment = await sync_to_async(Shipment.objects.get)(
                appointment_id=appointment_id
            )
            if existed_appointment:
                if existed_appointment.in_use:             
                    context.update({'error_messages': f"{appointment_id}已经在使用中！"})
                elif existed_appointment.is_canceled:
                    context.update({'error_messages': f"{appointment_id}已经被取消！"})
                elif (
                    existed_appointment.shipment_appointment.replace(tzinfo=pytz.UTC)
                    < timezone.now()
                ):
                    context.update({'error_messages': f"{appointment_id}在备约中登记的时间早于当前时间，请先修改备约！"})
                elif existed_appointment.destination != destination:
                    context.update({'error_messages': f"{appointment_id}在备约中登记的目的地和本次修改的目的地不同！"})
        except Shipment.DoesNotExist:
            # 如果查不到记录，直接返回成功消息
            context.update({'success_messages':f"已更换为新的{appointment_id}！"})
        return context
    
    async def handle_cancel_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointment_id = request.POST.get('appointment_id')
        shipment = await sync_to_async(Shipment.objects.get)(
            appointment_id=appointment_id
        )
        shipment_batch_number = shipment.shipment_batch_number

        request.POST = request.POST.copy()
        request.POST['shipment_batch_number'] = shipment_batch_number
        request.POST['type'] = 'td'     
        sm = ShippingManagement()
        context = await sm.handle_cancel_post(request,'post_nsop')   
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)    
        else:  
            return await self.handle_td_shipment_post(request,context)
    
    async def handle_shipment_add_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {}
        appointment_id = request.POST.get("appointment_id")
        pl_ids_str = request.POST.getlist("cargo_ids")
        selected = []
        if pl_ids_str:
            for id_str in pl_ids_str:
                if id_str.strip():
                    selected.extend([int(x.strip()) for x in id_str.split(',') if x.strip().isdigit()])

        plt_ids_str = request.POST.getlist("plt_ids")
        selected_plt = []
        if plt_ids_str:
            for id_str in plt_ids_str: 
                if id_str.strip():
                    selected_plt.extend([int(x.strip()) for x in id_str.split(',') if x.strip().isdigit()])

        if not selected and not selected_plt:
            context.update({"error_messages": f"{appointment_id}没有找到要添加po的id！"})
            return await self.handle_td_shipment_post(request,context)

        shipment = await sync_to_async(Shipment.objects.get)(
            appointment_id=appointment_id
        )

        shipment_batch_number = shipment.shipment_batch_number
        request.POST = request.POST.copy()
        request.POST['alter_type'] = 'add'
        request.POST['pl_ids'] = selected
        request.POST['plt_ids'] = selected_plt
        request.POST['shipment_batch_number'] = shipment_batch_number           
        sm = ShippingManagement()
        info = await sm.handle_alter_po_shipment_post(request,'post_nsop') 
        
        context.update({"success_messages": f"{appointment_id}添加成功！"})
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request,context)
    
    async def _get_unique_pickup_number(self, pickup_number: str) -> str:
        """
        获取唯一的pickup_number，如果有重复则自动添加序号
        """
        base_number = pickup_number
        counter = 1
        pickup_number = f"{base_number}-{counter}"
        
        while await Shipment.objects.filter(pickup_number=pickup_number).aexists():
            counter += 1
            pickup_number = f"{base_number}-{counter}"
                 
        return pickup_number

    async def _get_pl_plt_total_weight(self, ids: list[int], plt_ids: list[int]) -> Tuple[float, float, int, int]:
        total_weight = 0.0
        total_cbm = 0.0
        total_pcs = 0
        total_pallet = 0
        
        # 计算PackingList的统计信息
        if ids:
            # 使用aggregate计算总和
            packinglist_stats = await sync_to_async(
                PackingList.objects.filter(id__in=ids).aggregate
            )(
                total_weight_sum=Sum('total_weight_lbs'),
                total_cbm_sum=Sum('cbm'),
                total_pcs_sum=Sum('pcs'),
            )
            
            total_weight += packinglist_stats.get('total_weight_sum') or 0.0
            total_cbm += packinglist_stats.get('total_cbm_sum') or 0.0
            total_pcs += packinglist_stats.get('total_pcs_sum') or 0
            # PackingList的板数需要特殊计算：cbm/1.8取上限
            if packinglist_stats.get('total_cbm_sum'):
                total_pallet += math.ceil(packinglist_stats['total_cbm_sum'] / 1.8)
        
        # 计算Pallet的统计信息
        if plt_ids:
            # 使用aggregate计算总和
            pallet_stats = await sync_to_async(
                Pallet.objects.filter(id__in=plt_ids).aggregate
            )(
                total_weight_sum=Sum('weight_lbs'),
                total_cbm_sum=Sum('cbm'),
                total_pcs_sum=Sum('pcs'),
                pallet_count=Count('id'),  # 每个pallet算一个板
            )
            
            total_weight += pallet_stats.get('total_weight_sum') or 0.0
            total_cbm += pallet_stats.get('total_cbm_sum') or 0.0
            total_pcs += pallet_stats.get('total_pcs_sum') or 0
            total_pallet += pallet_stats.get('pallet_count') or 0
        
        # 四舍五入处理
        total_weight = round(total_weight, 2)
        total_cbm = round(total_cbm, 3)
        total_pallet = math.ceil(total_pallet)  # 确保板数是整数
        return (float(total_weight), float(total_cbm), int(total_pcs),  int(total_pallet) )

    async def handle_ltl_bind_group_shipment(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:    
        context = {}
        # 获取表单数据
        cargo_ids = request.POST.get('cargo_ids', '').strip()
        plt_ids = request.POST.get('plt_ids', '').strip()
        destination = request.POST.get('destination', '').strip()
        address = request.POST.get('address', '').strip()
        carrier = request.POST.get('carrier', '').strip()
        shipment_appointment = request.POST.get('shipment_appointment', '').strip()
        if not shipment_appointment:
            raise ValueError("提货时间不能为空！")
        arm_bol = request.POST.get('arm_bol', '').strip()
        arm_pro = request.POST.get('arm_pro', '').strip()
        is_print_label = request.POST.get('is_print_label', 'false').strip() == 'true'
        shipment_type = request.POST.get('shipment_type', '').strip()
        warehouse = request.POST.get('warehouse')
        auto_fleet = request.POST.get('auto_fleet', 'true').strip()
        auto_fleet_bool = auto_fleet.lower() == 'true'
        note = request.POST.get('note', '').strip()

        nj_shipped = False
        if shipment_type == "客户自提" and "NJ" in warehouse: 
            nj_shipped = True
            auto_fleet_bool = True
        
        # 解析ID列表
        packinglist_ids = []
        pallet_ids = []
        
        if cargo_ids:
            packinglist_ids = [int(id.strip()) for id in cargo_ids.split(',') if id.strip()]
        
        if plt_ids:
            pallet_ids = [int(id.strip()) for id in plt_ids.split(',') if id.strip()]
        
        if not packinglist_ids and not pallet_ids:
            context = {'error_messages':'请选择要绑定的货物！'}
            return await self.handle_ltl_unscheduled_pos_post(request, context)
        
        #计算总重量等参数
        total_weight, total_cbm, total_pcs, total_pallet = await self._get_pl_plt_total_weight(packinglist_ids,pallet_ids)
        # 时间字段处理
        try:
            pickup_time = timezone.make_aware(datetime.fromisoformat(shipment_appointment.replace('Z', '')))
        except (ValueError, TypeError):
            context = {'error_messages':'提货时间格式错误！'}
            return await self.handle_ltl_unscheduled_pos_post(request, context)
        
        if auto_fleet_bool:
            current_time = datetime.now()
            try:
                shipment_appointment_dt = datetime.fromisoformat(shipment_appointment.replace('Z', ''))
                month_day = shipment_appointment_dt.strftime("%m%d")
            except:
                month_day = current_time.strftime("%m%d")
            pickupNumber = "ZEM" + "-" + warehouse + "-" + "" + month_day + carrier + destination

            fleet_cost = (request.POST.get("fleet_cost", ""))
            if not fleet_cost:
                fleet_cost = 0.0
            else:
                fleet_cost = float(fleet_cost)
            fleet = Fleet(
                **{
                    "carrier": request.POST.get("carrier").strip(),
                    "fleet_type": shipment_type,
                    "pickup_number": pickupNumber,
                    "appointment_datetime": shipment_appointment,  # 车次的提货时间
                    "fleet_number": "FO"
                    + current_time.strftime("%m%d%H%M%S")
                    + str(uuid.uuid4())[:2].upper(),
                    "scheduled_at": current_time,
                    "total_weight": total_weight,
                    "total_cbm": total_cbm,
                    "total_pallet": total_pallet,
                    "total_pcs": total_pcs,
                    "origin": warehouse,
                    "fleet_cost": fleet_cost,
                }
            )
            # NJ仓的客户自提和UPS，都不需要确认出库和确认到达，客户自提需要POD上传
            if nj_shipped: 
                fleet.departured_at = shipment_appointment
                fleet.arrived_at = shipment_appointment
            await sync_to_async(fleet.save)()

        maersk_batch_number = request.POST.get('maersk_batch_number')
        if maersk_batch_number:
             batch_number = maersk_batch_number
        else:
            if len(destination) > 8:
                destination_name = destination[:8]
            else:
                destination_name = destination
            batch_number = await self.generate_unique_batch_number(destination_name)

        shipment_appointment_tz = self._parse_datetime(shipment_appointment)
        tzinfo = self._parse_tzinfo(warehouse)
        shipmentappointment_utc = self._parse_ts(shipment_appointment, tzinfo)
        current_time = timezone.now()
        # 创建Shipment记录
        shipment_data = {
            'shipment_batch_number': batch_number,
            'shipment_type': shipment_type,
            'destination': destination,
            'address': address,
            'carrier': carrier,
            'ARM_BOL': arm_bol,
            'ARM_PRO': arm_pro,
            'is_print_label': is_print_label,
            'pickup_time': pickup_time,
            'shipment_schduled_at': current_time,
            'shipment_appointment': shipment_appointment,
            'shipment_appointment_tz': shipment_appointment_tz,
            'shipment_appointment_utc': shipmentappointment_utc,
            'total_weight': total_weight,
            'total_cbm': total_cbm,
            'total_pallet': total_pallet,
            'total_pcs': total_pcs,
            'origin': warehouse,
            'note': note,
        }
        if auto_fleet_bool:
            shipment_data['fleet_number'] = fleet
        if nj_shipped: 
            # 客户自提的预约完要直接跳到POD上传,时间按预计提货时间
            shipment_data.update({
                'is_shipped': True,
                'shipped_at': shipment_appointment,
                'shipped_at_utc': shipmentappointment_utc,
                'is_arrived': True,
                'arrived_at': shipment_appointment,
                'arrived_at_utc': shipmentappointment_utc
            })
        shipment = await sync_to_async(Shipment.objects.create)(**shipment_data)
        
        if packinglist_ids:
            # 直接使用update()方法批量更新
            await sync_to_async(PackingList.objects.filter(id__in=packinglist_ids).update)(
                shipment_batch_number=shipment
            )

        if pallet_ids:
            # 直接使用update()方法批量更新
            await sync_to_async(Pallet.objects.filter(id__in=pallet_ids).update)(
                shipment_batch_number=shipment
            )
        
        success_msg = f'预约出库绑定成功! <br>批次号是:{batch_number}!'
        if auto_fleet_bool:
            success_msg += ' <br>已自动排车！'
        else:
            success_msg += ' <br>需手动排车！'
        context = {'success_messages': mark_safe(success_msg)}
        return await self.handle_ltl_unscheduled_pos_post(request, context)
    
    def _parse_datetime(self, datetime_string: str) -> tuple[str, str]:
        try:
            dt = datetime.fromisoformat(datetime_string)
            return ""   # 你原函数只返回 timezone，这里保持结构不破坏
        except ValueError:
            pass
        datetime_pattern = re.compile(
            r"""
            (?P<month>\d{1,2})          # Month (1 or 2 digits)
            [/\-]                       # Separator (/, -)
            (?P<day>\d{1,2})            # Day (1 or 2 digits)
            [/\-]                       # Separator (/, -)
            (?P<year>\d{4})             # Year (4 digits)
            [, ]*                       # Optional comma or space
            (?P<hour>\d{1,2})           # Hour (1 or 2 digits)
            [:]                         # Colon
            (?P<minute>\d{2})           # Minute (2 digits)
            [ ]*                        # Optional space
            (?P<period>AM|PM)           # AM or PM
            [ ,]*                       # Optional comma or space
            (?P<timezone>[A-Z+\-:\d]*)? # Optional timezone
            """,
            re.VERBOSE | re.IGNORECASE,
        )
        match = datetime_pattern.search(datetime_string)
        if not match:
            raise ValueError(f"Invalid datetime format: {datetime_string}")

        parts = match.groupdict()
        month, day, year = int(parts["month"]), int(parts["day"]), int(parts["year"])
        hour, minute = int(parts["hour"]), int(parts["minute"])
        period = parts["period"].upper()
        timezone = parts["timezone"].strip() if parts["timezone"] else ""
        return timezone
    
    async def handle_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:  
        '''港后的预约出库功能''' 
        context = {}
        shipment_type = request.POST.get('shipment_type')
        page = request.POST.get("page")
        if shipment_type == "FTL":         
            pickup_number_raw = request.POST.get('pickupNumber')        
            if page == "arm_appointment":
                pickup_number = await self._get_unique_pickup_number(pickup_number_raw)
            else:
                pickup_number = pickup_number_raw
        else:
            pickup_number = None
        appointment_id = request.POST.get('appointment_id')
        ids = request.POST.get("cargo_ids")
        plt_ids = request.POST.get("plt_ids")
        selected = [int(i) for i in ids.split(",") if i]
        selected_plt = [int(i) for i in plt_ids.split(",") if i]
        if not selected and not selected_plt:
            context.update({"error_messages": f"没有选择PO！"}) 
            if page == "arm_appointment":          
                return await self.handle_unscheduled_pos_post(request,context)
            else:
                return await self.handle_td_shipment_post(request,context)
        
        operation_type = request.POST.get('operation_type')
        shipment_cargo_id = request.POST.get('shipment_cargo_id')
        
        if operation_type == "remove_po":            
            shipment = await sync_to_async(Shipment.objects.get)(
                appointment_id=appointment_id
            )

            shipment_batch_number = shipment.shipment_batch_number
            
            request.POST = request.POST.copy()
            request.POST['alter_type'] = 'remove'
            request.POST['pl_ids'] = selected
            request.POST['plt_ids'] = selected_plt
            request.POST['shipment_batch_number'] = shipment_batch_number    
            sm = ShippingManagement()
            info = await sm.handle_alter_po_shipment_post(request,'post_nsop') 
            context.update({"success_messages": f"删除部分PO，批次号是{shipment_batch_number}"})
            if page == "arm_appointment":
                return await self.handle_unscheduled_pos_post(request,context)
            else:
                return await self.handle_td_shipment_post(request,context)
        
        destination = request.POST.get('destination')                  
        
        if selected or selected_plt:
            packing_list_selected = await self._get_packing_list(
                request.user,
                models.Q(id__in=selected)
                & models.Q(shipment_batch_number__isnull=True),
                models.Q(id__in=selected_plt)
                & models.Q(shipment_batch_number__isnull=True),
            )
            total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
            for pl in packing_list_selected:
                total_weight += (
                    pl.get("total_weight_lbs") if pl.get("total_weight_lbs") else 0
                )
                total_cbm += pl.get("total_cbm") if pl.get("total_cbm") else 0
                total_pcs += pl.get("total_pcs") if pl.get("total_pcs") else 0
                if pl.get("label") == "ACT":
                    total_pallet += pl.get("total_n_pallet_act")
                else:
                    if pl.get("total_n_pallet_est < 1"):
                        total_pallet += 1
                    elif pl.get("total_n_pallet_est") % 1 >= 0.45:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1 + 1)
                    else:
                        total_pallet += int(pl.get("total_n_pallet_est") // 1)
            address = request.POST.get('address')
            if not address:
                address = await self.get_address(destination)
                if not address:
                    raise ValueError('没找到地址')
            
            #先去查询一下shipment表，有没有这个记录，就是第一次预约出库，如果有就是修改
            try:
                shipment = await sync_to_async(Shipment.objects.get)(
                    appointment_id=appointment_id
                )
                if shipment.shipment_batch_number:  #已经有批次号了，说明这是修改PO的
                    shipment_batch_number = shipment.shipment_batch_number
                else:
                    shipment_batch_number = await self.generate_unique_batch_number(destination)

                #不管之前怎么样，目前都是要重新按plt_ids/pl_ids重新绑定，所以要把以前主约/约绑定这个的解绑               
                if selected_plt: 
                    await sync_to_async(
                        Pallet.objects.filter(master_shipment_batch_number=shipment).update
                    )(master_shipment_batch_number=None)
                    await sync_to_async(
                        Pallet.objects.filter(shipment_batch_number=shipment).update
                    )(shipment_batch_number=None)
                if selected:  #不管之前怎么样，目前都是要重新按plt_ids/pl_ids重新绑定，所以要把以前的解绑
                    await sync_to_async(
                        PackingList.objects.filter(master_shipment_batch_number=shipment).update
                    )(master_shipment_batch_number=None)
                    await sync_to_async(
                        PackingList.objects.filter(shipment_batch_number=shipment).update
                    )(shipment_batch_number=None)
            except ObjectDoesNotExist:
                #找不到，那就新建一条记录
                shipment_batch_number = await self.generate_unique_batch_number(destination)
                               
            except MultipleObjectsReturned:
                context.update({"error_messages": f"存在多条重复的{appointment_id}!"})  
                if page == "arm_appointment":
                    return await self.handle_unscheduled_pos_post(request,context)
                else:
                    return await self.handle_td_shipment_post(request,context)          

            shipment_data = {
                'shipment_batch_number': shipment_batch_number,
                'destination': destination,
                'total_weight': total_weight,
                'total_cbm': total_cbm,
                'total_pallet': total_pallet,
                'total_pcs': total_pcs,
                'total_pallet': total_pallet,
                'shipment_type': request.POST.get('shipment_type'),
                'shipment_account': request.POST.get('shipment_account'),
                'appointment_id': appointment_id,
                'shipment_cargo_id': shipment_cargo_id,
                'shipment_appointment': request.POST.get('shipment_appointment'),
                'load_type': request.POST.get('load_type'),
                'origin': request.POST.get('warehouse'),
                'note': request.POST.get('note'),
                'address': address,
                'pickup_number': pickup_number,
                'pickup_time': request.POST.get('pickup_time'),
            }
            request.POST = request.POST.copy()
            request.POST['shipment_data'] = str(shipment_data)
            request.POST['batch_number'] = shipment_batch_number   
            request.POST['address'] = address      
            request.POST['pl_ids'] = selected
            request.POST['plt_ids'] = selected_plt
            request.POST['type'] = 'td'
            request.POST['origin'] = request.POST.get('warehouse')  
            request.POST['load_type'] = request.POST.get('load_type')  
            request.POST['note'] = request.POST.get('note')  
            request.POST['destination'] = destination
            request.POST['shipment_type'] = request.POST.get('shipment_type')  
            request.POST['appointment_id'] = request.POST.get('appointment_id')  
            request.POST['shipment_cargo_id'] = request.POST.get('shipment_cargo_id')  
            request.POST['pickup_number'] = pickup_number  
            request.POST['pickup_time'] = request.POST.get('pickup_time') 
            
            sm = ShippingManagement()
            info = await sm.handle_appointment_post(request,'post_nsop') 
            context.update({"success_messages": f"绑定成功，批次号是{shipment_batch_number},pickupNumber是{pickup_number}"})
            if page == "arm_appointment":
                return await self.handle_unscheduled_pos_post(request,context)
            else:
                return await self.handle_td_shipment_post(request,context)
        else:
            context.update({"error_messages": f"没有选择PO！"}) 
        # template_name = request.POST.get('template_name')
        # if template_name and template_name == "unshipment":
        #     return await self.handle_td_unshipment_post(request,context)
        if page == "arm_appointment":          
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_td_shipment_post(request,context)
    
    async def handle_fleet_confirmation_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:     
        '''确认出库界面'''
        page = request.POST.get("page")
        selected_ids_str = request.POST.get("selected_ids")
        fleet_cost = request.POST.get("fleet_cost")

        if fleet_cost:
            try:
                fleet_cost_value = float(fleet_cost)
            except (ValueError, TypeError):
                fleet_cost_value = 0.0
        else:
            fleet_cost_value = 0.0  
        error_message = None
        if selected_ids_str:
            try:
                selected_ids_list = json.loads(selected_ids_str)
                selected_ids = [int(id) for id in selected_ids_list]
            except (json.JSONDecodeError, ValueError) as e:
                # 处理解析错误
                error_message = f"selected_ids 参数格式错误: {e}"
                # 根据你的错误处理方式选择
                raise ValueError(error_message)
        if page == "arm_appointment":
            selected_ids = await sync_to_async(list)(
                Shipment.objects.filter(appointment_id__in=selected_ids).values_list('id', flat=True)
            )

        request.POST = request.POST.copy()
        fm = FleetManagement()
        if selected_ids:
            #先生成fleet_number
            current_time = datetime.now()
            fleet_number = (
                "F"
                + current_time.strftime("%m%d%H%M%S")
                + str(uuid.uuid4())[:2].upper()
            )
            shipment_selected = await sync_to_async(list)(
                Shipment.objects.filter(id__in=selected_ids)
            )
            fleet_type = None
            shipment_types = set()
            total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
            for s in shipment_selected:
                shipment_types.add(s.shipment_type)
                total_weight += s.total_weight
                total_cbm += s.total_cbm
                total_pcs += s.total_pcs
                total_pallet += s.total_pallet
            if len(shipment_types) > 1:
                error_message = f"选中的预约批次包含不同的 shipment_type: {list(shipment_types)}"
                raise ValueError(error_message)
            else:
                fleet_type = shipment_selected[0].shipment_type if shipment_selected else None
            fleet_data_dict = {
                'fleet_number': fleet_number,
                'fleet_type': fleet_type,
                'origin': request.POST.get('warehouse'),
                'total_weight': total_weight,
                'total_cbm': total_cbm,
                'total_pallet': total_pallet,
                'total_pcs': total_pcs,
                'fleet_cost': fleet_cost_value,
            }
            
            request.POST['fleet_number'] = fleet_number
            request.POST['fleet_type'] = fleet_type
        request.POST['fleet_data'] = str(fleet_data_dict)
        request.POST['fleet_cost'] = fleet_cost_value
        request.POST['selected_ids'] = selected_ids
        
        info = await fm.handle_fleet_confirmation_post(request,'post_nsop')
        context = {}
        if error_message:
            context.update({"error_messages": error_message}) 
        _, context = await self.handle_td_shipment_post(request, context)
        context.update({"success_messages": f'排车成功!批次号是：{fleet_number}'})   
        
        # 分摊成本
        await fm.insert_fleet_shipment_pallet_fleet_cost(
            request, fleet_number, fleet_cost_value
        )
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        elif page == "ltl_unscheduledFleet":
            return await self.handle_ltl_unscheduled_pos_post(request,context)
        return await self.handle_td_shipment_post(request, context)

    async def handle_appointment_time(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointmentId = request.POST.get("appointmentId")
        shipment = await sync_to_async(Shipment.objects.get)(
            appointment_id=appointmentId
        )
        operation = request.POST.get("operation")
        if operation == "edit":
            appointmentTime = request.POST.get("appointmentTime")
            naive_datetime = parse(appointmentTime).replace(tzinfo=None)
            shipment.shipment_appointment = naive_datetime
            await sync_to_async(shipment.save)()
            context = {'success_messages':'备约修改成功！'}
        elif operation == "delete":
            shipment.is_canceled = True
            await sync_to_async(shipment.delete)()
            context = {'success_messages':'备约删除成功！'}
        page = request.POST.get("page")
        if page == "arm_appointment":
            return await self.handle_unscheduled_pos_post(request,context)
        else:
            return await self.handle_appointment_management_post(request,context)

    async def handle_export_pos(self, request: HttpRequest) -> HttpResponse:
        cargo_ids_str_list = request.POST.getlist("cargo_ids")
        pl_ids = [
            int(pl_id) 
            for sublist in cargo_ids_str_list 
            for pl_id in sublist.split(",") 
            if pl_id.strip()  # 非空才转换
        ]
        plt_ids_str_list = request.POST.getlist("plt_ids")
        plt_ids = [
            int(plt_id) 
            for sublist in plt_ids_str_list 
            for plt_id in sublist.split(",") 
            if plt_id.strip()  # 非空才转换
        ]

        if not pl_ids and not plt_ids:
            raise ValueError('没有获取到id')
        
        all_data = []
        #先把plt_id找到对应的pl_id
        if plt_ids:
            pallet_data = await sync_to_async(
                lambda: list(
                    Pallet.objects.select_related("container_number")
                    .filter(id__in=plt_ids,is_dropped_pallet=False)
                    .values(
                        "id",
                        "shipping_mark",
                        "fba_id",
                        "ref_id",
                        "address",
                        "zipcode",
                        "container_number__container_number",
                        "destination",
                        "cbm",
                        "pcs",
                        "PO_ID"
                    )
                    .annotate(
                        total_pcs=Sum("pcs"),
                        total_n_pallet_act=Count("id"),
                        total_cbm=Sum("cbm", output_field=FloatField()),
                        label=Value('ACT')
                    )
                    .distinct()
                    .order_by("destination", "container_number__container_number")
                )
            )()

            additional_packinglist_ids, unmatched_pallet_data = await self.find_packinglist_ids_by_pallet_data(pallet_data)

            # 合并packinglist IDs
            all_packinglist_ids = list(set(pl_ids + additional_packinglist_ids))
            # 处理未匹配的pallet数据（添加标记）
            if unmatched_pallet_data:
                for item in unmatched_pallet_data:
                    item = dict(item)
                    ref_ids = str(item["ref_id"]).split(",") if item["ref_id"] else [""]
                    
                    for ref_id in ref_ids:
                        new_row = item.copy()
                        new_row["ref_id"] = ref_id.strip()
                        new_row["check_id"] = "未找到对应PO记录，请手动处理"
                        new_row["is_unmatched"] = True
                        new_row["total_n_pallet_est"] = new_row.get("total_n_pallet_act", 0)
                        all_data.append(new_row)
        else:
            all_packinglist_ids = pl_ids

        # 查找柜号下的pl
        packing_list = await sync_to_async(
            lambda: list(
                PackingList.objects.select_related("container_number", "pallet")
                .filter(id__in=all_packinglist_ids)
                .values(
                    "id",
                    "shipping_mark",
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "container_number__container_number",
                    "destination",
                    "cbm"
                )
                .annotate(
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField(),
                        )
                    ),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                    total_cbm=Sum("cbm", output_field=FloatField()),
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
        )()
        
        pl_ids_list = [pl["id"] for pl in packing_list]
        check_map = await sync_to_async(
            lambda: {
                p.packing_list_id: p.id
                for p in PoCheckEtaSeven.objects.filter(packing_list_id__in=pl_ids_list)
            }
        )()

        # 展开数据：将ref_id按逗号分割成多行
        data = []
        for item in packing_list:
            item = dict(item)  # 因为 values() 返回的是 ValuesQuerySet
            item["check_id"] = check_map.get(item["id"])  # 如果没有对应记录就返回 None
            data.append(item)
        all_data += data
        if len(all_data) == 1:
            raise ValueError('没有找到任何数据！')
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
            "total_cbm",
            "destination", 
        ]
        df = pd.DataFrame.from_records(all_data)

        
        df["is_valid"] = None

        def get_est_pallet(n):
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)
        if 'total_n_pallet_est' not in df.columns:
            df['total_n_pallet_est'] = 0
        else:
            df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)

        if 'label' not in df.columns:
            df["label"] = "EST"
        df["est"] = df["label"] == "EST"
        df["Pallet Count"] = (
            df["total_n_pallet_est"] * df["est"]
        )
        df = df[keep].rename(
            {
                "fba_id": "PRO",
                "container_number__container_number": "BOL",
                "ref_id": "PO List (use , as separator) *",
                "total_pcs": "Carton Count",
                "total_cbm": "Total CBM",
                "destination": "Destination",
            },
            axis=1,
        )
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename=PO.csv"
        df.to_csv(path_or_buf=response, index=False)
        return response

    async def find_packinglist_ids_by_pallet_data(self, pallet_data):
        """
        根据pallet数据查找对应的packinglist IDs
        规则：根据PO_ID相同，且packinglist的fba_id和ref_id包含在pallet记录中
        """
        packinglist_ids = []
        unmatched_pallet_records = []
        
        for pallet_item in pallet_data:
            PO_ID = pallet_item.get('PO_ID')
            pallet_fba_id = pallet_item.get('fba_id', '').strip()
            pallet_ref_id = pallet_item.get('ref_id', '').strip()
            
            if not PO_ID:
                unmatched_pallet_records.append(pallet_item)
                continue
                
            # 构建查询条件
            base_query = models.Q(PO_ID=PO_ID)
            all_po_matching_packinglists = await sync_to_async(list)(
                PackingList.objects.filter(base_query).values('id', 'fba_id', 'ref_id')
            )
            matching_packinglists = [
                packinglist['id'] 
                for packinglist in all_po_matching_packinglists 
                if (not pallet_fba_id or (packinglist['fba_id'].strip() or '') in pallet_fba_id) and 
                (not pallet_ref_id or (packinglist['ref_id'].strip() or '') in pallet_ref_id)
            ]
            
            if matching_packinglists:
                packinglist_ids.extend(matching_packinglists)
            else:
                unmatched_pallet_records.append(pallet_item)
        
        # 去重并返回
        return list(set(packinglist_ids)), unmatched_pallet_records


    #这个是按照拿约的模板去导出   
    async def handle_export_pos_get_appointment(self, request: HttpRequest) -> HttpResponse:
        cargo_ids_str_list = request.POST.getlist("cargo_ids")
        pallet_ids = request.POST.getlist("plt_ids")

        packinglist_ids = []
        if cargo_ids_str_list and cargo_ids_str_list[0]:
            packinglist_ids = cargo_ids_str_list[0].split(',')
            packinglist_ids = [int(x) for x in packinglist_ids if x]
        else:
            packinglist_ids = []     
            
        if pallet_ids and pallet_ids[0]:
            pallet_ids = pallet_ids[0].split(',')
        else:
            pallet_ids = []
        if len(packinglist_ids) == 0 and pallet_ids == 0:
            raise ValueError('没有找到PO')
        all_data = []
        unmatched_pallet_data = []  # 存储找不到对应packinglist的pallet记录
        if pallet_ids:
            pallet_data = await sync_to_async(list)(
                Pallet.objects.select_related("container_number")
                .filter(id__in=pallet_ids)
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
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Sum("cbm", output_field=FloatField()),
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                )
                .distinct()
                .order_by("destination", "container_number__container_number")
            )

            # 根据pallet数据查找对应的packinglist IDs
            additional_packinglist_ids, unmatched_pallet_data = await self.find_packinglist_ids_by_pallet_data(pallet_data)
            # 合并packinglist IDs
            all_packinglist_ids = list(set(packinglist_ids + additional_packinglist_ids))
            # 处理未匹配的pallet数据（添加标记）
            if unmatched_pallet_data:
                expanded_unmatched_data = []
                for item in unmatched_pallet_data:
                    item = dict(item)
                    ref_ids = str(item["ref_id"]).split(",") if item["ref_id"] else [""]
                    
                    for ref_id in ref_ids:
                        new_row = item.copy()
                        new_row["ref_id"] = ref_id.strip()
                        new_row["check"] = "未找到对应PO记录，请手动处理"  # 添加特殊标记
                        new_row["is_unmatched"] = True  # 标记为未匹配记录
                        expanded_unmatched_data.append(new_row)
                all_data += expanded_unmatched_data
        else:
            all_packinglist_ids = packinglist_ids

        if all_packinglist_ids:
            packing_list_data = await sync_to_async(list)(
                PackingList.objects.select_related("container_number", "pallet")
                .filter(id__in=all_packinglist_ids)
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
                    total_pcs=Sum("pcs"),
                    total_cbm=Sum("cbm"),
                    total_weight_lbs=Sum("total_weight_lbs"),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                    label=Value("EST"),
                )
                .distinct()
                .order_by("destination", "container_number__container_number")
            )
            all_data += packing_list_data
        
        
        for p in all_data:
            if p.get("is_unmatched"):
                continue
            try:
                pl = await sync_to_async(PoCheckEtaSeven.objects.get)(
                    container_number__container_number=p["container_number__container_number"],
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
        data = [i for i in all_data]
        export_format = request.POST.get("export_format", "PO")
        if export_format == "PO":
            keep = [
                "fba_id",
                "container_number__container_number",
                "destination",
                "total_cbm",
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

        if "total_n_pallet_est" not in df.columns:
            df["total_n_pallet_est"] = 0
        if "total_n_pallet_act" not in df.columns:
            df["total_n_pallet_act"] = 0
        def get_est_pallet(n):
            if pd.isna(n) or n is None:
                return 0
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)

        if "total_n_pallet_est" in df.columns:
            df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)

        df["est"] = df["label"] == "EST"
        df["act"] = df["label"] == "ACT"

        df["Pallet Count"] = (
            df.get("total_n_pallet_act", 0) * df["act"]
            + df.get("total_n_pallet_est", 0) * df["est"]
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


    async def handle_unscheduled_pos_all_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {
            "warehouse_options": self.warehouse_options
        }
        #if await self._validate_user_four_major_whs(request.user):
        #不看权限了，就默认打开就是四大仓的
        warehouse = 'LA-91761'
        new_request = HttpRequest()
        new_request.method = request.method
        new_request.POST = request.POST.copy()
        new_request.POST['warehouse'] = warehouse
        new_request.user = request.user
        return await self.handle_unscheduled_pos_post(new_request)
    
    async def handle_appointment_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_main_dash, context

    async def handle_td_unshipment_get(    
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_td_unshipment, context

    async def handle_td_shipment_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_td_shipment, context
    
    async def handle_fleet_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_fleet_schedule, context
    
    async def _update_shipment_totals(self, sp_base_q):
        """
        更新shipment的总重量、总体积和总板数
        """
        shipment_list = await sync_to_async(list)(
            Shipment.objects.filter(sp_base_q).order_by("pickup_time", "shipment_appointment")
        )

        for shipment in shipment_list:
            total_weight = 0
            total_cbm = 0
            total_pallet = 0
            
            # 查询关联的packinglist
            packinglists = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number=shipment,
                    container_number__orders__offload_id__offload_at__isnull=True
                )
            )
            for packinglist in packinglists:
                if packinglist.total_weight_lbs:
                    total_weight += packinglist.total_weight_lbs
                if packinglist.cbm:
                    total_cbm += packinglist.cbm
                    total_pallet += math.ceil(packinglist.cbm / 2)
            
            # 查询关联的pallet
            pallets = await sync_to_async(list)(
                Pallet.objects.filter(
                    shipment_batch_number=shipment,
                    container_number__orders__offload_id__offload_at__isnull=False
                )
            )
            for pallet in pallets:
                if pallet.weight_lbs:
                    total_weight += pallet.weight_lbs
                if pallet.cbm:
                    total_cbm += pallet.cbm
                total_pallet += 1
            # 更新shipment的总重量和总体积
            shipment.total_weight = float(round(total_weight, 3))
            shipment.total_cbm = float(round(total_cbm, 3))
            shipment.total_pallet = int(total_pallet)
            shipment.shipped_weight = float(round(total_weight, 3))
            shipment.shipped_cbm = float(round(total_cbm, 3))
            shipment.shipped_pallet = int(total_pallet)
            await sync_to_async(shipment.save, thread_sensitive=True)()
              
    
    async def _fl_unscheduled_data(
        self, request: HttpRequest, warehouse:str, four_major_whs: str | None = None, group: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        target_date = datetime(2025, 10, 10)
        base_q = models.Q(
            origin=warehouse,
            fleet_number__isnull=True,
            in_use=True,
            is_canceled=False,
            is_notified_customer=True,
        )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            sp_base_q = base_q & models.Q(shipment_type__in=['LTL', '客户自提'])
        else:
            sp_base_q = base_q & models.Q(shipment_type="FTL")

        if four_major_whs == "four_major_whs":          
            sp_base_q &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)

        await self._update_shipment_totals(sp_base_q)

        shipment_list = await sync_to_async(list)(
            Shipment.objects.filter(sp_base_q).order_by("pickup_time", "shipment_appointment")
        )

        f_base_q = models.Q(
            origin=warehouse,
            departured_at__isnull=True,
            is_canceled=False,
        )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            fl_base_q = f_base_q & models.Q(fleet_type__in=['LTL', '客户自提'])
        else:
            fl_base_q = f_base_q & models.Q(fleet_type="FTL")

        if four_major_whs == "four_major_whs":
            fl_base_q &= models.Q(shipment__destination__in=FOUR_MAJOR_WAREHOUSES)
        
        #先查一下有没有没类型的车次，补上类型    
        await self._update_fleets_type(fl_base_q,target_date)

        fleet = await sync_to_async(list)(
            Fleet.objects.filter(fl_base_q).filter(
                Q(appointment_datetime__gt=target_date) | Q(appointment_datetime__isnull=True)
            )
            .prefetch_related("shipment")
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )
        # 在获取fleet列表后，添加具体柜号、仓点等详情
        for fleet_obj in fleet:
            detailed_shipments = []
            
            # 获取该车队的所有shipment
            shipments = await sync_to_async(list)(fleet_obj.shipment.all())
            if shipments:
                all_notified = all(shipment.is_notified_customer for shipment in shipments)
                fleet_obj.is_notified_customer = all_notified
            else:
                fleet_obj.is_notified_customer = False

            for shipment in shipments:
                shipment_batch_number = shipment.shipment_batch_number
                
                packinglists = await sync_to_async(list)(
                    PackingList.objects.filter(
                        shipment_batch_number__shipment_batch_number=shipment_batch_number,
                        container_number__orders__offload_id__offload_at__isnull=True
                    ).select_related('container_number')
                    .values('container_number__container_number', 'destination')
                    .annotate(
                        total_cbm=Sum('cbm'),
                        pallet_count=ExpressionWrapper(
                            Sum('cbm') / 2, 
                            output_field=FloatField()
                        )
                    )
                )
                pallets = await sync_to_async(list)(
                    Pallet.objects.filter(
                        shipment_batch_number__shipment_batch_number=shipment_batch_number,
                        container_number__orders__offload_id__offload_at__isnull=False
                    ).select_related('container_number')
                    .values('container_number__container_number', 'destination','is_dropped_pallet')
                    .annotate(
                        total_cbm=Sum('cbm'),
                        pallet_count=Count('id')  # pallet的板数就是数量
                    )
                )
                
                # 构建统一格式的数据
                for item in packinglists:
                    detailed_shipments.append({
                        "type": "EST",
                        "container_number": item["container_number__container_number"],
                        "destination": item["destination"],
                        "cbm": float(item["total_cbm"]) if item["total_cbm"] else 0,
                        "pallet_count": math.ceil(float(item["pallet_count"])) if item["pallet_count"] else 0,  # 向上取整
                        "shipment_batch_number": shipment_batch_number if shipment_batch_number else "",
                        "appointment_id": shipment.appointment_id,
                        "scheduled_time": shipment.shipment_appointment.strftime("%Y-%m-%d %H:%M") if shipment.shipment_appointment else "",
                        "note": shipment.note or "",
                        "is_dropped_pallet": False,
                    })
                
                for item in pallets:
                    detailed_shipments.append({
                        "type": "ACT",
                        "container_number": item["container_number__container_number"],
                        "destination": item["destination"],
                        "cbm": float(item["total_cbm"]) if item["total_cbm"] else 0,
                        "pallet_count": item["pallet_count"] or 0,
                        "shipment_batch_number": shipment_batch_number if shipment_batch_number else "",
                        "appointment_id": shipment.appointment_id,
                        "scheduled_time": shipment.shipment_appointment.strftime("%Y-%m-%d %H:%M") if shipment.shipment_appointment else "",
                        "note": shipment.note or "",
                        "is_dropped_pallet": item["is_dropped_pallet"],
                    })         
            fleet_obj.detailed_shipments = json.dumps(detailed_shipments) 
        context = {
            "shipment_list": shipment_list,
            "fleet_list": fleet,
        }
        return context

    async def _update_fleets_type(self, fl_base_q, target_date):
        fleets_without_type = await sync_to_async(list)(
            Fleet.objects.filter(fl_base_q).filter(
                Q(appointment_datetime__gt=target_date) | Q(appointment_datetime__isnull=True)
            ).filter(
                Q(fleet_type__isnull=True) | Q(fleet_type='')  # 直接筛选出没有类型的车队
            )
        )
        
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
            
            fleet_type = list(shipment_types)[0]
            fleet.fleet_type = fleet_type
            await fleet.asave()
    
    async def _fl_delivery_get(
        self, warehouse:str, four_major_whs: str | None = None, group:str |None = None
    ) -> dict[str, Any]:
        
        base_criteria = models.Q(
            is_arrived=False,
            is_canceled=False,
            is_shipped=True,
            origin=warehouse,
            fleet_number__isnull=False,
        ) & ~Q(status="Exception")

        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            criteria = base_criteria & models.Q(shipment_type__in=['LTL', '客户自提'])
        else:
            criteria = base_criteria & models.Q(shipment_type="FTL")

        if four_major_whs == "four_major_whs":
            criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)

        shipments = await sync_to_async(list)(
            Shipment.objects.prefetch_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        for s in shipments:
            pl_pairs = await sync_to_async(list)(
                PackingList.objects.select_related("container_number")
                .filter(
                    container_number__orders__offload_id__offload_at__isnull=False,
                    shipment_batch_number=s
                )
                .values("container_number__container_number", "shipping_mark")
                .distinct()
            )
            plt_pairs = await sync_to_async(list)(
                Pallet.objects.select_related("container_number")
                .filter(
                    container_number__orders__offload_id__offload_at__isnull=False,
                    shipment_batch_number=s
                )
                .values("container_number__container_number", "shipping_mark")
                .distinct()
            )
            pairs_set = set()
            for d in pl_pairs + plt_pairs:
                cont = d.get("container_number__container_number")
                mark = d.get("shipping_mark")
                if cont and mark:
                    pairs_set.add((cont, mark))
            s.container_mark_pairs = [
                {"container_number": c, "shipping_mark": m} for c, m in sorted(pairs_set)
            ]
        shipment_fleet_dict = {}
        for s in shipments:
            if s.fleet_number is None:
                continue
            if s.shipment_appointment is None:
                shipment_appointment = ""
            else:
                shipment_appointment = s.shipment_appointment.replace(
                    microsecond=0
                ).isoformat()
            if s.fleet_number.fleet_number not in shipment_fleet_dict:
                shipment_fleet_dict[s.fleet_number.fleet_number] = [
                    {
                        "shipment_batch_number": s.shipment_batch_number,
                        "appointment_id": s.appointment_id,
                        "destination": s.destination,
                        "carrier": s.carrier,
                        "shipment_appointment": shipment_appointment,
                        "origin": s.origin,
                    }
                ]
            else:
                shipment_fleet_dict[s.fleet_number.fleet_number].append(
                    {
                        "shipment_batch_number": s.shipment_batch_number,
                        "appointment_id": s.appointment_id,
                        "destination": s.destination,
                        "carrier": s.carrier,
                        "shipment_appointment": shipment_appointment,
                        "origin": s.origin,
                    }
                )
        context = {
            "shipments": shipments, #待确认送达批次
            "abnormal_fleet_options": self.abnormal_fleet_options,
            #"shipment": json.dumps(shipment_fleet_dict),
        }
        return context
    
    async def _fl_pod_get(
        self, warehouse:str, four_major_whs: str | None = None, group: str | None = None
    ) -> dict[str, Any]: 

        criteria = models.Q(
            models.Q(models.Q(pod_link__isnull=True) | models.Q(pod_link="")),
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_schduled_at__gte="2024-12-01",
            origin=warehouse,
        )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            criteria = criteria & models.Q(shipment_type__in=['LTL', '客户自提'])
        else:
            criteria = criteria & models.Q(shipment_type="FTL")

        if four_major_whs == "four_major_whs":
            criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)
        shipments = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        for shipment in shipments:
            # 获取与该shipment关联的所有pallet
            pallets = await sync_to_async(list)(
                Pallet.objects.filter(shipment_batch_number=shipment)
                .select_related('container_number')
            )
            
            customer_names = set()
            
            for pallet in pallets:
                if pallet.container_number:
                    # 获取与该container关联的所有order
                    orders = await sync_to_async(list)(
                        Order.objects.filter(container_number=pallet.container_number)
                        .select_related('customer_name')
                    )
                    
                    for order in orders:
                        if order.customer_name:
                            customer_names.add(order.customer_name.zem_name)
            
            # 将客户名用逗号拼接，并添加到shipment对象上
            shipment.customer = ", ".join(customer_names) if customer_names else "无客户信息"
        context = {
            "fleet": shipments,
        }
        return context
    
    async def _shipment_exceptions_data(
        self, warehouse:str
    ) -> dict[str, Any]:
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                status="Exception",
                is_canceled=False,
                origin=warehouse,
            ).order_by("shipment_appointment")
        )
        shipment_data = {
            s.shipment_batch_number: {
                "origin": s.origin,
                "load_type": s.load_type,
                "note": s.note if s.note and s.note != 'NaN' else '',
                "destination": s.destination,
                "address": s.address,
                "origin": s.origin,
            }
            for s in shipment
        }
        unused_appointment = await sync_to_async(list)(
            Shipment.objects.filter(in_use=False, is_canceled=False)
        )
        unused_appointment = {
            s.appointment_id: {
                "destination": s.destination.strip(),
                "shipment_appointment": s.shipment_appointment.replace(
                    microsecond=0
                ).isoformat(),
            }
            for s in unused_appointment
        }
        context = {
            "exception_shipments": shipment,           
            "unused_appointment": json.dumps(unused_appointment),
            "shipment_data": json.dumps(shipment_data),          
        }
        return context
    
    async def  handle_fleet_schedule_post(
        self, request: HttpRequest, context: dict| None = None
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        #异常约
        exception_sp = await self._shipment_exceptions_data(warehouse)
        #待出库
        ready_to_ship_data = await self._sp_ready_to_ship_data(warehouse,request.user)
        sum_fleet = []
        for i in ready_to_ship_data:
            sum_fleet.append(i['fleet_number'])
        # 待送达
        delivery_data = await self._fl_delivery_get(warehouse)
        #待传POD
        pod_data = await self._fl_pod_get(warehouse)
        
        summary = {
            'exception_count': len(exception_sp["exception_shipments"]),
            'ready_to_ship_count': len(ready_to_ship_data),
            'ready_count': len(delivery_data['shipments']),
            'pod_count': len(pod_data['fleet']),
        }
        if not context:
            context = {}
        context.update({
            'delivery_shipments': delivery_data['shipments'],
            'pod_shipments': pod_data['fleet'],
            'ready_to_ship_data': ready_to_ship_data,
            'sum_fleet': sum_fleet,
            'summary': summary,        
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "warehouse": warehouse,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "exception_shipments": exception_sp["exception_shipments"],
            "shipment_data": exception_sp["shipment_data"],
            "unused_appointment": exception_sp["unused_appointment"],
            'current_time': timezone.now(),
            "load_type_options": LOAD_TYPE_OPTIONS,
            "account_options": self.account_options,
            "shipment_type_options": self.shipment_type_options
        })     
        return self.template_fleet_schedule, context

    async def handle_td_unshipment_post(
        self, request: HttpRequest, context: dict| None = None, matching_suggestions: dict | None = None,
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        if not warehouse:
            if context:
                context.update({
                    "error_messages": "未选择仓库!",
                    'warehouse_options': self.warehouse_options
                })
            else:
                context = {
                    "error_messages":"未选择仓库!",
                    'warehouse_options': self.warehouse_options,
                }
            return self.template_td_unshipment, context
        st_type = request.POST.get("st_type", "pallet")
        # 生成匹配建议
        max_cbm, max_pallet = await self.get_capacity_limits(st_type)

        # 获取三类数据：未排约、已排约、待出库
        if not matching_suggestions:
            sp_result = await self.sp_unscheduled_data(
                warehouse, st_type, max_cbm, max_pallet, request.user
            )
            matching_suggestions = sp_result.get("matching_suggestions", [])
            destination_list = sp_result.get("destination_list", [])
        else:
            destination_list = []

        if not context:
            context = {}
        else:
            # 防止传入的 context 被意外修改
            context = context.copy()

        context.update({
            'warehouse': warehouse,
            'st_type': st_type,
            'matching_suggestions': matching_suggestions,
            'destination_list': destination_list,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "shipment_type_options": self.shipment_type_options,
            "carrier_options": self.carrier_options,
            'active_tab': request.POST.get('active_tab')
        }) 
        context["matching_suggestions_json"] = json.dumps(matching_suggestions, cls=DjangoJSONEncoder)
        context["warehouse_json"] = json.dumps(warehouse, cls=DjangoJSONEncoder)
        return self.template_td_unshipment, context
    
    async def _history_scheduled_data(self, warehouse: str, user, start_date, end_date) -> list:
        """获取已排约数据 - 按shipment_batch_number分组"""
        # 获取有shipment_batch_number但fleet_number为空的货物
        base_q = models.Q(
            shipment_appointment__gte=start_date,
            shipment_appointment__lte=end_date,
        )
        if "LA" in warehouse:
            # LA仓库：目的地要在LA_DES内，或者不在NJ_DES和SAV_DES内
            base_q &= (
                models.Q(destination__in=LA_DES) |
                ~models.Q(destination__in=NJ_DES + SAV_DES)
            )
        elif "NJ" in warehouse:
            # NJ仓库：目的地要在NJ_DES内，或者不在LA_DES和SAV_DES内
            base_q &= (
                models.Q(destination__in=NJ_DES) |
                ~models.Q(destination__in=LA_DES + SAV_DES)
            )
        elif "SAV" in warehouse:
            # SAV仓库：目的地要在SAV_DES内，或者不在LA_DES和NJ_DES内
            base_q &= (
                models.Q(destination__in=SAV_DES) |
                ~models.Q(destination__in=LA_DES + NJ_DES)
            )
        else:
            # 其他仓库：使用默认逻辑，或者根据具体需求调整
            pass
        shipment_list = await sync_to_async(list)(
            Shipment.objects.filter(base_q)
            .order_by("shipment_appointment")
        )
        result = []
    
        for shipment in shipment_list:
            # 初始化统计变量
            total_weight = 0.0
            total_cbm = 0.0
            total_pallet = 0  # 从0开始计数
            
            # 构建基础shipment信息
            shipment_data = {
                'shipment_batch_number': shipment.shipment_batch_number,
                'appointment_id': shipment.appointment_id,
                'shipment_cargo_id': shipment.shipment_cargo_id,
                'destination': shipment.destination,
                'shipment_appointment': shipment.shipment_appointment,
                'pickup_time': shipment.pickup_time,
                'load_type': shipment.load_type,
                'shipment_account': shipment.shipment_account,
                'origin': shipment.origin,
                'pickup_time': shipment.pickup_time,
                'pickup_number': shipment.pickup_number,
                'in_use': shipment.in_use,
                'is_canceled': shipment.is_canceled,
                'is_notified_customer': shipment.is_notified_customer,
                'cargos': []  # 绑定的PO列表
            }
            
            # 查询关联的packinglist (有入仓时间的)
            packinglists = await sync_to_async(list)(
                PackingList.objects.select_related("container_number").filter(
                    shipment_batch_number=shipment,
                    container_number__orders__offload_id__offload_at__isnull=False
                )
            )
            
            # 查询关联的pallet (有入仓时间的)
            pallets = await sync_to_async(list)(
                Pallet.objects.select_related("container_number").filter(
                    shipment_batch_number=shipment,
                    container_number__orders__offload_id__offload_at__isnull=False
                )
            )
            containers = []
            # 处理packinglist数据并计算统计
            for pl in packinglists:
                # 计算重量
                pl_weight = pl.total_weight_lbs or 0.0
                total_weight += pl_weight
                
                # 计算体积
                pl_cbm = pl.cbm or 0.0
                total_cbm += pl_cbm
                
                # 计算板数 - 优先使用实际板数，如果没有则根据CBM估算
                pl_pallet = math.ceil(pl_cbm / 2) if pl_cbm > 0 else 0
                
                total_pallet += pl_pallet
                
                cargo_data = {
                    'ids': str(pl.id),
                    'plt_ids': '',
                    'ref_ids': pl.ref_id or '',
                    'fba_ids': pl.fba_id or '',
                    'cns': pl.container_number.container_number if pl.container_number else '',
                    'destination': pl.destination or '',
                    'total_pallet': total_pallet,
                    'total_weight_lbs': pl_weight,
                    'total_cbm': pl_cbm,
                    'label': 'EST',
                    'offload_time': None,
                    'note_sp': pl.note_sp or ''
                }
                shipment_data['cargos'].append(cargo_data)
                containers.append(f"{pl.container_number.container_number} - {pl.destination}")
            
            # 处理pallet数据并计算统计
            for pallet in pallets:
                # 计算重量
                pallet_weight = pallet.weight_lbs or 0.0
                total_weight += pallet_weight
                
                # 计算体积
                pallet_cbm = pallet.cbm or 0.0
                total_cbm += pallet_cbm
                
                # 每个pallet算1板
                pallet_count = 1
                total_pallet += pallet_count
                
                cargo_data = {
                    'ids': '',
                    'plt_ids': str(pallet.id),
                    'ref_ids': pallet.ref_id or '',
                    'fba_ids': pallet.fba_id or '',
                    'cns': pallet.container_number.container_number if pallet.container_number else '',
                    'destination': pallet.destination or '',
                    'total_pallet': total_pallet,  # 实际计算的板数
                    'total_weight_lbs': pallet_weight,
                    'total_cbm': pallet_cbm,
                    'label': 'ACT',
                    'offload_time': None,
                    'note_sp': pallet.note_sp or ''
                }
                shipment_data['cargos'].append(cargo_data)
                containers.append(f"{pallet.container_number.container_number} - {pallet.destination}")
            containers = list(set(containers))
            new_containers = '\n'.join(set(containers))
            # 更新shipment的总统计信息
            shipment_data.update({
                'total_weight': float(round(total_weight, 3)),
                'total_cbm': float(round(total_cbm, 3)),
                'total_pallet': int(total_pallet),
                'shipped_weight': float(round(total_weight, 3)),
                'shipped_cbm': float(round(total_cbm, 3)),
                'shipped_pallet': int(total_pallet),
                'container_numbers': new_containers
            })
            
            # 异步更新数据库中的shipment记录
            shipment.total_weight = total_weight
            shipment.total_cbm = total_cbm
            shipment.total_pallet = total_pallet
            shipment.shipped_weight = total_weight
            shipment.shipped_cbm = total_cbm
            shipment.shipped_pallet = total_pallet
            await sync_to_async(shipment.save)()
            
            result.append(shipment_data)
    

        return result
    
    async def _history_scheduled_fleet_data(self, warehouse: str, user, start_date, end_date) -> list:
        fl_base_q = models.Q(
            origin=warehouse,
            fleet_type="FTL",
            appointment_datetime__gte=start_date,
            appointment_datetime__lte=end_date,
        )
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(fl_base_q)
            .prefetch_related("shipment")
            .annotate(
                shipment_batch_numbers=StringAgg(
                    "shipment__shipment_batch_number", delimiter=","
                ),
                appointment_ids=StringAgg("shipment__appointment_id", delimiter=","),
            )
            .order_by("appointment_datetime")
        )
        # 在获取fleet列表后，添加具体柜号、仓点等详情
        for fleet_obj in fleet:
            detailed_shipments = []
            
            # 获取该车队的所有shipment
            shipments = await sync_to_async(list)(fleet_obj.shipment.all())
            if shipments:
                all_notified = all(shipment.is_notified_customer for shipment in shipments)
                fleet_obj.is_notified_customer = all_notified
            else:
                fleet_obj.is_notified_customer = False

            for shipment in shipments:
                shipment_batch_number = shipment.shipment_batch_number
                
                packinglists = await sync_to_async(list)(
                    PackingList.objects.filter(
                        shipment_batch_number__shipment_batch_number=shipment_batch_number,
                        container_number__orders__offload_id__offload_at__isnull=True
                    ).select_related('container_number')
                    .values('container_number__container_number', 'destination')
                    .annotate(
                        total_cbm=Sum('cbm'),
                        pallet_count=ExpressionWrapper(
                            Sum('cbm') / 2, 
                            output_field=FloatField()
                        )
                    )
                )
                pallets = await sync_to_async(list)(
                    Pallet.objects.filter(
                        shipment_batch_number__shipment_batch_number=shipment_batch_number,
                        container_number__orders__offload_id__offload_at__isnull=False
                    ).select_related('container_number')
                    .values('container_number__container_number', 'destination','is_dropped_pallet')
                    .annotate(
                        total_cbm=Sum('cbm'),
                        pallet_count=Count('id')  # pallet的板数就是数量
                    )
                )
                
                # 构建统一格式的数据
                for item in packinglists:
                    detailed_shipments.append({
                        "type": "EST",
                        "container_number": item["container_number__container_number"],
                        "destination": item["destination"],
                        "cbm": float(item["total_cbm"]) if item["total_cbm"] else 0,
                        "pallet_count": math.ceil(float(item["pallet_count"])) if item["pallet_count"] else 0,  # 向上取整
                        "shipment_batch_number": shipment_batch_number if shipment_batch_number else "",
                        "appointment_id": shipment.appointment_id,
                        "scheduled_time": shipment.shipment_appointment.strftime("%Y-%m-%d %H:%M") if shipment.shipment_appointment else "",
                        "note": shipment.note or "",
                        "is_dropped_pallet": False,
                    })
                
                for item in pallets:
                    detailed_shipments.append({
                        "type": "ACT",
                        "container_number": item["container_number__container_number"],
                        "destination": item["destination"],
                        "cbm": float(item["total_cbm"]) if item["total_cbm"] else 0,
                        "pallet_count": item["pallet_count"] or 0,
                        "shipment_batch_number": shipment_batch_number if shipment_batch_number else "",
                        "appointment_id": shipment.appointment_id,
                        "scheduled_time": shipment.shipment_appointment.strftime("%Y-%m-%d %H:%M") if shipment.shipment_appointment else "",
                        "note": shipment.note or "",
                        "is_dropped_pallet": item["is_dropped_pallet"],
                    })         
            fleet_obj.detailed_shipments = json.dumps(detailed_shipments) 
        return fleet
    
    async def handle_history_shipment_post(
        self, request: HttpRequest, context: dict| None = None, 
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        if not warehouse:
            if context:
                context.update({
                    "error_messages": "未选择仓库!",
                    'warehouse_options': self.warehouse_options
                })
            else:
                context = {
                    "error_messages":"未选择仓库!",
                    'warehouse_options': self.warehouse_options,
                }
            return self.template_history_shipment, context
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        
        #已排约
        scheduled_data = await self._history_scheduled_data(warehouse, request.user, start_date, end_date)

        #已排车
        schedule_fleet_data = await self._history_scheduled_fleet_data(request, warehouse,start_date, end_date)
        if not context:
            context = {}
        else:
            # 防止传入的 context 被意外修改
            context = context.copy()
        summary = {
            'shipments':len(scheduled_data),
            'fleets': len(schedule_fleet_data),
        }
        context.update({
            'warehouse': warehouse,
            'scheduled_data': scheduled_data,
            'fleet_list': schedule_fleet_data,   #已排车
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "load_type_options": self.load_type_options,
            "shipment_type_options": self.shipment_type_options,
            "carrier_options": self.carrier_options,
            'active_tab': request.POST.get('active_tab'),
            'summary': summary,
            'start_date': start_date,
            'end_date': end_date,
        }) 
        return self.template_history_shipment, context
    
    async def handle_td_shipment_post(
        self, request: HttpRequest, context: dict| None = None, matching_suggestions: dict | None = None,
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        if not warehouse:
            if context:
                context.update({
                    "error_messages": "未选择仓库!",
                    'warehouse_options': self.warehouse_options
                })
            else:
                context = {
                    "error_messages":"未选择仓库!",
                    'warehouse_options': self.warehouse_options,
                }
            return self.template_td_shipment, context
        st_type = request.POST.get("st_type", "pallet")
        # 生成匹配建议
        max_cbm, max_pallet = await self.get_capacity_limits(st_type)
        
        # 获取三类数据：未排约、已排约、待出库
        if not matching_suggestions:
            sp_result = await self.sp_unscheduled_data(
                warehouse, st_type, 1000, 1000, request.user
            )
            matching_suggestions = sp_result.get("matching_suggestions", [])
            destination_list = sp_result.get("destination_list", [])
        else:
            destination_list = []
        #已排约
        scheduled_data = await self.sp_scheduled_data(warehouse, request.user)

        #未排车+已排车
        fleets = await self._fl_unscheduled_data(request, warehouse)
        #未排车
        unschedule_fleet_data = fleets['shipment_list']
        #已排车
        schedule_fleet_data = fleets['fleet_list']
        # 获取可用预约
        available_shipments = await self.sp_available_shipments(warehouse, st_type)
        
        # 计算统计数据
        summary = await self._sp_calculate_summary(matching_suggestions, scheduled_data, schedule_fleet_data, unschedule_fleet_data)       

        if not context:
            context = {}
        else:
            # 防止传入的 context 被意外修改
            context = context.copy()

        context.update({
            'warehouse': warehouse,
            'st_type': st_type,
            'matching_suggestions': matching_suggestions,
            'destination_list': destination_list,
            'scheduled_data': scheduled_data,
            'unschedule_fleet': unschedule_fleet_data,
            'fleet_list': schedule_fleet_data,   #已排车
            'unscheduled_fl_count': len(unschedule_fleet_data),
            'available_shipments': available_shipments,
            'summary': summary,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "load_type_options": self.load_type_options,
            "shipment_type_options": self.shipment_type_options,
            "carrier_options": self.carrier_options,
            'active_tab': request.POST.get('active_tab')
        }) 
        context["matching_suggestions_json"] = json.dumps(matching_suggestions, cls=DjangoJSONEncoder)
        context["warehouse_json"] = json.dumps(warehouse, cls=DjangoJSONEncoder)
        return self.template_td_shipment, context
    
    async def sp_unscheduled_data(self, warehouse: str, st_type: str, max_cbm, max_pallet, user) -> dict:
        """获取未排约数据"""
        delivery_method_filter = (
            ~models.Q(delivery_method__icontains='暂扣') &
            ~models.Q(delivery_method__icontains='UPS') &
            ~models.Q(delivery_method__icontains='FEDEX') &
            ~models.Q(delivery_method__icontains='自提')
        )
        has_any_timestamp = (
            models.Q(container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False) |
            models.Q(container_number__orders__retrieval_id__target_retrieval_timestamp_lower__isnull=False) |
            (
                models.Q(container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False) &
                models.Q(container_number__orders__retrieval_id__actual_retrieval_timestamp__gt=datetime(2025, 2, 1))
            )
        )
        unshipment_pos = await self._get_packing_list(
            user,
            has_any_timestamp
            & delivery_method_filter
            & models.Q(
                container_number__orders__offload_id__offload_at__isnull=True,
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__retrieval_id__retrieval_destination_precise=warehouse,
                delivery_type='public',
            ),
            delivery_method_filter
            & models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__gt=datetime(2025, 1, 1),
                location=warehouse,
                delivery_type='public',
            ), True
        )
             
        # 获取可用的shipment记录（shipment_batch_number为空的）
        shipments = await self._get_available_shipments(warehouse)
        # 生成智能匹配建议
        matching_suggestions = await self._generate_matching_suggestions(unshipment_pos, shipments, warehouse, max_cbm, max_pallet,st_type, user)
        
        destination_totals = {}
        for suggestion in matching_suggestions:
            primary_group = suggestion.get("primary_group", {})
            dest = (primary_group.get("destination") or "").strip()
            if not dest:
                continue
            cbm_value = primary_group.get("total_cbm", 0) or 0
            try:
                cbm_float = float(cbm_value)
            except Exception:
                cbm_float = 0.0
            destination_totals[dest] = destination_totals.get(dest, 0.0) + cbm_float

        destination_list = [
            {"destination": dest, "total_cbm": round(total, 2)}
            for dest, total in destination_totals.items()
        ]
        destination_list.sort(key=lambda x: x.get("total_cbm", 0), reverse=True)

        return {
            "matching_suggestions": matching_suggestions,
            "destination_list": destination_list,
        }

    async def _get_available_shipments(self, warehouse: str):
        """获取可用的shipment记录"""
        now = timezone.now()
        # 这里需要根据您的实际模型调整查询条件
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(
                models.Q(shipment_batch_number__isnull=True) | models.Q(shipment_batch_number=''),
                in_use=False,
                is_canceled=False,
                shipment_appointment__gt=now  
            ).order_by('shipment_appointment') 
        )
        return shipments

    async def _generate_matching_suggestions(self, unshipment_pos, shipments, warehouse, max_cbm, max_pallet,st_type, user):
        """生成智能匹配建议 - 基于功能A的逻辑但适配shipment匹配"""
        suggestions = []

        # 第一级分组：按目的地和派送方式预分组
        pre_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                raw_dest = (cargo.get('destination') or '').strip()
                dest = raw_dest.split('-')[-1].strip().upper() if raw_dest else ''

                delivery_method = cargo.get('custom_delivery_method')
                if not dest or not delivery_method:
                    continue
                    
                group_key = f"{dest}_{delivery_method}"
                if group_key not in pre_groups:
                    pre_groups[group_key] = {
                        'destination': dest,
                        'delivery_method': delivery_method,
                        'cargos': []
                    }
                pre_groups[group_key]['cargos'].append(cargo)

        # 在预分组循环之前初始化已使用的shipment集合
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__orders__vessel_id__vessel_eta') or '')
            
            # 按容量限制创建大组
            primary_groups = []
            current_primary_group = {
                'destination': pre_group['destination'],
                'delivery_method': pre_group['delivery_method'],
                'cargos': [],
                'total_pallets': 0,
                'total_cbm': 0,
            }
            
            for cargo in sorted_cargos:
                cargo_pallets = 0
                if cargo.get('label') == 'ACT':
                    cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                else:
                    cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                
                cargo_cbm = cargo.get('total_cbm', 0) or 0
                # 检查当前大组是否还能容纳这个货物
                if (current_primary_group['total_pallets'] + cargo_pallets <= max_pallet and 
                    current_primary_group['total_cbm'] + cargo_cbm <= max_cbm):
                    # 可以加入当前大组
                    current_primary_group['cargos'].append(cargo)
                    current_primary_group['total_pallets'] += cargo_pallets
                    current_primary_group['total_cbm'] += cargo_cbm
                else:
                    # 当前大组已满，保存并创建新的大组
                    if current_primary_group['cargos']:
                        primary_groups.append(current_primary_group)
                    
                    # 创建新的大组
                    current_primary_group = {
                        'destination': pre_group['destination'],
                        'delivery_method': pre_group['delivery_method'],
                        'cargos': [cargo],
                        'total_pallets': cargo_pallets,
                        'total_cbm': cargo_cbm,
                    }
            
            # 添加最后一个大组
            if current_primary_group['cargos']:
                primary_groups.append(current_primary_group)
            
            # 为每个大组寻找匹配的shipment
            for primary_group_index, primary_group in enumerate(primary_groups):
                # 计算大组的匹配度百分比
                pallets_percentage = min(100, (primary_group['total_pallets'] / max_pallet) * 100) if max_pallet > 0 else 0
                cbm_percentage = min(100, (primary_group['total_cbm'] / max_cbm) * 100) if max_cbm > 0 else 0
                
                # 寻找匹配的shipment，这里改成返回列表
                matched_shipment = await self._find_matching_shipment(primary_group, shipments, warehouse)
                
                # 如果匹配到shipment，将其标记为已使用
                result_intel = await self._find_intelligent_po_for_group(
                    primary_group, warehouse, user
                )
                
                intelligent_pos = result_intel['intelligent_pos']
                intelligent_pos_stats = result_intel['intelligent_pos_stats']
                
                intelligent_cargos = [{
                    'unique_id': pos.get('unique_id', ''),
                    'ids': pos.get('ids', ''),
                    'plt_ids': pos.get('plt_ids', ''),
                    'ref_ids': pos.get('ref_ids', ''),
                    'fba_ids': pos.get('fba_ids', ''),
                    'container_numbers': pos.get('container_numbers', ''),
                    'cns': pos.get('cns', ''),
                    'offload_time': pos.get('offload_time',''),
                    'delivery_window_start': pos.get('delivery_window_start'),
                    'delivery_window_end': pos.get('delivery_window_end'),
                    'total_n_pallet_act': pos.get('total_n_pallet_act') or pos.get('total_n_pallet_est', 0),
                    'total_cbm': pos.get('total_cbm', 0),
                    'total_weight': pos.get('total_weight', 0),
                    'label': pos.get('label', ''),
                    'destination': pos.get('destination', ''),
                    'location': pos.get('location') if pos.get('location') else pos.get('warehouse', ''),
                    'custom_delivery_method': pos.get('custom_delivery_method', ''),
                } for pos in intelligent_pos]
                
                # 无论是否匹配到shipment，都创建建议分组
                suggestion = {
                    'suggestion_id': f"{group_key}_{primary_group_index}",
                    'primary_group': {
                        'destination': primary_group['destination'],
                        'delivery_method': primary_group['delivery_method'],
                        'total_pallets': primary_group['total_pallets'],
                        'total_cbm': primary_group['total_cbm'],
                        'pallets_percentage': pallets_percentage,
                        'cbm_percentage': cbm_percentage,
                        'matched_shipment': matched_shipment,  # 可能为None
                        'suggestion_id': f"{group_key}_{primary_group_index}"
                    },
                    'cargos': [{
                        'ids': cargo.get('ids', ''),
                        'plt_ids': cargo.get('plt_ids', ''),
                        'ref_ids': cargo.get('ref_ids', ''),
                        'fba_ids': cargo.get('fba_ids', ''),
                        'container_numbers': cargo.get('container_numbers', ''),
                        'cns': cargo.get('cns', ''),
                        'destination': cargo.get('destination', ''),
                        'offload_time': cargo.get('offload_time', ''),
                        'delivery_window_start': cargo.get('delivery_window_start'),
                        'delivery_window_end': cargo.get('delivery_window_end'),
                        'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                        'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                        'total_cbm': cargo.get('total_cbm', 0),
                        'total_weight': cargo.get('total_weight_lbs', 0),
                        'label': cargo.get('label', ''),
                        'is_dropped_pallet': cargo.get('is_dropped_pallet'),
                        'rebuilt_is_dropped_pallet': cargo.get('rebuilt_is_dropped_pallet'),
                    } for cargo in primary_group['cargos']],
                    'intelligent_cargos': intelligent_cargos,
                    'intelligent_pos_stats': intelligent_pos_stats,
                    'virtual_fleet': []
                }
                suggestions.append(suggestion)
        #查找可以一提多卸的可能
        await self.calculate_virtual_fleet(suggestions, max_cbm, max_pallet)
        def to_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0       
        if st_type == "卡板":
            suggestions.sort(
                key=lambda s: (
                    to_float(s['primary_group'].get('cbm_percentage')),
                    to_float(s['primary_group'].get('pallets_percentage'))
                ),
                reverse=True
            )
        else:
            suggestions.sort(
                key=lambda x: x['primary_group']['cbm_percentage'], 
                reverse=True
            )
        return suggestions

    async def calculate_virtual_fleet(self, suggestions, max_cbm, max_pallet):
        """计算每个大组可以合并装车的其他大组"""
        for i, current_suggestion in enumerate(suggestions):
            current_group = current_suggestion['primary_group']
            current_cbm = current_group['total_cbm']
            current_pallets = current_group['total_pallets']
            
            # 计算当前大组的剩余容量
            remaining_cbm = 100 - current_cbm
            remaining_pallets = 100 - current_pallets
            
            # 寻找可以合并的其他大组
            compatible_groups = []
            
            for j, other_suggestion in enumerate(suggestions):
                if i == j:  # 跳过自己
                    continue
                    
                other_group = other_suggestion['primary_group']
                other_cbm = other_group['total_cbm']
                other_pallets = other_group['total_pallets']
                
                # 检查是否可以合并（不超过剩余容量）
                if other_cbm <= remaining_cbm and other_pallets <= remaining_pallets:
                    # 计算匹配度分数 - 越接近剩余容量的优先级越高
                    cbm_match_score = other_cbm / remaining_cbm if remaining_cbm > 0 else 0
                    pallets_match_score = other_pallets / remaining_pallets if remaining_pallets > 0 else 0
                    total_match_score = (cbm_match_score + pallets_match_score) / 2
                    
                    compatible_groups.append({
                        'suggestion_id': other_group['suggestion_id'],
                        'cbm_percentage': other_group['cbm_percentage'],
                        'pallets_percentage': other_group['pallets_percentage'],
                        'total_cbm': other_cbm,
                        'total_pallets': other_pallets,
                        'match_score': total_match_score
                    })
            
            # 按匹配度分数排序，匹配度高的排在前面
            compatible_groups.sort(key=lambda x: x['match_score'], reverse=True)         
            # 只存储suggestion_id列表
            current_suggestion['virtual_fleet'] = [group['suggestion_id'] for group in compatible_groups]

    async def _find_intelligent_po_for_group(self, primary_group, warehouse, user) -> Any:
        existing_pl_ids = []
        existing_plt_ids = []
        destination = None

        for cargo in primary_group.get("cargos", []):
            destination = cargo.get("destination")
            id_str = cargo.get("ids")
            if id_str:
                existing_pl_ids.extend([r.strip() for r in id_str.split(",") if r.strip()])

            plt_str = cargo.get("plt_ids")
            if plt_str:
                existing_plt_ids.extend([int(p.strip()) for p in plt_str.split(",") if p.strip().isdigit()])

        #预想，NJ和SAV的可以考虑转仓，LA的就不考虑了，所以提供智能匹配意见时，LA的不考虑别的仓
        if "LA" in warehouse:
            location_condition = models.Q(location=warehouse)
            retrieval_condition = (
                models.Q(container_number__orders__retrieval_id__retrieval_destination_precise=warehouse) |
                models.Q(container_number__orders__warehouse__name=warehouse)
            )
        else:
            location_condition = models.Q(location__in=["NJ-07001", "SAV-31326"])
            retrieval_condition = (
                models.Q(container_number__orders__retrieval_id__retrieval_destination_precise__in=["NJ-07001", "SAV-31326"]) |
                models.Q(container_number__orders__warehouse__name__in=["NJ-07001", "SAV-31326"])
            )

        intelligent_pos = await self._get_packing_list(
            user,
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__isnull=True,
                destination=destination,
                delivery_type='public',
                
            ) & retrieval_condition
            & ~models.Q(id__in=existing_pl_ids),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__isnull=False,
                destination=destination,
                container_number__orders__offload_id__offload_at__gt=datetime(2025, 1, 1),
                delivery_type='public',
            ) & location_condition
            & ~models.Q(id__in=existing_plt_ids),
        )
        
        sorted_intelligent_pos = sorted(intelligent_pos, key=lambda x: (
            0 if (x.get('location') or x.get('warehouse', '')) == warehouse else 1,
        ))
        
        intelligent_cargos = [{
            'unique_id': str(uuid.uuid4()), 
            'ids': pos.get('ids', ''),
            'plt_ids': pos.get('plt_ids', ''),
            'ref_ids': pos.get('ref_ids', ''),
            'fba_ids': pos.get('fba_ids', ''),
            'container_numbers': pos.get('container_numbers', ''),
            'cns': pos.get('cns', ''),
            'offload_time': pos.get('offload_time',''),
            'delivery_window_start': pos.get('delivery_window_start'),
            'delivery_window_end': pos.get('delivery_window_end'),
            'total_n_pallet_act': pos.get('total_n_pallet_act', 0),
            'total_n_pallet_est': pos.get('total_n_pallet_est', 0),
            'total_cbm': pos.get('total_cbm', 0),
            'total_weight': pos.get('total_weight_lbs', 0),
            'label': pos.get('label', ''),
            'destination': pos.get('destination', ''),
            'custom_delivery_method': pos.get('custom_delivery_method', ''),
            'location': pos.get('location') if pos.get('location') else pos.get('warehouse', '')
        } for pos in sorted_intelligent_pos]

        organized = {
            'ACT': {'normal': [], 'hold': []},
            'EST': {'normal': [], 'hold': []}
        }
        
        for cargo in intelligent_cargos:
            label = cargo.get('label', 'EST')
            delivery_method = cargo.get('custom_delivery_method', '')
            is_hold = False
            if delivery_method:
                is_hold = '暂扣' in delivery_method
            else:
                continue
            if label == 'ACT':
                if is_hold:
                    organized['ACT']['hold'].append(cargo)
                else:
                    organized['ACT']['normal'].append(cargo)
            else: 
                if is_hold:
                    organized['EST']['hold'].append(cargo)
                else:
                    organized['EST']['normal'].append(cargo)
        intelligent_pos_stats = {
            'ACT_normal_count': len(organized['ACT']['normal']),
            'ACT_hold_count': len(organized['ACT']['hold']),
            'EST_normal_count': len(organized['EST']['normal']),
            'EST_hold_count': len(organized['EST']['hold']),
            'total_count': len(organized['ACT']['normal']) + len(organized['ACT']['hold']) + 
                        len(organized['EST']['normal']) + len(organized['EST']['hold'])
        }
        return {
            'intelligent_pos': intelligent_cargos,
            'intelligent_pos_stats':intelligent_pos_stats
            }
    
    async def _find_matching_shipment(self, primary_group, shipments, warehouse):
        """为货物大组寻找匹配的shipment"""
        destination = primary_group['destination']
        matched_shipments = []
        
        for shipment in shipments:
            # 检查这个shipment是否已经被其他组使用了
            # 检查目的地是否匹配
            shipment_destination = (shipment.destination or '').strip().upper()
            if not self._is_destination_match(destination, shipment_destination):
                continue

            if shipment.origin != warehouse:
                continue
            # 检查时间窗口条件
            if not await self.check_time_window_match(primary_group, shipment):
                continue         
            # 匹配成功，添加到匹配列表
            matched_shipment = {
                'shipment_id': shipment.id,
                'appointment_id': shipment.appointment_id,
                'shipment_cargo_id': shipment.shipment_cargo_id,
                'shipment_type': shipment.shipment_type,
                'shipment_appointment': shipment.shipment_appointment,
                'pickup_time': shipment.pickup_time,
                'pickup_number': shipment.pickup_number,
                'origin': shipment.origin,
                'load_type': shipment.load_type,
                'shipment_account': shipment.shipment_account,
                'address': shipment.address,
                'carrier': shipment.carrier,
                'note': shipment.note,
                'ARM_BOL': shipment.ARM_BOL,
                'ARM_PRO': shipment.ARM_PRO,
                'express_number': shipment.express_number,
                'address_detail': await self.get_address(destination),            
                'destination': shipment.destination
            }
            matched_shipments.append(matched_shipment)
        if matched_shipments:
            matched_shipments.sort(key=lambda x: x.get('shipment_appointment') or datetime.max)
            # 在结尾添加空的shipment选项
            empty_shipment = {
                'shipment_id': None,
                'appointment_id': "不选择预约号",
                'shipment_cargo_id': None,
                'shipment_type': None,
                'shipment_appointment': None,
                'pickup_time': None,
                'pickup_number': None,
                'origin': None,
                'load_type': None,
                'shipment_account': None,
                'address': None,
                'carrier': None,
                'note': "",
                'ARM_BOL': None,
                'ARM_PRO': None,
                'express_number': None,
                'address_detail': await self.get_address(destination),
                'destination': destination,
                'is_empty_option': True
            }
            
            matched_shipments.append(empty_shipment)
        return matched_shipments
    
    def _is_destination_match(self, group_destination, shipment_destination):
        """检查目的地是否匹配"""
        if not shipment_destination:
            return False
        
        # 简单的目的地匹配逻辑，您可以根据实际需求调整
        group_dest_clean = group_destination.split('-')[-1].strip().upper()
        shipment_dest_clean = shipment_destination.split('-')[-1].strip().upper()
        
        return group_dest_clean == shipment_dest_clean

    async def get_address(self,destination):
        if destination in amazon_fba_locations:
            fba = amazon_fba_locations[destination]
            address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
            return address
        
        # 如果直接查找不到，尝试添加Walmart-前缀查找
        walmart_destination = f"Walmart-{destination}"
        if walmart_destination in amazon_fba_locations:
            fba = amazon_fba_locations[walmart_destination]
            address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
            return address
        return None
        # 如果两种方式都找不到，报错
        raise ValueError(f'找不到这个目的地的地址，请核实{destination}（已尝试{walmart_destination}）')
        
    async def check_time_window_match(self, primary_group, shipment):
        """检查时间窗口是否匹配"""
        shipment_appointment = shipment.shipment_appointment
        if not shipment_appointment:
            return False
        
        shipment_date = shipment_appointment.date()
        # 检查小组中的每个货物
        for cargo in primary_group['cargos']:
            window_start = cargo.get('delivery_window_start')
            window_end = cargo.get('delivery_window_end')
            
            # 如果货物有时间窗口，检查shipment时间是否在窗口内
            if window_start and window_end:
                if not (window_start <= shipment_date <= window_end):
                    return False
            # 如果货物没有时间窗口，跳过时间检查（只要求目的地匹配）
        
        return True

    async def sp_scheduled_data(self, warehouse: str, user, four_major_whs: str | None = None, group: str | None = None) -> list:
        """获取已排约数据 - 按shipment_batch_number分组"""
        # 获取有shipment_batch_number但fleet_number为空的货物
        target_date = datetime(2025, 10, 10)

        pl_criteria = models.Q(
                container_number__orders__warehouse__name=warehouse,
                shipment_batch_number__isnull=False,             
                container_number__orders__offload_id__offload_at__isnull=True,
                shipment_batch_number__shipment_appointment__gt=target_date,
                shipment_batch_number__fleet_number__isnull=True,
            )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            pl_criteria = pl_criteria & models.Q(delivery_type='other')
        else:
            pl_criteria = pl_criteria & models.Q(delivery_type='public')

        plt_criteria = models.Q(
                shipment_batch_number__isnull=False,
                shipment_batch_number__shipment_appointment__gt=target_date,
                container_number__orders__offload_id__offload_at__isnull=False,
                shipment_batch_number__fleet_number__isnull=True,
                location=warehouse,
            )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            plt_criteria = plt_criteria & models.Q(delivery_type='other')
        else:
            plt_criteria = plt_criteria & models.Q(delivery_type='public')

        if four_major_whs == "four_major_whs":
            pl_criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)
            plt_criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)

        raw_data = await self._get_packing_list(
            user,
            pl_criteria,
            plt_criteria,
        )
        
        # 按shipment_batch_number分组
        grouped_data = {}
        processed_batch_numbers = set()

        for item in raw_data:           
            batch_number = item.get('shipment_batch_number__shipment_batch_number')
            if "库存盘点" in batch_number:
                continue
            if batch_number not in grouped_data:
                # 获取预约信息
                try:
                    shipment = await sync_to_async(Shipment.objects.get)(
                        shipment_batch_number=batch_number,
                        shipment_appointment__gte=datetime(2025, 1, 1)
                    )
                except Shipment.DoesNotExist:
                    continue
                except MultipleObjectsReturned:
                    raise ValueError(f"shipment_batch_number={batch_number} 查询到多条记录，请检查数据")
                if shipment.status == "Exception": #异常的约不展示
                    continue
                address = await self.get_address(shipment.destination)
                grouped_data[batch_number] = {
                    'appointment_id': shipment.appointment_id,
                    'shipment_cargo_id': shipment.shipment_cargo_id,
                    'shipment_batch_number': shipment.shipment_batch_number,
                    'shipment_type': shipment.shipment_type,
                    'destination': shipment.destination,
                    'shipment_appointment': shipment.shipment_appointment,
                    'load_type': shipment.load_type,
                    'shipment_account': shipment.shipment_account,
                    'address': shipment.address,
                    'address_detail': address,
                    'cargos': [],
                    'pickup_time': shipment.pickup_time,
                    'pickup_number': shipment.pickup_number,
                    'is_notified_customer': shipment.is_notified_customer,
                }
                processed_batch_numbers.add(batch_number)
            grouped_data[batch_number]['cargos'].append(item)
        
        # 查询没有货物的shipment记录
        if four_major_whs == "four_major_whs":
            base_q = Q(
                shipped_at__isnull=True,
                shipment_appointment__gt=target_date,
                shipment_type='FTL',
                is_canceled=False,
                in_use=True,
            )
        else:
            base_q = Q(
                Q(fleet_number__isnull=False, fleet_number__is_virtual=True) |
                Q(fleet_number__isnull=True),
                shipment_appointment__gt=target_date,
                shipment_type='FTL',
                is_canceled=False,
                in_use=True,
            )
        if four_major_whs == "four_major_whs":
            base_q &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)
        exclude_q = ~Q(shipment_batch_number__in=processed_batch_numbers)
        empty_shipments = await sync_to_async(list)(
            Shipment.objects.filter(base_q).exclude(exclude_q)
        )
        
        # 添加没有货物的shipment记录
        for shipment in empty_shipments:
            has_packinglist = await sync_to_async(PackingList.objects.filter(shipment_batch_number=shipment).exists)()
            has_pallet = await sync_to_async(Pallet.objects.filter(shipment_batch_number=shipment).exists)()
            
            if has_packinglist or has_pallet:
                continue  # 已经绑定货物，跳过
            batch_number = shipment.shipment_batch_number
            if batch_number not in grouped_data:
                address = await self.get_address(shipment.destination)
                grouped_data[batch_number] = {
                    'appointment_id': shipment.appointment_id,
                    'shipment_cargo_id': shipment.shipment_cargo_id,
                    'shipment_batch_number': shipment.shipment_batch_number,
                    'shipment_type': shipment.shipment_type,
                    'destination': shipment.destination,
                    'shipment_appointment': shipment.shipment_appointment,
                    'load_type': shipment.load_type,
                    'shipment_account': shipment.shipment_account,
                    'address': shipment.address,
                    'address_detail': address,
                    'cargos': [],  # 空列表表示没有货物
                    'pickup_time': shipment.pickup_time,
                    'pickup_number': shipment.pickup_number,
                    'is_notified_customer': shipment.is_notified_customer,
                }
        return list(grouped_data.values())

    async def _sp_ready_to_ship_data(self, warehouse: str, user, four_major_whs: str | None = None, group: str | None = None) -> list:
        """获取待出库数据 - 按fleet_number分组"""
        # 获取指定仓库的未出发且未取消的fleet
        base_bq = models.Q(
            origin=warehouse,
            departured_at__isnull=True,
            is_canceled=False,
        )
        if group and 'ltl' in group.lower():  # 如果group包含ltl（不区分大小写）
            base_q = base_bq & models.Q(fleet_type__in=['LTL', '客户自提'])
        else:
            base_q = base_bq & models.Q(fleet_type="FTL")

        if four_major_whs == "four_major_whs":
            base_q &= models.Q(shipment__destination__in=FOUR_MAJOR_WAREHOUSES)
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(base_q).prefetch_related(
                Prefetch(
                    'shipment',
                    queryset=Shipment.objects.prefetch_related(
                        Prefetch(
                            'packinglist',
                            queryset=PackingList.objects.select_related('container_number')
                        ),
                        Prefetch(
                            'pallet', 
                            queryset=Pallet.objects.select_related('packing_list', 'container_number')
                        )
                    )
                )
            )
        )
        
        grouped_data = []
        
        for fleet in fleets:
            fleet_group = {
                'fleet_number': fleet.fleet_number,
                'fleet_type': fleet.fleet_type,
                'fleet_cost': fleet.fleet_cost or 0,
                'third_party_address': fleet.third_party_address,
                'pickup_number': fleet.pickup_number,
                'motor_carrier_number': fleet.motor_carrier_number,
                'license_plate': fleet.license_plate,
                'dot_number': fleet.dot_number,
                'appointment_datetime': fleet.appointment_datetime,
                'carrier': fleet.carrier,
                'is_virtual': fleet.is_virtual,
                'shipments': {},  # 改回字典结构，保持与前端兼容
                'pl_ids': [],
                'plt_ids': [],
                'total_cargos': 0,  # 总货物行数
            }
            
            shipments = await sync_to_async(list)(
                Shipment.objects.filter(fleet_number__fleet_number=fleet.fleet_number)
            )
            
            for shipment in shipments:
                if not shipment.shipment_batch_number:
                    continue

                batch_number = shipment.shipment_batch_number
                
                # 初始化shipment数据
                if batch_number not in fleet_group['shipments']:
                    fleet_group['shipments'][batch_number] = {
                        'shipment_batch_number': shipment.shipment_batch_number or '-',
                        'appointment_id': shipment.appointment_id or '-',
                        'destination': shipment.destination or '-',
                        'shipment_appointment': shipment.shipment_appointment,
                        'cargos': []
                    }
                
                # 处理packinglists
                raw_data = await self._get_packing_list(
                    user,
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__orders__offload_id__offload_at__isnull=True,
                    ),
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__orders__offload_id__offload_at__isnull=False,
                    ),
                )
                fleet_group['shipments'][batch_number]['cargos'].extend(raw_data)
            
            # 排序 shipments，cargos 为空的放后面
            fleet_group['shipments'] = dict(
                sorted(
                    fleet_group['shipments'].items(),
                    key=lambda item: not item[1]['cargos']
                )
            )
            fleet_group['total_cargos'] = sum(
                len(s['cargos']) if s['cargos'] else 1
                for s in fleet_group['shipments'].values()
            )
            if fleet_group["total_cargos"] <= 0:
                fleet_group["total_cargos"] = 1
            # 只有有数据的fleet才返回
            #if fleet_group['shipments']:
            grouped_data.append(fleet_group)
        # 按 appointment_datetime 排序，时间早的排在前面
        grouped_data.sort(
            key=lambda x: (
                x['appointment_datetime'].replace(tzinfo=None)
                if x['appointment_datetime'] else datetime.max
            )
        )
        return grouped_data

    async def sp_available_shipments(self, warehouse: str, st_type: str) -> list:
        """获取可用预约"""
        now = timezone.now()
        shipments = await sync_to_async(list)(
            Shipment.objects.filter(
                Q(origin__isnull=True) | Q(origin="") | Q(origin=warehouse),
                appointment_id__isnull=False,
                in_use=False,
                is_canceled=False,
                load_type=st_type.upper()
            ).order_by("shipment_appointment")
        )
        
        # 添加状态信息
        for shipment in shipments:
            is_expired = shipment.shipment_appointment_utc and shipment.shipment_appointment_utc < now
            is_urgent = (
                shipment.shipment_appointment_utc and 
                (shipment.shipment_appointment_utc - now).days < 7 and
                shipment.shipment_appointment_utc >= now and
                not is_expired
            )
            
            if is_expired:
                shipment.status = 'expired'
            elif is_urgent:
                shipment.status = 'urgent'
            else:
                shipment.status = 'available'
        
        return shipments

    def _create_primary_groups(self, cargos: list, max_cbm: float, max_pallet: int) -> list:
        """按容量限制创建大组"""
        primary_groups = []
        current_group = {
            'cargos': [],
            'total_pallets': 0,
            'total_cbm': 0,
            'destination': '',
            'delivery_method': ''
        }
        
        # 直接遍历，不排序
        for cargo in cargos:
            cargo_pallets = cargo.get('total_n_pallet_act', 0) or cargo.get('total_n_pallet_est', 0) or 0
            cargo_cbm = cargo.get('total_cbm', 0) or 0
            
            if not current_group['destination']:
                current_group['destination'] = cargo.get('destination')
                current_group['delivery_method'] = cargo.get('custom_delivery_method')
            
            # 检查容量
            if (current_group['total_pallets'] + cargo_pallets <= max_pallet and 
                current_group['total_cbm'] + cargo_cbm <= max_cbm):
                current_group['cargos'].append(cargo)
                current_group['total_pallets'] += cargo_pallets
                current_group['total_cbm'] += cargo_cbm
            else:
                if current_group['cargos']:
                    primary_groups.append(current_group.copy())
                current_group = {
                    'cargos': [cargo],
                    'total_pallets': cargo_pallets,
                    'total_cbm': cargo_cbm,
                    'destination': cargo.get('destination'),
                    'delivery_method': cargo.get('custom_delivery_method')
                }
        
        if current_group['cargos']:
            primary_groups.append(current_group)
        
        return primary_groups

    async def _check_time_window_match(self, window_start, window_end, shipment) -> bool:
        """检查时间窗口匹配"""
        # 简化实现，实际应根据业务逻辑完善
        if not window_start and not window_end:
            return True
        
        shipment_time = shipment.shipment_appointment
        
        # 如果只有开始时间，检查预约时间是否在开始时间之后
        if window_start and not window_end:
            return shipment_time >= window_start
        
        # 如果只有结束时间，检查预约时间是否在结束时间之前
        if not window_start and window_end:
            return shipment_time <= window_end
        
        # 如果既有开始时间又有结束时间，检查预约时间是否在时间窗口内
        if window_start and window_end:
            return window_start <= shipment_time <= window_end
        
        return False

    async def _sp_calculate_summary(self, unscheduled: list, scheduled: list, schedule_fleet_data: list, unscheduled_fl) -> dict:
        """计算统计数据"""
        # 计算各类数量
        unscheduled_sp_count = len(unscheduled)
        scheduled_sp_count = len(scheduled)
        schedule_fl_count = len(schedule_fleet_data)
        unscheduled_fl_count = len(unscheduled_fl)
        # 计算总板数
        total_pallets = 0
        for cargo in unscheduled:
            total_pallets += cargo.get('total_n_pallet_act', 0) or cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'unscheduled_sp_count': unscheduled_sp_count,
            'scheduled_sp_count': scheduled_sp_count,
            'schedule_fl_count': schedule_fl_count,
            'unscheduled_fl_count': unscheduled_fl_count,
            'total_pallets': int(total_pallets),
        }

    async def get_capacity_limits(self, st_type: str) -> tuple:
        """获取容量限制"""
        if st_type == "pallet":
            return 80, 35
        elif st_type == "floor":
            return 80, 38
        return 80, 35
    
    async def _ltl_packing_list(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        pl_criteria &= models.Q(container_number__orders__cancel_notification=False)& ~models.Q(container_number__orders__order_type='直送')
        plt_criteria &= models.Q(container_number__orders__cancel_notification=False)& ~models.Q(container_number__orders__order_type='直送')
        
        data = []
        if plt_criteria:
            pk_subquery = PackingList.objects.filter(
                shipping_mark=OuterRef("shipping_mark"),
                PO_ID=OuterRef("PO_ID")
            )
            pal_list = await sync_to_async(list)(
                Pallet.objects.prefetch_related(
                    "container_number",
                    "container_number__orders",
                    "container_number__orders__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__orders__offload_id",
                    "container_number__orders__customer_name",
                    "container_number__orders__retrieval_id",
                    "container_number__orders__vessel_id",
                )
                .filter(plt_criteria)
                .annotate(
                    str_id=Cast("id", CharField()),
                    str_container_number=Cast("container_number__container_number", CharField()),
                    data_source=Value("PALLET", output_field=CharField()),
                    is_pass=Value(True, output_field=BooleanField()),
                    # 重量换算：lbs 转 kg
                    weight_kg=ExpressionWrapper(
                        F("weight_lbs") * 0.453592,
                        output_field=FloatField()
                    ),
                    offload_at=Func(
                        F("container_number__orders__offload_id__offload_at"),
                        Value("MM-DD"),
                        function="to_char",
                        output_field=CharField(),
                    )
                )
                .values(
                    "destination",
                    "delivery_method",
                    "shipping_mark",
                    "abnormal_palletization",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "carrier_company",
                    "ltl_bol_num",
                    "ltl_pro_num",
                    "PickupAddr",
                    "container_number",
                    "address", 
                    "is_dropped_pallet",
                    "shipment_batch_number__shipment_batch_number",
                    "data_source",
                    "shipment_batch_number__fleet_number__fleet_number",
                    "location",
                    "is_pass",
                    "est_pickup_time",
                    "ltl_cost",
                    "ltl_quote",
                    "ltl_unit_quote",
                    "offload_at",
                    "ltl_follow_status",
                    "ltl_release_command",
                    "ltl_cost_note",
                    "ltl_quote_note",
                    "ltl_contact_method",
                    "ltl_plt_size_note",
                    "PO_ID",
                    "del_qty",
                    "ltl_correlation_id",
                    warehouse=F("container_number__orders__retrieval_id__retrieval_destination_precise"),
                    retrieval_destination_precise=F("container_number__orders__retrieval_id__retrieval_destination_precise"),
                    customer_name=F("container_number__orders__customer_name__zem_name"),
                    vessel_name=F("container_number__orders__vessel_id__vessel"),
                    vessel_eta=F("container_number__orders__vessel_id__vessel_eta"),                   
                )
                .annotate(
                    # 如果 ltl_follow_status 包含 'pickup' (不区分大小写用 icontains)，设为 0，否则设为 1
                    is_pickup_priority=Case(
                        When(ltl_follow_status__icontains="pickup", then=Value(0)),
                        default=Value(1),
                        output_field=IntegerField(),
                    ),
                    # 分组依据：destination + shipping_mark
                    custom_delivery_method=F("delivery_method"),
                    shipping_marks=F("shipping_mark"),  # 保持原有字段名
                    # 移除 fba_ids 和 ref_ids
                    plt_ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    # 柜号列表
                    container_numbers=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    cns=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 3),
                    total_weight_lbs=Subquery(pk_subquery.values('total_weight_lbs')[:1]),
                    # 新增：总重量kg
                    total_weight_kg=Subquery(pk_subquery.values('total_weight_kg')[:1]),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True),
                )
                .order_by("is_pickup_priority","offload_at","destination", "shipping_mark")
            )

            # 处理托盘尺寸信息
            
            # 收集涉及的所有 pallet ID
            all_pallet_ids = []
            for cargo in pal_list:
                if cargo.get('plt_ids'):
                    # plt_ids is a comma separated string of IDs
                    ids = str(cargo['plt_ids']).split(',')
                    all_pallet_ids.extend(ids)
            
            # 批量查询托盘详情
            pallet_details_map = {}
            if all_pallet_ids:
                # 去重
                unique_ids = list(set(all_pallet_ids))
                
                # 异步查询 Pallet
                pallets_qs = Pallet.objects.filter(id__in=unique_ids).values(
                    'id', 'length', 'width', 'height', 'pcs', 'weight_lbs'
                )
                pallets_data = await sync_to_async(list)(pallets_qs)
                
                for p in pallets_data:
                    pallet_details_map[str(p['id'])] = p

            processed_pal_list = []

            for cargo in pal_list:
                cargo['pallet_size_formatted'] = cargo['ltl_plt_size_note']
                
                # 构建 pallet_items 列表
                pallet_items = []
                if cargo.get('plt_ids'):
                    p_ids = str(cargo['plt_ids']).split(',')
                    for p_id in p_ids:
                        detail = pallet_details_map.get(p_id)
                        if detail:
                            pallet_items.append({
                                'length': detail['length'],
                                'width': detail['width'],
                                'height': detail['height'],
                                'pieces': detail['pcs'],
                                'weight': detail['weight_lbs'],
                                'description': ''  # 默认为空，前端可编辑
                            })
                
                # 转为 JSON 字符串，供前端直接使用
                cargo['pallet_items_json'] = json.dumps(pallet_items)
                
                processed_pal_list.append(cargo)

            data += processed_pal_list
        # 查询 PackingList 数据
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__orders",
                    "container_number__orders__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__orders__offload_id",
                    "container_number__orders__customer_name",
                    "container_number__orders__retrieval_id",
                    "container_number__orders__vessel_id",
                )
                .filter(pl_criteria)
                .annotate(
                    str_container_number=Cast("container_number__container_number", CharField()),
                    str_id=Cast("id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                    data_source=Value("PACKINGLIST", output_field=CharField()),
                    is_pass=Case(
                        When(
                            container_number__orders__retrieval_id__planned_release_time__isnull=False,
                            then=Value(True)
                        ),
                        When(
                            container_number__orders__retrieval_id__temp_t49_available_for_pickup=True,
                            then=Value(True)
                        ),
                        default=Value(False),
                        output_field=BooleanField()
                    ),
                )
                .values(
                    "destination",
                    "delivery_method",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "container_number",
                    "shipping_mark",
                    "address", 
                    "data_source",
                    "ltl_verify",
                    "carrier_company",
                    "ltl_bol_num",
                    "ltl_pro_num",
                    "PickupAddr",
                    "est_pickup_time",
                    "ltl_follow_status",
                    "ltl_release_command",
                    "ltl_contact_method",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_number",
                    "ltl_correlation_id",
                    warehouse=F("container_number__orders__retrieval_id__retrieval_destination_precise"),
                    vessel_eta=F("container_number__orders__vessel_id__vessel_eta"),
                    is_pass=F("is_pass"),
                    customer_name=F("container_number__orders__customer_name__zem_name"),
                    vessel_name=F("container_number__orders__vessel_id__vessel"),
                    actual_retrieval_time=F("container_number__orders__retrieval_id__actual_retrieval_timestamp"),
                    arm_time=F("container_number__orders__retrieval_id__generous_and_wide_target_retrieval_timestamp"),
                    estimated_time=F("container_number__orders__retrieval_id__target_retrieval_timestamp"),
                    offload_at=Case(
                        When(
                            container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Func(
                                F("container_number__orders__retrieval_id__actual_retrieval_timestamp"),
                                Value("MM-DD"),
                                function="to_char",
                                output_field=CharField(),
                            )
                        ),
                        When(
                            container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False,
                            then=Func(
                                F("container_number__orders__retrieval_id__target_retrieval_timestamp"),
                                Value("MM-DD"),
                                function="to_char",
                                output_field=CharField(),
                            )
                        ),
                        default=Func(
                            F("container_number__orders__vessel_id__vessel_eta"),
                            Value("MM-DD"),
                            function="to_char",
                            output_field=CharField(),
                        ),
                        output_field=CharField(),
                    ),
                    offload_tag=Case(
                        When(
                            container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Value("实际")
                        ),
                        When(
                            container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False,
                            then=Value("预计")
                        ),
                        default=Value("ETA")
                    )
                )
                .annotate(
                    # 如果 ltl_follow_status 包含 'pickup' (不区分大小写用 icontains)，设为 0，否则设为 1
                    is_pickup_priority=Case(
                        When(ltl_follow_status__icontains="pickup", then=Value(0)),
                        default=Value(1),
                        output_field=IntegerField(),
                    ),
                    # 分组依据：destination + shipping_mark
                    shipping_marks=StringAgg(
                        "str_shipping_mark",
                        delimiter=",",
                        distinct=True,
                        ordering="str_shipping_mark",
                    ),
                    # 移除 fba_ids 和 ref_ids
                    ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    # 柜号列表
                    container_numbers=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    cns=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    total_pcs=Sum("pcs", output_field=FloatField()),
                    total_cbm=Round(Sum("cbm", output_field=FloatField()), 3),
                    total_weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                    total_weight_kg=Sum("total_weight_kg", output_field=FloatField()),
                    total_n_pallet_est=Ceil(Sum("cbm", output_field=FloatField()) / 2),
                    label=Value("EST"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True)
                )
                .distinct()
                .order_by("is_pickup_priority","actual_retrieval_time")
            )

            data += pl_list
        
        # 1. 定义日期提取辅助函数 (内部函数)
        def get_pickup_date_key(status):
            if not status:
                return (99, 99)
            # 搜索数字/数字格式 (如 2/1 或 02/18)
            match = re.search(r'(\d+)/(\d+)', str(status))
            if match:
                try:
                    month = int(match.group(1))
                    day = int(match.group(2))
                    return (month, day)
                except ValueError:
                    pass
            return (99, 99) # 没匹配到日期的排在 pickup 类的最后

        # 2. 统一排序逻辑
        # Python 的 sort 是稳定的，可以一次性处理整个 data 列表
        data.sort(key=lambda x: (
            0 if x.get('ltl_correlation_id') else 1,             # 第一优先级：有关联ID的在前
            x.get('ltl_correlation_id') or '',                    # 第二优先级：相同 ID 聚合
            x.get('is_pickup_priority', 1),                      # 第三优先级：pickup 状态
            get_pickup_date_key(x.get('ltl_follow_status', '')), # 第四优先级：状态日期
            x.get('offload_at') or '',                           # 第五优先级
            x.get('container_number') or '',                          # 第六优先级
            x.get('destination') or '',                          # 第七优先级
            x.get('shipping_marks') or x.get('shipping_mark') or ''
        ))

        # 3. 为“一提多卸”组分配背景颜色
        # 定义一组浅色调背景，避开红色/黄色等警告色
        color_palette = ["#e8f4fd", "#eafaf1", "#fef9e7", "#f4ecf7", "#ebf5fb", "#fdf2e9", "#e8f8f5"]
        id_to_color = {}
        color_idx = 0
        
        for item in data:
            corr_id = item.get('ltl_correlation_id')
            if corr_id:
                if corr_id not in id_to_color:
                    # 分配颜色
                    id_to_color[corr_id] = color_palette[color_idx % len(color_palette)]
                    color_idx += 1
                item['correlation_bg_color'] = id_to_color[corr_id]
            else:
                item['correlation_bg_color'] = "" # 无颜色
        return data
    
    async def _ltl_unscheduled_cargo(self, pl_criteria, plt_criteria) -> Dict[str, Any]:
        """获取未放行货物 - Tab 1"""
        plt_criteria = models.Q(pk__isnull=True) & models.Q(pk__isnull=False)
        # 未放行条件：没有批次号，没有卸货完成，ETA在两周内，不是异常状态      
        raw_cargos = await self._ltl_packing_list(
            pl_criteria,
            plt_criteria
        )
        cargos = []
        for cargo in raw_cargos:
            if not cargo["is_pass"]:
                cargos.append(cargo)
        sorted_cargos = sorted(cargos, key=lambda x: (x.get('ltl_verify', False),))
        return sorted_cargos
    
    async def _ltl_scheduled_self_pickup(self, pl_criteria, plt_criteria) -> Dict[str, Any]:
        """获取已放行客提货物 - Tab 2"""
        
        # 已放行客提条件：有批次号，delivery_type为self_pickup
        pl_criteria = pl_criteria&Q(
            delivery_method__contains="自提"
        )
        
        plt_criteria = plt_criteria&Q(
            delivery_method__contains="自提"
        )
        
        raw_cargos = await self._ltl_packing_list(
            pl_criteria,
            plt_criteria
        )
        cargos = []
        for cargo in raw_cargos:
            if cargo["is_pass"]:
                cargos.append(cargo)
        return cargos
    
    async def _ltl_self_delivery(self, pl_criteria, plt_criteria) -> Dict[str, Any]:
        """获取已放行自发货物 - Tab 3"""
        pl_criteria = pl_criteria & ~Q(delivery_method__contains="自提")
        
        plt_criteria = plt_criteria & ~Q(delivery_method__contains="自提")
        raw_cargos = await self._ltl_packing_list(
            pl_criteria,
            plt_criteria
        )
        cargos = []
        for cargo in raw_cargos:
            if cargo["is_pass"]:
                cargos.append(cargo)
        return cargos
    
    async def export_ltl_unscheduled(
        self, request: HttpRequest
    ) -> HttpResponse:
        
        cargo_ids = request.POST.get('cargo_ids', '')
        
        # 构建筛选条件
        pl_criteria = Q()
        plt_criteria = models.Q(pk__isnull=True) & models.Q(pk__isnull=False)
        
        # 如果指定了 ID，则只导出选中的货物
        if cargo_ids:
            cargo_id_list = [int(id.strip()) for id in cargo_ids.split(',') if id.strip()]
            pl_criteria &= Q(id__in=cargo_id_list)
        
        # 获取数据
        release_cargos,_ ,_ = await self._ltl_unscheduled_cargo(pl_criteria, plt_criteria)
        
        # 准备 Excel 数据
        excel_data = []
        for cargo in release_cargos:
            # 获取数据并格式化
            customer_name = cargo.get('customer_name', '-')
            container_numbers = cargo.get('container_numbers', '-')
            destination = cargo.get('destination', '-')
            shipping_marks = cargo.get('shipping_marks', '-')
            address = cargo.get('address', '-')
            note = cargo.get('note', '-')
            
            # 格式化数字
            try:
                total_cbm = float(cargo.get('total_cbm', 0))
                total_cbm = round(total_cbm, 3)
            except (ValueError, TypeError):
                total_cbm = 0
            
            try:
                total_pcs = int(cargo.get('total_pcs', 0))
            except (ValueError, TypeError):
                total_pcs = 0
            
            try:
                weight_lbs = float(cargo.get('total_weight_lbs', 0))
                weight_lbs = round(weight_lbs, 2)
            except (ValueError, TypeError):
                weight_lbs = 0
            
            try:
                weight_kg = float(cargo.get('total_weight_kg', 0))
                weight_kg = round(weight_kg, 2)
            except (ValueError, TypeError):
                weight_kg = 0
            
            # 核实状态
            ltl_verify = cargo.get('ltl_verify', False)
            verify_status = '已核实' if ltl_verify else '未核实'
            
            row = {
                '客户': customer_name,
                '柜号': container_numbers,
                '目的地': destination,
                '唛头': shipping_marks,
                '详细地址': address,
                '备注': note,
                'CBM': total_cbm,
                '件数': total_pcs,
                '重量(lbs)': weight_lbs,
                '重量(kg)': weight_kg,
                '核实状态': verify_status,
            }
            
            excel_data.append(row)
        
        # 创建 DataFrame
        df = pd.DataFrame(excel_data)
        
        # 如果没有数据，创建一个空的DataFrame
        if df.empty:
            df = pd.DataFrame(columns=[
                '客户', '柜号', '目的地', '唛头', '详细地址', '备注',
                'CBM', '件数', '重量(lbs)', '重量(kg)', '核实状态'
            ])
        
        # 创建 Excel 文件
        output = BytesIO()
        
        # 使用 ExcelWriter
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 主数据 sheet
            df.to_excel(writer, sheet_name='未放行货物', index=False)
            
            # 获取 worksheet 对象
            worksheet = writer.sheets['未放行货物']
            
            # 设置列宽
            column_widths = {
                '客户': 20,
                '柜号': 25,
                '目的地': 15,
                '唛头': 25,
                '详细地址': 40,
                '备注': 40,
                'CBM': 10,
                '件数': 10,
                '重量(lbs)': 12,
                '重量(kg)': 12,
                '核实状态': 12,
            }
            
            # 设置列宽
            from openpyxl.utils import get_column_letter
            
            for i, column in enumerate(df.columns, 1):
                col_letter = get_column_letter(i)
                width = column_widths.get(column, 15)
                worksheet.column_dimensions[col_letter].width = width
            
            # 设置数字格式
            from openpyxl.styles import numbers
            
            # 设置CBM列为3位小数格式
            if 'CBM' in df.columns:
                cbm_col_idx = df.columns.get_loc('CBM') + 1
                cbm_col_letter = get_column_letter(cbm_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{cbm_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00  # 默认2位，Excel会自动显示实际小数位数
                    # 如果需要在Excel中强制显示3位小数，使用：
                    # cell.number_format = '0.000'
            
            # 设置重量列为2位小数格式
            if '重量(lbs)' in df.columns:
                lbs_col_idx = df.columns.get_loc('重量(lbs)') + 1
                lbs_col_letter = get_column_letter(lbs_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{lbs_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00
            
            if '重量(kg)' in df.columns:
                kg_col_idx = df.columns.get_loc('重量(kg)') + 1
                kg_col_letter = get_column_letter(kg_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{kg_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00
            
            # 设置样式：标题行加粗
            for cell in worksheet[1]:
                cell.font = cell.font.copy(bold=True)
            
            # 自动换行设置
            from openpyxl.styles import Alignment
            wrap_alignment = Alignment(wrap_text=True, vertical='top')
            
            # 对可能有多行内容的列设置自动换行
            wrap_columns = ['柜号', '详细地址', '备注', '唛头']
            for col_name in wrap_columns:
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name) + 1
                    col_letter = get_column_letter(col_idx)
                    for row in range(1, len(df) + 2):
                        cell = worksheet[f"{col_letter}{row}"]
                        cell.alignment = wrap_alignment
            
            # 添加筛选器
            worksheet.auto_filter.ref = f"A1:{get_column_letter(len(df.columns))}1"
            
            # 可选：冻结标题行
            worksheet.freeze_panes = 'A2'
        
        output.seek(0)
        
        # 创建 HTTP 响应
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # 生成文件名
        timestamp = timezone.now().strftime('_%m%d')
        filename = f'未放行货物_{timestamp}.xlsx'
        
        # 对文件名进行 URL 编码，确保中文正确处理
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename)
        
        # 创建 HTTP 响应
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # 使用 RFC 6266 标准设置 Content-Disposition
        # 这样能确保所有浏览器都能正确显示中文文件名
        response['Content-Disposition'] = f"attachment; filename*=UTF-8''{encoded_filename}"
        
        # 备用方案：对于不支持 RFC 6266 的旧浏览器
        response['Content-Disposition'] = f"attachment; filename={encoded_filename}"
        
        return response

    async def handle_save_releaseCommand(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''单条或批量保存未放行的指令数据'''
        # 1. 尝试获取批量数据
        batch_commands_raw = request.POST.get('batch_commands')
        tasks = []

        if batch_commands_raw:
            try:
                commands_list = json.loads(batch_commands_raw)
            except json.JSONDecodeError:
                commands_list = []
        else:
            cargo_id = request.POST.get('cargo_id')
            release_command = request.POST.get('release_command')
            if cargo_id:
                commands_list = [{'cargo_id': cargo_id, 'command': release_command}]
            else:
                commands_list = []
        num = 0
        for item in commands_list:
            c_id = item.get('cargo_id')
            command_text = item.get('command')
            if not c_id or not command_text: continue

            if c_id.startswith('plt_'):
                target_ids = c_id.replace('plt_', '').split(',')
                model = Pallet
                
            else:
                target_ids = c_id.split(',')
                model = PackingList
            update_data = {'ltl_release_command': command_text}
            await sync_to_async(model.objects.filter(id__in=target_ids).update)(**update_data)
            num+=1
        context = {'success_messages': f'保存成功{num}组数据!'}
        return await self.handle_ltl_unscheduled_pos_post(request,context)
    
    async def handle_save_selfdel_cargo(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''LTL保存自行编辑的货物信息'''
        # 1. 判断是批量保存还是单行保存
        batch_data_raw = request.POST.get('batch_data')
        if batch_data_raw:
            try:
                update_items = json.loads(batch_data_raw)
            except json.JSONDecodeError:
                update_items = []
        else:
            update_items = [{
                'cargo_id': request.POST.get('cargo_id'),
                'address': request.POST.get('address', '').strip(),
                'note': request.POST.get('note', '').strip(),
                'pallet_size': request.POST.get('pallet_size', '').strip(),
                'carrier_company': request.POST.get('carrier_company', '').strip(),
                'bol_number': request.POST.get('bol_number', '').strip(),
                'pro_number': request.POST.get('pro_number', '').strip(),
                'follow_status': request.POST.get('follow_status', '').strip(),
                'ltl_cost_note': request.POST.get('ltl_cost_note', '').strip(),
                'ltl_quote_note': request.POST.get('ltl_quote_note', '').strip(),
                'contact_method': request.POST.get('contact_method', '').strip(),
                'ltl_cost': request.POST.get('ltl_cost', '').strip(),
                'ltl_quote': request.POST.get('ltl_quote', '').strip(),
                'delivery_method': request.POST.get('delivery_method', '').strip(),
                'ltl_release_command': request.POST.get('ltl_release_command', '').strip(),
            }]

        total_status_messages = []
        username = request.user.username
        # 2. 循环处理每一个更新项
        for item in update_items:
            cargo_id = item.get('cargo_id')
            if not cargo_id: continue

            # --- 以下为你原有的单行处理逻辑，仅将 request.POST.get 替换为 item.get ---
            address = item.get('address', '')
            note = item.get('note', '')
            pallet_size = item.get('pallet_size', '')
            carrier_company = item.get('carrier_company', '')
            bol_number = item.get('bol_number', '')
            pro_number = item.get('pro_number', '')
            follow_status = item.get('follow_status', '')
            ltl_cost_note = item.get('ltl_cost_note', '')
            ltl_quote_note = item.get('ltl_quote_note', '')
            contact_method = item.get('contact_method', '')
            delivery_method = item.get('delivery_method')
            ltl_release_command = item.get('ltl_release_command')

            ltl_cost_raw = item.get('ltl_cost', '')
            has_ltl_cost_param = bool(ltl_cost_raw)
            ltl_quote_raw = item.get('ltl_quote', '')
            has_ltl_quote_param = bool(ltl_quote_raw)

            ltl_cost = float(ltl_cost_raw) if has_ltl_cost_param else None
            ltl_quote = float(ltl_quote_raw) if has_ltl_quote_param else None

            # 根据 ID 类型确定字段名
            if cargo_id.startswith('plt_'):
                ids = cargo_id.replace('plt_', '').split(',')
                model = Pallet
            else:
                ids = cargo_id.split(',')
                model = PackingList
        
            # 构建更新字典
            update_data = {}
            
            if carrier_company or carrier_company == '': update_data['carrier_company'] = carrier_company
            if address or address == '': update_data['address'] = address
            if bol_number or bol_number == '': update_data['ltl_bol_num'] = bol_number
            if pro_number or pro_number == '': update_data['ltl_pro_num'] = pro_number
            if note or note == '': update_data['note'] = note
            if follow_status or follow_status == '': update_data['ltl_follow_status'] = follow_status
            if has_ltl_cost_param: update_data['ltl_cost'] = ltl_cost
            if has_ltl_quote_param: update_data['ltl_quote'] = ltl_quote
            if ltl_cost_note: update_data["ltl_cost_note"] = ltl_cost_note
            if ltl_quote_note: update_data["ltl_quote_note"] = ltl_quote_note
            if contact_method: update_data["ltl_contact_method"] = contact_method
            if pallet_size: update_data["ltl_plt_size_note"] = pallet_size
            if delivery_method is not None: update_data['delivery_method'] = delivery_method
            if ltl_release_command is not None: update_data['ltl_release_command'] = ltl_release_command

            # 批量更新通用字段
            if update_data:
                await sync_to_async(model.objects.filter(id__in=ids).update)(**update_data)
        
            if cargo_id.startswith('plt_') and pallet_size:
                await self._save_pallet_sizes(ids, pallet_size)
            
            if has_ltl_quote_param or '组合柜' in ltl_quote_note:
                if not ltl_quote_note: ltl_quote_note = '派送费'
                msg = await self._delivery_account_entry(ids, ltl_quote, ltl_quote_note, username)
                if msg: total_status_messages.append(msg)

        success_message = f'成功保存 {len(update_items)} 组数据！'
        if total_status_messages:
            success_message = mark_safe(f"{success_message}<br>" + "<br>".join(set(total_status_messages)))

        context = {'success_messages': success_message}
        page = request.POST.get('page')
        if page == "history":
            return await self.handle_ltl_history_pos_post(request, context)
        return await self.handle_ltl_unscheduled_pos_post(request, context)

    async def _delivery_account_entry(self, ids, ltl_quote, ltl_quote_note, username):
        pallets = await sync_to_async(list)(
            Pallet.objects.filter(id__in=ids)
            .select_related('container_number')
        )

        # 按 PO_ID-shipping_marks-container_number 分组
        pallet_index = defaultdict(list)
        for pallet in pallets:
            po_id = getattr(pallet, "PO_ID", None) or "无PO_ID"
            if not po_id:
                raise ValueError('id为{pallet.id}的pallet没有PO_ID')
            shipping_mark = getattr(pallet, "shipping_mark")
            if not shipping_mark:
                raise ValueError('id为{pallet.id}的pallet没有唛头')
            container_num = pallet.container_number
            index_key = f"{po_id}-{shipping_mark}-{container_num.id}"
            pallet_index[index_key].append(pallet)

        # 遍历每组
        for index_key, group_pallets in pallet_index.items():
            if len(group_pallets) <= 0:
                continue
            first_pallet = group_pallets[0]
            po_id = getattr(first_pallet, "PO_ID")
            shipping_mark = getattr(first_pallet, "shipping_mark")
            container = first_pallet.container_number
            qty = len(group_pallets)
            total_cbm = sum(getattr(p, "cbm", 0) or 0 for p in group_pallets)
            total_weight = sum(getattr(p, "weight_lbs", 0) or 0 for p in group_pallets)

            # 检查 InvoiceItemv2 是否已有记录
            existing_item = await sync_to_async(
                lambda: InvoiceItemv2.objects.filter(
                    PO_ID=po_id,
                    shipping_marks=shipping_mark,
                    container_number=container
                ).first()
            )()

            if existing_item:
                receivable_statuses = await sync_to_async(
                    lambda: InvoiceStatusv2.objects.filter(
                        invoice=existing_item.invoice_number,
                        invoice_type="receivable"
                    ).first()
                )()
                if receivable_statuses and receivable_statuses.finance_status == "completed":
                    return '账单已被财务确认不可修改出库费！'
                if "组合柜" in ltl_quote_note:
                    await self._single_po_match_combina(container, group_pallets, False, username, qty, existing_item)
                else:
                    # 更新原记录
                    existing_item.qty = qty
                    existing_item.rate = ltl_quote
                    existing_item.cbm = total_cbm
                    existing_item.weight = total_weight
                    existing_item.amount = ltl_quote
                    existing_item.note = ltl_quote_note
                    existing_item.description = ltl_quote_note
                    existing_item.item_category = "delivery_other"
                    existing_item.warehouse_code = getattr(first_pallet, "destination", "")
                    await sync_to_async(existing_item.save)()
            else:
                # 查 invoice_number
                invoice_record = await sync_to_async(
                    lambda: Invoicev2.objects.filter(container_number=container).first()
                )()

                if not invoice_record:
                    # 调用自定义方法创建 invoice
                    invoice_record, invoice_status = await self._create_invoice_and_status(container)

                if "组合柜" in ltl_quote_note:
                    #按组合柜方式计算
                    await self._single_po_match_combina(container, group_pallets, invoice_record, username, qty, False)
                else:
                    # 创建新记录
                    item = InvoiceItemv2(
                        container_number=container,
                        invoice_number=invoice_record,
                        invoice_type="receivable",
                        item_category="delivery_other",
                        description=ltl_quote_note,
                        warehouse_code=getattr(first_pallet, "destination", ""),
                        shipping_marks=shipping_mark,
                        rate=ltl_quote,
                        amount=ltl_quote,
                        qty=qty,
                        cbm=total_cbm,
                        weight=total_weight,
                        delivery_type="selfdelivery",
                        PO_ID=po_id,
                        note=ltl_quote_note,
                        registered_user=username 
                    )
                    await sync_to_async(item.save)()

        # 看下这个柜子私仓派送是不是都录完了，录完了就改状态
        container = pallets[0].container_number
        status_message = await self._try_complete_delivery_other_status(container)
        return status_message
    
    async def _single_po_match_combina(self, container, group, invoice_record, username, qty, existing_item):
        '''私仓匹配组合柜计费'''
        order = await sync_to_async(
            Order.objects.select_related(
                'retrieval_id',
                'vessel_id',
                'customer_name'
            ).filter(container_number=container).first
        )()
        
        quotations = await self._get_fee_details(order, order.retrieval_id.retrieval_destination_area,order.customer_name.zem_name)
        if isinstance(quotations, dict) and quotations.get("error_messages"):
            return {"error_messages": quotations["error_messages"]}
        fee_details = quotations['fees']

        warehouse = order.retrieval_id.retrieval_destination_area
        container_type_temp = 0 if "40" in container.container_type else 1
        combina_key = f"{warehouse}_COMBINA"
        if combina_key not in fee_details:
            context = {
                "error_messages": f"未找到组合柜报价表规则 {combina_key},报价表是{quotations['filename']}"
            }
            return (context, [])  # 返回错误，空列表
        
        rules = fee_details.get(combina_key).details
        first_pallet = group[0]
        po_id = getattr(first_pallet, "PO_ID", "")
        destination_str = getattr(first_pallet, "destination", "")
        destination_origin, destination = self._process_destination(destination_str)
        is_combina_region = False

        # 去预报里找总cbm和重量
        total_pl_details = await sync_to_async(
            lambda: PackingList.objects.filter(
                PO_ID=po_id,
                container_number=container
            ).aggregate(
                total_cbm=Coalesce(Sum('cbm'), 0.0),
                total_weight=Coalesce(Sum('total_weight_lbs'), 0.0)
            )
        )()
        THRESHOLD = 0.001
        if total_pl_details['total_cbm'] > THRESHOLD:
            total_cbm = total_pl_details['total_cbm']
        else:
            total_cbm = sum(float(getattr(p, "cbm", 0.0) or 0.0) for p in group)

        if total_pl_details['total_weight'] > THRESHOLD:
            total_weight = total_pl_details['total_weight']
        else:
            total_weight = sum(float(getattr(p, "weight_lbs", 0.0) or 0.0) for p in group)
        
        for region, region_data in rules.items():
            for item in region_data:
                rule_locations = item.get("location", [])
                if isinstance(rule_locations, str):
                    rule_locations = [rule_locations] # 统一转成列表处理
                if any(destination == loc.replace(" ", "").upper() for loc in rule_locations):
                    is_combina_region = True
                    price = item["prices"][container_type_temp]
                    match_region = region
                    break
            if is_combina_region:
                break
        if destination == "UPS":
            is_combina_region = False

        if is_combina_region:
            # 计算总CBM
            total_container_cbm_result = await sync_to_async(
                lambda: PackingList.objects.filter(
                    container_number=container
                ).aggregate(
                    total_cbm=Coalesce(Sum('cbm'), 0.0)
                )
            )()
            cbm_ratio = round(total_cbm / total_container_cbm_result.get('total_cbm', 0), 4)
            ltl_quote = price * cbm_ratio
            shipping_marks=getattr(first_pallet, "shipping_mark", "")
            if existing_item:
                existing_item.qty = qty
                existing_item.rate = price
                existing_item.cbm = total_cbm
                existing_item.weight = total_weight
                existing_item.amount = ltl_quote
                existing_item.warehouse_code = destination_str
                existing_item.cbm_ratio = cbm_ratio
                existing_item.registered_user = username
                existing_item.delivery_type = "combine"
                existing_item.description = "派送费"
                existing_item.region = match_region
                existing_item.regionPrice = price
                existing_item.item_category = "delivery_other"
                existing_item.shipping_marks = shipping_marks
                await sync_to_async(existing_item.save)()
            else:
                item = InvoiceItemv2(
                    container_number=container,
                    invoice_number=invoice_record,
                    invoice_type="receivable",
                    item_category="delivery_other",
                    description="派送费",
                    warehouse_code=destination_str,
                    shipping_marks=shipping_marks,
                    rate=price,
                    amount=ltl_quote,
                    qty=qty,
                    cbm=total_cbm,
                    cbm_ratio=cbm_ratio,
                    weight=total_weight,
                    delivery_type="combine",
                    PO_ID=po_id,
                    region=match_region,
                    regionPrice=price,
                    registered_user=username 
                )
                await sync_to_async(item.save)() 
        else:
            raise ValueError(f"未在报价表的组合柜范围内找到这个区{quotations['filename']}")    

    async def _try_complete_delivery_other_status(self, container):
        """
        判断该 container 下所有应录入的 delivery_other 是否已完成
        完成则更新 InvoiceStatusv2.delivery_other_status = completed
        """

        # 1️⃣ 查该 container 下应计派送费的 pallet
        delivery_pallets = await sync_to_async(list)(
            Pallet.objects.filter(
                container_number=container,
                delivery_type="other"
            ).exclude(
                delivery_method__icontains="暂扣"
            )
        )

        if not delivery_pallets:
            return

        # 2️⃣ 构造“应存在”的索引集合
        expected_keys = set()
        for pallet in delivery_pallets:
            expected_keys.add(
                (
                    pallet.PO_ID,
                    pallet.shipping_mark,
                    container.id
                )
            )

        # 3️⃣ 查实际已存在的 InvoiceItemv2
        existing_keys = set(
            await sync_to_async(list)(
                InvoiceItemv2.objects.filter(
                    container_number=container,
                    item_category="delivery_other",
                    invoice_type="receivable"
                ).values_list(
                    "PO_ID",
                    "shipping_marks",
                    "container_number_id"
                )
            )
        )
        # 4️⃣ 全部已录 → 更新状态
        if expected_keys.issubset(existing_keys):
            invoice_status = await sync_to_async(
                lambda: InvoiceStatusv2.objects.filter(
                    container_number=container,
                    invoice_type="receivable"
                ).first()
            )()

            if invoice_status and invoice_status.delivery_other_status != "completed":
                invoice_status.delivery_other_status = "completed"
                await sync_to_async(invoice_status.save)()
            return f"该柜子所有私仓派送账单已录完"
        return None

    async def _create_invoice_and_status(
        self,
        container: Container
    ) -> tuple[Invoicev2, InvoiceStatusv2]:
        """异步创建账单和状态记录"""

        # 1️⃣ 查 Order（同步 ORM → async 包装）
        order = await sync_to_async(
            lambda: Order.objects.select_related(
                "customer_name", "container_number"
            ).get(container_number=container)
        )()

        current_date = datetime.now().date()
        order_id = str(order.id)
        customer_id = order.customer_name.id

        # 2️⃣ 查是否已有 Invoice
        existing_invoice = await sync_to_async(
            lambda: Invoicev2.objects.filter(
                container_number=container
            ).first()
        )()

        if existing_invoice:
            # 3️⃣ 查是否已有 Status
            existing_status = await sync_to_async(
                lambda: InvoiceStatusv2.objects.filter(
                    invoice=existing_invoice,
                    invoice_type="receivable"
                ).first()
            )()

            if existing_status:
                return existing_invoice, existing_status

        # 4️⃣ 创建 Invoice
        invoice = await sync_to_async(Invoicev2.objects.create)(
            container_number=container,
            invoice_number=(
                f"{current_date.strftime('%Y%m%d')}C{customer_id}{order_id}"
            ),
            created_at=current_date,
        )

        # 5️⃣ 创建 InvoiceStatus
        invoice_status = await sync_to_async(InvoiceStatusv2.objects.create)(
            container_number=container,
            invoice=invoice,
            invoice_type="receivable",
        )

        return invoice, invoice_status
    
    async def _delivery_account_selfpick_entry(self, ids, ltl_quote, ltl_quote_note, ltl_unit_quote, del_qty, username):
        '''自提的出库费录入'''
        pallets = await sync_to_async(list)(
            Pallet.objects.filter(id__in=ids)
            .select_related('container_number')
        )

        # 按 PO_ID-shipping_marks-container_number 分组
        pallet_index = defaultdict(list)
        for pallet in pallets:
            po_id = getattr(pallet, "PO_ID", None) or "无PO_ID"
            if not po_id:
                raise ValueError('id为{pallet.id}的pallet没有PO_ID')
            shipping_mark = getattr(pallet, "shipping_mark")
            if not shipping_mark:
                raise ValueError('id为{pallet.id}的pallet没有唛头')
            container_num = pallet.container_number
            index_key = f"{po_id}-{shipping_mark}-{container_num.id}"
            pallet_index[index_key].append(pallet)

        # 遍历每组
        for index_key, group_pallets in pallet_index.items():
            first_pallet = group_pallets[0]
            po_id = getattr(first_pallet, "PO_ID")
            shipping_mark = getattr(first_pallet, "shipping_mark")
            container = first_pallet.container_number
            total_cbm = sum(getattr(p, "cbm", 0) or 0 for p in group_pallets)
            total_weight = sum(getattr(p, "weight_lbs", 0) or 0 for p in group_pallets)

            # 检查 InvoiceItemv2 是否已有记录
            existing_item = await sync_to_async(
                lambda: InvoiceItemv2.objects.filter(
                    PO_ID=po_id,
                    shipping_marks=shipping_mark,
                    container_number=container
                ).first()
            )()

            if existing_item:              
                receivable_statuses = await sync_to_async(
                    lambda: InvoiceStatusv2.objects.filter(
                        invoice=existing_item.invoice_number,
                        invoice_type="receivable"
                    ).first()
                )()
                if receivable_statuses and receivable_statuses.finance_status == "completed":
                    return '账单已被财务确认不可修改出库费！'
                # 更新原记录
                existing_item.qty = del_qty
                existing_item.rate = ltl_unit_quote
                existing_item.cbm = total_cbm
                existing_item.weight = total_weight
                existing_item.amount = ltl_quote
                existing_item.description = '出库费'
                existing_item.note = ltl_quote_note
                existing_item.warehouse_code = getattr(first_pallet, "destination", "")
                await sync_to_async(existing_item.save)()
            else:
                # 查 invoice_number
                invoice_record = await sync_to_async(
                    lambda: Invoicev2.objects.filter(container_number=container).first()
                )()

                if not invoice_record:
                    # 调用自定义方法创建 invoice
                    invoice_record, invoice_status = await self._create_invoice_and_status(container)

                # 创建新记录
                item = InvoiceItemv2(
                    container_number=container,
                    invoice_number=invoice_record,
                    invoice_type="receivable",
                    item_category="delivery_other",
                    description="出库费",
                    warehouse_code=getattr(first_pallet, "destination", ""),
                    shipping_marks=shipping_mark,
                    rate=ltl_unit_quote,
                    amount=ltl_quote,
                    note=ltl_quote_note,
                    qty=del_qty,
                    cbm=total_cbm,
                    weight=total_weight,
                    delivery_type="selfdelivery",
                    PO_ID=po_id,
                    registered_user=username 
                )
                await sync_to_async(item.save)()

        # 看下这个柜子私仓派送是不是都录完了，录完了就改状态
        container = pallets[0].container_number
        status_message = await self._try_complete_delivery_other_status(container)
        return status_message
    
    async def handle_save_selfpick_cargo(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''LTL客服自提信息保存'''
        batch_data_raw = request.POST.get('batch_data')
        if batch_data_raw:
            try:
                update_items = json.loads(batch_data_raw)
            except json.JSONDecodeError:
                update_items = []
        else:
            ltl_quote_note = request.POST.get('ltl_quote_note', '').strip()
            update_items = [{
                'cargo_id': request.POST.get('cargo_id'),
                'address': request.POST.get('address', '').strip(),
                'pallet_size': request.POST.get('pallet_size', '').strip(),
                'follow_status': request.POST.get('follow_status', '').strip(),
                'bol_number': request.POST.get('bol_number', '').strip(),
                'carrier_company': request.POST.get('carrier_company', '').strip(),
                'pickup_date': request.POST.get('pickup_date', '').strip(),
                'del_qty': request.POST.get('del_qty', '').strip(),
                'ltl_unit_quote': request.POST.get('ltl_unit_quote', '').strip(),
                'ltl_quote': request.POST.get('ltl_quote', '').strip(),
                'ltl_quote_note': request.POST.get('ltl_quote_note', '').strip(),
                'delivery_method': request.POST.get('delivery_method', '').strip(),
                'note': request.POST.get('note', '').strip(),
                'ltl_release_command': request.POST.get('ltl_release_command', '').strip(),
            }]

        total_updated = 0
        username = request.user.username
        billing_messages = []
        # 2. 循环处理每一个更新项
        for item in update_items:
            cargo_id = item.get('cargo_id')
            if not cargo_id:
                continue
            follow_status = item.get('follow_status', '')
            pallet_size = item.get('pallet_size', '')
            del_qty_raw = item.get('del_qty', '')
            unit_quote_raw = item.get('ltl_unit_quote', '')
            ltl_quote_raw = item.get('ltl_quote', '')
            ltl_quote_note = item.get('ltl_quote_note', '')
            bol_number = item.get('bol_number', '')
            carrier_company = item.get('carrier_company', '')
            address = item.get('address', '')
            pickup_date = item.get('pickup_date', '')
            delivery_method = item.get('delivery_method')
            note = item.get('note')
            ltl_release_command = item.get('ltl_release_command')
            est_pickup_time = None

            if pickup_date:
                try:
                    # 解析日期字符串（格式：YYYY-MM-DD）
                    pickup_date = datetime.strptime(pickup_date, '%Y-%m-%d').date()
                    # 将日期转换为带时间的datetime，默认时间为00:00
                    pickup_date = timezone.make_aware(
                        datetime.combine(pickup_date, time.min)
                    )
                except ValueError as e:
                    # 尝试其他可能的格式
                    try:
                        # 如果传过来的是完整的时间格式
                        pickup_date = timezone.datetime.fromisoformat(pickup_date.replace('Z', '+00:00'))
                    except ValueError:
                        pickup_date = None
            is_pallet = False
            if cargo_id.startswith('plt_'):
                is_pallet = True
                ids = cargo_id.replace('plt_', '').split(',')
                model = Pallet
            else:
                ids = cargo_id.split(',')
                model = PackingList
            update_data = {}
            if follow_status or follow_status == '': update_data['ltl_follow_status'] = follow_status
            if carrier_company or carrier_company == '': update_data['carrier_company'] = carrier_company
            if address or address == '': update_data['address'] = address
            if bol_number or bol_number == '': update_data['ltl_bol_num'] = bol_number
            if (pickup_date is not None) and pickup_date or pickup_date != '': update_data['est_pickup_time'] = pickup_date
            if delivery_method is not None: update_data['delivery_method'] = delivery_method
            if note is not None: update_data['note'] = note
            if ltl_release_command is not None: update_data['ltl_release_command'] = ltl_release_command
            
            # 财务/尺寸相关字段
            if pallet_size: update_data['ltl_plt_size_note'] = pallet_size
            
            if ltl_quote_raw and not del_qty_raw:
                del_qty_raw = len(ids)
            if ltl_quote_raw and not unit_quote_raw:
                unit_quote_raw = float(ltl_quote_raw)
            # 处理数值字段
            if is_pallet:
                if del_qty_raw: update_data['del_qty'] = float(del_qty_raw)
                if unit_quote_raw: update_data['ltl_unit_quote'] = float(unit_quote_raw)
                if ltl_quote_raw: update_data['ltl_quote'] = float(ltl_quote_raw)
                if ltl_quote_note: update_data['ltl_quote_note'] = ltl_quote_note
                if ltl_quote_raw and not del_qty_raw:
                    update_data['del_qty'] = len(ids)
            # 执行数据库更新
            if update_data:
                await sync_to_async(model.objects.filter(id__in=ids).update)(**update_data)
                total_updated += 1

            # 如果是托盘数据且有尺寸，调用特殊的保存方法
            if cargo_id.startswith('plt_') and pallet_size:
                await self._save_pallet_sizes(ids, pallet_size)

            # 同步账单逻辑 (如果录入了报价)
            if ltl_quote_raw:
                msg = await self._delivery_account_selfpick_entry(ids, float(ltl_quote_raw), ltl_quote_note, unit_quote_raw, float(del_qty_raw), username)
                if msg: billing_messages.append(msg)
        
        success_msg = f"成功更新 {total_updated} 条记录。"
        if billing_messages:
            success_msg += "<br>" + "<br>".join(set(billing_messages))
        context = {'success_messages': mark_safe(success_msg)}

        page = request.POST.get('page')
        if page == "history":
            return await self.handle_ltl_history_pos_post(request, context)
        return await self.handle_ltl_unscheduled_pos_post(request, context)

    async def _save_pallet_sizes(self, plt_ids: List[str], pallet_size: str) -> Tuple[bool, str]:
        """
        单独保存托盘尺寸
        格式：长*宽*高*数量板 件数件 重量kg（换行分隔不同尺寸）
        示例：32*35*60*3板\n30*30*50*2板
        """
        # 1. 获取所有托盘
        pallets = await sync_to_async(list)(Pallet.objects.filter(id__in=plt_ids))
        total_pallets = len(pallets)
        
        # 2. 解析托盘尺寸
        lines = [line.strip() for line in pallet_size.split('\n') if line.strip()]
        if not lines:
            return True, ""  # 空尺寸，直接返回成功
        
        #如果就给了一组尺寸，查到的板子都按这个赋值
        if len(lines) == 1:
            line = lines[0]
            # 格式：长*宽*高 x件 xkg
            if '件' in line and 'kg' in line:
                parts = line.split()
                clean_parts = [part for part in parts if part != '']
                for part in clean_parts:
                    if '件' in part:
                        pcs = part.replace('件', '')
                    if 'kg' in part:
                        weight = part.replace('kg', '')
                    else:
                        parts = line.split(' ')[0].replace('板', '').split('*')
            else:
                parts = line.split('*')
                pcs = None
                weight = None
            if len(parts) == 3:
                try:
                    length = float(parts[0]) if parts[0] else None
                    width = float(parts[1]) if parts[1] else None
                    height = float(parts[2]) if parts[2] else None
                    
                    # 所有pallet都按这个尺寸赋值
                    for pallet in pallets:
                        pallet.length = length
                        pallet.width = width
                        pallet.height = height
                        if pcs:
                            pallet.pcs = pcs
                        if weight:
                            pallet.weight_lbs = float(weight) * 2.20462
                        await sync_to_async(pallet.save)()
                    
                    return True, ""
                except ValueError:
                    return False, f"数值错误：'{line}'中的长宽高必须是数字"
        
        # 给了多组尺寸，解析尺寸数据
        size_assignments = []
        total_specified = 0
        
        valid_lines = [line for line in lines if line.strip()]
        has_piece_kg_line = any(('件' in line and 'kg' in line) for line in valid_lines)
        if has_piece_kg_line:
            for idx, line in enumerate(valid_lines, start=1):
                if not ('件' in line and 'kg' in line):
                    raise ValueError(f"第 {idx} 行缺少 件 或 kg")
                
        for line in lines:
            if not line:
                continue
            
            if '件' in line and 'kg' in line:
                parts = line.split()
                clean_parts = [part for part in parts if part != '']
                for part in clean_parts:
                    if '件' in part:
                        pcs = part.replace('件', '')
                    if 'kg' in part:
                        weight = part.replace('kg', '')
                    else:
                        parts = line.split(' ')[0].replace('板', '').split('*')
            else:
                line_clean = line.replace('板', '')
                parts = line_clean.split('*')
                pcs = None
                weight = None
            
            if len(parts) == 3:
                # 格式：长*宽*高（默认1板）
                length, width, height = parts
                count = 1
            elif len(parts) == 4:
                # 格式：长*宽*高*板数
                length, width, height, count_str = parts
                count = int(count_str) if count_str.isdigit() else 1
            else:
                return False, f"托盘尺寸的格式错误：'{line}'"
            
            # 转换为数值
            try:
                length_val = float(length) if length else None
                width_val = float(width) if width else None
                height_val = float(height) if height else None
                pcs_val = float(pcs) if pcs else None
                weight_val = float(weight) if weight else None
            except ValueError:
                return False, f"托盘尺寸数值错误：'{line}'中的长宽高必须是数字,{length_val},{width_val},{height_val},{pcs_val},{weight_val}"
            
            size_assignments.append({
                'length': length_val,
                'width': width_val,
                'height': height_val,
                'count': count,
                'pcs': pcs_val,
                'weight': weight_val,
            })
            total_specified += count
        
        # 3. 验证总数
        if total_specified != total_pallets:
            return False, f"托盘尺寸赋值时，板数不匹配：尺寸给出{total_specified}板，实际系统有{total_pallets}板"
        
        # 4. 分配尺寸
        idx = 0
        for size_info in size_assignments:
            for _ in range(size_info['count']):
                if idx >= total_pallets:
                    break
                    
                pallet = pallets[idx]
                pallet.length = size_info['length']
                pallet.width = size_info['width']
                pallet.height = size_info['height']
                if pcs:
                    pallet.pcs = size_info['pcs']
                if weight:
                    pallet.weight_lbs = size_info['weight'] * 2.20462
                await sync_to_async(pallet.save)()
                idx += 1
        
        # 5. 验证是否所有托盘都已处理
        if idx != total_pallets:
            return False, f"托盘尺寸分配错误：只分配了{idx}个托盘"
        
        return True, ""
    
    async def handle_update_pod_status(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        """LTL对po更改核实状态"""
        pod_to_customer_str = request.POST.get("pod_to_customer")
        new_status = True if pod_to_customer_str == "True" else False
        
        target_ids = []
        # 单行修改
        single_id = request.POST.get('shipment_batch_number')
        if single_id:
            target_ids.append(single_id)
        # 多行修改
        batch_ids_json = request.POST.get('batch_ids_json')
        if batch_ids_json:
            try:
                ids_list = json.loads(batch_ids_json)
                if isinstance(ids_list, list):
                    target_ids.extend(ids_list)
            except json.JSONDecodeError:
                pass
        
        target_ids = list(set(target_ids))

        if target_ids:
            await sync_to_async(Shipment.objects.filter(shipment_batch_number__in=target_ids).update)(
                pod_to_customer=new_status
            )
        succes_len = len(target_ids)
        context = {'success_messages': f'成功更新{succes_len}条POD回传状态！'}
        return await self.handle_ltl_unscheduled_pos_post(request,context)
    
    async def handle_save_shipping_tracking(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        """LTL对po更改核实状态"""
        fleet_number = request.POST.get('fleet_number')
        ltl_shipping_tracking = request.POST.get('ltl_shipping_tracking')
        
        if fleet_number and ltl_shipping_tracking:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
            fleet.ltl_shipping_tracking = ltl_shipping_tracking
            await sync_to_async(fleet.save)()
        
        return await self.handle_ltl_unscheduled_pos_post(request)
    
    async def handle_save_fleet_cost(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        """LTL对po更改核实状态"""
        fleet_number = request.POST.get('fleet_number')
        fleet_cost = request.POST.get('fleet_cost')
        
        if fleet_number and fleet_cost:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
            fleet.fleet_cost = float(fleet_cost)
            await sync_to_async(fleet.save)()
            #分摊成本
            fm = FleetManagement()
            await fm.insert_fleet_shipment_pallet_fleet_cost(
                request, fleet_number, fleet_cost
            )
        
        return await self.handle_ltl_unscheduled_pos_post(request)

    async def handle_verify_ltl_cargo(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        """LTL对po更改核实状态"""
        if not context:
            context = {}
        cargo_ids = request.POST.get('cargo_ids', '')
        ltl_verify = request.POST.get('ltl_verify', 'false').lower() == 'true'
        
        # 处理 PackingList 的核实
        if cargo_ids:
            cargo_id_list = [int(id.strip()) for id in cargo_ids.split(',') if id.strip()]
            packinglist_ids = cargo_id_list
            if packinglist_ids:
                # 更新 PackingList 的核实状态
                await sync_to_async(PackingList.objects.filter(
                    id__in=packinglist_ids
                ).update)(
                    ltl_verify=ltl_verify
                )
        return await self.handle_ltl_unscheduled_pos_post(request)
        
    async def handle_ltl_history_pos_post(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        '''LTL组的港后全流程'''
        warehouse = request.POST.get("warehouse")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        if not context:
            context = {}
        if warehouse:
            warehouse_name = warehouse.split('-')[0]
        else:
            context.update({'error_messages':"没选仓库！"})
            return self.template_unscheduled_pos_all, context
        
        # 未给定时间时，自动查询过去三个月的
        current_date = datetime.now().date()
        start_date = (
            (current_date + timedelta(days=-30)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = current_date.strftime("%Y-%m-%d") if not end_date else end_date
        
        pl_criteria = Q(
            container_number__orders__offload_id__offload_at__isnull=True,
            container_number__orders__retrieval_id__retrieval_destination_area=warehouse_name,
            container_number__orders__retrieval_id__actual_retrieval_timestamp__gte=start_date,
            container_number__orders__retrieval_id__actual_retrieval_timestamp__lte=end_date,
            delivery_type="other"
        )
        plt_criteria = Q(
            location=warehouse,            
            container_number__orders__offload_id__offload_at__gte=start_date,
            container_number__orders__offload_id__offload_at__lte=end_date, 
            delivery_type="other"
        )

        # 已放行-客提
        release_cargos, selfpick_cargos, selfdel_cargos = await self._get_classified_cargos(pl_criteria, plt_criteria)
        #selfpick_cargos = await self._ltl_scheduled_self_pickup(pl_criteria, plt_criteria)
        # 已放行-自发
        #selfdel_cargos = await self._ltl_self_delivery(pl_criteria, plt_criteria)
        # 历史车次
        #fleet_cargos = await self._ltl_unscheduled_data(request, warehouse, start_date, end_date)
        fleet_cargos = None
        summary = {
            'selfpick_count': len(selfpick_cargos),
            'selfdel_count': len(selfdel_cargos),
            'fleet_count': 0,
        }
        if not context:
            context = {}
        context.update({
            'warehouse': warehouse,
            'warehouse_options': self.warehouse_options,
            'account_options': self.arm_account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
            "selfpick_cargos": selfpick_cargos,
            "selfdel_cargos": selfdel_cargos,
            "fleet_cargos": fleet_cargos,
            "summary": summary,
            'shipment_type_options': self.shipment_type_options,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "warehouse_name": warehouse_name,
            "start_date": start_date,
            "end_date": end_date,
        })
        active_tab = request.POST.get('active_tab')
        if active_tab:
            context.update({'active_tab':active_tab})
        return self.template_ltl_history_pos, context
    
    async def _get_classified_cargos(self, pl_criteria, plt_criteria):
        """一次性获取并分类LTL的所有PO"""
        # 获取全量数据 (注意：此时不要在 criteria 里加“自提”限制，获取该仓库下的所有)
        all_raw_data = await self._ltl_packing_list(pl_criteria, plt_criteria)
        
        release_cargos = []     # 未放行 (Tab 1)
        selfpick_cargos = []    # 已放行-客提 (Tab 2)
        selfdel_cargos = []     # 已放行-自发 (Tab 3)

        for item in all_raw_data:
            is_pass = item.get('is_pass', False)
            # 兼容 Pallet 和 PackingList 的 delivery_method 字段名
            delivery_method = item.get('delivery_method') or item.get('custom_delivery_method') or ""
            
            if not is_pass:
                # 未放行逻辑
                release_cargos.append(item)
            else:
                # 已放行逻辑
                if "自提" in delivery_method:
                    selfpick_cargos.append(item)
                else:
                    selfdel_cargos.append(item)

        # 针对未放行进行排序 (对应你原来的逻辑)
        release_cargos.sort(key=lambda x: (x.get('ltl_verify', False),))
        
        return release_cargos, selfpick_cargos, selfdel_cargos

    async def handle_ltl_unscheduled_pos_post(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        '''LTL组的港后全流程'''
        warehouse = request.POST.get("warehouse")
        if not context:
            context = {}
        if warehouse:
            warehouse_name = warehouse.split('-')[0]
        else:
            context.update({'error_messages':"没选仓库！"})
            return self.template_unscheduled_pos_all, context
        
        pl_criteria = Q(
            container_number__orders__offload_id__offload_at__isnull=True,
            shipment_batch_number__shipment_batch_number__isnull=True,
            container_number__orders__retrieval_id__retrieval_destination_area=warehouse_name,
            delivery_type="other"
        )
        plt_criteria = Q(
            location=warehouse,
            shipment_batch_number__shipment_batch_number__isnull=True,
            container_number__orders__offload_id__offload_at__gt=datetime(2025, 12, 1),
            delivery_type="other"
        )
        # 未放行、已放行-客提、已放行-自发
        release_cargos, selfpick_cargos, selfdel_cargos = await self._get_classified_cargos(pl_criteria, plt_criteria)
        #release_cargos = await self._ltl_unscheduled_cargo(pl_criteria, plt_criteria)

        # 已放行-客提
        #selfpick_cargos = await self._ltl_scheduled_self_pickup(pl_criteria, plt_criteria)
        # 已放行-自发
        #selfdel_cargos = await self._ltl_self_delivery(pl_criteria, plt_criteria)

        #未排车
        unschedule_fleet = await self._ltl_unscheduled_data(request, warehouse)
        #待出库
        ready_to_ship_data = await self._ltl_ready_to_ship_data(warehouse,request.user)
        # 待送达
        delivery_data_raw = await self._fl_delivery_get(warehouse, None, 'ltl')
        delivery_data = delivery_data_raw['shipments']
        # #待传POD
        pod_data = await self._ltl_pod_get(warehouse)

        pod_data = sorted(
            pod_data,
            key=lambda p: p.pod_to_customer is True
        )
        summary = {
            'release_count': len(release_cargos),
            'selfpick_count': len(selfpick_cargos),
            'selfdel_count': len(selfdel_cargos),
            'ready_to_ship_count': len(ready_to_ship_data),
            'ready_count': len(delivery_data),
            'pod_count': len(pod_data),
            'unfleet_count': len(unschedule_fleet),
        }
        if not context:
            context = {}
        context.update({
            'warehouse': warehouse,
            'warehouse_options': self.warehouse_options,
            'account_options': self.arm_account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
            "release_cargos": release_cargos,
            "selfpick_cargos": selfpick_cargos,
            "selfdel_cargos": selfdel_cargos,
            "ready_to_ship_data": ready_to_ship_data,
            "delivery_data": delivery_data,
            "pod_data": pod_data,
            "summary": summary,
            'shipment_type_options': self.shipment_type_options,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "warehouse_name": warehouse_name,
            'unschedule_fleet': unschedule_fleet,
        })
        active_tab = request.POST.get('active_tab')
        if active_tab:
            context.update({'active_tab':active_tab})
        return self.template_ltl_pos_all, context
    
    async def _ltl_unscheduled_data(
        self, request: HttpRequest, warehouse:str, start_date: str | None = None, end_date: str | None = None
    ) -> tuple[str, dict[str, Any]]:
        target_date = datetime(2025, 10, 10)
        base_q = models.Q(
            origin=warehouse,
            fleet_number__isnull=True,
            in_use=True,
            is_canceled=False,
            shipment_type__in=['LTL', '客户自提'],
        )

        if start_date and end_date:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            time_filter_q = models.Q(
                shipment_appointment__range=(start_datetime, end_datetime)
            )
            
        else:
            time_filter_q = models.Q(shipment_appointment__gt=target_date)
        base_q = base_q & time_filter_q
        shipment_list = await sync_to_async(list)(
            Shipment.objects.filter(base_q).select_related("fleet_number").order_by("pickup_time", "shipment_appointment")
        )
        for shipment in shipment_list:
            #is_public = self._check_destination_delivery_type(shipment.destination)
            # if is_public:
            #     continue
            shipment.fleet_display_name = None
            if shipment.fleet_number:
                try:
                    shipment.fleet_display_name = shipment.fleet_number.fleet_number
                except ObjectDoesNotExist:
                    shipment.fleet_display_name = None
            # 从packinglist表获取唛头
            packinglist_marks = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number=shipment.id
                ).values_list('shipping_mark', flat=True).distinct()
            )
            
            # 从pallet表获取唛头
            pallet_marks = await sync_to_async(list)(
                Pallet.objects.filter(
                    shipment_batch_number=shipment.id
                ).values_list('shipping_mark', flat=True).distinct()
            )
            
            # 合并并去重唛头
            all_marks = set(packinglist_marks + pallet_marks)
            all_marks = [mark for mark in all_marks if mark]  # 过滤空值
            
            # 添加唛头字段到shipment对象
            shipment.shipping_marks = all_marks  # 列表形式
            shipment.shipping_marks_display = "，".join(all_marks) if all_marks else "无唛头"  # 显示用
            if shipment.pod_link:
                shipment.status_display = "已上传POD"
                shipment.status_class = "status-pod"  # 绿色
            elif shipment.is_arrived:
                shipment.status_display = "已送达"
                shipment.status_class = "status-arrived"  # 蓝色
            elif shipment.is_shipped:
                shipment.status_display = "已出库"
                shipment.status_class = "status-shipped"  # 黄色
            else:
                shipment.status_display = "待处理"
                shipment.status_class = "status-pending"  # 灰色
        return shipment_list
    
    async def _check_destination_delivery_type(self, destination: str) -> bool:
        """检查目的地对应的delivery_type是否包含自提或自发"""
        
        if not destination:
            return False
        # 1. 统一转为大写并去除两端空格
        dest_upper = str(destination).strip().upper()
        
        # 2. 移除中间可能的空格进行正则校验 (例如将 "ONT 8" 视为 "ONT8")
        dest_compact = dest_upper.replace(" ", "")

        # 3. 正则表达式校验 (匹配标准仓库代码)
        if self.RE_PUBLIC_WH.match(dest_compact):
            return True

        # 4. 关键字校验 (匹配包含关键词的情况)
        if any(kw in dest_upper for kw in self.PUBLIC_KEYWORDS):
            return True

        # 5. 特殊规则：如果目的地全是数字（某些仓点的内部编号）
        if dest_compact.isdigit() and len(dest_compact) >= 4:
            return True

        return False

    async def _ltl_pod_get(
        self, warehouse:str,
    ) -> dict[str, Any]: 

        criteria = models.Q(
            models.Q(pod_link__isnull=True) | models.Q(pod_link=""),
            pod_to_customer=False,       
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_schduled_at__gte="2024-12-01",
            origin=warehouse,
        )
        criteria = criteria & models.Q(shipment_type__in=['LTL', '客户自提'])

        shipments = await sync_to_async(list)(
            Shipment.objects.select_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
        for shipment in shipments:
            # 获取与该shipment关联的所有pallet
            pallets = await sync_to_async(list)(
                Pallet.objects.filter(shipment_batch_number=shipment)
                .select_related('container_number')
            )
             
            customer_names = set()
            details_set = set()
            for pallet in pallets:
                if pallet.container_number:
                    # 获取与该container关联的所有order
                    orders = await sync_to_async(list)(
                        Order.objects.filter(container_number=pallet.container_number)
                        .select_related('customer_name')
                    )
                    
                    for order in orders:
                        if order.customer_name:
                            customer_names.add(order.customer_name.zem_name)
                # 拼接 details 信息
                container_num = pallet.container_number.container_number if pallet.container_number else "无柜号"
                destination = getattr(pallet, "destination", "") or ""
                shipping_mark = getattr(pallet, "shipping_mark", "") or ""
                detail_str = (
                    f"<span style='color:blue;'>{container_num}</span>"
                    f"<span style='color:red;'>-</span>"
                    f"<span style='color:green;'>{destination}</span>"
                    f"<span style='color:red;'>-</span>"
                    f"<span style='color:orange;'>{shipping_mark}</span>"
                )
                details_set.add(detail_str)

            
            # 将客户名用逗号拼接，并添加到shipment对象上
            shipment.customer = ", ".join(customer_names) if customer_names else "无客户信息"
            shipment.details = "<br>".join(details_set) if details_set else None
        
        return shipments
    
    async def _ltl_ready_to_ship_data(self, warehouse: str, user:User) -> list:
        """获取待出库数据 - 按fleet_number分组"""
        # 获取指定仓库的未出发且未取消的fleet
        base_bq = models.Q(
            origin=warehouse,
            departured_at__isnull=True,
            is_canceled=False,
            fleet_type__in=['LTL', '客户自提']
        )
        
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(base_bq).prefetch_related(
                Prefetch(
                    'shipment',
                    queryset=Shipment.objects.prefetch_related(
                        Prefetch(
                            'packinglist',
                            queryset=PackingList.objects.select_related('container_number')
                        ),
                        Prefetch(
                            'pallet', 
                            queryset=Pallet.objects.select_related('packing_list', 'container_number')
                        )
                    )
                )
            )
        )
        
        grouped_data = []
        
        for fleet in fleets:
            arm_pickup = await self._get_fleet_arm_pickup(fleet)
            fleet_group = {
                'fleet_number': fleet.fleet_number,
                'fleet_type': fleet.fleet_type,
                'fleet_cost': fleet.fleet_cost or 0,
                'third_party_address': fleet.third_party_address,
                'pickup_number': fleet.pickup_number,
                'motor_carrier_number': fleet.motor_carrier_number,
                'license_plate': fleet.license_plate,
                'dot_number': fleet.dot_number,
                'appointment_datetime': fleet.appointment_datetime,
                'carrier': fleet.carrier,
                'is_virtual': fleet.is_virtual,
                'shipments': {},  # 改回字典结构，保持与前端兼容
                'pl_ids': [],
                'plt_ids': [],
                'total_cargos': 0,  # 总货物行数
                'arm_pickup':arm_pickup,
            }
            
            shipments = await sync_to_async(list)(
                Shipment.objects.filter(fleet_number__fleet_number=fleet.fleet_number)
            )
            
            for shipment in shipments:
                if not shipment.shipment_batch_number:
                    continue

                batch_number = shipment.shipment_batch_number
                
                # 初始化shipment数据
                if batch_number not in fleet_group['shipments']:
                    fleet_group['shipments'][batch_number] = {
                        'shipment_batch_number': shipment.shipment_batch_number or '-',
                        'appointment_id': shipment.appointment_id or '-',
                        'destination': shipment.destination or '-',
                        'shipment_appointment': shipment.shipment_appointment,
                        'cargos': []
                    }
                
                # 处理packinglists
                raw_data = await self._get_packing_list(
                    user,
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__orders__offload_id__offload_at__isnull=True,
                    ),
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__orders__offload_id__offload_at__isnull=False,
                    ),
                )
                fleet_group['shipments'][batch_number]['cargos'].extend(raw_data)
            
            # 排序 shipments，cargos 为空的放后面
            fleet_group['shipments'] = dict(
                sorted(
                    fleet_group['shipments'].items(),
                    key=lambda item: not item[1]['cargos']
                )
            )
            fleet_group['total_cargos'] = sum(
                len(s['cargos']) if s['cargos'] else 1
                for s in fleet_group['shipments'].values()
            )
            # 只有有数据的fleet才返回
            #if fleet_group['shipments']:
            grouped_data.append(fleet_group)
        # 按 appointment_datetime 排序，时间早的排在前面
        grouped_data.sort(
            key=lambda x: (
                x['appointment_datetime'].replace(tzinfo=None)
                if x['appointment_datetime'] else datetime.max
            )
        )
        return grouped_data
    
    async def _get_fleet_arm_pickup(self, fleet:Fleet):
        arm_pickup = await sync_to_async(list)(
            Pallet.objects.select_related(
                "container_number__container_number",
                "shipment_batch_number__fleet_number",
            )
            .filter(
                shipment_batch_number__fleet_number=fleet
            )
            .values(
                "container_number__container_number",
                "zipcode",
                "shipping_mark",
                "destination",
                "shipment_batch_number__ARM_BOL",
                "shipment_batch_number__ARM_PRO",
                "shipment_batch_number__fleet_number__carrier",
                "shipment_batch_number__fleet_number__appointment_datetime",
                "address",
                "slot",
                "shipment_batch_number__note",
            )
            .annotate(
                total_pcs=Sum("pcs"),
                total_pallet=Count("pallet_id", distinct=True),
                total_weight=Sum("weight_lbs"),
                total_cbm=Sum("cbm"),
            )
        )
        for arm in arm_pickup:
            marks = arm["shipping_mark"]
            if marks:
                array = marks.split(",")
                if len(array) > 2:
                    parts = []
                    for i in range(0, len(array), 2):
                        part = ",".join(array[i : i + 2])
                        parts.append(part)
                    new_marks = "\n".join(parts)
                else:
                    new_marks = marks
                arm["shipping_mark"] = new_marks
            else:
                arm["shipping_mark"] = ""
            arm["address_parts"] = {
                "company": "",
                "road": "",
                "city": "",
                "name": "",
                "phone": ""
            }
        arm_json = []
        for item in arm_pickup:
            arm_json.append({
                'container_number': item.get('container_number__container_number', ''),
                'zipcode': item.get('zipcode', ''),
                'shipping_mark': item.get('shipping_mark', ''),
                'destination': item.get('destination', ''),
                'address': item.get('address', ''),
                'slot': item.get('slot', ''),
                'arm_bol': item.get('shipment_batch_number__ARM_BOL', ''),
                'arm_pro': item.get('shipment_batch_number__ARM_PRO', ''),
                'carrier': item.get('shipment_batch_number__fleet_number__carrier', ''),
                'appointment_datetime': (
                    item.get('shipment_batch_number__fleet_number__appointment_datetime').isoformat()
                    if item.get('shipment_batch_number__fleet_number__appointment_datetime')
                    else ''
                ),
                'note': item.get('shipment_batch_number__note', ''),
                'total_pcs': int(item.get('total_pcs', 0)),
                'total_pallet': int(item.get('total_pallet', 0)),
                'total_weight': float(item.get('total_weight', 0)),
                'total_cbm': float(item.get('total_cbm', 0)),
                'address_parts': dict(item.get('address_parts') or {
                    'company': '', 'road': '', 'city': '', 'name': '', 'phone': ''
                })
            })

        arm_json_str = json.dumps(arm_json, cls=DjangoJSONEncoder)
        return arm_json_str
    
    async def handle_unscheduled_pos_post(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        #warehouse = request.POST.get("warehouse")
        warehouse = 'LA-91761'
        if not context:
            context = {}
        
        nowtime = timezone.now()
        two_weeks_later = nowtime + timezone.timedelta(weeks=2)   
        pl_criteria = (models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__isnull=True,
                container_number__orders__vessel_id__vessel_eta__lte=two_weeks_later, 
                container_number__orders__retrieval_id__retrieval_destination_precise=warehouse,
                container_number__is_abnormal_state=False,
                destination__in=FOUR_MAJOR_WAREHOUSES
            )&
            ~(
                models.Q(delivery_method__icontains='暂扣') |
                models.Q(delivery_method__icontains='自提') |
                models.Q(delivery_method__icontains='UPS') |
                models.Q(delivery_method__icontains='FEDEX')
            )
        )

        plt_criteria = (models.Q(
                location=warehouse,
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__gt=datetime(2025, 1, 1),
                destination__in=FOUR_MAJOR_WAREHOUSES
            )&
            ~(
                models.Q(delivery_method__icontains='暂扣') |
                models.Q(delivery_method__icontains='自提') |
                models.Q(delivery_method__icontains='UPS') |
                models.Q(delivery_method__icontains='FEDEX')
            )
        )
        unshipment_pos = await self._get_packing_list(
            request.user,
            pl_criteria,
            plt_criteria,
        )
        if len(unshipment_pos) == 0:
            context.update({'error_messages':"没有查到相关库存！"})
            return self.template_unscheduled_pos_all, context
        #未使用的约和异常的约
        shipments = await self.get_shipments_by_warehouse(warehouse,request,"four_major_whs")
        #已排约
        scheduled_data = await self.sp_scheduled_data(warehouse, request.user, "four_major_whs")

        #未排车+已排车
        fleets = await self._fl_unscheduled_data(request, warehouse, "four_major_whs")
        #已排车
        schedule_fleet_data = fleets['fleet_list']

        #待出库
        ready_to_ship_data = await self._sp_ready_to_ship_data(warehouse,request.user, "four_major_whs")
        # 待送达
        delivery_data_raw = await self._fl_delivery_get(warehouse, "four_major_whs")
        delivery_data = delivery_data_raw['shipments']
        #待传POD
        pod_data_raw = await self._fl_pod_get(warehouse, "four_major_whs")
        pod_data = pod_data_raw['fleet']

        summary = await self._four_major_calculate_summary(unshipment_pos, shipments, scheduled_data, schedule_fleet_data, ready_to_ship_data, delivery_data, pod_data, warehouse)

        #四大仓的不看船列表    
        destination_list = []
        for item in unshipment_pos:
            destination = item.get('destination')
            destination_list.append(destination)
            
        #if await self._validate_user_four_major_whs(request.user):
        vessel_dict = {}       
        destination_list = sorted(list(set(destination_list)))
        if not context:
            context = {}
        context.update({
            'warehouse': warehouse,
            'warehouse_options': self.warehouse_options,
            'cargos': unshipment_pos,
            'shipments': shipments,
            'summary': summary,
            'cargo_count': len(unshipment_pos),
            'appointment_count': len(shipments),
            "vessel_dict": vessel_dict,
            "destination_list": destination_list,
            'account_options': self.arm_account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
            "scheduled_data": scheduled_data,
            "schedule_fleet_data": schedule_fleet_data,
            "ready_to_ship_data": ready_to_ship_data,
            "delivery_shipments": delivery_data,
            "pod_shipments": pod_data,
            'shipment_type_options': self.shipment_type_options,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
        })
        active_tab = request.POST.get('active_tab')
        
        if active_tab:
            context.update({'active_tab':active_tab})
        return self.template_unscheduled_pos_all, context


    async def handle_appointment_management_post(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        if warehouse:
            warehouse_name = warehouse.split('-')[0]
        else:
            raise ValueError('未选择仓库！')
        
        nowtime = timezone.now()
        two_weeks_later = nowtime + timezone.timedelta(weeks=2)
        # 1、PO管理——所有没约且两周内到港的货物
        unshipment_pos = await self._get_packing_list(
            request.user,
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__orders__offload_id__offload_at__isnull=True,
                container_number__orders__vessel_id__vessel_eta__lte=two_weeks_later, 
                container_number__orders__retrieval_id__retrieval_destination_area=warehouse_name,
                delivery_type='public',
                container_number__is_abnormal_state=False,
                #container_number__orders__warehouse__name=warehouse,
            )&
            ~(
                models.Q(delivery_method__icontains='暂扣') |
                models.Q(delivery_method__icontains='自提') |
                models.Q(delivery_method__icontains='UPS') |
                models.Q(delivery_method__icontains='FEDEX')
            ),
            models.Q(
                container_number__orders__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
        )
        
        # 2、预约管理——未使用的约和异常的约
        shipments = await self.get_shipments_by_warehouse(warehouse,request)
        
        summary = await self.calculate_summary(unshipment_pos, shipments, warehouse)

        # 3、智能匹配内容——暂不使用
        # st_type = request.POST.get('st_type')
        # max_cbm, max_pallet = await self.get_capacity_limits(st_type)
        # matching_suggestions = await self.get_matching_suggestions(unshipment_pos, shipments,max_cbm,max_pallet)
        # primary_group_keys = set()
        # for suggestion in matching_suggestions:
        #     group_key = f"{suggestion['primary_group']['destination']}_{suggestion['primary_group']['delivery_method']}"
        #     primary_group_keys.add(group_key)
        # auto_matches = await self.get_auto_matches(unshipment_pos, shipments)
        
        # 4、失效的PO管理
        invalid_pos = await self._invalid_po_check(warehouse)

        vessel_names = []
        vessel_dict = OrderedDict()
        destination_list = []
        vessel_eta_dict = {}
        for item in unshipment_pos:
            destination = item.get('destination')
            destination_list.append(destination)
            vessel_name = item.get('vessel_name')
            vessel_voyage = item.get('vessel_voyage')
            vessel_eta = item.get('vessel_eta')

            if vessel_name and vessel_name not in vessel_names:
                eta_date = vessel_eta
                vessel_eta_dict[vessel_name] = eta_date
                display_text = f"{vessel_name} / {vessel_voyage} → {str(vessel_eta).split()[0] if vessel_eta else '未知'}"
                vessel_dict[vessel_name] = display_text
                vessel_names.append(vessel_name)
        vessel_dict = OrderedDict(
            sorted(vessel_dict.items(), key=lambda x: vessel_eta_dict[x[0]])
        )        
        destination_list = sorted(list(set(destination_list)))
        if not context:
            context = {}
        context.update({
            'warehouse': warehouse,
            'warehouse_options': self.warehouse_options,
            'cargos': unshipment_pos,
            'shipments': shipments,
            'summary': summary,
            'cargo_count': len(unshipment_pos),
            'appointment_count': len(shipments),
            'invalid_po_count': len(invalid_pos),
            #'matching_count': len(primary_group_keys),
            #'matching_suggestions': matching_suggestions,
            #'auto_matches': auto_matches,
            #'st_type': st_type,
            #'max_cbm': max_cbm,
            #'max_pallet': max_pallet,
            "invalid_pos": invalid_pos,
            "vessel_names": vessel_names,
            "vessel_dict": vessel_dict,
            "destination_list": destination_list,
            'account_options': self.account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
        })
        return self.template_main_dash, context
    
    async def handle_po_invalid_save(self, request):
        '''PO校验保存'''
        json_data = request.POST.get('updated_data_json')
        if json_data:
            items_to_update = json.loads(json_data)
            
            # 使用列表来批量更新或循环更新
            # 如果数据量不大，循环更新最简单安全
            count = 0
            for item in items_to_update:
                po_id = item.get('id')
                # 查找对应的对象 (确保是PoCheckEtaSeven模型)
                po_obj = await sync_to_async(
                    lambda pid=po_id: PoCheckEtaSeven.objects.filter(id=pid).first()
                )()
                
                if po_obj:
                    po_obj.fba_id = item.get('fba_id')
                    po_obj.ref_id = item.get('ref_id')
                    po_obj.handling_method = item.get('handling_method')
                    
                    # 处理 Boolean 字段
                    new_notify_status = item.get('is_notified')
                    po_obj.is_notified = new_notify_status
                    
                    # 如果状态变为已通知且之前没有时间，记录时间 (可选逻辑)
                    if new_notify_status and not po_obj.notified_time:
                        po_obj.notified_time = datetime.now()
                    elif not new_notify_status:
                        po_obj.notified_time = None
                        
                    po_obj.is_active = item.get('is_active')
                    
                    await sync_to_async(po_obj.save)()
                    count += 1
            context = {"success_messages": f'成功更新 {count} 条 PO 记录!'}
        
        return await self.handle_appointment_management_post(request,context)

    async def _invalid_po_check(
        self, warehouse
    ) -> dict[str, dict]:
        '''查询失效的po'''
        warehouse = warehouse.split('-')[0]
        # 如果提柜前一天状态为失效，或者提柜前一天没有查，到港前一周查了是失效
        # --- 1. 构建查询条件 (保持不变) ---
        query1 = models.Q(last_retrieval_checktime__isnull=False) & models.Q(
            last_retrieval_status=False
        )
        query2 = (
            models.Q(last_retrieval_checktime__isnull=True)
            & models.Q(last_eta_checktime__isnull=False)
            & models.Q(last_eta_status=False)
        )
        query = query1 | query2
        query &= models.Q(
            container_number__orders__retrieval_id__retrieval_destination_area=warehouse
        )
        query &= models.Q(ref_id__isnull=False)
        query &= ~models.Q(ref_id="")

        # --- 2. 定义同步查询函数 (关键修改) ---
        def get_po_data():
            # 使用 select_related 预加载外键字段
            # 请确认 'customer_name' 和 'container_number' 是你的外键字段名
            # 如果还有其他外键在模板中显示（如 destination 也是外键），也需要加进去
            qs = PoCheckEtaSeven.objects.filter(query).select_related(
                'customer_name', 
                'container_number'
            ).distinct() 
            return list(qs)
        # --- 3. 异步调用 ---
        po_checks_list = await sync_to_async(get_po_data)()
        # 排序
        po_checks_list.sort(key=lambda po: po.is_notified)

        return po_checks_list
    
    async def _get_packing_list(
        self,user,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
        name: str | None = None
    ) -> list[Any]:
        pl_criteria &= models.Q(container_number__orders__cancel_notification=False)& ~models.Q(container_number__orders__order_type='直送')
        plt_criteria &= models.Q(container_number__orders__cancel_notification=False)& ~models.Q(container_number__orders__order_type='直送')
        if await self._validate_user_four_major_whs(user):
            pl_criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)
            plt_criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)

        def sort_key(item):
            custom_method = item.get("custom_delivery_method")
            if custom_method is None:
                custom_method = ""
            keywords = ["暂扣", "HOLD", "留仓"]
            return (any(k in custom_method for k in keywords),)
        
        def sort_key_pl(item):
            # 第一优先级：按四组分类 + 时间排序
            if item.get('has_actual_retrieval'):
                # 实际提柜
                actual_time = item.get('actual_retrieval_time')
                group = 0
                sort_time = actual_time or datetime.min

            elif item.get('has_appointment_retrieval'):
                # 码头预约
                arm_time = item.get('arm_time')
                group = 1
                sort_time = arm_time or datetime.min

            elif item.get('has_estimated_retrieval'):
                # 预计提柜
                estimated_time = item.get('estimated_time')
                group = 2
                sort_time = estimated_time or datetime.min

            else:
                # 无计划
                group = 3
                sort_time = datetime.min
            
            # 优先级2: 把包含暂扣的放最后面
            custom_method = item.get("custom_delivery_method", "") or ""
            keywords = ["暂扣", "HOLD", "留仓"]
            has_hold = any(k in custom_method.upper() for k in keywords)
            hold_flag = 1 if has_hold else 0
            return (group, sort_time, hold_flag)
        
        data = []
        if plt_criteria:
            pal_list = await sync_to_async(list)(
                Pallet.objects.prefetch_related(
                    "container_number",
                    "container_number__orders",
                    "container_number__orders__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__orders__offload_id",
                    "container_number__orders__customer_name",
                    "container_number__orders__retrieval_id",
                    "container_number__orders__vessel_id",
                )
                .filter(plt_criteria)
                .annotate(
                    str_id=Cast("id", CharField()),
                    str_container_number=Cast("container_number__container_number", CharField()),                  
                    # 格式化vessel_eta为月日
                    formatted_offload_at=Func(
                        F('container_number__orders__offload_id__offload_at'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    is_zhunshida=Case(
                        When(container_number__orders__customer_name__zem_name__icontains='准时达', then=Value(True)),
                        default=Value(False),
                        output_field=BooleanField()
                    ),
                    # 创建完整的组合字段，通过前缀区分状态
                    container_with_eta_retrieval=Concat(
                        Value("[已入仓]"),
                        "container_number__container_number",
                        Value(" 入仓:"),
                        "formatted_offload_at",
                        output_field=CharField()
                    ),
                    data_source=Value("PALLET", output_field=CharField()),  # 添加数据源标识
                    is_pass=Value(True, output_field=BooleanField()),
                    rebuilt_is_dropped_pallet=Case(
                        When(
                            is_dropped_pallet=True,
                            then=Func(
                                F('master_shipment_batch_number__shipment_appointment'),
                                Value('YYYY-MM-DD'),
                                function='to_char',
                                output_field=CharField()
                            )
                        ),
                        default=Value(""),
                        output_field=CharField()
                    )                   
                )
                .values(
                    "destination",
                    "delivery_method",
                    "abnormal_palletization",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "container_number",
                    "is_dropped_pallet",
                    "rebuilt_is_dropped_pallet",
                    "shipment_batch_number__shipment_batch_number",
                    "data_source",  # 包含数据源标识
                    "shipment_batch_number__fleet_number__fleet_number",
                    "location",  # 添加location用于比较
                    "is_pass",
                    "is_zhunshida",
                    warehouse=F(
                        "container_number__orders__retrieval_id__retrieval_destination_precise"
                    ),               
                    retrieval_destination_precise=F("container_number__orders__retrieval_id__retrieval_destination_precise"),
                )
                .annotate(
                    custom_delivery_method=F("delivery_method"),
                    fba_ids=F("fba_id"),
                    ref_ids=F("ref_id"),
                    shipping_marks=F("shipping_mark"),
                    plt_ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    container_numbers=StringAgg(  # 聚合完整的组合字段
                        "container_with_eta_retrieval", delimiter="\n", distinct=True, ordering="container_with_eta_retrieval"
                    ),
                    cns=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    offload_time=StringAgg(
                        "formatted_offload_at", delimiter="\n", distinct=True, ordering="formatted_offload_at"
                    ),
                    total_pcs=Sum("pcs", output_field=IntegerField()),
                    total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                    total_weight_lbs=Round(Sum("weight_lbs", output_field=FloatField()),3),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True),
                )
                .order_by("container_number__orders__offload_id__offload_at")
            )
            #去排查是否有转仓的，有转仓的要特殊处理
            pal_list_trans = await self._find_transfer(pal_list)
            pal_list_sorted = sorted(pal_list_trans, key=sort_key)
            data += pal_list_sorted
        
        # PackingList 查询 - 添加数据源标识
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__orders",
                    "container_number__orders__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__orders__offload_id",
                    "container_number__orders__customer_name",
                    "container_number__orders__retrieval_id",
                    "container_number__orders__vessel_id",
                )
                .filter(pl_criteria)
                .annotate(
                    #方便后续排序
                    has_actual_retrieval=Case(
                        When(container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False, then=Value(1)),
                        default=Value(0),
                        output_field=IntegerField()
                    ),
                    has_appointment_retrieval=Case(
                        When(container_number__orders__retrieval_id__generous_and_wide_target_retrieval_timestamp__isnull=False, then=Value(1)),
                        default=Value(0),
                        output_field=IntegerField()
                    ),
                    has_estimated_retrieval=Case(
                        When(
                            Q(container_number__orders__retrieval_id__target_retrieval_timestamp_lower__isnull=False) |
                            Q(container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False),
                            then=Value(1)
                        ),
                        default=Value(0),
                        output_field=IntegerField()
                    ),
                    is_zhunshida=Case(
                        When(container_number__orders__customer_name__zem_name__icontains='准时达', then=Value(True)),
                        default=Value(False),
                        output_field=BooleanField()
                    ),
                    custom_delivery_method=Case(
                        When(
                            Q(delivery_method="暂扣留仓(HOLD)")
                            | Q(delivery_method="暂扣留仓"),
                            then=Concat(
                                "delivery_method",
                                Value("-"),
                                "fba_id",
                                Value("-"),
                                "id",
                            ),
                        ),
                        default=F("delivery_method"),
                        output_field=CharField(),
                    ),     
                    str_container_number=Cast("container_number__container_number", CharField()),    
                    # 格式化vessel_eta为月日
                    formatted_vessel_eta=Func(
                        F('container_number__orders__vessel_id__vessel_eta'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 格式化实际提柜时间为月日
                    formatted_actual_retrieval=Func(
                        F('container_number__orders__retrieval_id__actual_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    # 格式化码头预约时间为月日
                    formatted_appointment_retrieval=Func(
                        F('container_number__orders__retrieval_id__generous_and_wide_target_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    # 格式化预计提柜时间为月日
                    formatted_target_low=Func(
                        F('container_number__orders__retrieval_id__target_retrieval_timestamp_lower'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    formatted_target=Func(
                        F('container_number__orders__retrieval_id__target_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 创建完整的组合字段，通过前缀区分状态
                    container_with_eta_retrieval=Case(
                        # 有实际提柜时间 - 使用前缀 [实际]
                        When(container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Concat(
                                # Value(" "),
                                # "container_number__orders__vessel_id__vessel", 
                                Value("[已提柜]"),
                                "container_number__container_number",                          
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                Value(" 提柜:"),
                                "formatted_actual_retrieval",
                                output_field=CharField()
                            )),
                        # 有码头预约时间 - 使用前缀 [码头预约]
                        When(container_number__orders__retrieval_id__generous_and_wide_target_retrieval_timestamp__isnull=False,
                            then=Concat(
                                Value("[码头预约]"),
                                "container_number__container_number",
                                Value(" 预计提柜:"),
                                "formatted_appointment_retrieval",
                                output_field=CharField()
                            )),
                        # 有预计提柜时间范围 - 使用前缀 [预计]
                        When(container_number__orders__retrieval_id__target_retrieval_timestamp_lower__isnull=False,
                            then=Concat( 
                                # Value(" "),
                                # "container_number__orders__vessel_id__vessel", 
                                Value("[预计]"),
                                "container_number__container_number",
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                Value(" 提柜:"),
                                "formatted_target_low",
                                Value("~"),
                                Coalesce("formatted_target", "formatted_target_low"),
                                output_field=CharField()
                            )),
                        # 没有提柜计划 - 使用前缀 [未安排]
                        default=Concat(
                            # Value(" "),
                            # "container_number__orders__vessel_id__vessel", 
                            Value("[未安排提柜]"),
                            "container_number__container_number",
                            Value(" ETA:"),
                            "formatted_vessel_eta",
                            output_field=CharField()
                        ),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                    data_source=Value("PACKINGLIST", output_field=CharField()),  # 添加数据源标识
                    is_pass=Case(
                        # 1. 先看 planned_release_time 是否有值
                        When(
                            container_number__orders__retrieval_id__planned_release_time__isnull=False,
                            then=Value(True)
                        ),
                        # 2. 如果没有 planned_release_time，看 temp_t49_available_for_pickup 是否为 True
                        When(
                            container_number__orders__retrieval_id__temp_t49_available_for_pickup=True,
                            then=Value(True)
                        ),
                        # 3. 都不满足则为 False
                        default=Value(False),
                        output_field=BooleanField()
                    ),
                )
                .values(
                    "destination",
                    "custom_delivery_method",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "container_number",
                    "data_source",  # 包含数据源标识
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_number",
                    # 排序字段
                    "has_actual_retrieval",
                    "has_appointment_retrieval", 
                    "has_estimated_retrieval",
                    "is_zhunshida", 
                    warehouse=F(
                        "container_number__orders__retrieval_id__retrieval_destination_precise"
                    ),
                    vessel_name=F("container_number__orders__vessel_id__vessel"),
                    vessel_voyage=F("container_number__orders__vessel_id__voyage"),
                    vessel_eta=F("container_number__orders__vessel_id__vessel_eta"),
                    is_pass=F("is_pass"),
                    # 添加时间字段用于排序
                    actual_retrieval_time=F("container_number__orders__retrieval_id__actual_retrieval_timestamp"),
                    arm_time=F("container_number__orders__retrieval_id__generous_and_wide_target_retrieval_timestamp"),
                    estimated_time=F("container_number__orders__retrieval_id__target_retrieval_timestamp"),                  
                )
                .annotate(
                    fba_ids=StringAgg(
                        "str_fba_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_fba_id",
                    ),
                    ref_ids=StringAgg(
                        "str_ref_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_ref_id",
                    ),
                    shipping_marks=StringAgg(
                        "str_shipping_mark",
                        delimiter=",",
                        distinct=True,
                        ordering="str_shipping_mark",
                    ),
                    ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    container_numbers=StringAgg(  # 聚合完整的组合字段
                        "container_with_eta_retrieval", delimiter="\n", distinct=True, ordering="container_with_eta_retrieval"
                    ),
                    cns=StringAgg(
                        "str_container_number", delimiter="\n", distinct=True, ordering="str_container_number"
                    ),
                    offload_time = Case(
                        # 有实际提柜时间
                        When(
                            container_number__orders__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Concat(
                                Value("实际提柜："),
                                Func(
                                    F('container_number__orders__retrieval_id__actual_retrieval_timestamp'),
                                    Value('YYYY-MM-DD'),
                                    function='to_char'
                                ),
                                output_field=CharField()
                            )
                        ),

                        # 同时有上下限 → 范围
                        When(
                            Q(container_number__orders__retrieval_id__target_retrieval_timestamp_lower__isnull=False)
                            & Q(container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False),
                            then=Concat(
                                Value("预计提柜："),
                                Func(
                                    F('container_number__orders__retrieval_id__target_retrieval_timestamp_lower'),
                                    Value('YYYY-MM-DD'),
                                    function='to_char'
                                ),
                                Value("~"),
                                Func(
                                    F('container_number__orders__retrieval_id__target_retrieval_timestamp'),
                                    Value('YYYY-MM-DD'),
                                    function='to_char'
                                ),
                                output_field=CharField()
                            )
                        ),

                        # 只有下限
                        When(
                            container_number__orders__retrieval_id__target_retrieval_timestamp_lower__isnull=False,
                            then=Concat(
                                Value("预计提柜："),
                                Func(
                                    F('container_number__orders__retrieval_id__target_retrieval_timestamp_lower'),
                                    Value('YYYY-MM-DD'),
                                    function='to_char'
                                ),
                                output_field=CharField()
                            )
                        ),

                        # 只有上限
                        When(
                            container_number__orders__retrieval_id__target_retrieval_timestamp__isnull=False,
                            then=Concat(
                                Value("预计提柜："),
                                Func(
                                    F('container_number__orders__retrieval_id__target_retrieval_timestamp'),
                                    Value('YYYY-MM-DD'),
                                    function='to_char'
                                ),
                                output_field=CharField()
                            )
                        ),

                        # 都没有
                        default=Value("无预计提柜"),
                        output_field=CharField()
                    ),
                    total_pcs=Sum("pcs", output_field=FloatField()),
                    total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                    total_weight_lbs=Round(Sum("total_weight_lbs", output_field=FloatField()),3),
                    total_n_pallet_est= Ceil(Sum("cbm", output_field=FloatField()) / 2),
                    label=Value("EST"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True)
                )
                .distinct()
            )
            pl_list_sorted = sorted(pl_list, key=sort_key_pl)
            data += pl_list_sorted      
        return data

    async def _find_transfer(self, pal_list:list):
        # 第一步：先筛选出需要修改的记录
        need_update_pallets = []
        for pallet in pal_list:
            retrieval_destination = pallet.get('retrieval_destination_precise')
            current_location = pallet.get('location')
            
            # 检查是否需要修改
            if retrieval_destination and current_location and retrieval_destination != current_location:
                need_update_pallets.append(pallet)

        # 第二步：只对需要修改的记录查询TransferLocation
        if need_update_pallets:
            # 获取需要查询的pallet IDs
            all_need_update_ids = set()
            plt_ids_to_pallet_map = {} 
            for pallet in need_update_pallets:
                plt_ids_str = pallet.get('plt_ids', '')
                if plt_ids_str:
                    try:
                        plt_id_list = [pid.strip() for pid in plt_ids_str.split(',') if pid.strip()]
                        for plt_id in plt_id_list:
                            if plt_id.isdigit():
                                plt_id_int = int(plt_id)
                                all_need_update_ids.add(plt_id_int)
                                plt_ids_to_pallet_map[plt_id_int] = pallet
                    except (ValueError, AttributeError):
                        continue
            # 批量查询TransferLocation记录
            transfer_locations = await sync_to_async(list)(
                TransferLocation.objects.filter(plt_ids__isnull=False)
            )
            
            # 创建plt_id到TransferLocation的映射
            plt_id_transfer_map = {}
            for transfer in transfer_locations:
                if transfer.plt_ids:
                    try:
                        transfer_plt_ids = [pid.strip() for pid in transfer.plt_ids.split(',') if pid.strip()]
                        for plt_id in transfer_plt_ids:
                            if plt_id.isdigit():
                                plt_id_int = int(plt_id)
                                if plt_id_int in all_need_update_ids:
                                    plt_id_transfer_map[plt_id_int] = transfer
                    except (ValueError, AttributeError):
                        continue
            
            # 第三步：处理每个需要更新的pallet记录
            processed_pallets = set()  # 记录已经处理过的pallet记录（避免重复处理）
            
            for plt_id, transfer_record in plt_id_transfer_map.items():
                pallet = plt_ids_to_pallet_map.get(plt_id)
                if pallet and id(pallet) not in processed_pallets:
                    retrieval_destination = pallet.get('retrieval_destination_precise')
                    
                    if transfer_record:
                        # 提取原始仓名称（retrieval_destination_precise以-分组，取前面的值）
                        original_warehouse = retrieval_destination.split('-')[0] if '-' in retrieval_destination else retrieval_destination
                        if transfer_record.arrival_time:              
                            # 格式化到达时间
                            arrival_time_str = transfer_record.arrival_time.strftime('%m-%d')
                        elif transfer_record.ETA: 
                            # 格式化到达时间
                            arrival_time_str = transfer_record.ETA.strftime('%m-%d')
                        else:
                            arrival_time_str = "转仓中"                     
                        # 修改offload_time
                        pallet['offload_time'] = f"{original_warehouse}-{arrival_time_str}"
                    
                    processed_pallets.add(id(pallet))
            
            # 第四步：处理没有找到TransferLocation记录但需要更新的pallet
            for pallet in need_update_pallets:
                if id(pallet) not in processed_pallets:
                    retrieval_destination = pallet.get('retrieval_destination_precise')
                    original_warehouse = retrieval_destination.split('-')[0] if '-' in retrieval_destination else retrieval_destination
                    pallet['offload_time'] = f"{original_warehouse}-转仓中"
        return pal_list

    async def get_shipments_by_warehouse(self, warehouse, request, four_major_whs: str | None = None):
        """异步获取指定仓库相关的预约数据"""
        criteria = (
            (
                models.Q(origin__isnull=True)
                | models.Q(origin="")
                | models.Q(origin=warehouse)
            )
            & models.Q(appointment_id__isnull=False)
            & models.Q(in_use=False, is_canceled=False)
        )
        if four_major_whs == "four_major_whs":
            criteria &= models.Q(destination__in=FOUR_MAJOR_WAREHOUSES)
        appointment = await sync_to_async(list)(
            Shipment.objects.filter(criteria).order_by("shipment_appointment","shipment_account")
        )
        
        return appointment
    
    async def get_used_pallets(self, shipment):
        """异步计算预约已使用的板数"""
        
        # 异步获取相关订单
        related_orders = await sync_to_async(list)(shipment.order_set.all())
        total_pallets = 0
        
        async def process_order(order):
            order_pallets = 0
            packing_lists = await sync_to_async(list)(order.container_number.packinglist_set.all())
            for packing in packing_lists:
                order_pallets += packing.n_pallet or 0
            return order_pallets
        
        tasks = [process_order(order) for order in related_orders]
        order_pallets_list = await asyncio.gather(*tasks)
        total_pallets = sum(order_pallets_list)
        
        return total_pallets
    
    def _parse_tzinfo(self, s: str) -> str:
        if not s:
            return "America/New_York"
        if "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    async def _now_time_get(self, warehouse):
        today = timezone.now()
        if 'LA' in warehouse:
            local_tz = pytz.timezone("America/Los_Angeles")
        else:
            local_tz = pytz.timezone("America/New_York")

        today = timezone.localtime(today, local_tz)
        return today
    
    async def _four_major_calculate_summary(self, unshipment_pos, shipments, scheduled_data, schedule_fleet_data, ready_to_ship_data, delivery_data, pod_data, warehouse):
        now = await self._now_time_get(warehouse)
        # 计算预约状态统计
        expired_count = 0
        urgent_count = 0
        available_count = 0
        used_count = 0  # 已使用的预约数量
        
        for shipment in shipments:
            # 检查预约是否已过期
            is_expired = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc < now
            )
            
            # 检查预约是否即将过期（7天内）
            is_urgent = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc - now < timedelta(days=7) and
                not is_expired
            )
            
            # 检查预约是否已被使用（通过 PackingList 或 Pallet 绑定）
            has_packinglist = await self.has_related_packinglist(shipment)
            has_pallet = await self.has_related_pallet(shipment)
            is_used = has_packinglist or has_pallet
            
            if is_used:
                shipment.status = "used"
                used_count += 1
            elif is_expired:
                shipment.status = "expired"
                expired_count += 1
            elif is_urgent:
                shipment.status = "urgent"
                urgent_count += 1
            else:
                shipment.status = "available"
                available_count += 1
        
        # 计算货物统计
        pending_cargos_count = len(unshipment_pos)
        
        # 计算总板数
        total_pallets = 0
        for cargo in unshipment_pos:
            if cargo.get('label') == 'ACT':  # 实际板数
                total_pallets += cargo.get('total_n_pallet_act', 0) or 0
            else:  # 预估板数
                total_pallets += cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'expired_count': expired_count,
            'urgent_count': urgent_count,
            'available_count': available_count,
            'used_count': used_count,  # 已使用的预约数量
            'pending_cargo_count': pending_cargos_count,
            'total_pallets': int(total_pallets),
            'scheduled_sp_count': len(scheduled_data),
            'schedule_fl_count': len(schedule_fleet_data),
            'ready_to_ship_count': len(ready_to_ship_data),
            'ready_count': len(delivery_data),
            'pod_count': len(pod_data),
        }

    async def calculate_summary(self, unshipment_pos, shipments, warehouse):
        """异步计算统计数据 - 适配新的数据结构"""
        now = await self._now_time_get(warehouse)
        # 计算预约状态统计
        expired_count = 0
        urgent_count = 0
        available_count = 0
        used_count = 0  # 已使用的预约数量
        
        for shipment in shipments:
            # 检查预约是否已过期
            is_expired = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc < now
            )
            
            # 检查预约是否即将过期（7天内）
            is_urgent = (
                shipment.shipment_appointment_utc and 
                shipment.shipment_appointment_utc - now < timedelta(days=7) and
                not is_expired
            )
            
            # 检查预约是否已被使用（通过 PackingList 或 Pallet 绑定）
            has_packinglist = await self.has_related_packinglist(shipment)
            has_pallet = await self.has_related_pallet(shipment)
            is_used = has_packinglist or has_pallet
            
            if is_used:
                shipment.status = "used"
                used_count += 1
            elif is_expired:
                shipment.status = "expired"
                expired_count += 1
            elif is_urgent:
                shipment.status = "urgent"
                urgent_count += 1
            else:
                shipment.status = "available"
                available_count += 1
        
        # 计算货物统计
        pending_cargos_count = len(unshipment_pos)
        
        # 计算总板数
        total_pallets = 0
        for cargo in unshipment_pos:
            if cargo.get('label') == 'ACT':  # 实际板数
                total_pallets += cargo.get('total_n_pallet_act', 0) or 0
            else:  # 预估板数
                total_pallets += cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'expired_count': expired_count,
            'urgent_count': urgent_count,
            'available_count': available_count,
            'used_count': used_count,  # 已使用的预约数量
            'pending_cargo_count': pending_cargos_count,
            'total_pallets': int(total_pallets),
        }

    async def has_related_packinglist(self, shipment):
        """检查预约是否有相关的 PackingList 记录"""
        
        try:
            # 使用 sync_to_async 包装数据库查询
            packinglist_exists = await sync_to_async(
                PackingList.objects.filter(shipment_batch_number=shipment).exists
            )()
            return packinglist_exists
        except Exception:
            return False

    async def has_related_pallet(self, shipment):
        """检查预约是否有相关的 Pallet 记录"""
        
        try:
            # 使用 sync_to_async 包装数据库查询
            pallet_exists = await sync_to_async(
                Pallet.objects.filter(shipment_batch_number=shipment).exists
            )()
            return pallet_exists
        except Exception:
            return False
    
    async def has_appointment(self, cargo):
        """异步判断货物是否已有预约 - 适配新的数据结构"""
        # 根据你的数据结构，判断是否有预约号
        return cargo.get('shipment_batch_number__shipment_batch_number') is not None
    
    async def get_matching_suggestions(self, unshipment_pos, shipments, max_cbm,max_pallet):
        """异步生成智能匹配建议 - 适配新的数据结构"""
        
        suggestions = []

        # 第一级分组：按目的地和派送方式预分组
        pre_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = (cargo.get('destination') or '').strip().upper()
                raw_method = (cargo.get('custom_delivery_method') or '').strip()
                if '卡车派送' in raw_method:
                    delivery_method = '卡派'
                elif '暂扣' in raw_method:
                    delivery_method = '暂扣'
                elif '客户自提' in raw_method:
                    delivery_method = '客提'
                else:
                    delivery_method = raw_method.upper()
                if not dest or not delivery_method:
                    continue
                    
                group_key = f"{dest}_{delivery_method}"
                if group_key not in pre_groups:
                    pre_groups[group_key] = {
                        'destination': dest,
                        'delivery_method': delivery_method,
                        'cargos': []
                    }
                pre_groups[group_key]['cargos'].append(cargo)
        
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__orders__vessel_id__vessel_eta') or '')
            
            # 按容量限制创建大组
            primary_groups = []
            current_primary_group = {
                'destination': pre_group['destination'],
                'delivery_method': pre_group['delivery_method'],
                'cargos': [],  # 这个大组包含的所有货物（每个货物就是一个小组）
                'total_pallets': 0,
                'total_cbm': 0,
            }
            
            for cargo in sorted_cargos:
                cargo_pallets = 0
                if cargo.get('label') == 'ACT':
                    cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                else:
                    cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                
                cargo_cbm = cargo.get('total_cbm', 0) or 0
                
                # 检查当前大组是否还能容纳这个货物
                if (current_primary_group['total_pallets'] + cargo_pallets <= max_pallet and 
                    current_primary_group['total_cbm'] + cargo_cbm <= max_cbm):
                    # 可以加入当前大组
                    current_primary_group['cargos'].append(cargo)
                    current_primary_group['total_pallets'] += cargo_pallets
                    current_primary_group['total_cbm'] += cargo_cbm
                else:
                    # 当前大组已满，保存并创建新的大组
                    if current_primary_group['cargos']:
                        primary_groups.append(current_primary_group)
                    
                    # 创建新的大组
                    current_primary_group = {
                        'destination': pre_group['destination'],
                        'delivery_method': pre_group['delivery_method'],
                        'cargos': [cargo],
                        'total_pallets': cargo_pallets,
                        'total_cbm': cargo_cbm,
                    }
            
            # 添加最后一个大组
            if current_primary_group['cargos']:
                primary_groups.append(current_primary_group)
            
            # 为每个大组创建建议，大组中的每个货物都是一个小组（一行）
            for primary_group_index, primary_group in enumerate(primary_groups):
                # 计算大组的匹配度百分比
                pallets_percentage = min(100, (primary_group['total_pallets'] / max_pallet) * 100) if max_pallet > 0 else 0
                cbm_percentage = min(100, (primary_group['total_cbm'] / max_cbm) * 100) if max_cbm > 0 else 0
                
                # 大组中的每个货物都是一个小组（一行）
                for subgroup_index, cargo in enumerate(primary_group['cargos']):
                    # 计算这个货物的板数和CBM
                    cargo_pallets = 0
                    if cargo.get('label') == 'ACT':
                        cargo_pallets = cargo.get('total_n_pallet_act', 0) or 0
                    else:
                        cargo_pallets = cargo.get('total_n_pallet_est', 0) or 0
                    
                    cargo_cbm = cargo.get('total_cbm', 0) or 0
                    
                    suggestion = {
                        'id': f"{group_key}_{primary_group_index}_{subgroup_index}",
                        'primary_group': {
                            'destination': primary_group['destination'],
                            'delivery_method': primary_group['delivery_method'],
                            'total_pallets': primary_group['total_pallets'],
                            'total_cbm': primary_group['total_cbm'],
                            'pallets_percentage': pallets_percentage,
                            'cbm_percentage': cbm_percentage,
                        },
                        'subgroup': {
                            'cargos': [{
                                'ids': cargo.get('ids', ''),  # 确保包含ids
                                'plt_ids': cargo.get('plt_ids', ''),  # 确保包含plt_ids
                                'container_numbers': cargo.get('container_numbers', ''),
                                'cns': cargo.get('cns', ''),
                                'offload_time': cargo.get('offload_time',''),
                                'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                                'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                                'total_cbm': cargo.get('total_cbm', 0),
                                'label': cargo.get('label', ''),
                            }],
                            'total_pallets': cargo_pallets,
                            'total_cbm': cargo_cbm,
                            'container_numbers': cargo.get('container_numbers', ''),
                            'cns': cargo.get('cns', ''),
                            'offload_time': cargo.get('offload_time',''),
                            'cargo_count': 1
                        },
                        'subgroup_index': subgroup_index + 1,
                    }
                    suggestions.append(suggestion)
        return suggestions
    
    async def is_shipment_available(self, shipment):
        """判断预约是否可用"""     
        now = timezone.now()
        
        # 已发货的不可用
        if shipment.shipped_at:
            return False
        
        # 已过期的不可用
        if (shipment.shipment_appointment and 
            shipment.shipment_appointment < now):
            return False
        
        return True
    
    async def get_auto_matches(self, unshipment_pos, shipments):
        """异步获取自动匹配结果 - 适配新的数据结构"""
        matches = []
        
        # 按目的地分组货物
        destination_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = cargo.get('destination')
                if dest not in destination_groups:
                    destination_groups[dest] = []
                destination_groups[dest].append(cargo)
        
        # 为每个目的地生成匹配组合
        match_id = 1
        for destination, cargo_list in destination_groups.items():
            # 按板数排序，优先匹配大板数的货物
            sorted_cargos = sorted(cargo_list, 
                                 key=lambda x: x.get('total_n_pallet_act', 0) or x.get('total_n_pallet_est', 0) or 0, 
                                 reverse=True)
            
            # 生成匹配组合（尽量接近35板）
            current_group = []
            current_pallets = 0
            
            for cargo in sorted_cargos:
                cargo_pallets = (cargo.get('total_n_pallet_act', 0) or 
                               cargo.get('total_n_pallet_est', 0) or 0)
                
                if current_pallets + cargo_pallets <= 35:
                    current_group.append(cargo)
                    current_pallets += cargo_pallets
                else:
                    # 当前组已满，创建匹配
                    if current_group:
                        match_percentage = min(int((current_pallets / 35) * 100), 100)
                        
                        # 查找最佳预约
                        best_shipment = await self.find_best_shipment_for_match(destination, current_pallets, shipments)
                        
                        matches.append({
                            'id': match_id,
                            'destination': destination,
                            'cargo_count': len(current_group),
                            'total_pallets': int(current_pallets),
                            'recommended_appointment': best_shipment,
                            'match_percentage': match_percentage,
                            'cargos': current_group[:5]  # 只显示前5个货物详情
                        })
                        match_id += 1
                    
                    # 开始新组
                    current_group = [cargo]
                    current_pallets = cargo_pallets
            
            # 处理最后一组
            if current_group:
                match_percentage = min(int((current_pallets / 35) * 100), 100)
                best_shipment = await self.find_best_shipment_for_match(destination, current_pallets, shipments)
                
                matches.append({
                    'id': match_id,
                    'destination': destination,
                    'cargo_count': len(current_group),
                    'total_pallets': int(current_pallets),
                    'recommended_appointment': best_shipment,
                    'match_percentage': match_percentage,
                    'cargos': current_group[:5]
                })
                match_id += 1
        
        return matches[:10]  # 限制返回数量
    
    async def find_best_shipment_for_match(self, destination, total_pallets, shipments):
        """为匹配组合查找最佳预约"""
        best_shipment = None
        best_capacity_diff = float('inf')
        
        for shipment in shipments:
            if (shipment.destination == destination and 
                await self.is_shipment_available(shipment)):
                
                used_pallets = await self.get_used_pallets(shipment)
                remaining_capacity = 35 - used_pallets
                capacity_diff = abs(remaining_capacity - total_pallets)
                
                if capacity_diff < best_capacity_diff:
                    best_capacity_diff = capacity_diff
                    best_shipment = shipment
        
        return best_shipment

    async def get_used_pallets(self, shipment):
        """异步计算预约已使用的板数 - 适配新的数据结构"""
        # 根据你的数据结构，这里需要计算该预约已经使用的板数
        # 由于你的数据结构中没有直接关联，这里可能需要根据实际情况调整
        
        # 临时实现：假设从关联的订单中计算
        try:
            # 获取该预约关联的所有货物
            related_packing_lists = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number=shipment
                )
            )
            
            total_pallets = 0
            for pl in related_packing_lists:
                total_pallets += pl.n_pallet or 0
            
            return total_pallets
        except:
            return 0
    
    def _parse_ts(self, ts: str, tzinfo: str) -> str:
        if ts:
            if isinstance(ts, str):
                ts_naive = datetime.fromisoformat(ts)
            else:
                ts_naive = ts.replace(tzinfo=None)
            tz = pytz.timezone(tzinfo)
            ts = tz.localize(ts_naive).astimezone(timezone.utc)
            return ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
        
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
    
    async def _validate_user_four_major_whs(self, user: User) -> bool:       
        return await sync_to_async(
            lambda: user.groups.filter(name="four_major_whs").exists()
        )()
