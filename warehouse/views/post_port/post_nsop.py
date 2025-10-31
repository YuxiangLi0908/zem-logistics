from datetime import datetime, timedelta
from typing import Any
from django.db.models import Prefetch

import pandas as pd
import json
import uuid
import pytz
import io
import zipfile
from django.utils.safestring import mark_safe
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Round, Cast, Coalesce
from django.core.exceptions import ObjectDoesNotExist
from simple_history.utils import bulk_update_with_history
from django.db import models
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
import asyncio
from django.core.exceptions import MultipleObjectsReturned
from django.db.models.functions import Cast, Concat
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View
from django.utils import timezone
from datetime import timedelta
from dateutil.parser import parse
from django.core.serializers.json import DjangoJSONEncoder

from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.fleet import Fleet
from warehouse.models.order import Order
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.shipment import Shipment
from django.contrib import messages
from warehouse.models.transfer_location import TransferLocation
from warehouse.views.post_port.shipment.fleet_management import FleetManagement
from warehouse.views.post_port.shipment.shipping_management import ShippingManagement
from warehouse.views.po import PO
from warehouse.utils.constants import (
    LOAD_TYPE_OPTIONS,
    amazon_fba_locations,
)

class PostNsop(View):
    template_main_dash = "post_port/new_sop/01_appointment/01_appointment_management.html"
    template_td_shipment = "post_port/new_sop/02_shipment/02_td_shipment.html"
    template_fleet_schedule = "post_port/new_sop/03_fleet_schedule.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    warehouse_options = {"":"", "NJ-07001": "NJ-07001", "SAV-31326": "SAV-31326", "LA-91761": "LA-91761"}
    account_options = {
        "": "",
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
    }
    shipment_type_options = {
        "FTL": "FTL",
        "LTL": "LTL",
        "外配": "外配",
        "快递": "快递",
        "客户自提": "客户自提",
    }
    
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
        elif step == "fleet_management":
            template, context = await self.handle_fleet_management_get(request)
            return render(request, template, context)
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
        elif step == "td_shipment_warehouse":
            template, context = await self.handle_td_shipment_post(request)
            return render(request, template, context)
        elif step == "fleet_schedule_warehouse":
            template, context = await self.handle_fleet_schedule_post(request)
            return render(request, template, context)
        elif step == "export_pos":
            return await self.handle_export_pos(request)
        elif step == "appointment_time_modify":
            template, context = await self.handle_appointment_time(request)
            return render(request, template, context)
        elif step == "update_fleet":
            fm = FleetManagement()
            context = await fm.handle_update_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": "更新出库成功!"}) 
            return render(request, template, context)
        elif step == "fleet_confirmation":
            template, context = await self.handle_fleet_confirmation_post(request)
            return render(request, template, context) 
        elif step == "cancel_fleet":
            fm = FleetManagement()
            context = await fm.handle_cancel_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '取消批次成功!'})  
            return render(request, template, context)
        elif step == "confirm_delivery":
            fm = FleetManagement()
            context = await fm.handle_confirm_delivery_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '确认送达成功!'})  
            return render(request, template, context)
        elif step == "abnormal_fleet":
            fm = FleetManagement()
            context = await fm.handle_abnormal_fleet_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": '异常处理成功!'})  
            return render(request, template, context)
        elif step == "pod_upload":
            fm = FleetManagement()
            context = await fm.handle_pod_upload_post(request,'post_nsop')
            template, context = await self.handle_fleet_schedule_post(request)
            context.update({"success_messages": 'POD上传成功!'})           
            return render(request, template, context)
        elif step == "bind_group_shipment":
            template, context = await self.handle_appointment_post(request)
            return render(request, template, context) 
        elif step == "unassign_shipment":
            template, context = await self.handle_cancel_appointment_post(request)
            return render(request, template, context) 
        elif step == "fleet_departure":
            template, context = await self.handle_fleet_departure_post(request)
            return render(request, template, context)
        elif step == "add_pallet":
            template, context = await self.handle_add_pallet_post(request)
            return render(request, template, context)      
        elif step == "fleet_add_pallet":
            template, context = await self.handle_fleet_add_pallet_post(request)
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
            template, context = await self.handle_appointment_management_post(request,context)
            return render(request, template, context)    
        elif step == "create_empty_appointment":
            sm = ShippingManagement()
            info = await sm.handle_create_empty_appointment_post(request,'post_nsop') 
            context = {'success_messages':'备约登记成功！'}
            template, context = await self.handle_appointment_management_post(request,context)
            return render(request, template, context)   
        elif step == "download_empty_appointment_template":
            sm = ShippingManagement()
            return await sm.handle_download_empty_appointment_template_post()  
        elif step == "upload_and_create_empty_appointment":
            sm = ShippingManagement()
            info = await sm.handle_upload_and_create_empty_appointment_post(request,'post_nsop') 
            context = {'success_messages':'备约批量登记成功！'}
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
        else:
            raise ValueError('输入错误',step)
    
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
        
        selected_cargos_raw = request.POST.get('selected_cargos')

        move_ids = []
        if selected_cargos_raw:
            cargos_list = json.loads(selected_cargos_raw)
            move_ids = [sl['ids'] for sl in cargos_list]
        
        cargos.extend(cargos_list)
         # === 更新 primary_group 的统计数据 ===
        total_pallets = sum(c.get('total_n_pallet_act', 0) or c.get('total_n_pallet_est', 0) for c in cargos)
        total_cbm = sum(c.get('total_cbm', 0) for c in cargos)

        primary_group = suggestion_data.get('primary_group', {})
        if primary_group:
            # 更新主组的板数和CBM
            primary_group['total_pallets'] = total_pallets
            primary_group['total_cbm'] = total_cbm
        new_intelligent_cargos = [c for c in intelligent_cargos if c['ids'] not in move_ids]

        suggestion_data['cargos'] = cargos
        suggestion_data['intelligent_cargos'] = new_intelligent_cargos

        # 替换掉 all_suggestions 中对应项
        for i, s in enumerate(all_suggestions):
            if s['suggestion_id'] == suggestion_id:
                all_suggestions[i] = suggestion_data
                break
        return await self.handle_td_shipment_post(request,{},all_suggestions)

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
            return await self.handle_td_shipment_post(request,context)
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
        return await self.handle_td_shipment_post(request,context)
    
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
        tab = request.POST.get("tab")
        criteria_p = models.Q(
            (
                models.Q(container_number__order__order_type="转运")
                | models.Q(container_number__order__order_type="转运组合")
            ),
            container_number__order__packing_list_updloaded=True,
            shipment_batch_number__isnull=True,
            container_number__order__created_at__gte="2024-09-01",
        )
        pl_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=True,
            container_number__order__retrieval_id__retrieval_destination_area=warehouse,
            destination=destination,
        )
        plt_criteria = criteria_p & models.Q(
            container_number__order__offload_id__offload_at__isnull=False,
            location__startswith=warehouse,
            destination=destination,
        )
        packing_list_not_scheduled = await self._get_packing_list(
            pl_criteria, plt_criteria
        )
        context.update({
            "warehouse": warehouse,
            "destination": destination,
            "appointment_id": request.POST.get("appointment_id"),
            "packing_list_not_scheduled": packing_list_not_scheduled,
            #"step": step,  # ← 前端靠这个判断要不要弹窗
            "active_tab": tab,          # ← 用来控制前端打开哪个标签页    
        })
        if 'show_add_po_inventory_modal' not in context:
            context.update({"show_add_po_inventory_modal": True})# ← 控制是否直接弹出“添加PO”弹窗
        return await self.handle_td_shipment_post(request, context)
    
    async def handle_add_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        destination = request.POST.get("destination")
        tab = request.POST.get("tab")
        step = request.POST.get("step")
        criteria_plt = models.Q(
            shipment_batch_number__fleet_number__fleet_number__isnull=True,
            location=warehouse,
            destination=destination,
            container_number__order__offload_id__offload_at__isnull=False,
        )
        plt_unshipped = await self._get_packing_list(
            models.Q(
                container_number__order__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
            criteria_plt
        )
        
        context = {
            "warehouse": warehouse,
            "destination": destination,
            "plt_unshipped": plt_unshipped,
            "step": step,  # ← 前端靠这个判断要不要弹窗
            "active_tab": tab,          # ← 用来控制前端打开哪个标签页
            "show_add_po_modal": True,   # ← 控制是否直接弹出“添加PO”弹窗
            "add_po_title": "加塞",
            "add_pallet_ISA": request.POST.get('add_pallet_ISA')
        }
        return await self.handle_td_shipment_post(request, context)

    async def handle_fleet_departure_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        batch_number = request.POST.getlist('batch_number')
        new_batch = []
        for i in batch_number:
            shipment = await sync_to_async(Shipment.objects.get)(appointment_id=i)
            new_batch.append(shipment.shipment_batch_number)

        request.POST = request.POST.copy()
        request.POST.setlist('batch_number', new_batch)
        fm = FleetManagement()
        context = await fm.handle_fleet_departure_post(request,'post_nsop')
        return await self.handle_td_shipment_post(request,context)         
    
    async def handle_export_virtual_fleet_pos_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
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
        packinglist_data = await sync_to_async(
            lambda: list(
                PackingList.objects.select_related("container_number")
                .filter(id__in=pl_ids)
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
        pallet_data = await sync_to_async(
            lambda: list(
                Pallet.objects.select_related("container_number")
                .filter(id__in=plt_ids)
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
                    "pcs"
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
        # 合并数据
        combined_data = packinglist_data + pallet_data

        if not combined_data:
            raise ValueError("未找到匹配记录")

        # 聚合计算
        df = pd.DataFrame.from_records(combined_data)
        # 计算合计字段
        grouped = (
            df.groupby(
                [
                    "shipping_mark",
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "container_number__container_number",
                    "destination",
                    "label",
                ],
                as_index=False,
            )
            .agg({
                "cbm": "sum",
                "total_pcs": "sum",
                "id": "count",  # 👈 新增，统计 pallet 数
            })
            .rename(columns={"id": "total_n_pallet_act", "cbm": "total_cbm"})
        )
        grouped["total_n_pallet_est"] = grouped["total_cbm"] / 2
        def get_est_pallet(n):
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)

        grouped["total_n_pallet_est"] = grouped["total_n_pallet_est"].apply(get_est_pallet)
        grouped["is_valid"] = None
        grouped["is_est"] = grouped["label"] == "EST"
        grouped["Pallet Count"] = grouped.apply(
            lambda row: row["total_n_pallet_est"] if row["is_est"] else max(1, row.get("total_n_pallet_act", 1)),
            axis=1
        ).astype(int)

        # 重命名列以符合导出格式
        keep = [
            "shipping_mark",
            "container_number__container_number",
            "fba_id",
            "ref_id",
            "total_pcs",
            "Pallet Count",
            "label",
            "is_valid",
            "total_cbm",
            "destination",
        ]

        grouped = grouped[keep].rename(
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

        # 导出 CSV
        # 按 Destination 分组
        grouped_by_dest = {}
        for _, row in grouped.iterrows():
            dest = row["Destination"]
            grouped_by_dest.setdefault(dest, []).append(row.to_dict())

        # 如果只有一个 Destination，保持原来返回单 CSV
        if len(grouped_by_dest) == 1:
            df_single = pd.DataFrame.from_records(list(grouped_by_dest.values())[0])
            response = HttpResponse(content_type="text/csv")
            response["Content-Disposition"] = f"attachment; filename=PO_virtual_fleet.csv"
            df_single.to_csv(path_or_buf=response, index=False)
            return response

        # 多个 Destination 打包 zip
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for dest, rows in grouped_by_dest.items():
                df_dest = pd.DataFrame.from_records(rows)
                csv_buffer = io.StringIO()
                df_dest.to_csv(csv_buffer, index=False)
                zf.writestr(f"{dest}.csv", csv_buffer.getvalue())

        zip_buffer.seek(0)
        response = HttpResponse(zip_buffer, content_type="application/zip")
        response["Content-Disposition"] = "attachment; filename=PO_virtual_fleet.zip"
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
        
        # 存储处理结果
        success_groups = []
        failed_groups = []
        success_appointment_ids = []
        # 为每个大组分别处理预约
        for group_index, group_data in enumerate(booking_data, 1):
            # 准备调用 handle_appointment_post 所需的参数
            cargo_ids = group_data.get('cargo_ids', '')
            plt_ids = group_data.get('plt_ids', '')
            destination = group_data.get('destination', '')
            appointment_id = group_data.get('appointment_id', '')
            shipment_cargo_id = group_data.get('shipment_cargo_id', '')
            shipment_type = group_data.get('shipment_type', '')
            shipment_account = group_data.get('shipment_account', '')
            shipment_appointment = group_data.get('shipment_appointment', '')
            load_type = group_data.get('load_type', '')
            origin = group_data.get('origin', '')
            note = group_data.get('note', '')
            suggestion_id = group_data.get('suggestion_id')
            result = await self._process_single_group_booking(request, suggestion_id, cargo_ids, plt_ids, destination, appointment_id,shipment_cargo_id,shipment_type,
                                                              shipment_account,shipment_appointment,load_type,origin,note)
            
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
            messages.append(mark_safe(success_msg + "<br>"))
        
        # 存储成功创建的shipment IDs，方便后续约车使用
        if success_appointment_ids:
            # 排车
            fleet_number = await self._add_appointments_to_fleet(success_appointment_ids)
            success_msg = f"成功排车，车次号是 {fleet_number}"
            messages.append(success_msg)
        if messages:
            context.update({"success_messages": mark_safe("<br>".join(messages))})
    
        return await self.handle_td_shipment_post(request, context)

    async def _add_appointments_to_fleet(self,appointment_ids):
        print('appointment_ids',appointment_ids)
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
        print('shipment_info',shipment_info)
        shipment_ids = [item['id'] for item in shipment_info]
        shipment_types = list(set(item['shipment_type'] for item in shipment_info))
        origins = list(set(item['origin'] for item in shipment_info))
        print('shipment_types',shipment_types)
        print('origins',origins)

        total_weight, total_cbm, total_pcs, total_pallet = 0.0, 0.0, 0, 0
        #记录总数
        if shipment_ids:
            # 获取所有Pallet记录
            pallet_records = await sync_to_async(list)(
                Pallet.objects.filter(
                    shipment_batch_number_id__in=shipment_ids,
                    container_number__order__offload_id__offload_at__isnull=False
                )
            )
            
            # 获取所有PackingList记录  
            packinglist_records = await sync_to_async(list)(
                PackingList.objects.filter(
                    shipment_batch_number_id__in=shipment_ids,
                    container_number__order__offload_id__offload_at__isnull=True
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
                                                              shipment_account,shipment_appointment,load_type,origin,note) -> dict:
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
                context.update({'error_messages': "更新失败！"})
                return await self.handle_td_shipment_post(request,context)

        # 更新 Pallet
        if plt_id_list:
            updated_count = await sync_to_async(
                lambda: Pallet.objects.filter(id__in=plt_id_list).update(note_sp=note_sp)
            )()
            if updated_count == 0:
                context.update({'error_messages': "更新失败！"})
                return await self.handle_td_shipment_post(request,context)
        context.update({'sucess_messages':"更新备注成功！"}) 
        return await self.handle_td_shipment_post(request,context)


    async def handle_edit_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {}
        appointment_id_old = request.POST.get('appointment_id', '').strip()
        old_shipments = await sync_to_async(list)(
            Shipment.objects.filter(appointment_id=appointment_id_old)
        )      
        if not old_shipments:
            context.update({'error_messages':f"未找到 ISA={appointment_id_old}!"})     
        if len(old_shipments) > 1:
            context.update({'error_messages':f"找到多条相同 ISA={appointment_id_old}的记录，请检查数据!"})   
        
        old_shipment = old_shipments[0]

        appointment_id_new = request.POST.get('appointment_id_input', '').strip()
        shipment_appointment = request.POST.get('shipment_appointment')
        destination = request.POST.get('destination')
        load_type = request.POST.get('load_type')
        origin = request.POST.get('origin')

        if appointment_id_new == appointment_id_old:
            old_shipment.shipment_appointment = shipment_appointment
            old_shipment.destination = destination
            old_shipment.load_type = load_type
            old_shipment.origin = origin
            await sync_to_async(old_shipment.save)()
            context.update({'success_messages':'预约信息修改成功!'})
            return await self.handle_td_shipment_post(request)
        else:
            context.update({'error_messages':f"ISA不能修改"}) 
            return await self.handle_td_shipment_post(request,context)
    
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
        return await self.handle_td_shipment_post(request,context)
    
    async def handle_shipment_add_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {}
        appointment_id = request.POST.get("appointment_id")
        selected = request.POST.getlist("cargo_ids")
        selected_plt = request.POST.getlist("plt_ids")
        
        context = {}
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
        return await self.handle_td_shipment_post(request,context)
    
    async def handle_appointment_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        appointment_id = request.POST.get('appointment_id')
        ids = request.POST.get("cargo_ids")
        plt_ids = request.POST.get("plt_ids")
        selected = [int(i) for i in ids.split(",") if i]
        selected_plt = [int(i) for i in plt_ids.split(",") if i]
        context = {}
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
            context.update({"success_messages": f"删除成功，批次号是{shipment_batch_number}"})
            return await self.handle_td_shipment_post(request,context)
        
        destination = request.POST.get('destination')                  
        
        if selected or selected_plt:
            packing_list_selected = await self._get_packing_list(
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
            }
            request.POST = request.POST.copy()
            request.POST['shipment_data'] = str(shipment_data)
            request.POST['batch_number'] = shipment_batch_number     
            request.POST['pl_ids'] = selected
            request.POST['plt_ids'] = selected_plt
            request.POST['type'] = 'td'
            
            sm = ShippingManagement()
            info = await sm.handle_appointment_post(request,'post_nsop') 
            context.update({"success_messages": f"绑定成功，批次号是{shipment_batch_number}"})
        else:
            context.update({"error_messages": f"没有选择PO！"}) 
        
        return await self.handle_td_shipment_post(request,context)
    
    async def handle_fleet_confirmation_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        selected_ids_str = request.POST.get("selected_ids")
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
            }
            
        request.POST = request.POST.copy()
        request.POST['fleet_data'] = str(fleet_data_dict)
        request.POST['selected_ids'] = selected_ids
        fm = FleetManagement()
        info = await fm.handle_fleet_confirmation_post(request,'post_nsop')
        context = {}
        if error_message:
            context.update({"error_messages": error_message}) 
        _, context = await self.handle_fleet_schedule_post(request, context)
        context.update({"success_messages": f'排车成功!批次号是：{fleet_number}'}) 
        
        return await self.handle_fleet_schedule_post(request, context)

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
        elif operation == "delete":
            shipment.is_canceled = True
            await sync_to_async(shipment.delete)()
        return await self.handle_appointment_management_post(request)

    async def handle_export_pos(self, request: HttpRequest) -> HttpResponse:
        print(request.POST)
        cargo_ids_str_list = request.POST.getlist("cargo_ids")
        pl_ids = [
            int(pl_id) 
            for sublist in cargo_ids_str_list 
            for pl_id in sublist.split(",") 
            if pl_id.strip()  # 非空才转换
        ]

        if not pl_ids:
            raise ValueError('没有获取到id')
        # 查找柜号下的pl
        packing_list = await sync_to_async(
            lambda: list(
                PackingList.objects.select_related("container_number", "pallet")
                .filter(id__in=pl_ids)
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
        # 给每条 packing_list 添加 check_id
        data = []
        for item in packing_list:
            item = dict(item)  # 因为 values() 返回的是 ValuesQuerySet
            item["check_id"] = check_map.get(item["id"])  # 如果没有对应记录就返回 None
            data.append(item)
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
        df = pd.DataFrame.from_records(data)
        df["is_valid"] = None

        def get_est_pallet(n):
            if n < 1:
                return 1
            elif n % 1 >= 0.45:
                return int(n // 1 + 1)
            else:
                return int(n // 1)

        df["total_n_pallet_est"] = df["total_n_pallet_est"].apply(get_est_pallet)
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

        if packinglist_ids:
            packing_list_data = await sync_to_async(list)(
                PackingList.objects.select_related("container_number", "pallet")
                .filter(id__in=packinglist_ids)
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
            all_data += pallet_data
        for p in all_data:
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

    async def handle_appointment_management_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_main_dash, context

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
    
    async def _fl_unscheduled_data(
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:
        shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                origin=warehouse,
                fleet_number__isnull=True,
                in_use=True,
                is_canceled=False,
                shipment_type="FTL",
            ).order_by("-batch", "shipment_appointment")
        )
        fleet = await sync_to_async(list)(
            Fleet.objects.filter(
                origin=warehouse,
                departured_at__isnull=True,
                is_canceled=False,
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
        context = {
            "shipment_list": shipment,
            "fleet_list": fleet,
        }
        return context

    async def _fl_delivery_get(
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:
        criteria = models.Q(
            is_arrived=False,
            is_canceled=False,
            is_shipped=True,
            origin=warehouse,
            shipment_type="FTL",
        ) & ~Q(status="Exception")
        shipments = await sync_to_async(list)(
            Shipment.objects.prefetch_related("fleet_number")
            .filter(criteria)
            .order_by("shipped_at")
        )
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
        self, request: HttpRequest, warehouse:str
    ) -> tuple[str, dict[str, Any]]:

        criteria = models.Q(
            models.Q(models.Q(pod_link__isnull=True) | models.Q(pod_link="")),
            shipped_at__isnull=False,
            arrived_at__isnull=False,
            shipment_type='FTL',
            shipment_schduled_at__gte="2024-12-01",
            origin=warehouse,
        )
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
    
    async def handle_fleet_schedule_post(
        self, request: HttpRequest, context: dict| None = None
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")

        # 获取三类数据：未排车、待送达、待传POD
        sp_fl = await self._fl_unscheduled_data(request, warehouse)
        delivery_data = await self._fl_delivery_get(request, warehouse)
        pod_data = await self._fl_pod_get(request, warehouse)

        summary = {
            'unscheduled_count': len(sp_fl['shipment_list']),
            'scheduled_count': len(sp_fl['fleet_list']),
            'ready_count': len(delivery_data['shipments']),
            'pod_count': len(pod_data['fleet']),
        }
        if not context:
            context = {}
        context.update({
            'shipment_list': sp_fl['shipment_list'],
            'fleet_list': sp_fl['fleet_list'],
            'delivery_shipments': delivery_data['shipments'],
            'pod_shipments': pod_data['fleet'],
            'summary': summary,        
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "warehouse": warehouse,
            "carrier_options": self.carrier_options,
            "abnormal_fleet_options": self.abnormal_fleet_options,
            'current_time': timezone.now(),
        })     
        return self.template_fleet_schedule, context
    
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
            matching_suggestions = await self.sp_unscheduled_data(warehouse, st_type, max_cbm, max_pallet)

        scheduled_data = await self.sp_scheduled_data(warehouse)
        #未排车
        unschedule_fleet = await self._fl_unscheduled_data(request, warehouse)
        unschedule_fleet_data = unschedule_fleet['shipment_list']
        ready_to_ship_data = await self._sp_ready_to_ship_data(warehouse)
        
        # 获取可用预约
        available_shipments = await self.sp_available_shipments(warehouse, st_type)
        
        # 计算统计数据
        summary = await self._sp_calculate_summary(matching_suggestions, scheduled_data, ready_to_ship_data, unschedule_fleet_data)       

        if not context:
            context = {}
        else:
            # 防止传入的 context 被意外修改
            context = context.copy()

        context.update({
            'warehouse': warehouse,
            'st_type': st_type,
            'matching_suggestions': matching_suggestions,
            'scheduled_data': scheduled_data,
            'unschedule_fleet': unschedule_fleet_data,
            'unscheduled_fl_count': len(unschedule_fleet.get('shipment_list', [])) if unschedule_fleet else 0,
            'ready_to_ship_data': ready_to_ship_data,
            'available_shipments': available_shipments,
            'summary': summary,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
            'warehouse_options': self.warehouse_options,
            "account_options": self.account_options,
            "load_type_options": LOAD_TYPE_OPTIONS,
            "shipment_type_options": self.shipment_type_options,
            "carrier_options": self.carrier_options,
        }) 
        context["matching_suggestions_json"] = json.dumps(matching_suggestions, cls=DjangoJSONEncoder)
        context["warehouse_json"] = json.dumps(warehouse, cls=DjangoJSONEncoder)
        return self.template_td_shipment, context
    
    async def sp_unscheduled_data(self, warehouse: str, st_type: str, max_cbm, max_pallet) -> list:
        """获取未排约数据"""
        unshipment_pos = await self._get_packing_list(
            models.Q(
                container_number__order__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__gt=datetime(2025, 1, 1),
                location=warehouse,
                delivery_type='public',
            )& ~models.Q(delivery_method__contains='暂扣'), True
        )
        
        
        # 获取可用的shipment记录（shipment_batch_number为空的）
        shipments = await self.get_available_shipments(warehouse)
        
        # 生成智能匹配建议
        matching_suggestions = await self.generate_matching_suggestions(unshipment_pos, shipments, warehouse, max_cbm, max_pallet,st_type)
        
        # 只返回匹配建议，不返回原始未排约数据
        return matching_suggestions

    async def get_available_shipments(self, warehouse: str):
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

    async def generate_matching_suggestions(self, unshipment_pos, shipments, warehouse, max_cbm, max_pallet,st_type):
        """生成智能匹配建议 - 基于功能A的逻辑但适配shipment匹配"""
        suggestions = []

        # 第一级分组：按目的地和派送方式预分组
        pre_groups = {}
        for cargo in unshipment_pos:
            if not await self.has_appointment(cargo):
                dest = cargo.get('destination')
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
        
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__order__vessel_id__vessel_eta') or '')
            
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
                
                # 寻找匹配的shipment
                matched_shipment = await self.find_matching_shipment(primary_group, shipments)

                result_intel = await self._find_intelligent_po_for_group(
                    primary_group, warehouse
                )
                
                intelligent_pos = result_intel['intelligent_pos']
                intelligent_pos_stats = result_intel['intelligent_pos_stats']
                intelligent_cargos = [{
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
                    'label': pos.get('label', ''),
                    'destination': pos.get('destination', ''),
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
                        'offload_time': cargo.get('offload_time', ''),
                        'delivery_window_start': cargo.get('delivery_window_start'),
                        'delivery_window_end': cargo.get('delivery_window_end'),
                        'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                        'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                        'total_cbm': cargo.get('total_cbm', 0),
                        'label': cargo.get('label', ''),
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
            remaining_cbm = max_cbm - current_cbm
            remaining_pallets = max_pallet - current_pallets
            
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

    async def _find_intelligent_po_for_group(self, primary_group, warehouse) -> Any:
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
                models.Q(container_number__order__retrieval_id__retrieval_destination_precise=warehouse) |
                models.Q(container_number__order__warehouse__name=warehouse)
            )
        else:
            location_condition = models.Q(location__in=["NJ-07001", "SAV-31326"])
            retrieval_condition = (
                models.Q(container_number__order__retrieval_id__retrieval_destination_precise__in=["NJ-07001", "SAV-31326"]) |
                models.Q(container_number__order__warehouse__name__in=["NJ-07001", "SAV-31326"])
            )

        intelligent_pos = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
                destination=destination,
                delivery_type='public',
                
            ) & retrieval_condition
            & ~models.Q(id__in=existing_pl_ids),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=False,
                destination=destination,
                container_number__order__offload_id__offload_at__gt=datetime(2025, 1, 1),
                delivery_type='public',
            ) & location_condition
            & ~models.Q(id__in=existing_plt_ids),
        )
        intelligent_cargos = [{
            'ids': pos.get('ids', ''),
            'plt_ids': pos.get('plt_ids', ''),
            'ref_ids': pos.get('ref_ids', ''),
            'fba_ids': pos.get('fba_ids', ''),
            'container_numbers': pos.get('container_numbers', ''),
            'cns': pos.get('cns', ''),
            'offload_time': cargo.get('offload_time',''),
            'delivery_window_start': pos.get('delivery_window_start'),
            'delivery_window_end': pos.get('delivery_window_end'),
            'total_n_pallet_act': pos.get('total_n_pallet_act', 0),
            'total_n_pallet_est': pos.get('total_n_pallet_est', 0),
            'total_cbm': pos.get('total_cbm', 0),
            'label': pos.get('label', ''),
            'destination': pos.get('destination', ''),
            'custom_delivery_method': pos.get('custom_delivery_method', ''),
        } for pos in intelligent_pos]

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
            'intelligent_pos': intelligent_pos,
            'intelligent_pos_stats':intelligent_pos_stats
            }
    
    async def find_matching_shipment(self, primary_group, shipments):
        """为货物大组寻找匹配的shipment"""
        destination = primary_group['destination']
        matched_shipments = []
        
        for shipment in shipments:
            # 检查目的地是否匹配
            if shipment.destination != destination:
                continue
            # 检查时间窗口条件
            if await self.check_time_window_match(primary_group, shipment):
                matched_shipments.append(shipment)

        # 这里简单返回第一个匹配的，您可以根据需要调整策略
        if matched_shipments:
            matched = matched_shipments[0]
            return {
                'appointment_id': matched.appointment_id,
                'shipment_cargo_id': matched.shipment_cargo_id,
                'shipment_type': matched.shipment_type,
                'shipment_appointment': matched.shipment_appointment,
                'origin': matched.origin,
                'load_type': matched.load_type,
                'shipment_account': matched.shipment_account,
                'shipment_type': matched.shipment_type,
                'address': matched.address,
                'carrier': matched.carrier,
                'note': matched.note,
                'ARM_BOL': matched.ARM_BOL,
                'ARM_PRO': matched.ARM_PRO,
                'express_number': matched.express_number,
                'address_detail': await self.get_address(destination),
            }
        return None
    
    async def get_address(self,destination):
        if destination in amazon_fba_locations:
            fba = amazon_fba_locations[destination]
            address = f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
            return address
        else:
            return None
        
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

    async def sp_scheduled_data(self, warehouse: str) -> list:
        """获取已排约数据 - 按shipment_batch_number分组"""
        # 获取有shipment_batch_number但fleet_number为空的货物
        raw_data = await self._get_packing_list(
            models.Q(
                container_number__order__warehouse__name=warehouse,
                shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=True,
                shipment_batch_number__shipment_batch_number__isnull=False,
                shipment_batch_number__fleet_number__isnull=True,
                delivery_type='public',
            ),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=False,
                container_number__order__offload_id__offload_at__isnull=False,
                shipment_batch_number__fleet_number__isnull=True,
                location=warehouse,
                delivery_type='public',
            ),
        )
        
        # 按shipment_batch_number分组
        grouped_data = {}
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
                    'cargos': []
                }
            grouped_data[batch_number]['cargos'].append(item)
        
        return list(grouped_data.values())

    async def _sp_ready_to_ship_data(self, warehouse: str) -> list:
        """获取待出库数据 - 按fleet_number分组"""
        # 获取指定仓库的未出发且未取消的fleet
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(
                origin=warehouse,
                departured_at__isnull=True,
                is_canceled=False,
                fleet_type='FTL',
            ).prefetch_related(
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
                'third_party_address': fleet.third_party_address,
                'pickup_number': fleet.pickup_number,
                'motor_carrier_number': fleet.motor_carrier_number,
                'license_plate': fleet.license_plate,
                'dot_number': fleet.dot_number,
                'appointment_datetime': fleet.appointment_datetime,
                'is_virtual': fleet.is_virtual,
                'shipments': {},  # 改回字典结构，保持与前端兼容
                'pl_ids': [],
                'plt_ids': [],
                'total_cargos': 0  # 总货物行数
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
                        'appointment_id': shipment.appointment_id or '-',
                        'destination': shipment.destination or '-',
                        'cargos': []
                    }
                
                # 处理packinglists
                raw_data = await self._get_packing_list(
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__order__offload_id__offload_at__isnull=True,
                    ),
                    models.Q(
                        shipment_batch_number__shipment_batch_number=batch_number,
                        container_number__order__offload_id__offload_at__isnull=False,
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

    async def _sp_calculate_summary(self, unscheduled: list, scheduled: list, ready: list, unscheduled_fl) -> dict:
        """计算统计数据"""
        # 计算各类数量
        unscheduled_count = len(unscheduled)
        scheduled_count = len(scheduled)
        ready_count = len(ready)
        unscheduled_fl_count = len(unscheduled_fl)
        # 计算总板数
        total_pallets = 0
        for cargo in unscheduled:
            total_pallets += cargo.get('total_n_pallet_act', 0) or cargo.get('total_n_pallet_est', 0) or 0
        
        return {
            'unscheduled_count': unscheduled_count,
            'scheduled_count': scheduled_count,
            'ready_count': ready_count,
            'unscheduled_fl_count': unscheduled_fl_count,
            'total_pallets': int(total_pallets),
        }

    async def get_capacity_limits(self, st_type: str) -> tuple:
        """获取容量限制"""
        if st_type == "pallet":
            return 72, 35
        elif st_type == "floor":
            return 80, 38
        return 72, 35

    async def handle_appointment_management_post(
        self, request: HttpRequest, context: dict| None = None,
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        if warehouse:
            warehouse_name = warehouse.split('-')[0]
        
        nowtime = timezone.now()
        two_weeks_later = nowtime + timezone.timedelta(weeks=2)
        #所有没约且两周内到港的货物
        unshipment_pos = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
                container_number__order__vessel_id__vessel_eta__lte=two_weeks_later, 
                container_number__order__retrieval_id__retrieval_destination_area=warehouse_name,
                delivery_type='public',
                #container_number__order__warehouse__name=warehouse,
            )&
            ~(
                models.Q(delivery_method__icontains='暂扣') |
                models.Q(delivery_method__icontains='自提') |
                models.Q(delivery_method__icontains='UPS') |
                models.Q(delivery_method__icontains='FEDEX')
            ),
            models.Q(
                container_number__order__offload_id__offload_at__isnull=False,
            )& models.Q(pk=0),
        )
        
        #未使用的约和异常的约
        shipments = await self.get_shipments_by_warehouse(warehouse)
        
        summary = await self.calculate_summary(unshipment_pos, shipments)

        #智能匹配内容
        st_type = request.POST.get('st_type')
        if st_type == "pallet":
            max_cbm = 68
            max_pallet = 30
        elif st_type == "floor":
            max_cbm = 75
            max_pallet = 75
        matching_suggestions = await self.get_matching_suggestions(unshipment_pos, shipments,max_cbm,max_pallet)
        primary_group_keys = set()
        for suggestion in matching_suggestions:
            group_key = f"{suggestion['primary_group']['destination']}_{suggestion['primary_group']['delivery_method']}"
            primary_group_keys.add(group_key)


        auto_matches = await self.get_auto_matches(unshipment_pos, shipments)
        
        vessel_names = []
        vessel_dict = {} 
        destination_list = []
        for item in unshipment_pos:
            destination = item.get('destination')
            destination_list.append(destination)
            vessel_name = item.get('vessel_name')
            vessel_voyage = item.get('vessel_voyage')
            vessel_eta = item.get('vessel_eta')

            if vessel_name and vessel_name not in vessel_names:
                vessel_names.append(vessel_name)
                eta_date = str(vessel_eta).split()[0] if vessel_eta else "未知"
                vessel_dict[vessel_name] = f"{vessel_name} / {vessel_voyage} → {eta_date}"
        destination_list = list(set(destination_list))
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
            'matching_count': len(primary_group_keys),
            'matching_suggestions': matching_suggestions,
            'auto_matches': auto_matches,
            'st_type': st_type,
            'max_cbm': max_cbm,
            'max_pallet': max_pallet,
            "vessel_names": vessel_names,
            "vessel_dict": vessel_dict,
            "destination_list": destination_list,
            'account_options': self.account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
        })
        return self.template_main_dash, context
    
    async def _get_packing_list(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
        name: str | None = None
    ) -> list[Any]:
        def sort_key(item):
            custom_method = item.get("custom_delivery_method")
            if custom_method is None:
                custom_method = ""
            keywords = ["暂扣", "HOLD", "留仓"]
            return (any(k in custom_method for k in keywords),)
        
        data = []
        if plt_criteria:
            pal_list = await sync_to_async(list)(
                Pallet.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "container_number__order__retrieval_id",
                    "container_number__order__vessel_id",
                )
                .filter(plt_criteria)
                .annotate(
                    str_id=Cast("id", CharField()),
                    str_container_number=Cast("container_number__container_number", CharField()), 
                    
                    # 格式化vessel_eta为月日
                    formatted_offload_at=Func(
                        F('container_number__order__offload_id__offload_at'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
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
                )
                .values(
                    "destination",
                    "delivery_method",
                    "abnormal_palletization",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "shipment_batch_number__shipment_batch_number",
                    "data_source",  # 包含数据源标识
                    "shipment_batch_number__fleet_number__fleet_number",
                    "location",  # 添加location用于比较
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                    vessel_name=F("container_number__order__vessel_id__vessel"),
                    vessel_voyage=F("container_number__order__vessel_id__voyage"),
                    vessel_eta=F("container_number__order__vessel_id__vessel_eta"),                 
                    retrieval_destination_precise=F("container_number__order__retrieval_id__retrieval_destination_precise"),
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
                    total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                    total_n_pallet_act=Count("pallet_id", distinct=True),
                    label=Value("ACT"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True)
                )
                .order_by("container_number__order__offload_id__offload_at")
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
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "shipment_batch_number__fleet_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "container_number__order__retrieval_id",
                    "container_number__order__vessel_id",
                )
                .filter(pl_criteria)
                .annotate(
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
                        F('container_number__order__vessel_id__vessel_eta'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 格式化实际提柜时间为月日
                    formatted_actual_retrieval=Func(
                        F('container_number__order__retrieval_id__actual_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 格式化预计提柜时间为月日
                    formatted_target_low=Func(
                        F('container_number__order__retrieval_id__target_retrieval_timestamp_lower'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    formatted_target=Func(
                        F('container_number__order__retrieval_id__target_retrieval_timestamp'),
                        Value('MM-DD'),
                        function='to_char',
                        output_field=CharField()
                    ),
                    
                    # 创建完整的组合字段，通过前缀区分状态
                    container_with_eta_retrieval=Case(
                        # 有实际提柜时间 - 使用前缀 [实际]
                        When(container_number__order__retrieval_id__actual_retrieval_timestamp__isnull=False,
                            then=Concat(
                                # Value(" "),
                                # "container_number__order__vessel_id__vessel", 
                                Value("[已提柜]"),
                                "container_number__container_number",                          
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                Value(" 提柜:"),
                                "formatted_actual_retrieval",
                                output_field=CharField()
                            )),
                        # 有预计提柜时间范围 - 使用前缀 [预计]
                        When(container_number__order__retrieval_id__target_retrieval_timestamp_lower__isnull=False,
                            then=Concat( 
                                # Value(" "),
                                # "container_number__order__vessel_id__vessel", 
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
                            # "container_number__order__vessel_id__vessel", 
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
                )
                .values(
                    "destination",
                    "custom_delivery_method",
                    "delivery_window_start",
                    "delivery_window_end",
                    "note",
                    "data_source",  # 包含数据源标识
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__fleet_number__fleet_number",
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                    vessel_name=F("container_number__order__vessel_id__vessel"),
                    vessel_voyage=F("container_number__order__vessel_id__voyage"),
                    vessel_eta=F("container_number__order__vessel_id__vessel_eta"),
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
                    offload_time=Value("", output_field=CharField()),
                    total_pcs=Sum("pcs", output_field=FloatField()),
                    total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                    total_n_pallet_est= Round(Sum("cbm", output_field=FloatField()) / 2, 2),
                    label=Value("EST"),
                    note_sp=StringAgg("note_sp", delimiter=",", distinct=True)
                )
                .distinct()
            )
            pl_list_sorted = sorted(pl_list, key=sort_key)
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
                    
                    if transfer_record and transfer_record.arrival_time:
                        # 提取原始仓名称（retrieval_destination_precise以-分组，取前面的值）
                        original_warehouse = retrieval_destination.split('-')[0] if '-' in retrieval_destination else retrieval_destination
                        
                        # 格式化到达时间
                        arrival_time_str = transfer_record.arrival_time.strftime('%m-%d')
                        
                        # 修改offload_time
                        pallet['offload_time'] = f"{original_warehouse}-{arrival_time_str}"
                    else:
                        # 如果没有到达时间，使用原始仓名称
                        original_warehouse = retrieval_destination.split('-')[0] if '-' in retrieval_destination else retrieval_destination
                        pallet['offload_time'] = f"{original_warehouse}-转仓中"
                    
                    processed_pallets.add(id(pallet))
            
            # 第四步：处理没有找到TransferLocation记录但需要更新的pallet
            for pallet in need_update_pallets:
                if id(pallet) not in processed_pallets:
                    retrieval_destination = pallet.get('retrieval_destination_precise')
                    original_warehouse = retrieval_destination.split('-')[0] if '-' in retrieval_destination else retrieval_destination
                    pallet['offload_time'] = f"{original_warehouse}-转仓中"
        return pal_list

    async def get_shipments_by_warehouse(self, warehouse):
        """异步获取指定仓库相关的预约数据"""
        appointment = await sync_to_async(list)(
            Shipment.objects.filter(
                (
                    models.Q(origin__isnull=True)
                    | models.Q(origin="")
                    | models.Q(origin=warehouse)
                ),
                models.Q(appointment_id__isnull=False),
                models.Q(in_use=False, is_canceled=False),
            ).order_by("shipment_appointment","shipment_account")
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
        
    async def calculate_summary(self, unshipment_pos, shipments):
        """异步计算统计数据 - 适配新的数据结构"""
    
        # 计算预约状态统计
        expired_count = 0
        urgent_count = 0
        available_count = 0
        used_count = 0  # 已使用的预约数量
        
        for shipment in shipments:
            tzinfo = self._parse_tzinfo(shipment.origin)
            timezone_str = pytz.timezone(tzinfo)
            now = timezone.now().astimezone(timezone_str)
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
                dest = cargo.get('destination')
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
        
        # 对每个预分组按容量限制创建大组
        for group_key, pre_group in pre_groups.items():
            cargos = pre_group['cargos']
            
            # 按ETA排序，优先安排早的货物
            sorted_cargos = sorted(cargos, key=lambda x: x.get('container_number__order__vessel_id__vessel_eta') or '')
            
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
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False