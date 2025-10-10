from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db.models.functions import Round, Cast, Coalesce
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

from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.order import Order
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.shipment import Shipment
from warehouse.views.post_port.shipment.fleet_management import FleetManagement


class PostNsop(View):
    template_main_dash = "post_port/new_sop/appointment_management.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX"}
    warehouse_options = {"":"", "NJ-07001": "NJ-07001", "SAV-31326": "SAV-31326", "LA-91761": "LA-91761"}

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "appointment_management":
            template, context = await self.handle_appointment_management_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "schedule_shipment":
            template, context = await self.handle_appointment_management_get(request)
            return render(request, template, context)
        elif step == "fleet_management":
            template, context = await self.handle_appointment_management_get(request)
            return render(request, template, context)
        else:
            raise ValueError('输入错误')

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "appointment_management_warehouse":
            template, context = await self.handle_appointment_management_post(request)
            return render(request, template, context)
        elif step == "export_pos":
            return await self.handle_export_pos(request)
        elif step == "appointment_time_modify":
            template, context = await self.handle_appointment_time(request)
            return render(request, template, context)
        else:
            raise ValueError('输入错误')

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
        packinglist_ids = request.POST.getlist("cargo_ids")
        pallet_ids = request.POST.getlist("plt_ids")

        if packinglist_ids and packinglist_ids[0]:
            packinglist_ids = packinglist_ids[0].split(',')
        else:
            packinglist_ids = []

        if pallet_ids and pallet_ids[0]:
            pallet_ids = pallet_ids[0].split(',')
        else:
            pallet_ids = []
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
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField(),
                        )
                    ),
                    total_cbm=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("cbm")),
                            default=F("pallet__cbm"),
                            output_field=FloatField(),
                        )
                    ),
                    total_weight_lbs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("total_weight_lbs")),
                            default=F("pallet__weight_lbs"),
                            output_field=FloatField(),
                        )
                    ),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
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
    
    async def handle_appointment_management_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        nowtime = timezone.now()
        two_weeks_later = nowtime + timezone.timedelta(weeks=2)
        #所有没约且两周内到港的货物
        unshipment_pos = await self._get_packing_list(
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=True,
                container_number__order__vessel_id__vessel_eta__lte=two_weeks_later, 
                container_number__order__retrieval_id__retrieval_destination_precise=warehouse,
                delivery_type='public',
            )& ~ models.Q(delivery_method__icontains='暂扣'),
            models.Q(
                shipment_batch_number__shipment_batch_number__isnull=True,
                container_number__order__offload_id__offload_at__isnull=False,
                location=warehouse,
                delivery_type='public',
            ) & ~ models.Q(delivery_method__icontains='暂扣'),
        )
        
        #未使用的约和异常的约
        shipments = await self.get_shipments_by_warehouse(warehouse)
        
        summary = await self.calculate_summary(unshipment_pos, shipments)

        #智能匹配内容
        st_type = request.POST.get('st_type')
        if st_type == "pallet":
            max_cbm = 72
            max_pallet = 35
        elif st_type == "floor":
            max_cbm = 80
            max_pallet = 38
        matching_suggestions = await self.get_matching_suggestions(unshipment_pos, shipments,max_cbm,max_pallet)
        primary_group_keys = set()
        for suggestion in matching_suggestions:
            group_key = f"{suggestion['primary_group']['destination']}_{suggestion['primary_group']['delivery_method']}"
            primary_group_keys.add(group_key)


        auto_matches = await self.get_auto_matches(unshipment_pos, shipments)
        
        context = {
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
        }
        
        return self.template_main_dash, context
    
    async def _get_packing_list(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        data = []
        pal_list = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
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
                "po_expired",
                "data_source",  # 包含数据源标识
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
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
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet_act=Count("pallet_id", distinct=True),
                label=Value("ACT"),
            )
            .order_by("container_number__order__offload_id__offload_at")
        )
        data += pal_list
        
        # PackingList 查询 - 添加数据源标识
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
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
                                Value("[已提柜]"),
                                "container_number__container_number",
                                # Value(" ETA:"),
                                # "formatted_vessel_eta",
                                # Value(" 提柜:"),
                                # "formatted_actual_retrieval",
                                output_field=CharField()
                            )),
                        # 有预计提柜时间范围 - 使用前缀 [预计]
                        When(container_number__order__retrieval_id__target_retrieval_timestamp_lower__isnull=False,
                            then=Concat(
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
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
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
                    total_pcs=Sum("pcs", output_field=FloatField()),
                    total_cbm = Round(Sum("cbm", output_field=FloatField()), 3),
                    total_n_pallet_est= Round(Sum("cbm", output_field=FloatField()) / 2, 2),
                    label=Value("EST"),
                )
                .distinct()
            )
            data += pl_list
        
        return data

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
            ).order_by("shipment_appointment")
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
    
    async def calculate_summary(self, unshipment_pos, shipments):
        """异步计算统计数据 - 适配新的数据结构"""
        now = timezone.now()
    
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
                used_count += 1
            elif is_expired:
                expired_count += 1
            elif is_urgent:
                urgent_count += 1
            else:
                # 可用预约：没过期、不紧急、未被使用
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
                                'total_n_pallet_act': cargo.get('total_n_pallet_act', 0),
                                'total_n_pallet_est': cargo.get('total_n_pallet_est', 0),
                                'total_cbm': cargo.get('total_cbm', 0),
                                'label': cargo.get('label', ''),
                            }],
                            'total_pallets': cargo_pallets,
                            'total_cbm': cargo_cbm,
                            'container_numbers': cargo.get('container_numbers', ''),
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