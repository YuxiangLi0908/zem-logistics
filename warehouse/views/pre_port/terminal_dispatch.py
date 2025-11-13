from datetime import datetime, timedelta
from typing import Any

from django.contrib.auth.models import User
from django.utils import timezone
import pytz,json 
from asgiref.sync import sync_to_async
from django.db import models
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils.dateparse import parse_datetime
from django.views import View

from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.retrieval import Retrieval
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import (
    ADDITIONAL_CONTAINER,
    CONTAINER_PICKUP_CARRIER,
    WAREHOUSE_OPTIONS,
)


class TerminalDispatch(View):
    template_terminal_dispatch = (
        "pre_port/vessel_terminal_tracking/02_terminal_dispatch.html"
    )
    template_schedule_container_pickup = (
        "pre_port/vessel_terminal_tracking/03_schedule_container_pickup.html"
    )
    template_batch_schedule_container = (
        "pre_port/vessel_terminal_tracking/03_batch_schedule_container_pickup.html"
    )

    template_update_container_pickup_schedule = (
        "pre_port/vessel_terminal_tracking/04_update_container_pickup_schedule.html"
    )
    template_batch_update_container_pickup_schedule = (
        "pre_port/vessel_terminal_tracking/04_batch_update_container_pickup_schedule.html"
    )
    template_handle_generous_and_wide_planted = (
        "pre_port/vessel_terminal_tracking/05_handle_generous_and_wide_planted.html"
    )
    

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        print('step-GET',step)
        if step == "all":
            template, context = await self.handle_all_get()
            return await sync_to_async(render)(request, template, context)
        elif step == "schedule_container_pickup":
            template, context = await self.handle_schedule_container_pickup_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "update_pickup_schedule":
            template, context = await self.hanlde_update_pickup_schedule_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_confirm_pickup_v1":
            template, context = await self.handle_batch_confirm_pickup_v1_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "handle_generous_and_wide_planted":
            template, context = await self.handle_generous_and_wide_planted(request)
            return await sync_to_async(render)(request, template, context)
        else:
            context = {}
            return await sync_to_async(render)(
                request, self.template_terminal_dispatch, context
            )

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        print('step-POST',step)
        if step == "pickup_schedule_confirmation":
            template, context = await self.handle_pickup_schedule_confirmation_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "pickup_schedule_update":
            template, context = await self.handle_pickup_schedule_confirmation_post(
                request
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "confirm_pickup":
            template, context = await self.handle_confirm_pickup_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "confirm_pickup_appointment_time":
            template, context = await self.handle_confirm_pickup_post_appointment_time(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_schedule_container":
            template, context = await self.handle_batch_schedule_container_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_pickup_schedule_confirmation":
            template, context = await self.handle_batch_pickup_schedule_confirmation(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_confirm_pickup":
            template, context = await self.handle_batch_confirm_pickup_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_confirm_pickup_submit":
            template, context = await self.handle_batch_confirm_pickup_submit_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_confirm_pickup_submit_appointment_time":
            template, context = await self.handle_batch_confirm_pickup_submit_post_appointment_time(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "generous_and_wide_target_retrieval_timestamp_save":
            template, context = await self.handle_generous_and_wide_target_retrieval_timestamp_save(request)
            return await sync_to_async(render)(request, template, context)


    async def handle_all_get(self) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        orders_not_scheduled = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
                    & models.Q(retrieval_id__retrieval_carrier__isnull=True)
                    & models.Q(
                        vessel_id__vessel_eta__lte=timezone.now() + timedelta(weeks=2)
                    )
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("vessel_id__vessel_eta")
        )
        orders_not_pickup = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            )
            .filter(
                (
                    models.Q(add_to_t49=True)
                    & models.Q(retrieval_id__actual_retrieval_timestamp__isnull=True)
                    & models.Q(retrieval_id__retrieval_carrier__isnull=False)
                    & models.Q(cancel_notification=False)
                )
                & (
                    models.Q(created_at__gte="2024-08-19")
                    | models.Q(
                        container_number__container_number__in=ADDITIONAL_CONTAINER
                    )
                )
            )
            .order_by("vessel_id__vessel_eta")
        )
        context = {
            "orders_not_scheduled": orders_not_scheduled,
            "orders_not_pickup": orders_not_pickup,
            "current_date": current_date,
        }
        return self.template_terminal_dispatch, context

    async def handle_batch_confirm_pickup_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_numbers_json = request.POST.get("selected_containers")
        container_numbers = json.loads(container_numbers_json)
        
        # 获取所有选中的订单
        selected_orders = []
        for cn in container_numbers:
            try:
                order = await sync_to_async(
                    Order.objects.select_related(
                        "container_number", "customer_name", "vessel_id", "retrieval_id"
                    ).get
                )(container_number__container_number=cn)
                selected_orders.append(order)
            except Order.DoesNotExist:
                continue
        
        _, context = await self.handle_all_get()
        context["selected_orders"] = selected_orders
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
    
        return self.template_batch_update_container_pickup_schedule, context

    async def handle_batch_confirm_pickup_v1_get(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_numbers = request.GET.getlist("containers[]")
        selected_orders = []
        for cn in container_numbers:
            try:
                order = await sync_to_async(
                    Order.objects.select_related(
                        "container_number", "customer_name", "vessel_id", "retrieval_id"
                    ).get
                )(container_number__container_number=cn)
                selected_orders.append(order)
            except Order.DoesNotExist:
                continue

        _, context = await self.handle_all_get()
        context["selected_orders"] = selected_orders
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER

        return self.template_batch_update_container_pickup_schedule, context

    async def handle_batch_schedule_container_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_numbers_json = request.POST.get("selected_containers")
        container_numbers = json.loads(container_numbers_json)
        
        # 获取所有选中的订单
        selected_orders = []
        for cn in container_numbers:
            order = await sync_to_async(
                Order.objects.select_related(
                    "container_number", "customer_name", "vessel_id", "retrieval_id"
                ).get
            )(container_number__container_number=cn)
            selected_orders.append(order)
        
        _, context = await self.handle_all_get()
        context["selected_orders"] = selected_orders
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
        
        return self.template_batch_schedule_container, context
    
    async def handle_schedule_container_pickup_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.GET.get("container_number")
        _, context = await self.handle_all_get()
        order = await sync_to_async(
            Order.objects.select_related(
                "container_number", "customer_name", "vessel_id", "retrieval_id"
            ).get
        )(container_number__container_number=container_number)
        if order.order_type == "直送":
            packing_list = await sync_to_async(PackingList.objects.filter)(
                container_number__container_number=container_number
            )
            context["packing_list"] = packing_list
        context["container_number"] = container_number
        context["selected_order"] = order
        context["warehouse_options"] = [
            (k, v) for k, v in WAREHOUSE_OPTIONS if k not in ["N/A(直送)", "Empty"]
        ]
        context["carrier_options"] = CONTAINER_PICKUP_CARRIER
        return self.template_schedule_container_pickup, context

    async def hanlde_update_pickup_schedule_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        _, context = await self.handle_schedule_container_pickup_get(request)
        return self.template_update_container_pickup_schedule, context

    async def handle_generous_and_wide_planted(self, request: HttpRequest) -> tuple[Any, Any]:
        def get_orders():
            return Order.objects.select_related(
                "container_number", "retrieval_id", "offload_id", "vessel_id"
            ).filter(
                retrieval_id__retrieval_destination_area="LA",
                offload_id__offload_at__isnull=True,
            ).order_by("-vessel_id__vessel_eta").all()
        orders = await sync_to_async(get_orders)()
        context = {"orders": orders}
        return self.template_handle_generous_and_wide_planted, context


    async def handle_generous_and_wide_target_retrieval_timestamp_save(self, request: HttpRequest) -> tuple[Any, Any]:
        time_str = request.POST.get("generous_and_wide_target_retrieval_timestamp")
        retrieval_id = request.POST.get("retrieval_id")
        try:
            target_datetime = parse_datetime(time_str)
        except ValueError:
            target_datetime = None
        def update_retrieval_time():
            Retrieval.objects.filter(retrieval_id=retrieval_id).update(
                generous_and_wide_target_retrieval_timestamp=target_datetime
            )
        await sync_to_async(update_retrieval_time)()
        template, context = await self.handle_generous_and_wide_planted(request)
        return template, context


    async def handle_batch_pickup_schedule_confirmation(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_numbers = request.POST.getlist("container_numbers")
        retrieval_destination = request.POST.get("retrieval_destination")
        retrieval_carrier = request.POST.get("retrieval_carrier")
        tzinfo = self._parse_tzinfo(retrieval_destination)
        note = request.POST.get("note", "").strip()

        for cn in container_numbers:
            order = await sync_to_async(Order.objects.select_related('retrieval_id').get)(
                container_number__container_number=cn
            )
            if order.order_type == "转运" or order.order_type == "转运组合":
                warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=retrieval_destination)
                order.warehouse = warehouse
                await sync_to_async(order.save)()
            # 更新retrieval记录
            order.retrieval_id.retrieval_destination_precise = retrieval_destination
            order.retrieval_id.retrieval_carrier = retrieval_carrier
            if request.POST.get("target_retrieval_timestamp"):
                ts = request.POST.get("target_retrieval_timestamp")
                order.retrieval_id.target_retrieval_timestamp = self._parse_ts(ts, tzinfo)
            else:
                order.retrieval_id.target_retrieval_timestamp = None
            if request.POST.get("target_retrieval_timestamp_lower"):
                ts = request.POST.get("target_retrieval_timestamp_lower")
                order.retrieval_id.target_retrieval_timestamp_lower = self._parse_ts(ts, tzinfo)
            else:
                order.retrieval_id.target_retrieval_timestamp_lower = None
            order.retrieval_id.note = note
            order.retrieval_id.scheduled_at = datetime.now()
            
            await sync_to_async(order.retrieval_id.save)()
            # 有提柜计划后，就将记录归为“提柜前一天
            po_checks = await sync_to_async(list)(
                PoCheckEtaSeven.objects.filter(
                    container_number__container_number=cn
                )
            )
            try:
                for p in po_checks:
                    p.time_status = False
                    await sync_to_async(p.save)()
            except PoCheckEtaSeven.DoesNotExist:
                pass       
        return await self.handle_all_get()

    async def handle_pickup_schedule_confirmation_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        destination = request.POST.get("retrieval_destination").upper().strip()
        order_type = request.POST.get("order_type")
        planned_release_time_str = request.POST.get("planned_release_time")
        if planned_release_time_str:
            try:
                planned_release_time = datetime.strptime(
                    planned_release_time_str,
                    "%Y-%m-%dT%H:%M"
                )
                planned_release_time = timezone.make_aware(planned_release_time)
            except ValueError:
                return "error_template.html", {"error": "无效的时间格式，应为 'YYYY-MM-DDTHH:MM'"}
        else:
            planned_release_time = None
        order = await sync_to_async(Order.objects.select_related("retrieval_id").get)(
            models.Q(container_number__container_number=container_number)
        )
        if order_type == "转运" or order_type == "转运组合":
            warehouse = await sync_to_async(ZemWarehouse.objects.get)(name=destination)
            order.warehouse = warehouse
            await sync_to_async(order.save)()
        retrieval = order.retrieval_id
        retrieval.retrieval_destination_precise = destination
        retrieval.retrieval_carrier = request.POST.get("retrieval_carrier").strip()
        if destination and request.POST.get("retrieval_carrier").strip():
            retrieval.retrieval_delegation_status = True
        retrieval.planned_release_time = planned_release_time
        tzinfo = self._parse_tzinfo(destination)
        if request.POST.get("target_retrieval_timestamp"):
            ts = request.POST.get("target_retrieval_timestamp")
            retrieval.target_retrieval_timestamp = self._parse_ts(ts, tzinfo)
        else:
            retrieval.target_retrieval_timestamp = None
        if request.POST.get("target_retrieval_timestamp_lower"):
            ts = request.POST.get("target_retrieval_timestamp_lower")
            retrieval.target_retrieval_timestamp_lower = self._parse_ts(ts, tzinfo)
        else:
            retrieval.target_retrieval_timestamp_lower = None
        retrieval.note = request.POST.get("note", "").strip()
        retrieval.scheduled_at = timezone.now()
        if container_number and destination and planned_release_time and request.POST.get(
                "target_retrieval_timestamp") and request.POST.get("target_retrieval_timestamp_lower") and request.POST.get("retrieval_carrier").strip():
            retrieval.actual_release_status = True
        if request.POST.get("retrieval_carrier") == "客户自提":
            if request.POST.get("target_retrieval_timestamp"):
                ts = request.POST.get("target_retrieval_timestamp")
                retrieval.target_retrieval_timestamp = self._parse_ts(ts, tzinfo)
            else:
                retrieval.target_retrieval_timestamp = None
            if request.POST.get("target_retrieval_timestamp_lower"):
                ts = request.POST.get("target_retrieval_timestamp_lower")
                retrieval.target_retrieval_timestamp_lower = self._parse_ts(ts, tzinfo)
            else:
                retrieval.target_retrieval_timestamp_lower = None
        await sync_to_async(retrieval.save)()
        # 有提柜计划后，就将记录归为“提柜前一天
        po_checks = await sync_to_async(list)(
            PoCheckEtaSeven.objects.filter(
                container_number__container_number=container_number
            )
        )
        try:
            for p in po_checks:
                p.time_status = False
                await sync_to_async(p.save)()
        except PoCheckEtaSeven.DoesNotExist:
            pass

        return await self.handle_all_get()

    async def handle_batch_confirm_pickup_submit_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """处理批量确认提柜的请求"""
        container_numbers = request.POST.getlist("container_numbers")
        actual_retrieval_timestamp = request.POST.get("actual_retrieval_timestamp")
        
        for cn in container_numbers:
            # 获取retrieval记录
            retrieval = await sync_to_async(Retrieval.objects.get)(
                order__container_number__container_number=cn
            )
            
            # 解析时区信息
            tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
            actual_retrieval_ts = self._parse_ts(actual_retrieval_timestamp, tzinfo)
            
            # 更新retrieval记录
            retrieval.actual_retrieval_timestamp = actual_retrieval_ts
            
            # 填了实际提柜但是没有写预计提柜的，就默认预计提柜时间为实际提柜时间
            if not retrieval.target_retrieval_timestamp:
                retrieval.target_retrieval_timestamp = actual_retrieval_ts
            if not retrieval.target_retrieval_timestamp_lower:
                retrieval.target_retrieval_timestamp_lower = actual_retrieval_ts
            
            # 处理当天提柜的逻辑
            today = datetime.now()
            actual_ts = datetime.fromisoformat(actual_retrieval_ts)
            
            # 如果是当天提柜
            if actual_ts <= today + timedelta(days=1):
                try:
                    orders = await sync_to_async(list)(
                        PoCheckEtaSeven.objects.filter(
                            container_number__container_number=cn
                        )
                    )
                    for o in orders:
                        o.time_status = False
                        await sync_to_async(o.save)()
                except PoCheckEtaSeven.DoesNotExist:
                    pass
            
            await sync_to_async(retrieval.save)()
        return await self.handle_all_get()

    async def handle_batch_confirm_pickup_submit_post_appointment_time(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        """待确认提柜-处理批量确认预约时间的请求"""
        container_numbers = request.POST.getlist("container_numbers")
        appointment_time_start = request.POST.get("appointment_time_start")
        appointment_time_end = request.POST.get("appointment_time_end")
        planned_release_time = request.POST.get("planned_release_time")
        for cn in container_numbers:
            # 获取retrieval记录
            retrieval = await sync_to_async(Retrieval.objects.get)(
                order__container_number__container_number=cn
            )
            # 解析时区信息
            tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
            appointment_time_start_ts = self._parse_ts(appointment_time_start, tzinfo)
            appointment_time_end_ts = self._parse_ts(appointment_time_end, tzinfo)
            planned_release_time_ts = self._parse_ts(planned_release_time, tzinfo)

            # 更新retrieval记录
            retrieval.target_retrieval_timestamp_lower = appointment_time_start_ts
            retrieval.target_retrieval_timestamp = appointment_time_end_ts
            retrieval.planned_release_time = planned_release_time_ts
            await sync_to_async(retrieval.save)()
        return await self.handle_all_get()


    async def handle_confirm_pickup_post(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        retrieval = await sync_to_async(Retrieval.objects.get)(
            order__container_number__container_number=container_number
        )
        tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
        ts = request.POST.get("actual_retrieval_timestamp")
        actual_retrieval_ts = self._parse_ts(ts, tzinfo)
        retrieval.actual_retrieval_timestamp = actual_retrieval_ts
        # 填了实际提柜但是没有写预计提柜的，就默认预计提柜时间为实际提柜时间
        if not retrieval.target_retrieval_timestamp:
            retrieval.target_retrieval_timestamp = actual_retrieval_ts
        if not retrieval.target_retrieval_timestamp_lower:
            retrieval.target_retrieval_timestamp_lower = actual_retrieval_ts
        today = datetime.now()
        actual_ts = datetime.fromisoformat(actual_retrieval_ts)
        # 如果是当天提柜
        if actual_ts <= today + timedelta(days=1):
            orders = await sync_to_async(list)(
                PoCheckEtaSeven.objects.filter(
                    container_number__container_number=container_number
                )
            )
            try:
                for o in orders:
                    o.time_status = False
                    await sync_to_async(o.save)()
            except PoCheckEtaSeven.DoesNotExist:
                pass
        await sync_to_async(retrieval.save)()
        return await self.handle_all_get()

    async def handle_confirm_pickup_post_appointment_time(self, request: HttpRequest) -> tuple[Any, Any]:
        container_number = request.POST.get("container_number")
        retrieval = await sync_to_async(Retrieval.objects.get)(
            order__container_number__container_number=container_number
        )
        tzinfo = self._parse_tzinfo(retrieval.retrieval_destination_precise)
        appointment_time_start = request.POST.get("appointment_time_start")
        appointment_time_end = request.POST.get("appointment_time_end")
        appointment_time_start_ts = self._parse_ts(appointment_time_start, tzinfo)
        appointment_time_end_ts = self._parse_ts(appointment_time_end, tzinfo)
        retrieval.target_retrieval_timestamp_lower = appointment_time_start_ts
        retrieval.target_retrieval_timestamp = appointment_time_end_ts

        await sync_to_async(retrieval.save)()
        return await self.handle_all_get()

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    def _parse_tzinfo(self, s: str) -> str:
        if "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: str, tzinfo: str) -> str:
        ts_naive = datetime.fromisoformat(ts)
        tz = pytz.timezone(tzinfo)
        ts = tz.localize(ts_naive).astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    async def _validate_user_four_major_whs(self, user: User) -> bool:
        return await sync_to_async(
            lambda: user.groups.filter(name="four_major_whs").exists()
        )()
