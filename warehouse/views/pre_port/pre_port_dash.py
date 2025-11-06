from datetime import datetime, timedelta
from typing import Any
from urllib import request

import pandas as pd
from asgiref.sync import sync_to_async
from django.db import models
from django.db.models import Case, When, Value, IntegerField, F
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View

from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval


class PrePortDash(View):
    template_main = "pre_port/dashboard/01_pre_port_summary.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.handle_all_get(tab="summary")
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
        # 根据建单时间和ETA进行筛选
        if step == "search_orders":
            start_date_eta = request.POST.get("start_date_eta")
            end_date_eta = request.POST.get("end_date_eta")
            template, context = await self.handle_all_get(
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                tab="summary",
            )
            return await sync_to_async(render)(request, template, context)
        elif step == "download_eta_file":
            start_date_eta = request.POST.get("start_date_eta")
            end_date_eta = request.POST.get("end_date_eta")
            return await self.download_eta_file(
                start_date_eta, end_date_eta
            )
        elif step == "get_note_preport_dispatch":
            template, context = await self.get_note_preport_dispatch(request)
            return render(request, template, context)
        elif step == "get_retrieval_cabinet_arrangement_time":
            template, context = await self.get_retrieval_cabinet_arrangement_time(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "get_offload_at":
            template, context = await self.get_offload_at(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "batch_get_offload_at":
            template, context = await self.batch_get_offload_at(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "save_is_abnormal_state":
            template, context = await self.save_is_abnormal_state(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "get_is_abnormal_state":
            template, context = await self.get_is_abnormal_state(request)
            return await sync_to_async(render)(request, template, context)

        else:
            return await sync_to_async(render)(request, self.template_main, {})

    async def download_eta_file(
        self, start_date_eta, end_date_eta
    ) -> HttpResponse:
        current_date = datetime.now().date()
        start_date_eta = (
            current_date.strftime("%Y-%m-%d") if not start_date_eta else start_date_eta
        )
        end_date_eta = (
            (datetime.now().date() + timedelta(days=7)).strftime("%Y-%m-%d")
            if not end_date_eta
            else end_date_eta
        )
        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            )
            .filter(
                    models.Q(
                        vessel_id__vessel_eta__gte=start_date_eta,
                        vessel_id__vessel_eta__lte=end_date_eta,
                    )
                    | models.Q(eta__gte=start_date_eta, eta__lte=end_date_eta)
            )
            .values(
                "container_number__container_number",
                "customer_name__zem_code",
                "retrieval_id__retrieval_destination_area",
                "warehouse__name",
                "vessel_id__shipping_line",
                "vessel_id__vessel",
                "vessel_id__vessel_eta",
            )
        )
        df = pd.DataFrame(orders)
        # 修改列名为柜号，客户，所属仓/直送地址，具体仓库，ETA，shipping/vessel信息
        df = df.rename(
            {
                "container_number__container_number": "container",
                "customer_name__zem_code": "customer",
                "retrieval_id__retrieval_destination_area": "destination_area",
                "warehouse__name": "warehouse",
                "vessel_id__shipping_line": "shipping_line",
                "vessel_id__vessel": "vessel",
                "vessel_id__vessel_eta": "ETA",
            },
            axis=1,
        )
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename=ETA_week.csv"
        df.to_csv(path_or_buf=response, index=False, encoding="utf-8-sig")
        return response

    async def save_is_abnormal_state(self, request):
        is_abnormal_state = request.POST.get("is_abnormal_state")
        container_number = request.POST.get("container_number")
        start_date_eta = request.POST.get("start_date_eta")
        end_date_eta = request.POST.get("end_date_eta")
        await sync_to_async(
            lambda: Container.objects.filter(container_number=container_number).update(
                is_abnormal_state=is_abnormal_state
            )
        )()
        template, context = await self.handle_all_get(
            start_date_eta=start_date_eta,
            end_date_eta=end_date_eta,
            tab="summary",
        )
        return template, context

    async def get_is_abnormal_state(self, request: HttpRequest) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        criteria = models.Q(
            cancel_notification=False,
            container_number__is_abnormal_state=True
        )


        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            )
            .filter(criteria)
            .annotate(
                priority=Case(
                    When(retrieval_id__empty_returned=True, then=Value(1)),
                    When(
                        offload_id__offload_at__isnull=False,
                        retrieval_id__empty_returned=False,
                        then=Value(2)
                    ),
                    When(
                        retrieval_id__actual_retrieval_timestamp__isnull=False,
                        offload_id__offload_at__isnull=True,
                        then=Value(3)
                    ),
                    default=Value(4),
                    output_field=IntegerField()
                ),
                sort_time=Case(
                    When(priority__in=[1, 2, 3], then=F('retrieval_id__actual_retrieval_timestamp')),
                    default=F('retrieval_id__target_retrieval_timestamp'),
                )
            )
            .order_by("priority", "sort_time")
        )

        # 转换回字符串格式供前端使用
        context = {
            "customers": customers,
            "orders": orders,
            "current_date": current_date,
            "tab": "summary",
        }
        return self.template_main, context

    async def handle_all_get(
            self,
            start_date_eta: str = None,
            end_date_eta: str = None,
            tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        criteria = models.Q(
            cancel_notification=False,
        )

        # 处理ETA日期条件
        def parse_eta_date(date_str):
            if not date_str:
                return None
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            # 转换为带时区的datetime（当天0点）
            naive_datetime = datetime.combine(date_obj, datetime.min.time())
            return timezone.make_aware(naive_datetime)  # 关键：添加时区信息

        start_eta = parse_eta_date(start_date_eta)
        end_eta = parse_eta_date(end_date_eta)

        if start_eta:
            criteria &= models.Q(vessel_id__vessel_eta__gte=start_eta) | models.Q(
                eta__gte=start_eta
            )
        if end_eta:
            # 结束日期设置为当天23:59:59
            end_eta = end_eta.replace(hour=23, minute=59, second=59)
            criteria &= models.Q(vessel_id__vessel_eta__lte=end_eta) | models.Q(
                eta__lte=end_eta
            )


        customers = await sync_to_async(list)(Customer.objects.all())
        customers = {c.zem_name: c.zem_name for c in customers}
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                "vessel_id",
                "container_number",
                "customer_name",
                "retrieval_id",
                "offload_id",
                "warehouse",
            )
            .filter(criteria)
            .annotate(
                priority=Case(
                    When(retrieval_id__empty_returned=True, then=Value(1)),
                    When(
                        offload_id__offload_at__isnull=False,
                        retrieval_id__empty_returned=False,
                        then=Value(2)
                    ),
                    When(
                        retrieval_id__actual_retrieval_timestamp__isnull=False,
                        offload_id__offload_at__isnull=True,
                        then=Value(3)
                    ),
                    default=Value(4),
                    output_field=IntegerField()
                ),
                sort_time=Case(
                    When(priority__in=[1, 2, 3], then=F('retrieval_id__actual_retrieval_timestamp')),
                    default=F('retrieval_id__target_retrieval_timestamp'),
                )
            )
            .order_by("priority", "sort_time")
        )

        # 转换回字符串格式供前端使用
        context = {
            "customers": customers,
            "orders": orders,
            "start_date_eta": start_date_eta,
            "end_date_eta": end_date_eta,
            "current_date": current_date,
            "tab": tab,
        }
        return self.template_main, context

    async def get_retrieval_cabinet_arrangement_time(self, request: HttpRequest) -> tuple[Any, Any]:
        time_str = request.POST.get("retrieval_cabinet_arrangement_time")
        retrieval_id = request.POST.get("retrieval_id")
        retrieval_obj = await Retrieval.objects.aget(retrieval_id=retrieval_id)
        if time_str:
            naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            aware_datetime = timezone.make_aware(naive_datetime)
            retrieval_obj.retrieval_cabinet_arrangement_time = aware_datetime
        else:
            retrieval_obj.retrieval_cabinet_arrangement_time = None
        await retrieval_obj.asave()
        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)
        template, context = await self.handle_all_get(
            start_date_eta=start_date_eta,
            end_date_eta=end_date_eta,
            tab="summary",
        )
        return template, context

    async def batch_get_offload_at(self, request: HttpRequest) -> tuple[Any, Any]:
        offload_ids = request.POST.getlist("offload_ids[]")
        time_str = request.POST.get("offload_at")
        if offload_ids:
            for offload_id in offload_ids:
                try:
                    offload_obj = await Offload.objects.aget(offload_id=offload_id)
                    if time_str:
                        naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
                        aware_datetime = timezone.make_aware(naive_datetime)
                        offload_obj.offload_at = aware_datetime
                    else:
                        offload_obj.offload_at = None
                    await offload_obj.asave()
                except Offload.DoesNotExist:
                    continue

        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)

        template, context = await self.handle_all_get(
            start_date_eta=start_date_eta,
            end_date_eta=end_date_eta,
            tab="summary",
        )
        return template, context

    async def get_offload_at(self, request: HttpRequest) -> tuple[Any, Any]:
        time_str = request.POST.get("offload_at")
        offload_id = request.POST.get("offload_id")
        offload_obj = await Offload.objects.aget(offload_id=offload_id)
        if time_str:
            naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            aware_datetime = timezone.make_aware(naive_datetime)
            offload_obj.offload_at = aware_datetime
        else:
            offload_obj.offload_at = None
        await offload_obj.asave()
        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)
        template, context = await self.handle_all_get(
            start_date_eta=start_date_eta,
            end_date_eta=end_date_eta,
            tab="summary",
        )
        return template, context

    async def get_note_preport_dispatch(self, request: HttpRequest) -> tuple[Any, Any]:
        def process_empty(value):
            if value in ['None', '']:
                return None
            return value
        note_preport_dispatch = request.POST.get("note_preport_dispatch")
        start_date_eta = process_empty(request.POST.get("start_date_eta"))  # 处理后为None
        end_date_eta = process_empty(request.POST.get("end_date_eta"))
        retrieval_id = request.POST.get("retrieval_id")
        await sync_to_async(
                lambda: Retrieval.objects.filter(retrieval_id=retrieval_id).update(
                    note_preport_dispatch=note_preport_dispatch
                )
            )()
        template_main, context = await self.handle_all_get(start_date_eta, end_date_eta)
        return template_main, context


    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
