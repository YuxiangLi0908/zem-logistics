from datetime import datetime, timedelta
from typing import Any, Optional
from urllib import request

import pandas as pd
import pytz
from asgiref.sync import sync_to_async
from django.db import models
from django.db.models import Case, When, Value, IntegerField, F
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views import View
from office365.runtime.compat import is_absolute_url
from pyhanko.sign.validation.ltv import retrieve_adobe_revocation_info

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
            template, context = await self.handle_all_get(warehouse="all", tab="summary")
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
        if step == "search_orders":
            warehouse = request.POST.get("warehouse")
            time_type = request.POST.get("time_type", "eta")
            start_date = request.POST.get("start_date")
            end_date = request.POST.get("end_date")
            template = ""
            context = {}

            if time_type == "eta":
                template, context = await self.handle_all_get(
                    start_date_eta=start_date,
                    end_date_eta=end_date,
                    warehouse=warehouse,
                    tab="summary",
                )
            elif time_type == "planned_release_time":
                template, context = await self.handle_all_get_planned_release_time(
                    start_date_planned_release_time=start_date,
                    end_date_planned_release_time=end_date,
                    warehouse=warehouse,
                    tab="summary",
                )

            for order in context.get("orders", []):
                # 获取关联的 retrieval 实例（注意：可能为 None，需判断）
                retrieval = getattr(order, "retrieval_id", None)
                if not retrieval:
                    continue  # 无关联retrieval，跳过

                # 1. 获取仓库和原始 UTC 时间（从 retrieval 中取）
                current_warehouse = getattr(retrieval, "retrieval_destination_area", "")
                t49_empty = getattr(retrieval, "t49_empty_returned_at", None)
                t49_pod_full = getattr(retrieval, "t49_pod_full_out_at", None)

                # 2. 转换为本地时间（调用你的时区转换函数）
                local_t49_empty = self._convert_utc_to_local(t49_empty, current_warehouse)
                local_t49_pod_full = self._convert_utc_to_local(t49_pod_full, current_warehouse)

                # 3. 直接修改 retrieval 实例的属性（模板中渲染 retrieval.xxx 时生效）
                # 注意：这是内存中修改，不会同步到数据库（仅用于页面渲染）
                setattr(retrieval, "t49_empty_returned_at", local_t49_empty)
                setattr(retrieval, "t49_pod_full_out_at", local_t49_pod_full)

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

    def _convert_utc_to_local(self, utc_time: Optional[datetime | str], warehouse: str) -> str:
        """
        将数据库的 UTC 时间（datetime 对象或字符串）转换为仓库对应时区的本地时间（自动处理夏令时）
        :param utc_time: 数据库中的 UTC 时间（datetime 对象 或 字符串，如 "2025-11-20T12:00:00Z"）
        :param warehouse: 仓库名称（retrieval_destination_area）
        :return: 格式化后的本地时间字符串（如 "2025-11-20 08:00:00"），空值返回空字符串
        """
        if not utc_time:
            return ""

        try:
            WAREHOUSE_TIMEZONES = {
                "NJ": pytz.timezone("America/New_York"),  # 新泽西（UTC-5/UTC-4）
                "SAV": pytz.timezone("America/New_York"),  # 萨凡纳（UTC-5/UTC-4）
                "LA": pytz.timezone("America/Los_Angeles"),  # 洛杉矶（UTC-8/UTC-7）
                "MO": pytz.timezone("America/Chicago"),  # 圣路易斯（UTC-6/UTC-5）
                "TX": pytz.timezone("America/Chicago"),  # 德克萨斯（UTC-6/UTC-5）
                "TH": pytz.timezone("America/Chicago"),  # 休斯顿（UTC-6/UTC-5）
                "CA": pytz.timezone("America/Los_Angeles"),  # 加州（UTC-8/UTC-7）
            }
            DEFAULT_TIMEZONE = pytz.UTC

            # 2. 处理输入类型：如果是 datetime 对象，直接使用；如果是字符串，解析为 datetime
            if isinstance(utc_time, datetime):
                # 若 datetime 无时区信息，标记为 UTC
                if not utc_time.tzinfo:
                    utc_datetime = pytz.UTC.localize(utc_time)
                else:
                    # 若已有时区，转换为 UTC（防止非 UTC 时间传入）
                    utc_datetime = utc_time.astimezone(pytz.UTC)
            else:
                # 字符串类型：兼容 "2025-11-20T12:00:00Z" 或 "2025-11-20 12:00:00" 格式
                utc_time_str = str(utc_time).replace("Z", "+00:00")
                utc_datetime = datetime.fromisoformat(utc_time_str)
                if not utc_datetime.tzinfo:
                    utc_datetime = pytz.UTC.localize(utc_datetime)

            # 3. 根据仓库匹配目标时区
            target_timezone = DEFAULT_TIMEZONE
            for warehouse_key, tz in WAREHOUSE_TIMEZONES.items():
                if warehouse_key in warehouse.upper():  # 不区分大小写模糊匹配
                    target_timezone = tz
                    break

            # 4. 转换为目标时区并格式化
            local_time = utc_datetime.astimezone(target_timezone)
            return local_time.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            # 异常处理：兼容各种错误情况，避免影响整体功能
            if isinstance(utc_time, datetime):
                # 若是 datetime 对象，直接格式化为字符串返回
                return utc_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # 若是字符串，去除时区后缀后返回（兼容 "2025-11-20 12:00:00+00:00" 格式）
                return str(utc_time).split("+")[0].strip() if utc_time else ""

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
        warehouse = request.POST.get("warehouse")
        await sync_to_async(
            lambda: Container.objects.filter(container_number=container_number).update(
                is_abnormal_state=is_abnormal_state
            )
        )()
        is_abnormal_state = request.POST.get("is_abnormal_state")
        if is_abnormal_state:
            template, context = await self.get_is_abnormal_state(request)
        else:
            template, context = await self.handle_all_get(
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                tab="summary",
                warehouse=warehouse
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
            "warehouse_options": self.warehouse_options,
            "is_abnormal_state": True
        }
        return self.template_main, context

    async def handle_all_get(
            self,
            start_date_eta: str = None,
            end_date_eta: str = None,
            warehouse: str = None,
            tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()
        if warehouse == "all" or warehouse =='NJ,SAV,LA,MO,TX':
            warehouse = ["NJ","SAV","LA","MO","TX"]
        elif warehouse == "NJ":
            warehouse = ["NJ"]
        elif warehouse == "SAV":
            warehouse = ["SAV"]
        elif warehouse == "LA":
            warehouse = ["LA"]
        elif warehouse == "MO":
            warehouse = ["MO"]
        elif warehouse == "TX":
            warehouse = ["TX"]

        # 首次进入时设置默认时间为当前月份
        if not start_date_eta and not end_date_eta:
            first_day_of_month = current_date.replace(day=1)
            start_date_eta = first_day_of_month.strftime("%Y-%m-%d")

            # 当前月份的最后一天
            if current_date.month == 12:
                next_month_first_day = current_date.replace(year=current_date.year + 1, month=1, day=1)
            else:
                next_month_first_day = current_date.replace(month=current_date.month + 1, day=1)
            last_day_of_month = (next_month_first_day - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date_eta = last_day_of_month

        criteria = models.Q(
            cancel_notification=False,
            retrieval_id__retrieval_destination_area__in=warehouse,
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
        warehouse = ','.join(warehouse)        # 转换回字符串格式供前端使用
        context = {
            "customers": customers,
            "orders": orders,
            "start_date": start_date_eta,
            "end_date": end_date_eta,
            "current_date": current_date,
            "tab": tab,
            "time_type": "eta",
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
        }
        return self.template_main, context

    async def handle_all_get_planned_release_time(
            self,
            start_date_planned_release_time: str = None,
            end_date_planned_release_time: str = None,
            warehouse: str = None,
            tab: str = None,
    ) -> tuple[Any, Any]:
        current_date = datetime.now().date()

        if warehouse == "all" or warehouse =='NJ,SAV,LA,MO,TX':
            warehouse = ["NJ","SAV","LA","MO","TX"]
        elif warehouse == "NJ":
            warehouse = ["NJ"]
        elif warehouse == "SAV":
            warehouse = ["SAV"]
        elif warehouse == "LA":
            warehouse = ["LA"]
        elif warehouse == "MO":
            warehouse = ["MO"]
        elif warehouse == "TX":
            warehouse = ["TX"]

        criteria = models.Q(
            cancel_notification=False,
            retrieval_id__retrieval_destination_area__in=warehouse,
        )

        # 处理ETA日期条件
        def parse_eta_date(date_str):
            if not date_str:
                return None
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            # 转换为带时区的datetime（当天0点）
            naive_datetime = datetime.combine(date_obj, datetime.min.time())
            return timezone.make_aware(naive_datetime)  # 关键：添加时区信息

        start_date = parse_eta_date(start_date_planned_release_time)
        end_date = parse_eta_date(end_date_planned_release_time)

        if start_date:
            criteria &= models.Q(retrieval_id__planned_release_time__gte=start_date)
        if end_date:
            # 结束日期设置为当天23:59:59
            end_eta = end_date.replace(hour=23, minute=59, second=59)
            criteria &= models.Q(retrieval_id__planned_release_time__lte=end_eta)

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
        warehouse = ','.join(warehouse)
        # 转换回字符串格式供前端使用
        context = {
            "customers": customers,
            "orders": orders,
            "start_date": start_date_planned_release_time,
            "end_date": end_date_planned_release_time,
            "current_date": current_date,
            "tab": tab,
            "time_type": "planned_release_time",
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
        }
        return self.template_main, context

    async def get_retrieval_cabinet_arrangement_time(self, request: HttpRequest) -> tuple[Any, Any]:
        time_str = request.POST.get("retrieval_cabinet_arrangement_time")
        retrieval_id = request.POST.get("retrieval_id")
        retrieval_obj = await Retrieval.objects.aget(retrieval_id=retrieval_id)
        warehouse = request.POST.get("warehouse")
        if time_str:
            naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            aware_datetime = timezone.make_aware(naive_datetime)
            retrieval_obj.retrieval_cabinet_arrangement_time = aware_datetime
        else:
            retrieval_obj.retrieval_cabinet_arrangement_time = None
        await retrieval_obj.asave()
        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)
        is_abnormal_state = request.POST.get("is_abnormal_state")
        if is_abnormal_state:
            template, context = await self.get_is_abnormal_state(request)
        else:
            template, context = await self.handle_all_get(
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                tab="summary",
                warehouse=warehouse,
            )
        return template, context

    async def batch_get_offload_at(self, request: HttpRequest) -> tuple[Any, Any]:
        offload_ids = request.POST.getlist("offload_ids[]")
        warehouse = request.POST.get("warehouse")
        time_str = request.POST.get("offload_at_container")
        if offload_ids:
            for offload_id in offload_ids:
                try:
                    offload_obj = await Offload.objects.aget(offload_id=offload_id)
                    if time_str:
                        naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
                        aware_datetime = timezone.make_aware(naive_datetime)
                        offload_obj.offload_at_container = aware_datetime
                    else:
                        offload_obj.offload_at_container = None
                    await offload_obj.asave()
                except Offload.DoesNotExist:
                    continue

        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)
        is_abnormal_state = request.POST.get("is_abnormal_state")
        if is_abnormal_state:
            template, context = await self.get_is_abnormal_state(request)
        else:
            template, context = await self.handle_all_get(
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                tab="summary",
                warehouse=warehouse,
            )
        return template, context

    async def get_offload_at(self, request: HttpRequest) -> tuple[Any, Any]:
        time_str = request.POST.get("offload_at_container")
        offload_id = request.POST.get("offload_id")
        offload_obj = await Offload.objects.aget(offload_id=offload_id)
        warehouse = request.POST.get("warehouse")
        if time_str:
            naive_datetime = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
            aware_datetime = timezone.make_aware(naive_datetime)
            offload_obj.offload_at_container = aware_datetime
        else:
            offload_obj.offload_at_container = None
        await offload_obj.asave()
        start_date_eta = request.POST.get("start_date_eta", None)
        end_date_eta = request.POST.get("end_date_eta", None)
        is_abnormal_state = request.POST.get("is_abnormal_state")
        if is_abnormal_state:
            template, context = await self.get_is_abnormal_state(request)
        else:
            template, context = await self.handle_all_get(
                start_date_eta=start_date_eta,
                end_date_eta=end_date_eta,
                warehouse=warehouse,
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
        warehouse = request.POST.get("warehouse")
        await sync_to_async(
                lambda: Retrieval.objects.filter(retrieval_id=retrieval_id).update(
                    note_preport_dispatch=note_preport_dispatch
                )
            )()
        is_abnormal_state = request.POST.get("is_abnormal_state")
        if is_abnormal_state:
            template, context = await self.get_is_abnormal_state(request)
        else:
            template, context = await self.handle_all_get(start_date_eta, end_date_eta,warehouse=warehouse,tab="summary")
        return template, context


    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    warehouse_options = {
        "所有仓库": "all",
        "NJ": "NJ",
        "SAV": "SAV",
        "LA": "LA",
        "MO": "MO",
        "TX": "TX",
    }
