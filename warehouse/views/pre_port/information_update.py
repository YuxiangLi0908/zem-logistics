from datetime import datetime

from django.utils import timezone
from typing import Any, Coroutine

from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.db.models import When, Value, F, CharField, Case, Q
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect
from django.shortcuts import redirect, render
from django.views import View

from warehouse.models.order import Order
from warehouse.utils.constants import PORT_TO_WAREHOUSE_AREA


class InformationUpdate(View):
    template_main = "pre_port/information_update/01_information_update_all.html"

    async def get(self, request: HttpRequest) -> Any | None:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "all":
            template, context = await self.information_update_all()
            return render(request, template, context)

    async def post(self, request: HttpRequest) -> Any | None:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "warehouse":
            template, context = await self.information_update_search(request)
            return render(request, template, context)
        elif step == "information_update_edit":
            template, context = await self.information_update_edit(request)
            return render(request, template, context)

    async def _user_authenticate(self, request: HttpRequest) -> bool:
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def information_update_all(self):
        context = {
            "warehouse_options": self.warehouse_options,
        }
        return self.template_main, context

    async def information_update_search(self, request: HttpRequest):
        warehouse = request.POST.get("warehouse")
        eta_start = request.POST.get("eta_start")
        eta_end = request.POST.get("eta_end")

        pallets_list = []
        if eta_start and eta_end:
            if warehouse == "NJ,SAV,LA,MO,TX":
                warehouse_list = warehouse.split(",")
                for w in warehouse_list:
                    pallets = await self._get_pallet(w, eta_start, eta_end)
                    pallets_list.extend(pallets)
            else:
                pallets_list = await self._get_pallet(warehouse, eta_start, eta_end)


        # 3. 构建上下文
        context = {
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
            "datas": pallets_list,  # 替换为处理后的列表
            "eta_start": eta_start,
            "eta_end": eta_end,
        }

        return self.template_main, context

    async def _get_pallet(self, warehouse: str, eta_start: str, eta_end: str) -> list[dict]:
        # 时间处理：将年月转换为完整的时间范围（月初到月末最后一秒）
        eta_start_datetime = timezone.datetime.strptime(eta_start, "%Y-%m")
        eta_end_datetime = timezone.datetime.strptime(eta_end, "%Y-%m")
        eta_end_plus_1month = eta_end_datetime + relativedelta(months=1)
        eta_end_last_second = eta_end_plus_1month - relativedelta(seconds=1)

        eta_start_datetime = timezone.make_aware(eta_start_datetime)
        eta_end_datetime = timezone.make_aware(eta_end_last_second)

        filter_conditions = Q(
            vessel_id__vessel_eta__gte=eta_start_datetime,
            vessel_id__vessel_eta__lte=eta_end_datetime,
        ) & ~Q(cancel_notification=True)

        # 组合条件：直送订单通过港口映射获取仓点，其他订单直接匹配仓点
        filter_conditions &= Q(
            # 直送订单：港口映射到仓点
            Q(
                order_type="直送",
                vessel_id__destination_port__in=[
                    port for port, wh in PORT_TO_WAREHOUSE_AREA.items() if wh == warehouse
                ]
            ) |
            # 其他订单：直接匹配仓点
            Q(
                order_type__in=["转运组合", "转运"],
                retrieval_id__retrieval_destination_area=warehouse
            )
        )

        # 关键修复：CharField 不支持 __ne，改用 ~Q(字段=F("对比字段")) 表示不等于
        t49_diff_conditions = (
                ~Q(vessel_id__vessel=F("vessel_id__vessel_t49")) |  # 船名不一致（取反等于）
                ~Q(vessel_id__vessel_etd=F("vessel_id__vessel_etd_t49")) |  # ETD 不一致（DateTimeField 也适用）
                ~Q(vessel_id__vessel_eta=F("vessel_id__vessel_eta_t49")) |  # ETA 不一致
                ~Q(vessel_id__destination_port=F("vessel_id__destination_port_t49"))  # 码头不一致
        )

        # 组合所有条件
        filter_conditions &= t49_diff_conditions

        # 补充：values() 中添加所有 T49 字段，前端才能渲染
        return await sync_to_async(list)(
            Order.objects.prefetch_related(
                "container_number",
                "retrieval_id",
                "vessel_id",
            )
            .filter(filter_conditions)
            # 移除多余的 values()：先 annotate 再 values，避免字段丢失
            .annotate(
                retrieval_destination_area=Case(
                    When(order_type="直送",
                         then=Value(warehouse)
                         ),
                    When(order_type="转运组合",
                         then=F("retrieval_id__retrieval_destination_area")
                         ),
                    When(order_type="转运",
                         then=F("retrieval_id__retrieval_destination_area")
                         ),
                    default=Value(""),
                    output_field=CharField()
                ),
                vessel=F("vessel_id__vessel"),
                vessel_t49=F("vessel_id__vessel_t49"),
                vessel_etd=F("vessel_id__vessel_etd"),
                vessel_etd_t49=F("vessel_id__vessel_etd_t49"),
                vessel_eta=F("vessel_id__vessel_eta"),
                vessel_eta_t49=F("vessel_id__vessel_eta_t49"),
                destination_port=F("vessel_id__destination_port"),
                destination_port_t49=F("vessel_id__destination_port_t49"),
                container=F("container_number__container_number"),
            )
            # 最终返回字段：包含前端需要的所有 T49 字段
            .values(
                "container", "retrieval_destination_area",
                "vessel", "vessel_t49",  # 船名 + T49 船名
                "vessel_eta", "vessel_eta_t49",  # ETA + T49 ETA
                "vessel_etd", "vessel_etd_t49",  # ETD + T49 ETD
                "destination_port", "destination_port_t49",  # 码头 + T49 码头
            )
            .order_by('vessel_eta', 'container')
        )

    async def information_update_edit(self, request: HttpRequest) -> Any:
        try:
            container_number = request.POST.get("container", "").strip()
            vessel = request.POST.get("vessel", "").strip()
            vessel_eta_str = request.POST.get("vessel_eta", "").strip()
            vessel_etd_str = request.POST.get("vessel_etd", "").strip()
            destination_port = request.POST.get("destination_port", "").strip()

            if not container_number:
                template, context = await self.information_update_search(request)
                context["error_msg"] = "柜号不能为空，更新失败"
                return template, context

            def parse_datetime(time_str: str):
                """
                修正：支持两种时间格式（兼容纯日期和年月日时分秒）
                - 前端传递格式：Y-m-d H:i:s（时分秒）或 Y-m-d（纯日期）
                - 返回带时区的 datetime 对象（与后端时区一致）
                """
                if not time_str:
                    return None
                # 定义支持的时间格式（优先匹配时分秒格式）
                formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
                for fmt in formats:
                    try:
                        # 解析为本地时间（datetime 对象）
                        date_obj = datetime.strptime(time_str, fmt)
                        # 转换为带时区的时间（使用 Django 配置的时区，如 UTC+8）
                        aware_date = timezone.make_aware(date_obj)
                        return aware_date
                    except ValueError:
                        continue
                print(f"时间解析失败：不支持的时间格式（输入值：{time_str}）")
                return None

            @sync_to_async
            def update_order_data():
                vessel_eta = parse_datetime(vessel_eta_str)
                vessel_etd = parse_datetime(vessel_etd_str)

                # 筛选条件优化：确保只查询有效订单
                orders = Order.objects.select_related("vessel_id", "container_number").filter(
                    container_number__container_number=container_number,
                    cancel_notification=False,
                    vessel_id__isnull=False
                )

                if not orders.exists():
                    return False, f"未找到柜号为【{container_number}】的有效订单"

                # 批量更新（优化性能）
                updated_count = 0
                for order in orders:
                    vessel_instance = order.vessel_id
                    has_update = False

                    # 只更新有变化的字段（避免无用写入）
                    if vessel and vessel_instance.vessel != vessel:
                        vessel_instance.vessel = vessel
                        has_update = True

                    if vessel_eta and vessel_instance.vessel_eta != vessel_eta:
                        vessel_instance.vessel_eta = vessel_eta
                        has_update = True

                    if vessel_etd and vessel_instance.vessel_etd != vessel_etd:
                        vessel_instance.vessel_etd = vessel_etd
                        has_update = True

                    if destination_port and vessel_instance.destination_port != destination_port:
                        vessel_instance.destination_port = destination_port
                        has_update = True

                    if has_update:
                        vessel_instance.save()
                        updated_count += 1

                return True, f"成功更新 {updated_count} 条订单数据"

            # 执行更新并获取结果
            update_success, msg = await update_order_data()

            # 重新获取搜索结果页面
            template, context = await self.information_update_search(request)
            if update_success:
                context["success_msg"] = msg
            else:
                context["error_msg"] = msg

            return template, context

        except Exception as e:
            template, context = await self.information_update_search(request)
            context["error_msg"] = f"更新失败：{str(e)}"
            return template, context

    warehouse_options = {
        "所有仓库": "NJ,SAV,LA,MO,TX",
        "NJ": "NJ",
        "SAV": "SAV",
        "LA": "LA",
        "MO": "MO",
        "TX": "TX",
    }