from datetime import datetime

from django.db import transaction
from django.db.models.functions import TruncDate
from django.utils import timezone
from typing import Any, Coroutine

from asgiref.sync import sync_to_async
from dateutil.relativedelta import relativedelta
from django.db.models import When, Value, F, CharField, Case, Q
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect
from django.shortcuts import redirect, render
from django.views import View

from warehouse.models.order import Order
from warehouse.models.vessel import Vessel
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
        elif step == "batch_update":
            template, context = await self.information_update_batch(request)
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
                # ETD 仅比较日期部分（忽略时分秒）
                ~Q(
                    vessel_id__vessel_etd__date=TruncDate(F("vessel_id__vessel_etd_t49"))
                ) |
                # ETA 仅比较日期部分（忽略时分秒）
                ~Q(
                    vessel_id__vessel_eta__date=TruncDate(F("vessel_id__vessel_eta_t49"))
                ) |
                ~Q(vessel_id__destination_port=F("vessel_id__destination_port_t49"))  # 码头不一致
        )

        # 组合所有条件
        filter_conditions &= t49_diff_conditions

        results = await sync_to_async(list)(
            Order.objects.prefetch_related(
                "container_number",
                "retrieval_id",
                "vessel_id",
            )
            .filter(filter_conditions)
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
            .values(
                "container", "retrieval_destination_area",
                "vessel", "vessel_t49",
                "vessel_eta", "vessel_eta_t49",
                "vessel_etd", "vessel_etd_t49",
                "destination_port", "destination_port_t49",
            )
            .order_by('vessel_eta', 'container')
        )

        # 在 Python 层面处理字符串分割
        for item in results:
            item['vessel_v1'] = item['vessel'].split(' ')[0].upper if item['vessel'] else ''
            item['vessel_t49_v1'] = item['vessel_t49'].split(' ')[0].upper if item['vessel_t49'] else ''
        return results

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
                强制补全纯日期为 00:00:00，确保格式统一：
                - 输入 Y-m-d → 解析为 Y-m-d 00:00:00（带时区）
                - 输入 Y-m-d H:i:s / Y-m-d H:i → 保持原时分秒（带时区）
                - 无效格式返回 None
                """
                if not time_str:
                    return None

                # 定义格式：优先匹配纯日期（前端主要传递格式），再匹配带时分秒格式
                formats = [
                    "%Y-%m-%d",  # 纯日期（优先）
                    "%Y-%m-%d %H:%M:%S",  # 完整时间
                    "%Y-%m-%d %H:%M"  # 带时分无秒
                ]

                for fmt in formats:
                    try:
                        date_obj = datetime.strptime(time_str, fmt)
                        # 关键：如果是纯日期格式，强制补全时分秒为 00:00:00
                        if fmt == "%Y-%m-%d":
                            date_obj = date_obj.replace(hour=0, minute=0, second=0)
                        # 转换为带时区的 datetime（与 Django 配置一致）
                        aware_date = timezone.make_aware(date_obj)
                        return aware_date
                    except ValueError:
                        continue

                print(f"时间解析失败：不支持的时间格式（输入值：{time_str}），仅支持 Y-m-d 或 Y-m-d H:i:s 格式")
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

                    # 关键：允许时分秒从非00:00:00更新为00:00:00（比如用户只改日期）
                    if vessel_eta is not None:  # 注意：这里用 is not None（允许用户清空？根据需求调整）
                        if vessel_instance.vessel_eta != vessel_eta:
                            vessel_instance.vessel_eta = vessel_eta
                            has_update = True

                    if vessel_etd is not None:
                        if vessel_instance.vessel_etd != vessel_etd:
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

    async def information_update_batch(self, request: HttpRequest):
        batch_field = request.POST.get('batch_field')  # vessel_eta 或 vessel_etd
        batch_time = request.POST.get('batch_time')  # 前端传递格式：YYYY-MM-DD
        selected_containers = request.POST.get('selected_containers', '').split(',')  # 选中的柜号列表
        warehouse = request.POST.get('warehouse')

        selected_containers = [container.strip() for container in selected_containers if container.strip()]

        success_msg = None
        error_msg = None

        try:
            if not batch_field or batch_field not in ['vessel_eta', 'vessel_etd']:
                error_msg = '无效的更新字段！仅支持ETA时间和ETD时间更新'
                template, context = await self.information_update_search(request)
                context.update({'error_msg': error_msg})
                return template, context

            if not batch_time:
                error_msg = '请选择目标时间！'
                template, context = await self.information_update_search(request)
                context.update({'error_msg': error_msg})
                return template, context

            if not selected_containers:
                error_msg = '请至少选择一个柜号！'
                template, context = await self.information_update_search(request)
                context.update({'error_msg': error_msg})
                return template, context

            if not warehouse:
                error_msg = '仓库参数缺失！'
                template, context = await self.information_update_search(request)
                context.update({'error_msg': error_msg})
                return template, context

            # 统一日期解析逻辑：纯日期补全 00:00:00
            def parse_datetime(time_str: str):
                if not time_str:
                    return None
                formats = [
                    "%Y-%m-%d",  # 纯日期（优先）
                    "%Y-%m-%d %H:%M:%S",  # 完整时间
                    "%Y-%m-%d %H:%M"  # 带时分无秒
                ]
                for fmt in formats:
                    try:
                        date_obj = datetime.strptime(time_str, fmt)
                        if fmt == "%Y-%m-%d":
                            date_obj = date_obj.replace(hour=0, minute=0, second=0)
                        aware_date = timezone.make_aware(date_obj)
                        return aware_date
                    except ValueError:
                        continue
                raise ValueError(f"不支持的时间格式：{time_str}（仅支持 Y-m-d 或 Y-m-d H:i:s）")

            # 解析前端传递的日期（强制补全 00:00:00）
            try:
                parsed_datetime = parse_datetime(batch_time)
            except ValueError as e:
                error_msg = str(e)
                template, context = await self.information_update_search(request)
                context.update({'error_msg': error_msg})
                return template, context

            @sync_to_async
            def sync_batch_update():
                """批量更新函数：先查Order，再更新关联的Vessel字段"""
                with transaction.atomic():
                    orders = Order.objects.select_related(
                        "vessel_id", "container_number"
                    ).filter(
                        container_number__container_number__in=selected_containers,
                        cancel_notification=False,
                        vessel_id__isnull=False,
                    )

                    vessel_ids = orders.values_list('vessel_id__id', flat=True).distinct()
                    if not vessel_ids:
                        return 0

                    # 构造更新数据（使用解析后的带时区 datetime）
                    update_data = {batch_field: parsed_datetime}
                    updated_count = Vessel.objects.filter(id__in=vessel_ids).update(**update_data)
                    return updated_count

            # 执行批量更新
            updated_count = await sync_batch_update()
            success_msg = f'成功更新 {updated_count} 艘船舶的{"ETA" if batch_field == "vessel_eta" else "ETD"}日期（选中 {len(selected_containers)} 个柜号）！'

        except Exception as e:
            error_msg = f'批量更新失败：{str(e)}'
            print(f'批量更新异常详情：{repr(e)}')

        # 重新返回搜索结果页面，保留筛选条件
        template, context = await self.information_update_search(request)
        context.update({
            'success_msg': success_msg,
            'error_msg': error_msg,
            'warehouse': warehouse,
            'eta_start': request.POST.get('eta_start', ''),
            'eta_end': request.POST.get('eta_end', ''),
            'container_search': request.POST.get('container_search', ''),
            'search_vessel': request.POST.get('search_vessel', ''),
        })
        return template, context
    warehouse_options = {
        "所有仓库": "NJ,SAV,LA,MO,TX",
        "NJ": "NJ",
        "SAV": "SAV",
        "LA": "LA",
        "MO": "MO",
        "TX": "TX",
    }