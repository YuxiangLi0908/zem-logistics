from datetime import datetime, timedelta
from django.utils import timezone
from typing import Any, Coroutine

from asgiref.sync import sync_to_async
from django.db import models
from django.db.models import Prefetch, Q
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from sqlalchemy.sql.functions import current_time

from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.views.export_file import export_palletization_list
from warehouse.views.post_port.warehouse.palletization import Palletization


class WarehouseOperations(View):
    template_warehousing_operation = "post_port/warehouse_operations/01_warehousing_operation.html"
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "TX-77503": "TX-77503",
    }
    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "warehousing_operation":
            template, context = await self.warehousing_operation_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpResponse | JsonResponse | HttpResponseRedirect:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "export_palletization_list":
            # 先执行导出操作
            await export_palletization_list(request)

            retrieval_id = request.POST.get('retrieval_id', '').strip()
            warehouse_unpacking_time = request.POST.get("first_time_download")
            if warehouse_unpacking_time and retrieval_id:  # 同时验证retrieval_id
                def sync_update_single_offload_select():
                    related_orders = Order.objects.filter(
                        retrieval_id__retrieval_id=retrieval_id,
                        offload_id__isnull=False,
                        offload_id__warehouse_unpacked_time__isnull=True
                    ).select_related('offload_id')

                    if related_orders.exists():
                        updated_count = 0
                        for order in related_orders:
                            order.offload_id.warehouse_unpacking_time = warehouse_unpacking_time
                            order.offload_id.save()
                            updated_count += 1
                        return updated_count
                    return 0

                async_update_offload = sync_to_async(sync_update_single_offload_select, thread_sensitive=True)
                affected_rows = await async_update_offload()

                def sync_update_single_select():
                    retrieval = Retrieval.objects.get(retrieval_id=retrieval_id)
                    retrieval.unpacking_status = "2"
                    retrieval.save()

                if affected_rows > 0:
                    async_update_re = sync_to_async(sync_update_single_select, thread_sensitive=True)
                    await async_update_re()
            template, context = await self.warehousing_operation_get(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "export_pallet_label":
            palletization_view = Palletization()
            return await palletization_view._export_pallet_label(request)
        elif step == "update_warehouse":
            template, context = await self.warehousing_operation_update(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "warehouse_daily_get":
            template, context = await self.warehousing_operation_post(request)
            return await sync_to_async(render)(request, template, context)

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def warehousing_operation_get(self, request: HttpRequest):
        context = {
            "warehouse_options": self.warehouse_options,
        }
        return self.template_warehousing_operation, context

    async def warehousing_operation_post(self, request: HttpRequest):
        """
        入库操作-页面展示
        """
        current_time = datetime.now()
        future_four_days = current_time + timedelta(days=4)
        warehouse = request.POST.get("warehouse_filter", None)
        ORDER_FILTER_CRITERIA = Q(
            offload_id__offload_required=True,
            offload_id__offload_at__isnull=False,
            cancel_notification=False,
            warehouse__name = warehouse
        ) & Q(
            Q(retrieval_id__temp_t49_available_for_pickup=True) |
            Q(vessel_id__vessel_eta__lte=future_four_days)
        )

        def sync_get_retrieval():
            order_queryset = (
                Order.objects.select_related(
                    "customer_name",
                    "container_number",
                    "retrieval_id",
                    "offload_id",
                    "warehouse",
                    "vessel_id"
                )
                .filter(ORDER_FILTER_CRITERIA)
                .only(
                    "container_number", "unpacking_priority",
                    "offload_id", "customer_name__zem_code",
                    "retrieval_id", "warehouse", "vessel_id"
                )
            )

            return (
                Retrieval.objects.prefetch_related(
                    Prefetch(
                        "order_set",
                        queryset=order_queryset,
                        to_attr="filtered_orders"  # 自定义属性名，避免与默认order_set冲突
                    )
                )
                .filter(
                    actual_retrieval_timestamp__isnull=False,
                )
                .only(
                    "actual_retrieval_timestamp",
                    "arrival_location", "unpacking_status"
                )
                .order_by("actual_retrieval_timestamp")
            )

        retrieval = await sync_to_async(
            sync_get_retrieval
        )()

        context = {
            "retrieval": retrieval,
            "warehouse_options": self.warehouse_options,
            "warehouse": warehouse,
        }
        return self.template_warehousing_operation, context

    async def warehousing_operation_update(self, request: HttpRequest):
        try:
            retrieval_id = request.POST.get('retrieval_id', '').strip()
            arrival_location = request.POST.get('arrival_location', '').strip()
            unpacking_status = request.POST.get('unpacking_status', '').strip()

            # 1. 定义更新Retrieval表的同步函数
            def sync_update_single():
                return Retrieval.objects.filter(retrieval_id=retrieval_id).update(
                    arrival_location=arrival_location,
                    unpacking_status=unpacking_status
                )

            # 2. 定义更新Offload表的同步函数
            def sync_update_single_offload():
                related_orders = Order.objects.filter(
                    retrieval_id__retrieval_id=retrieval_id,
                    offload_id__isnull=False,
                    offload_id__warehouse_unpacked_time__isnull=True
                ).select_related('offload_id')

                if related_orders.exists():
                    current_time = timezone.now()
                    updated_count = 0
                    for order in related_orders:
                        order.offload_id.warehouse_unpacked_time = current_time
                        order.offload_id.save()
                        updated_count += 1
                    return updated_count  # 返回更新的记录数
                return 0

            # 3. 包装同步函数为异步函数
            async_update = sync_to_async(sync_update_single, thread_sensitive=True)
            # 关键：必须为sync_update_single_offload也创建异步包装
            async_update_offload = sync_to_async(sync_update_single_offload, thread_sensitive=True)

            # 4. 执行更新操作（通过包装后的异步函数）
            affected_rows = await async_update()

            # 5. 当拆柜状态为1时，执行Offload更新
            if unpacking_status == "1":
                offload_affected = await async_update_offload()  # 正确调用方式

        except Exception as e:
            self.logger.error(f"更新记录{retrieval_id}时发生错误：{str(e)}", exc_info=True)

        template, context = await self.warehousing_operation_get(request)
        return template, context

    async def warehousing_operation_first_time_download(self, request: HttpRequest):
        warehouse_unpacking_time = request.GET.get("first_time_download")
        template, context = await self.warehousing_operation_first_time_download(request)
        return template, context
