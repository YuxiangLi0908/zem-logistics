from datetime import datetime, timedelta
from django.utils import timezone
from typing import Any, Coroutine, Tuple
import os

from asgiref.sync import sync_to_async
from django.db import models
from django.db.models import Prefetch, Q
from django.core.exceptions import ValidationError
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from django.db.models import Sum, Count, F, FloatField, Case, When, Value
from django.db.models.functions import Coalesce
#from sqlalchemy.sql.functions import current_time

from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.fleet import Fleet
from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.views.export_file import export_palletization_list
from warehouse.views.post_port.warehouse.palletization import Palletization
from warehouse.views.post_port.shipment.fleet_management import FleetManagement


class WarehouseOperations(View):
    template_warehousing_operation = "post_port/warehouse_operations/01_warehousing_operation.html"
    template_upcoming_fleet = "post_port/warehouse_operations/03_upcoming_fleet.html"

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
        elif step == "upcoming_fleet":
            template, context = await self.handle_upcoming_fleet_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpResponse | JsonResponse | HttpResponseRedirect:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "export_palletization_list":
            return await export_palletization_list(request)
        elif step == "export_pallet_label":
            palletization_view = Palletization()
            return await palletization_view._export_pallet_label(request)
        elif step == "update_warehouse":
            template, context = await self.warehousing_operation_update(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "first_time_download":
            template, context = await self.warehousing_operation_first_time_download(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "upcoming_fleet_warehouse":
            template, context = await self.handle_upcoming_fleet_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "checkin_fleet":
            template, context = await self.handle_checkin_fleet_post(request)
            return render(request, template, context)
        elif step == "loading_fleet":
            template, context = await self.handle_loading_fleet_post(request)
            return render(request, template, context)
        elif step == "complete_loading":
            template, context = await self.handle_complete_loading_post(request)
            return render(request, template, context)
        elif step =="export_bol":
            return await self.handle_bol_post(request)


    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def warehousing_operation_get(self, request: HttpRequest):
        """
        入库操作-页面展示
        """
        current_time = datetime.now()
        future_four_days = current_time + timedelta(days=4)
        ORDER_FILTER_CRITERIA = Q(
            offload_id__offload_required=True,
            offload_id__offload_at__isnull=False,
            cancel_notification=False
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
            "retrieval": retrieval
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

    async def handle_upcoming_fleet_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_upcoming_fleet, context

    async def handle_bol_post(self, request: HttpRequest) -> HttpResponse:
        #准备参数
        mutable_post = request.POST.copy()
        fleet_number = request.POST.get("fleet_number")
        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        shipment = await sync_to_async(list)(Shipment.objects.filter(fleet_number=fleet))
        if len(shipment) > 1:
            raise ValueError('这个约有多个批次，不知道怎么下载BOL')
        else:
            mutable_post["shipment_batch_number"] = shipment[0].shipment_batch_number   

        mutable_post["customerInfo"] = None
        mutable_post["pickupList"] = None
  
        request.POST = mutable_post
        fm = FleetManagement()    
        return await fm.handle_export_bol_post(request)

    async def handle_complete_loading_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        #上传出库凭证
        try:
            receipt_image = request.FILES.get("receipt_image")
            if receipt_image:
                valid_extensions = [".jpg", ".png", ".jpeg"]
                ext = os.path.splitext(receipt_image.name)[1].lower()
                if ext not in valid_extensions:
                    raise ValidationError("仅支持JPG/PNG格式图片")
                if receipt_image.size > 5 * 1024 * 1024:  # 5MB
                    raise ValidationError("图片大小不能超过5MB")
        except ValidationError as e:
            raise ValidationError("图片格式错误")
        if receipt_image:
            conn = self._get_sharepoint_auth()
            link = self._upload_image_to_sharepoint(conn, receipt_image)
        else:
            link = ""
        #更新车次状态
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status="shipped", shipped_cert_link=link)
        
        return await self.handle_upcoming_fleet_post(request)


    async def handle_loading_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")

        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status="loading")

        if updated == 0:
            return ValueError('未查到该车次')
        return await self.handle_upcoming_fleet_post(request)


    async def handle_checkin_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        driver_name = request.POST.get("driver_name")
        driver_phone = request.POST.get("driver_phone")
        license_plate = request.POST.get("license_plate")

        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(
            warehouse_process_status="check_in",
            driver_name=driver_name,
            driver_phone=driver_phone,
            license_plate=license_plate
        )

        if updated == 0:
            return ValueError('未查到该车次')
        return await self.handle_upcoming_fleet_post(request)


    async def handle_upcoming_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
    
        # 获取未来三天的时间范围
        today = timezone.now().date()
        three_days_later = today + timedelta(days=3)
        
        # 异步查询未来三天内的车队数据（使用Prefetch优化查询）
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(
                appointment_datetime__date__range=[today, three_days_later],
                origin = warehouse,
            ).prefetch_related(
                Prefetch(
                    'shipment',
                    queryset=Shipment.objects.prefetch_related(
                        'pallet',
                        'packinglist'
                    )
                )
            )
        )
        
        fleet_data = []
        for fleet in fleets:
            pallet_stats = await sync_to_async(fleet.shipment.aggregate)(
                total_pallets=Count('pallet', distinct=True),
                total_pallet_cbm=Sum('pallet__cbm')
            )
            
            packinglist_stats = await sync_to_async(fleet.shipment.aggregate)(
                total_packinglist_cbm=Sum('packinglist__cbm')
            )
            
            pallet_pallets = pallet_stats['total_pallets'] or 0
            pallet_cbm = pallet_stats['total_pallet_cbm'] or 0.0
            packinglist_cbm = packinglist_stats['total_packinglist_cbm'] or 0.0
            
            packinglist_pallets = round(packinglist_cbm / 1.8) if packinglist_cbm else 0
            
            is_estimated = pallet_pallets == 0 and packinglist_pallets > 0
            
            total_pallets = pallet_pallets + packinglist_pallets
            total_cbm = pallet_cbm + packinglist_cbm
            
            days_diff = (fleet.appointment_datetime.date() - today).days
            
            fleet_data.append({
                'fleet_number': fleet.fleet_number,
                'warehouse_process_status': fleet.warehouse_process_status,
                'pickup_number': fleet.pickup_number,
                'appointment_datetime': fleet.appointment_datetime,
                'driver_name': fleet.driver_name,
                'driver_phone': fleet.driver_phone,
                'license_plate': fleet.license_plate,
                'pallets': total_pallets,
                'cbm': round(total_cbm, 2),
                'is_estimated': is_estimated,
                'days_diff': days_diff
            })
        # 按日期分组统计
        today_count = len([f for f in fleet_data if f['days_diff'] == 0])
        tomorrow_count = len([f for f in fleet_data if f['days_diff'] == 1])
        day_after_count = len([f for f in fleet_data if f['days_diff'] == 2])
        
        # 汇总统计
        total_pallets = sum(f['pallets'] for f in fleet_data)
        total_cbm = sum(f['cbm'] for f in fleet_data)
        
        context = {
            'warehouse_options': self.warehouse_options,
            'fleets': fleet_data,
            'warehouse': warehouse,
            'summary': {
                'total_fleets': len(fleet_data),
                'total_pallets': total_pallets,
                'total_cbm': total_cbm,
                'completed_count': 0,
                'today_count': today_count,
                'tomorrow_count': tomorrow_count,
                'day_after_count': day_after_count,
            }
        }
        return self.template_upcoming_fleet, context
