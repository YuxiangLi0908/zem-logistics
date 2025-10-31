from datetime import datetime, timedelta
from django.utils import timezone
from typing import Any, Coroutine, Tuple
import os, re
from io import BytesIO
import zipfile
from office365.sharepoint.client_context import ClientContext
from django.contrib.postgres.aggregates import StringAgg

from asgiref.sync import sync_to_async
from django.db import models
from django.db.models import Prefetch, Q, IntegerField, DateTimeField, Min
from django.core.exceptions import ValidationError
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from django.db.models import Sum, Count, F, FloatField, IntegerField, CharField, Value, Case, When
from django.db.models.functions import Cast
from django.db.models.functions import Coalesce
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from sqlalchemy.sql.functions import current_time

from warehouse.models.container import Container
from warehouse.models.export_unpacking_cabinets import ExportUnpackingCabinets
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.retrieval import Retrieval
from warehouse.models.fleet import Fleet
from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.views.export_file import export_palletization_list, export_palletization_list_v2
from warehouse.views.post_port.warehouse.palletization import Palletization
from warehouse.views.post_port.shipment.fleet_management import FleetManagement
from warehouse.utils.constants import (
    SP_CLIENT_ID,
    SP_PRIVATE_KEY,
    SP_SCOPE,
    SP_TENANT,
    SP_THUMBPRINT,
    SP_URL,
    SP_DOC_LIB,
    SYSTEM_FOLDER,
    APP_ENV
)

class WarehouseOperations(View):
    template_warehousing_operation = "post_port/warehouse_operations/01_warehousing_operation.html"
    template_upcoming_fleet = "post_port/warehouse_operations/03_upcoming_fleet.html"
    template_counting_pallet = "post_port/warehouse_operations/02_counting_pallet.html"

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
        elif step == "counting_pallet":
            template, context = await self.handle_counting_pallet_get(request)
            return await sync_to_async(render)(request, template, context)

    async def post(self, request: HttpRequest, **kwargs) -> HttpResponse | JsonResponse | HttpResponseRedirect:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step", None)
        if step == "export_palletization_list":
            action_type = request.POST.get('action_type', 'export')
            # 1. 第一次请求：执行导出操作
            if action_type == 'export':
                response_down = await export_palletization_list(request)
                response_down['X-Action'] = 'export'
                return response_down
            elif action_type == 'new_export':
                response_down = await export_palletization_list_v2(request)
                response_down['X-Action'] = 'new_export'
                return response_down
            # 2. 第二次请求：执行更新并返回页面
            elif action_type == 'render':
                template, context = await self.warehousing_operation_down_render(request)
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
        elif step == "upcoming_fleet_warehouse":
            template, context = await self.handle_upcoming_fleet_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "checkin_fleet":
            template, context = await self.handle_checkin_fleet_post(request)
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                fleet_number = request.POST.get("fleet_number")
                fleet = next((f for f in context["fleets"] if f["fleet_number"] == fleet_number), None)

                if not fleet:
                    return JsonResponse({"success": False, "error": "未找到该车次"})

                return JsonResponse({
                    "success": True,
                    "new_status": fleet["warehouse_process_status"],
                    "display_text": "已签到",
                    "icon": "fa-user-check",
                    "driver_name": fleet["driver_name"],
                    "driver_phone": fleet["driver_phone"],
                    "trailer_number": fleet.get("trailer_number"),
                    "PRO": fleet.get("PRO"),
                })

            return render(request, template, context)
        elif step == "loading_fleet":
            template, context = await self.handle_loading_fleet_post(request)
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                fleet_number = request.POST.get("fleet_number")
                fleet = next((f for f in context["fleets"] if f["fleet_number"] == fleet_number), None)
                if not fleet:
                    return JsonResponse({"success": False, "error": "未找到该车次"})

                return JsonResponse({
                    "success": True,
                    "new_status": fleet["warehouse_process_status"],  # 'loading'
                    "display_text": "装柜中",
                    "icon": "fa-truck-loading"
                })
            return render(request, template, context)
        elif step == "shipped_fleet":
            template, context = await self.handle_shipped_fleet_post(request)
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                fleet_number = request.POST.get("fleet_number")
                fleet = next((f for f in context["fleets"] if f["fleet_number"] == fleet_number), None)
                if not fleet:
                    return JsonResponse({"success": False, "error": "未找到该车次"})

                # 返回更新后的状态给前端
                return JsonResponse({
                    "success": True,
                    "new_status": "shipped",
                    "display_text": "已出库",
                    "icon": "fa-check-double"
                })
            return render(request, template, context)
        elif step == "complete_loading":
            file_path_name = "outbound_file"
            template, context = await self.handle_complete_loading_post(request, file_path_name)
            return render(request, template, context)
        elif step == "warehousing_complete_loading":
            file_path_name = "palletization_list"
            template, context = await self.handle_complete_loading_post_palletization_list(request, file_path_name)
            return await sync_to_async(render)(request, template, context)
        elif step == "report_issue":
            template, context = await self.handle_report_issue_post(request)
            return render(request, template, context)
        elif step =="export_bol":
            return await self.handle_bol_post(request)
        elif step == "counting_pallet_warehouse":
            template, context = await self.handle_counting_pallet_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "adjust_inventory":
            template, context = await self.handle_adjust_inventory_post(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "record_load":
            template, context = await self.handle_record_load_post(request)
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                fleet_number = request.POST.get("fleet_number")
                pre_load = request.POST.get("pre_load")

                # 找到对应 fleet
                fleet = next((f for f in context["fleets"] if f["fleet_number"] == fleet_number), None)
                if not fleet:
                    return JsonResponse({"success": False, "error": "未找到该车次"})

                # 更新数据库/上下文（假设 handle_record_load_post 已做）
                fleet["pre_load"] = pre_load

                return JsonResponse({
                    "success": True,
                    "pre_load": fleet["pre_load"]
                })

            return await sync_to_async(render)(request, template, context)


    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False

    async def warehousing_operation_down_render(self, request: HttpRequest):
        """
        下载拆柜单后返回前端页面
        """
        offload_id = request.POST.get('offload_id', '').strip()
        warehouse_unpacking_time = request.POST.get("first_time_download")
        container_number = request.POST.get("container_number")
        if warehouse_unpacking_time and offload_id:
            def sync_update_records(offload_id):
                container = Container.objects.get(container_number=container_number)
                related_orders = Order.objects.filter(
                    offload_id__offload_id=offload_id).select_related('offload_id', 'export_unpacking_id')
                if related_orders.exists():
                    for order in related_orders:
                        if not order.export_unpacking_id:
                                # 表为空或该订单无关联记录，创建新记录
                                new_record = ExportUnpackingCabinets.objects.create(
                                    container_number=container,
                                    download_date=warehouse_unpacking_time,
                                    download_num=1  # 首次下载，次数为1
                                )
                                order.export_unpacking_id = new_record
                                order.save()
                        else:
                            order.export_unpacking_id.download_num += 1
                            order.export_unpacking_id.save()

            async_update = sync_to_async(sync_update_records, thread_sensitive=True)
            await async_update(offload_id)
        template, context = await self.warehousing_operation_post(request)
        return template, context

    async def warehousing_operation_get(self, request: HttpRequest):
        context = {
            "warehouse_options": self.warehouse_options,
        }
        return self.template_warehousing_operation, context

    async def warehousing_operation_post(self, request: HttpRequest):
        """
        入库操作-页面展示（增加超时判断与置顶排序）
        """
        current_time = timezone.now()  # 改用带时区的当前时间，避免时区偏差
        future_four_days = current_time + timedelta(days=4)
        warehouse = request.POST.get("warehouse_filter", None)
        ORDER_FILTER_CRITERIA = Q(
            offload_id__offload_required=True,
            offload_id__offload_at__isnull=True,
            cancel_notification=False,
            warehouse__name=warehouse,
            created_at__gte=timezone.make_aware(timezone.datetime(2025, 1, 1))
        ) & Q(
            Q(retrieval_id__temp_t49_available_for_pickup=True) |
            Q(vessel_id__vessel_eta__lte=future_four_days)
        )

        def sync_get_retrieval_and_count():
            # 1. 订单查询：增加“超时阈值”和“是否超时未拆”的注解
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
                .annotate(
                    # 注解1：优先级排序（原有逻辑保留）
                    priority_order=Case(
                        When(unpacking_priority="P1", then=Value(1)),
                        When(unpacking_priority="P2", then=Value(2)),
                        When(unpacking_priority="P3", then=Value(3)),
                        When(unpacking_priority="P4", then=Value(4)),
                        default=Value(5),
                        output_field=IntegerField()
                    ),
                    # 注解2：计算超时阈值（实际提柜时间 + 对应时效）
                    timeout_threshold=Case(
                        # P1/P2/P3：实际提柜时间 +24小时
                        When(
                            unpacking_priority__in=["P1", "P2", "P3"],
                            then=F("retrieval_id__actual_retrieval_timestamp") + timedelta(hours=24)
                        ),
                        # P4：实际提柜时间 +48小时
                        When(
                            unpacking_priority="P4",
                            then=F("retrieval_id__actual_retrieval_timestamp") + timedelta(hours=48)
                        ),
                        # 无实际提柜时间时，阈值设为无穷大（不触发超时）
                        default=Value(timezone.make_aware(datetime.max)),
                        output_field=DateTimeField()
                    ),
                    # 注解3：标记“是否超时未拆”（1=超时未拆，0=正常）
                    is_overtime_unpacked=Case(
                        When(
                            # 条件：当前时间>超时阈值 + 拆柜状态≠已拆（unpacking_status≠1）
                            Q(timeout_threshold__lt=current_time) &
                            ~Q(offload_id__unpacking_status="1"),
                            then=Value(1)
                        ),
                        default=Value(0),
                        output_field=IntegerField()
                    )
                )
                # 订单排序：先按“是否超时未拆”（1在前），再按优先级
                .order_by("-is_overtime_unpacked", "priority_order")
            )

            # 2. Offload查询：关联订单并调整排序（优先显示超时的记录）
            # 修改retrieval查询部分的代码
            retrieval = (
                Offload.objects.prefetch_related(
                    Prefetch(
                        "order",
                        queryset=order_queryset,
                        to_attr="filtered_orders"
                    )
                )
                .only(
                    "arrival_location", "unpacking_status", "id"
                )
                .annotate(
                    status_order=Case(
                        When(unpacking_status=2, then=Value(1)),
                        When(unpacking_status=0, then=Value(2)),
                        When(unpacking_status=1, then=Value(3)),
                        default=Value(4),
                        output_field=IntegerField()
                    ),
                    has_overtime_order=Case(
                        When(
                            id__in=order_queryset.filter(is_overtime_unpacked=1).values_list("offload_id", flat=True),
                            then=Value(1)
                        ),
                        default=Value(0),
                        output_field=IntegerField()
                    ),
                    # 关键修复：直接在主查询中定义订单优先级（不依赖子查询的注解）
                    min_priority_order=Min(
                        Case(
                            When(order__unpacking_priority="P1", then=Value(1)),
                            When(order__unpacking_priority="P2", then=Value(2)),
                            When(order__unpacking_priority="P3", then=Value(3)),
                            When(order__unpacking_priority="P4", then=Value(4)),
                            default=Value(5),
                            output_field=IntegerField()
                        )
                    )
                )
                # 按新定义的字段排序
                .order_by(
                    "-has_overtime_order",
                    "status_order",
                    "min_priority_order"
                )
            )

            # 统计总记录数（原有逻辑保留）
            total_count = 0
            for ret in retrieval:
                total_count += len(ret.filtered_orders)
            return retrieval, total_count

        # 异步调用同步函数（原有逻辑保留）
        retrieval, total_count = await sync_to_async(sync_get_retrieval_and_count)()

        context = {
            'retrieval': retrieval,
            'warehouse_options': self.warehouse_options,
            'warehouse': request.POST.get('warehouse_filter'),
            'total_count': total_count,
            'current_time': current_time  # 传递当前时间到前端（可选，用于前端二次验证）
        }
        return self.template_warehousing_operation, context

    async def warehousing_operation_update(self, request: HttpRequest):
        try:
            offload_id = request.POST.get('offload_id', '').strip()
            arrival_location = request.POST.get('arrival_location', '').strip()
            unpacking_status = request.POST.get('unpacking_status', '').strip()

            # 1. 定义更新Retrieval表的同步函数
            def sync_update_single():
                return Offload.objects.filter(offload_id=offload_id).update(
                    arrival_location=arrival_location,
                    unpacking_status=unpacking_status
                )

            # 2. 定义更新Offload表的同步函数
            def sync_update_single_offload():
                related_orders = Order.objects.filter(
                    offload_id__offload_id=offload_id,
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

            def sync_update_single_offload_unpacking():
                related_orders = Order.objects.filter(
                    offload_id__offload_id=offload_id,
                    offload_id__warehouse_unpacking_time__isnull=True
                ).select_related('offload_id')

                if related_orders.exists():
                    current_time = timezone.now()
                    updated_count = 0
                    for order in related_orders:
                        order.offload_id.warehouse_unpacking_time = current_time
                        order.offload_id.save()
                        updated_count += 1
                    return updated_count  # 返回更新的记录数
                return 0

            # 3. 包装同步函数为异步函数
            async_update = sync_to_async(sync_update_single, thread_sensitive=True)
            # 关键：必须为sync_update_single_offload也创建异步包装
            async_update_offload = sync_to_async(sync_update_single_offload, thread_sensitive=True)
            async_update_offload_unpacking = sync_to_async(sync_update_single_offload_unpacking, thread_sensitive=True)

            # 4. 执行更新操作（通过包装后的异步函数）
            affected_rows = await async_update()

            # 5. 已拆柜状态时，执行Offload更新
            if unpacking_status == "1":
                offload_affected = await async_update_offload()
            # 拆柜中
            elif unpacking_status == "2":
                offload_affected = await async_update_offload_unpacking()

        except Exception as e:
            self.logger.error(f"更新记录{offload_id}时发生错误：{str(e)}", exc_info=True)

        template, context = await self.warehousing_operation_post(request)
        return template, context

    async def warehousing_operation_first_time_download(self, request: HttpRequest):
        warehouse_unpacking_time = request.GET.get("first_time_download")
        template, context = await self.warehousing_operation_first_time_download(request)
        return template, context

    async def handle_counting_pallet_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_counting_pallet, context
    
    async def handle_upcoming_fleet_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_upcoming_fleet, context

    async def handle_bol_post(self, request: HttpRequest) -> HttpResponse:
        #准备参数
        mutable_post = request.POST.copy()
        fleet_number = request.POST.get("fleet_number")
        mutable_post["customerInfo"] = None
        mutable_post["pickupList"] = None

        request.POST = mutable_post
        fm = FleetManagement()

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        
        if fleet.fleet_type == 'LTL':
            return await fm._export_ltl_bol(request)
            
        elif fleet.fleet_type == 'FTL':   
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
        else:
            raise ValueError('暂不支持下载非FTL和LTL以外的BOL')

        

    async def handle_report_issue_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        issue_type = request.POST.get("issue_type")
        issue_description = request.POST.get("issue_description")
        issue = issue_type + ":" + issue_description

        fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        fleet.warehouse_process_status='abnormal'
        fleet.abnormal_reason = issue
        fleet.is_canceled = True
        fleet.status = "Exception"
        fleet.status_description = issue
        await sync_to_async(fleet.save)()

        #更新完车次之后，要把这个约变为异常，展示给异常预约里面
        updated_count = await sync_to_async(
            lambda: Shipment.objects.filter(fleet_number=fleet).update(
                status="Exception",
                status_description=issue,
                fleet_number=None
            ),
            thread_sensitive=True
        )()
        return await self.handle_upcoming_fleet_post(request)

    async def handle_complete_loading_post(
        self, request: HttpRequest, file_path_name
    ) -> tuple[str, dict[str, Any]]:
        """
        回传拆柜数据
        """
        fleet_number = request.POST.get("fleet_number")
        offload_id = request.POST.get("offload_id")
        #上传出库凭证

        receipt_images = request.FILES.getlist("receipt_images")
        uploaded_links = []

        valid_extensions = [".jpg", ".png", ".jpeg"]
        max_size = 5 * 1024 * 1024  # 5MB
        for img in receipt_images:
            if not img:
                continue

            # 校验格式
            ext = os.path.splitext(img.name)[1].lower()
            if ext not in valid_extensions:
                raise ValidationError(f"仅支持JPG/PNG格式图片: {img.name}")

            # 校验大小
            if img.size > max_size:
                raise ValidationError(f"图片大小不能超过5MB: {img.name}")

            # 上传
            conn = self._get_sharepoint_auth()
            link = await self._upload_image_to_sharepoint(conn, img, file_path_name)
            uploaded_links.append(link)

        #更新车次状态
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status="proof_uploaded", shipped_cert_link=uploaded_links)

        return await self.handle_upcoming_fleet_post(request)

    async def handle_complete_loading_post_palletization_list(
        self, request: HttpRequest, file_path_name
    ) -> tuple[str, dict[str, Any]]:
        """
        入库回传拆柜数据
        """
        offload_id = request.POST.get("offload_id")
        offload_note = request.POST.get("offload_note")
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
        link = ""
        if receipt_image:
            conn = self._get_sharepoint_auth()
            link = await self._upload_image_to_sharepoint(conn, receipt_image, file_path_name)

        #更新回传拆柜数据上传时间 添加回传拆柜数据备注
        current_upload_time = timezone.now()
        data = await sync_to_async(
            Offload.objects.filter(offload_id=offload_id).update
        )(uploaded_at=current_upload_time, offload_note=offload_note)

        template, context = await self.warehousing_operation_post(request)
        return template, context

    async def _get_sharepoint_auth(self) -> ClientContext:
        ctx = ClientContext(SP_URL).with_client_certificate(
            SP_TENANT,
            SP_CLIENT_ID,
            SP_THUMBPRINT,
            private_key=SP_PRIVATE_KEY,
            scopes=[SP_SCOPE],
        )
        return ctx

    async def _upload_image_to_sharepoint(self, conn, image, file_path_name) -> None:

        image_name = image.name  # 提取文件名
        file_path = os.path.join(
            SP_DOC_LIB, f"{SYSTEM_FOLDER}/warehouse_operation/{file_path_name}/{APP_ENV}"
        )  # 文档库名称，系统文件夹名称，当前环境
        # 上传到SharePoint
        client = await conn
        sp_folder = client.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(f"{image_name}", image).execute_query()
        # 生成并获取链接
        link = (
            resp.share_link(SharingLinkKind.OrganizationView)
            .execute_query()
            .value.to_json()["sharingLinkInfo"]["Url"]
        )
        return link

    async def handle_record_load_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        pre_load= request.POST.get("pre_load")
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(pre_load=pre_load)

        if updated == 0:
            return ValueError('未查到该车次')
        return await self.handle_upcoming_fleet_post(request)
    
    async def handle_shipped_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")

        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status="shipped")

        if updated == 0:
            return ValueError('未查到该车次')
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
        trailer_number = request.POST.get("trailer_number")
        PRO = request.POST.get("PRO")
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(
            warehouse_process_status="check_in",
            driver_name=driver_name,
            driver_phone=driver_phone,
            trailer_number=trailer_number
        )
        
        if updated == 0:
            return ValueError('未查到该车次')
        if PRO:
            try:
                await sync_to_async(
                    Shipment.objects.filter(fleet_number__fleet_number=fleet_number).update
                )(ARM_PRO=PRO)
            except Exception as e:
                # 可以记录日志
                print(f"更新 PRO 出错: {e}")
        return await self.handle_upcoming_fleet_post(request)

    async def handle_adjust_inventory_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        plt_ids = request.POST.get('plt_ids')
        plt_list = plt_ids.split(',') 
        plt_int_list = [int(id) for id in plt_list]
        actual_pallets = request.POST.get('actual_pallets')
        pallets = await sync_to_async(list)(Pallet.objects.filter(id__in=plt_int_list))
        if len(pallets) > int(actual_pallets):
            print('是减少库存')
        else:
            print('增加库存')
        return await self.handle_counting_pallet_post(request)

    async def handle_counting_pallet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        #查找所有板子      
        total_inventory = await self._get_inventory_pallet(warehouse)

        private_inventory = []
        public_inventory = []
        hold_inventory = []
        express_inventory = []
        
        for plt in total_inventory:
            if plt['delivery_method'] == "卡车派送":
                public_inventory.append(plt)
            elif '暂扣' in plt['delivery_method']:
                hold_inventory.append(plt)
            elif 'UPS' in plt['delivery_method'] or 'FEDEX' in plt['delivery_method']:
                express_inventory.append(plt)

            if plt['delivery_type'] == "other":
                private_inventory.append(plt)
        context = {
            'warehouse_options': self.warehouse_options,
            'warehouse': warehouse,
            'total_inventory': total_inventory,
            'private_inventory': private_inventory,
            'hold_inventory': hold_inventory,
            'public_inventory': public_inventory,
            'express_inventory': express_inventory,
            'summary': {
                'total_count': len(total_inventory),
                'private_count': len(private_inventory),
                'hold_count': len(hold_inventory),
                'public_count': len(public_inventory),
                'express_count': len(express_inventory),
            },
            'summary_detail': {
                    'total_count': {
                        'total_weight': sum(item['weight'] for item in total_inventory),
                        'total_cbm': sum(item['cbm'] for item in total_inventory) ,
                        'total_pallet': sum(item['n_pallet'] for item in total_inventory) ,
                        'has_shipment': sum(1 for item in total_inventory   
                                            if item.get('shipment') not in [None, '', 'None', 'null'])
                        },
                    'private_count': {
                        'total_weight': sum(item['weight'] for item in private_inventory),
                        'total_cbm': sum(item['cbm'] for item in private_inventory) ,
                        'total_pallet': sum(item['n_pallet'] for item in private_inventory) ,
                        'has_shipment': sum(1 for item in private_inventory   
                                            if item.get('shipment') not in [None, '', 'None', 'null']),
                    },
                    'hold_count': {
                        'total_weight': sum(item['weight'] for item in hold_inventory),
                        'total_cbm': sum(item['cbm'] for item in hold_inventory) ,
                        'total_pallet': sum(item['n_pallet'] for item in hold_inventory) ,
                        'has_shipment': sum(1 for item in hold_inventory   
                                            if item.get('shipment') not in [None, '', 'None', 'null']),
                    },
                    'public_count': {
                        'total_weight': sum(item['weight'] for item in public_inventory),
                        'total_cbm': sum(item['cbm'] for item in public_inventory) ,
                        'total_pallet': sum(item['n_pallet'] for item in public_inventory) ,
                        'has_shipment': sum(1 for item in public_inventory   
                                            if item.get('shipment') not in [None, '', 'None', 'null']),
                    },
                    'express_count': {
                        'total_weight': sum(item['weight'] for item in express_inventory),
                        'total_cbm': sum(item['cbm'] for item in express_inventory) ,
                        'total_pallet': sum(item['n_pallet'] for item in express_inventory) ,
                        'has_shipment': sum(1 for item in express_inventory   
                                            if item.get('shipment') not in [None, '', 'None', 'null']),
                    },

            }
        }
        return self.template_counting_pallet, context

    async def _get_inventory_pallet(
        self, warehouse: str, criteria: models.Q | None = None
    ) -> list[Pallet]:
        if criteria:
            criteria &= models.Q(location=warehouse)
            criteria &= models.Q(
                models.Q(shipment_batch_number__isnull=True)
                | models.Q(shipment_batch_number__is_shipped=False)
            )
        else:
            criteria = models.Q(
                models.Q(location=warehouse)
                & models.Q(
                    models.Q(shipment_batch_number__isnull=True)
                    | models.Q(shipment_batch_number__is_shipped=False)
                )
            )
        return await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "shipment_batch_number",
                "container_number__order__customer_name",
            )
            .filter(criteria)
            .annotate(str_id=Cast("id", CharField()))
            .values(
                "destination",
                "delivery_method",
                "delivery_type",
                "shipping_mark",
                "fba_id",
                "ref_id",
                "note",
                "PO_ID",
                "address",
                "zipcode",
                "location",
                "slot",
                "container_number__order__offload_id__offload_at",
                customer_name=F("container_number__order__customer_name__zem_name"),
                container=F("container_number__container_number"),
                shipment=F("shipment_batch_number__shipment_batch_number"),
                appointment_time=F("shipment_batch_number__shipment_appointment"),
                
            )
            .annotate(
                shipping_marks=StringAgg("shipping_mark", delimiter=",", distinct=True, ordering="shipping_mark"),
                fba_ids=StringAgg("fba_id", delimiter=",", distinct=True, ordering="fba_id"),
                ref_ids=StringAgg("ref_id", delimiter=",", distinct=True, ordering="ref_id"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                weight=Sum("weight_lbs", output_field=FloatField()),
                n_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-container_number__order__offload_id__offload_at")
        )
    
    async def handle_upcoming_fleet_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        # 获取未来三天的时间范围
        today = timezone.now().date()
        three_days_later = today + timedelta(days=3)
        one_week_ago = today - timedelta(days=7) 

        criteria1 = models.Q(appointment_datetime__date__range=[today, three_days_later])
        criteria2 = models.Q(
            appointment_datetime__date__range=[one_week_ago, today],
        )
        warehouse_condition = models.Q(origin=warehouse)
        query_conditions = (criteria1 | criteria2) & warehouse_condition
        fleets = await sync_to_async(list)(
            Fleet.objects.filter(query_conditions)
            .annotate(
                shipped_order=Case(
                    When(warehouse_process_status='shipped', then=Value(1)),
                    default=Value(0),
                    output_field=IntegerField()
                )
            )
            .order_by('appointment_datetime', 'shipped_order') 
            .prefetch_related(
                Prefetch(
                    'shipment',
                    queryset=Shipment.objects.only('ARM_PRO','is_print_label').prefetch_related(
                        'pallet',
                        'packinglist'
                    )
                )
            )
        )

        fleet_data = []
        day_stats = {
            0: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            1: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            2: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            3: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0}
        }

        type_stats = {
            'all': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            'FTL': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            'LTL': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            '客户自提': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            '外配': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            '快递': {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0}
        }
        for fleet in fleets:
            pls_details = await sync_to_async(
                lambda: list(
                    PackingList.objects.filter(
                        shipment_batch_number__fleet_number=fleet,
                        container_number__order__offload_id__offload_at__isnull=True
                    ).select_related('container_number').values(
                        'container_number__container_number',
                        'shipping_mark',
                        'destination',
                        'cbm',
                        'pcs'
                    )
                )
            )() 
            pls_cbm = sum(item['cbm'] for item in pls_details) if pls_details else 0
            pls_pcs = sum(item['pcs'] for item in pls_details) if pls_details else 0

            plt_details = await sync_to_async(
                lambda: list(
                    Pallet.objects.filter(
                        shipment_batch_number__fleet_number=fleet,
                        container_number__order__offload_id__offload_at__isnull=False
                    ).select_related('container_number').values(
                        'container_number__container_number',
                        'shipping_mark',
                        'destination',
                        'cbm',
                        'pcs'
                    )
                )
            )()               
            plts_cbm = sum(item['cbm'] for item in plt_details) if plt_details else 0
            plts_pcs = sum(item['pcs'] for item in plt_details) if plt_details else 0
            plts_count = len(plt_details)
            
            total_cbm = pls_cbm + plts_cbm
            try:
                total_pcs = pls_pcs + plts_pcs
            except TypeError as e:
                total_pcs = 0
            total_pallets = round(plts_count + round(pls_cbm / 1.8, 2))

            is_estimated = plts_count == 0 and total_pallets > 0
            days_diff = (fleet.appointment_datetime.date() - today).days
            all_details = pls_details + plt_details
            details = {}
            fleet_type = fleet.fleet_type
            if fleet_type == 'LTL':
                # 获取container_number和shipping_mark信息（去重）
                seen = set()
                details['柜号'] = []
                details['唛头'] = []
                for item in all_details:
                    container_num = item.get('container_number__container_number')
                    shipping_mark = item.get('shipping_mark')
                    if container_num and shipping_mark:
                        key = f"{container_num}-{shipping_mark}"
                        if key not in seen:
                            details['柜号'].append(container_num)
                            details['唛头'].append(shipping_mark)
                            seen.add(key)                         
            elif fleet_type == '外配':
                # 获取destination信息（去重）
                details['仓点'] = []
                destinations_set = set()
                for item in all_details:
                    destination = item.get('destination')
                    if destination and destination not in destinations_set:
                        details['仓点'].append(destination)
                        destinations_set.add(destination)                
            elif fleet_type == '快递':
                # 获取container_number信息（去重）
                details['柜号'] = []
                container_set = set()
                for item in all_details:
                    container_num = item.get('container_number__container_number')
                    if container_num and container_num not in container_set:
                        details['柜号'].append(container_num)
                        container_set.add(container_num)

            display_day = days_diff
            arm_pro_combined = None
            is_print_label_combined = None
            if fleet_type != 'FTL':
                arm_pro_list = [
                    str(getattr(shipment, 'ARM_PRO', ''))
                    for shipment in fleet.shipment.all()
                    if getattr(shipment, 'ARM_PRO', None)
                ]
                arm_pro_combined = '|'.join(arm_pro_list)

                is_print_label_list = [
                    str(getattr(shipment, 'is_print_label', ''))
                    for shipment in fleet.shipment.all()
                    if getattr(shipment, 'is_print_label', None) is not None
                ]

                # 如果所有值都一样，就只显示一个；否则用 | 连接所有值
                if len(set(is_print_label_list)) == 1:
                    is_print_label_combined = is_print_label_list[0]  # 所有值都一样，只取第一个
                else:
                    is_print_label_combined = '|'.join(is_print_label_list)
            fleet_item = {
                'fleet_number': fleet.fleet_number,
                'details': details,
                'warehouse_process_status': fleet.warehouse_process_status,
                'pickup_number': fleet.pickup_number,
                'fleet_type': fleet.fleet_type,
                'appointment_datetime': fleet.appointment_datetime,
                'driver_name': fleet.driver_name,
                'driver_phone': fleet.driver_phone,
                'trailer_number': fleet.trailer_number,
                'pre_load': fleet.pre_load,
                'carrier': fleet.carrier,
                'pallets': total_pallets,
                'pcs': total_pcs,
                'is_estimated': is_estimated,
                'days_diff': days_diff,
                'display_day': 0 if days_diff < 0 else days_diff, 
                'abnormal_reason': fleet.abnormal_reason,
                'PRO': arm_pro_combined,
                'is_print_label':is_print_label_combined,
            }
            fleet_data.append(fleet_item)

            #按类型，计算总数量什么的
            if fleet_type in type_stats:
                type_stats[fleet_type]['fleets'].append(fleet_item)
                type_stats[fleet_type]['total_pallets'] += total_pallets
                type_stats[fleet_type]['total_cbm'] += total_cbm
                if fleet.warehouse_process_status == 'shipped':
                    type_stats[fleet_type]['completed_count'] += 1
                elif fleet.warehouse_process_status == 'abnormal':
                    type_stats[fleet_type]['abnormal_count'] += 1
            
            # 同时添加到all统计
            type_stats['all']['fleets'].append(fleet_item)
            type_stats['all']['total_pallets'] += total_pallets
            type_stats['all']['total_cbm'] += total_cbm
            if fleet.warehouse_process_status == 'shipped':
                type_stats['all']['completed_count'] += 1
            elif fleet.warehouse_process_status == 'abnormal':
                type_stats['all']['abnormal_count'] += 1

            if days_diff < 0:
                day_to_count = 3  #过去一周的
            elif 0 <= days_diff <= 2:
                day_to_count = days_diff  

            if day_to_count in day_stats:
                day_stats[day_to_count]['fleets'].append(fleet_item)
                day_stats[day_to_count]['total_pallets'] += total_pallets
                day_stats[day_to_count]['total_cbm'] += total_cbm
                if fleet.warehouse_process_status == 'shipped':
                    day_stats[day_to_count]['completed_count'] += 1
                elif fleet.warehouse_process_status == 'abnormal':
                    day_stats[day_to_count]['abnormal_count'] += 1
        for day in [0, 1, 2, 3]:
            total_fleets = len(day_stats[day]['fleets'])
            completed_count = day_stats[day]['completed_count']
            abnormal_count = day_stats[day]['abnormal_count']
            normal_count = total_fleets - abnormal_count

            if normal_count > 0:
                completion_rate = round((completed_count / normal_count) * 100)
            else:
                completion_rate = 0

            day_stats[day]['completion_rate'] = completion_rate
            day_stats[day]['normal_count'] = normal_count

        for stats in type_stats.values():
            total_fleets = len(stats['fleets'])
            completed_count = stats['completed_count']
            abnormal_count = stats['abnormal_count']
            normal_count = total_fleets - abnormal_count
            
            if normal_count > 0:
                completion_rate = round((completed_count / normal_count) * 100)
            else:
                completion_rate = 0
                
            stats['completion_rate'] = completion_rate
            stats['normal_count'] = normal_count
            stats['total_fleets'] = total_fleets

        fleet_data.sort(key=lambda x: x['days_diff'])
        shipment_type_filter = request.POST.get("shipment_type_filter") or "all"
        
        context = {
            'warehouse_options': self.warehouse_options,
            'fleets': fleet_data,
            'warehouse': warehouse,
            'shipment_type_filter': shipment_type_filter,
            'type_stats': type_stats,
            'summary': {
                'total_fleets': len(fleet_data),
                'total_pallets': sum(f['pallets'] for f in fleet_data),
                'total_pcs': sum(f['pcs'] for f in fleet_data),
                'completed_count': len([f for f in fleet_data if f['warehouse_process_status'] == 'shipped']),
                'abnormal_count': len([f for f in fleet_data if f['warehouse_process_status'] == 'abnormal']),
                'today_count': len(day_stats[0]['fleets']),
                'tomorrow_count': len(day_stats[1]['fleets']),
                'day_after_count': len(day_stats[2]['fleets']),
                'past_count': len(day_stats[3]['fleets']),
                'day_stats': {
                    0: {
                        'total_fleets': len(day_stats[0]['fleets']),
                        'total_pallets': day_stats[0]['total_pallets'],
                        'total_cbm': round(day_stats[0]['total_cbm'], 2),
                        'completed_count': day_stats[0]['completed_count'],
                        'abnormal_count': day_stats[0]['abnormal_count'],
                        'normal_count': day_stats[0]['normal_count'],
                        'completion_rate': day_stats[0]['completion_rate']
                    },
                    1: {
                        'total_fleets': len(day_stats[1]['fleets']),
                        'total_pallets': day_stats[1]['total_pallets'],
                        'total_cbm': round(day_stats[1]['total_cbm'], 2),
                        'completed_count': day_stats[1]['completed_count'],
                        'abnormal_count': day_stats[1]['abnormal_count'],
                        'normal_count': day_stats[1]['normal_count'],
                        'completion_rate': day_stats[1]['completion_rate']
                    },
                    2: {
                        'total_fleets': len(day_stats[2]['fleets']),
                        'total_pallets': day_stats[2]['total_pallets'],
                        'total_cbm': round(day_stats[2]['total_cbm'], 2),
                        'completed_count': day_stats[2]['completed_count'],
                        'abnormal_count': day_stats[2]['abnormal_count'],
                        'normal_count': day_stats[2]['normal_count'],
                        'completion_rate': day_stats[2]['completion_rate']
                    },
                    3: {
                        'total_fleets': len(day_stats[3]['fleets']),
                        'total_pallets': day_stats[3]['total_pallets'],
                        'total_cbm': round(day_stats[3]['total_cbm'], 2),
                        'completed_count': day_stats[3]['completed_count'],
                        'abnormal_count': day_stats[3]['abnormal_count'],
                        'normal_count': day_stats[3]['normal_count'],
                        'completion_rate': day_stats[3]['completion_rate']
                    },
                }
            }
        }
        day_type_stats = {0: {}, 1: {}, 2: {}, 3: {}}
        for day in [0, 1, 2, 3]:
            fleets = day_stats[day]['fleets']
            grouped = {}
            for f in fleets:
                t = f['fleet_type']
                if t not in grouped:
                    grouped[t] = {
                        'total_fleets': 0,
                        'total_pallets': 0,
                        'total_cbm': 0,
                        'completed_count': 0,
                        'abnormal_count': 0
                    }
                grouped[t]['total_fleets'] += 1
                grouped[t]['total_pallets'] += f['pallets']
                grouped[t]['total_cbm'] += f['pcs']
                if f['warehouse_process_status'] == 'shipped':
                    grouped[t]['completed_count'] += 1
                elif f['warehouse_process_status'] == 'abnormal':
                    grouped[t]['abnormal_count'] += 1

            # 计算完成率
            for g in grouped.values():
                normal = g['total_fleets'] - g['abnormal_count']
                g['completion_rate'] = round((g['completed_count'] / normal) * 100) if normal > 0 else 0
            day_type_stats[day] = grouped
        context["day_type_stats"] = day_type_stats
        return self.template_upcoming_fleet, context
