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
from django.db.models import Prefetch, Q
from django.core.exceptions import ValidationError
from django.http import HttpRequest, HttpResponse, HttpResponsePermanentRedirect, HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.views import View
from django.db.models import Sum, Count, F, FloatField, IntegerField, CharField, Value
from django.db.models.functions import Cast
from django.db.models.functions import Coalesce
from office365.sharepoint.sharing.links.kind import SharingLinkKind
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
        
        try:
            fleet = await sync_to_async(Fleet.objects.get)(fleet_number=fleet_number)
        except Exception as e:
            raise ValueError('该约尚未排车')
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

    async def handle_report_issue_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        fleet_number = request.POST.get("fleet_number")
        issue_type = request.POST.get("issue_type")
        issue_description = request.POST.get("issue_description")
        issue = issue_type + issue_description
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status='abnormal', abnormal_reason=issue)
        return await self.handle_upcoming_fleet_post(request)

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
        link = ""
        if receipt_image:
            conn = self._get_sharepoint_auth()
            link = self._upload_image_to_sharepoint(conn, receipt_image)
            
        #更新车次状态
        updated = await sync_to_async(
            Fleet.objects.filter(fleet_number=fleet_number).update
        )(warehouse_process_status="shipped", shipped_cert_link=link)
        
        return await self.handle_upcoming_fleet_post(request)

    async def _get_sharepoint_auth(self) -> ClientContext:
        ctx = ClientContext(SP_URL).with_client_certificate(
            SP_TENANT,
            SP_CLIENT_ID,
            SP_THUMBPRINT,
            private_key=SP_PRIVATE_KEY,
            scopes=[SP_SCOPE],
        )
        return ctx
    
    def _upload_image_to_sharepoint(self, conn, image) -> None:

        image_name = image.name  # 提取文件名
        file_path = os.path.join(
            SP_DOC_LIB, f"{SYSTEM_FOLDER}/warehouse_operation/{APP_ENV}"
        )  # 文档库名称，系统文件夹名称，当前环境
        # 上传到SharePoint
        sp_folder = conn.web.get_folder_by_server_relative_url(file_path)
        resp = sp_folder.upload_file(f"{image_name}", image).execute_query()
        # 生成并获取链接
        link = (
            resp.share_link(SharingLinkKind.OrganizationView)
            .execute_query()
            .value.to_json()["sharingLinkInfo"]["Url"]
        )
        return link
    
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
        return await self.handle_upcoming_fleet_post(request)

    async def handle_adjust_inventory_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        print('收到的信息',request.POST)
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
        day_stats = {
            0: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            1: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0},
            2: {'fleets': [], 'total_pallets': 0, 'total_cbm': 0, 'completed_count': 0, 'abnormal_count': 0}
        }
        
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
            
            fleet_item = {
                'fleet_number': fleet.fleet_number,
                'warehouse_process_status': fleet.warehouse_process_status,
                'pickup_number': fleet.pickup_number,
                'appointment_datetime': fleet.appointment_datetime,
                'driver_name': fleet.driver_name,
                'driver_phone': fleet.driver_phone,
                'trailer_number': fleet.trailer_number,
                'pallets': total_pallets,
                'cbm': round(total_cbm, 2),
                'is_estimated': is_estimated,
                'days_diff': days_diff,
                'abnormal_reason': fleet.abnormal_reason,
            }
            
            fleet_data.append(fleet_item)
            if 0 <= days_diff <= 2:
                day_stats[days_diff]['fleets'].append(fleet_item)
                day_stats[days_diff]['total_pallets'] += total_pallets
                day_stats[days_diff]['total_cbm'] += total_cbm
                if fleet.warehouse_process_status == 'shipped':
                    day_stats[days_diff]['completed_count'] += 1
                elif fleet.warehouse_process_status == 'abnormal':
                    day_stats[days_diff]['abnormal_count'] += 1
        
        for day in [0, 1, 2]:
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

        context = {
            'warehouse_options': self.warehouse_options,
            'fleets': fleet_data,
            'warehouse': warehouse,
            'summary': {
                'total_fleets': len(fleet_data),
                'total_pallets': sum(f['pallets'] for f in fleet_data),
                'total_cbm': sum(f['cbm'] for f in fleet_data),
                'completed_count': len([f for f in fleet_data if f['warehouse_process_status'] == 'shipped']),
                'abnormal_count': len([f for f in fleet_data if f['warehouse_process_status'] == 'abnormal']),
                'today_count': len(day_stats[0]['fleets']),
                'tomorrow_count': len(day_stats[1]['fleets']),
                'day_after_count': len(day_stats[2]['fleets']),
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
                    }
                }
            }
        }
        return self.template_upcoming_fleet, context
