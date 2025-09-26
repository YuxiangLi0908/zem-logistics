

from datetime import datetime, timedelta
from typing import Any

from django.contrib.auth.decorators import login_required
from django.db import models
from django.forms import formset_factory
from django.forms.models import model_to_dict
from asgiref.sync import sync_to_async
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Concat
from django.contrib.postgres.aggregates import StringAgg
from django.views import View

from warehouse.forms.clearance_form import ClearanceSelectForm
from warehouse.forms.container_form import ContainerForm
from warehouse.forms.offload_form import OffloadForm
from warehouse.forms.order_form import OrderForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.forms.retrieval_form import RetrievalForm, RetrievalSelectForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.clearance import Clearance
from warehouse.models.container import Container
from warehouse.models.customer import Customer
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.pallet import Pallet
from warehouse.models.packing_list import PackingList
from warehouse.utils.constants import ORDER_TYPES, PACKING_LIST_TEMP_COL_MAPPING


class Home(View):
    template_main = "home.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step", None)
        if step == "download_template":
            return await self.handle_download_pl_template_post(request)
        else:
            return render(request, self.template_main)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "trajectory_query":
            template, context = await self.handle_trajectory_query_post(request)
            return render(request, template, context)
        else:
            raise ValueError(f"{request.POST}")

    async def handle_trajectory_query_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        query_params = {
            'container_number': request.POST.get("container_number", "").strip() or None,
            'shipment_batch_number': request.POST.get("shipment_batch_number", "").strip() or None,
            'appointment_id': request.POST.get("appointment_id", "").strip() or None,
            'destination': request.POST.get("act_destination", "").strip() or None,
            'shipping_marks': request.POST.get("shipping_marks", "").strip() or None,
            'fba_ids': request.POST.get("fba_ids", "").strip() or None,
            'ref_ids': request.POST.get("ref_ids", "").strip() or None,
        }
        context = {'query_params': query_params}
        pl_criteria = models.Q()
        plt_criteria = models.Q()
        if bool(query_params['shipping_marks'] or query_params['fba_ids'] or query_params['ref_ids'] or query_params['destination']):
            #这个就只展示柜子的基本信息，以表格的形式
            if query_params['shipping_marks']:
                pl_criteria &= models.Q(
                    shipping_mark__contains=query_params['shipping_marks']
                )
                plt_criteria &= models.Q(
                    shipping_mark__contains=query_params['shipping_marks']
                )
            elif query_params['fba_ids']:
                pl_criteria &= models.Q(
                    fba_id__contains=query_params['fba_ids']
                )
                plt_criteria &= models.Q(
                    fba_id__contains=query_params['fba_ids']
                )
            elif query_params['ref_ids']:
                pl_criteria &= models.Q(
                    ref_id__contains=query_params['ref_ids']
                )
                plt_criteria &= models.Q(
                    ref_id__contains=query_params['ref_ids']
                )
            elif query_params['destination']:
                pl_criteria &= models.Q(
                    destination=query_params['destination']
                )
                plt_criteria = models.Q(
                    destination=query_params['destination']
                )
            elif query_params['container_number']:
                pl_criteria &= models.Q(
                    container_number__container_number=query_params['container_number']
                )
                plt_criteria = models.Q(
                    container_number__container_number=query_params['container_number']
                )
            temp = await self._get_post_port_data(pl_criteria,plt_criteria) 
            context['post_port_table'] = temp['warehouses']
        else:
            #如果是只查柜号，才展示港前信息
            if bool(query_params['container_number']):
                context['pre_port_data'] = await self._get_pre_port_data(query_params)
                pl_criteria &= models.Q(container_number__container_number=query_params['container_number'])
                plt_criteria &= models.Q(container_number__container_number=query_params['container_number'])
            elif bool(query_params['shipment_batch_number'] or query_params['appointment_id'] ):
                if query_params['shipment_batch_number']:
                    pl_criteria &= models.Q(shipment_batch_number__shipment_batch_number=query_params['shipment_batch_number'])
                    plt_criteria &= models.Q(shipment_batch_number__shipment_batch_number=query_params['shipment_batch_number'])
                elif query_params['appointment_id']:
                    pl_criteria &= models.Q(shipment_batch_number__appointment_id=query_params['appointment_id'])
                    plt_criteria &= models.Q(shipment_batch_number__appointment_id=query_params['appointment_id'])
            #查后端信息
            temp = await self._get_post_port_data(pl_criteria,plt_criteria) 
            context['post_port_data'] = temp['warehouses']
            context['status_summary'] = temp['status_summary']
        return self.template_main, context
    
    async def _get_pre_port_data(self, query_params: dict) -> list[dict]:
        criteria = models.Q()
        if query_params['container_number']:
            criteria &= models.Q(container_number__container_number=query_params['container_number'])
        elif query_params['destination']:
            criteria &= models.Q(packinglist__destination=query_params['destination'])  # 直接从Order关联PackingList
        elif query_params['shipping_marks']:
            criteria &= models.Q(packinglist__shipping_mark__icontains=query_params['shipping_marks'])
        elif query_params['fba_ids']:
            criteria &= models.Q(packinglist__fba_id__icontains=query_params['fba_ids'])
        elif query_params['ref_ids']:
            criteria &= models.Q(packinglist__ref_id__icontains=query_params['ref_ids'])

        containers = await sync_to_async(list)(
            Order.objects.prefetch_related(
                'container_number', 
                'customer_name',
                'warehouse',
                'vessel_id',
                'retrieval_id',
                'offload_id',
                'packinglist',
            )
            .filter(criteria)
            .distinct()
            .values(
                'container_number__container_number',
                'customer_name__zem_name',
                'warehouse__name',
                'created_at',
                'vessel_id__vessel_eta',
                'retrieval_id__target_retrieval_timestamp',
                'retrieval_id__actual_retrieval_timestamp',
                'retrieval_id__arrive_at',
                'offload_id__offload_at',
            )
        )
        pre_port_data = []
        for container in containers:
            pre_port_data.append({
                'container_number': container['container_number__container_number'],
                'customer_name': container['customer_name__zem_name'],
                'warehouse_name': container['warehouse__name'],
                'order_created': container['created_at'],
                'port_arrival': container['vessel_id__vessel_eta'],
                'target_retrieval': container['retrieval_id__target_retrieval_timestamp'],
                'actual_retrieval': container['retrieval_id__actual_retrieval_timestamp'],
                'warehouse_arrival': container['retrieval_id__arrive_at'],
                'unloading_completed': container['offload_id__offload_at'],
                'status': self._get_container_status(container)
            })
        
        return pre_port_data
    
    def _get_container_status(self, container: dict) -> str:
        #规范柜子状态
        if container['offload_id__offload_at']:
            return 'completed'
        elif container['retrieval_id__arrive_at']:
            return 'at_warehouse'
        elif container['retrieval_id__actual_retrieval_timestamp']:
            return 'retrieved'
        elif container['vessel_id__vessel_eta']:
            return 'at_port'
        else:
            return 'ordered'
    
    def _get_warehouse_status(self, item: dict) -> str:
        #规范仓点状态
        if item.get('shipment_batch_number__pod_link'):
            return 'completed'
        elif item.get('shipment_batch_number__arrived_at'):
            return 'delivered'
        elif item.get('shipment_batch_number__shipped_at'):
            return 'in_transit'
        elif item.get('shipment_batch_number__shipment_appointment'):
            return 'scheduled'
        else:
            return 'pending'
    
    async def _get_post_port_data(self, pl_criteria, plt_criteria) -> list[dict]:
        pl_criteria = models.Q()
        plt_criteria = models.Q()
        #根据界面输入的条件，判断查pl和plt的查询条件
        pl_criteria &= models.Q(container_number__order__offload_id__offload_at__isnull=True)
        plt_criteria &= models.Q(container_number__order__offload_id__offload_at__isnull=False)
        
        packing_data = await self._get_packing_list(pl_criteria, plt_criteria)
        
        status_summary = {
            'pending': 0,
            'scheduled': 0, 
            'in_transit': 0,
            'delivered': 0,
            'completed': 0
        }
        for warehouse in packing_data:
            # 确定状态
            warehouse['status'] = self._get_warehouse_status(warehouse)
            warehouse['status_display'] = self._get_status_display(warehouse['status'])
            
            # 计算板数和类型
            if warehouse.get("label") == "ACT":
                warehouse['pallet_count'] = warehouse.get('total_n_pallet_act', 0)
                warehouse['pallet_type'] = 'ACT'
            else:
                pallet_est = warehouse.get('total_n_pallet_est', 0)
                if pallet_est < 1:
                    warehouse['pallet_count'] = 1
                elif pallet_est % 1 >= 0.45:
                    warehouse['pallet_count'] = int(pallet_est // 1 + 1)
                else:
                    warehouse['pallet_count'] = int(pallet_est // 1)
                warehouse['pallet_type'] = 'EST'
            
            # 统计状态
            status_summary[warehouse['status']] += 1
        
        return {
            'warehouses': packing_data,
            'status_summary': status_summary
        }
        
    def _get_status_display(self, status: str) -> str:
        status_map = {
            'pending': '待预约',
            'scheduled': '已预约', 
            'in_transit': '运输中',
            'delivered': '已送达',
            'completed': '已完成'
        }
        return status_map.get(status, status)

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
                schedule_status=Case(
                    When(
                        Q(
                            container_number__order__offload_id__offload_at__lte=datetime.now().date()
                            + timedelta(days=-7)
                        ),
                        then=Value("past_due"),
                    ),
                    default=Value("on_time"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__order__customer_name__zem_name",
                "container_number__order__warehouse__name",
                "destination",
                "address",
                "delivery_method",
                "container_number__order__offload_id__offload_at",
                "container_number__order__retrieval_id__target_retrieval_timestamp",
                "container_number__order__retrieval_id__actual_retrieval_timestamp",
                "container_number__order__vessel_id__vessel_eta",
                "schedule_status",
                "abnormal_palletization",
                "po_expired",
                "shipment_batch_number__shipment_batch_number",
                "shipment_batch_number__appointment_id",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__shipped_at",
                "shipment_batch_number__arrived_at",
                "shipment_batch_number__pod_link",
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
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet_act=Count("pallet_id", distinct=True),
                label=Value("ACT"),
            )
            .order_by("container_number__order__offload_id__offload_at")
        )
        data += pal_list
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "pallet",
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
                    schedule_status=Case(
                        When(
                            Q(
                                container_number__order__offload_id__offload_at__lte=datetime.now().date()
                                + timedelta(days=-7)
                            ),
                            then=Value("past_due"),
                        ),
                        default=Value("on_time"),
                        output_field=CharField(),
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                )
                .values(
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "container_number__order__warehouse__name",
                    "destination",
                    "address",
                    "custom_delivery_method",
                    "container_number__order__offload_id__offload_at",
                    "container_number__order__retrieval_id__target_retrieval_timestamp",
                    "container_number__order__retrieval_id__actual_retrieval_timestamp",
                    "container_number__order__vessel_id__vessel_eta",
                    "schedule_status",
                    "pcs",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__appointment_id",
                    "shipment_batch_number__shipment_appointment",
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
                    total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
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
            )
            data += pl_list
        return data
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
