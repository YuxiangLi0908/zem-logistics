import re
from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from asgiref.sync import sync_to_async,async_to_sync
from django.db.models import Sum, FloatField, IntegerField, Count
from django.contrib.postgres.aggregates import StringAgg
from django.db.models import Case, Value, CharField, F, Sum, Max, When, Count, Q
from django.db.models.functions import Concat, Cast
from django.db.models import Subquery, OuterRef, Case, When, Value, CharField,Exists
from warehouse.models.po_check_eta import PoCheckEtaSeven
from django.core.exceptions import MultipleObjectsReturned

from warehouse.models.shipment import Shipment
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.order import Order
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_bol,export_report

@method_decorator(login_required(login_url='login'), name='dispatch')
class BOL(View):
    template_main = "bol.html"
    template_summary = "summary_table.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA"}

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "select":
            return render(request, self.template_main, self.handle_select_get(request))
        elif step == "summary_table":
            context ={"area_options":self.area_options}
            return render(request, self.template_summary, context)
        else:
            current_date = datetime.now().date()
            start_date = current_date + timedelta(days=-30)
            end_date = current_date + timedelta(days=30)
            context = {
                "warehouse_form": ZemWarehouseForm(),
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
            }
        return render(request, self.template_main, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "search":
            return render(request, self.template_main, self.handle_search_post(request))
        elif step == "export_bol":
            return export_bol(self.handle_bol_post(request))
        elif step == "summary_warehouse":
            context = async_to_sync(self.summary_table)(request)
            return render(request, self.template_summary,context)
        elif step == "export_report":
            return export_report(request)
        raise ValueError(f"{request.POST}")
    
    async def summary_table(self, request: HttpRequest) -> dict[str, Any]:
        if request.POST.get("area"):
            area = request.POST.get("area")
            criteria = models.Q(
                (models.Q(container_number__order__order_type="转运") | models.Q(container_number__order__order_type="转运组合")),
                container_number__order__retrieval_id__retrieval_destination_area=area,
                container_number__order__packing_list_updloaded=True,
                shipment_batch_number__isnull=True,
                container_number__order__created_at__gte='2024-09-01',
            ) & (
                # TODOs: 考虑按照安排提柜时间筛选
                models.Q(container_number__order__vessel_id__vessel_eta__lte=datetime.now().date() + timedelta(days=7)) |
                models.Q(container_number__order__eta__lte=datetime.now().date() + timedelta(days=7))
            )
            pl_criteria = criteria & models.Q(container_number__order__offload_id__offload_at__isnull=True)
            plt_criteria = criteria & models.Q(container_number__order__offload_id__offload_at__isnull=False)
            packing_list = await self._get_packing_list(pl_criteria, plt_criteria)
            cbm_act, cbm_est, pallet_act, pallet_est = 0, 0, 0, 0
            await sync_to_async(print)('长度为',len(packing_list))
            for pl in packing_list:
                if pl.get("label") == "ACT":
                    cbm_act += pl.get("total_cbm")
                    pallet_act += pl.get("total_n_pallet_act")
                else:
                    cbm_est += pl.get("total_cbm")
                    if pl.get("total_n_pallet_est") < 1:
                        pallet_est += 1
                    elif pl.get("total_n_pallet_est")%1 >= 0.45:
                        pallet_est += int(pl.get("total_n_pallet_est") // 1 + 1)
                    else:
                        pallet_est += int(pl.get("total_n_pallet_est") // 1)
            
            context = {
                "packing_list":packing_list,
                "selected_area":area,
                "area_options":self.area_options      
            }
            
            return context
        
    async def _get_packing_list(
        self, 
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        data = [] 
        pal_list = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number",
                "container_number__order__offload_id", "container_number__order__customer_name", "container_number__order__retrieval_id"
            ).filter(
                plt_criteria
            ).annotate(
                schedule_status = Case(
                    When(Q(container_number__order__offload_id__offload_at__lte = datetime.now().date() + timedelta(days=-7)), then = Value("past_due")),
                    default = Value("on_time"),
                    output_field = CharField()
                ),
                str_id = Cast("id", CharField()),
            ).values(
                'container_number__container_number',
                'container_number__order__customer_name__zem_name',
                'destination',
                'address',
                'delivery_method',
                'container_number__order__offload_id__offload_at',
                'schedule_status',
                'abnormal_palletization',
                'po_expired',
                "fba_id",
                "ref_id",
                "shipping_mark",
                warehouse = F('container_number__order__retrieval_id__retrieval_destination_precise'),
            ).annotate(
                custom_delivery_method = F('delivery_method'),
                fba_ids = F('fba_id'),
                ref_ids = F('ref_id'),
                shipping_marks = F('shipping_mark'),
                plt_ids = StringAgg("str_id", delimiter = ",", distinct=True, ordering = "str_id"),
                total_pcs = Sum("pcs", output_field = IntegerField()),
                total_cbm = Sum("cbm", output_field = FloatField()),
                total_weight_lbs = Sum("weight_lbs", output_field = FloatField()),
                total_n_pallet_act = Count("pallet_id", distinct=True),
                label = Value("ACT"),
            ).order_by('container_number__order__offload_id__offload_at')
        )
        # for p in pal_list:
        #     try:
        #         po = await sync_to_async(PoCheckEtaSeven.objects.get)(
        #             shipping_mark = p['shipping_marks'],
        #             fba_id = p['fba_ids'],
        #             ref_id = p['ref_ids']
        #         )
        #         if not po.last_eta_checktime and not po.last_retrieval_checktime:
        #             p['check'] = '未校验'
        #         elif po.last_retrieval_checktime and not po.last_retrieval_status:
        #             p['check'] = '失效'
        #         elif not po.last_retrieval_checktime and po.last_eta_checktime and not po.last_eta_status:
        #             p['check'] = '失效'
        #         else:
        #             p['check'] = '有效'
        #     except PoCheckEtaSeven.DoesNotExist:
        #         # 如果没有找到对应的PoCheckEtaSeven记录，可以根据需求设置默认状态
        #         p['check'] = '未关联'
        #     except MultipleObjectsReturned:
        #             p['check'] = "不对应"
        data += pal_list
        
        if pl_criteria:
            await sync_to_async(print)("有这个条件")
            pl_list =  await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number", "container_number__order", "container_number__order__warehouse", "shipment_batch_number",
                    "container_number__order__offload_id", "container_number__order__customer_name", "pallet", "container_number__order__retrieval_id"
                ).filter(pl_criteria).annotate(
                    custom_delivery_method=Case(
                        When(Q(delivery_method='暂扣留仓(HOLD)') | Q(delivery_method='暂扣留仓'), then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                        default=F('delivery_method'),
                        output_field=CharField()
                    ),
                    schedule_status=Case(
                        When(Q(container_number__order__offload_id__offload_at__lte=datetime.now().date() + timedelta(days=-7)), then=Value("past_due")),
                        default=Value("on_time"),
                        output_field=CharField()
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField())
                ).values(
                    'container_number__container_number',
                    'container_number__order__customer_name__zem_name',
                    'destination',
                    'address',
                    'custom_delivery_method',
                    'container_number__order__offload_id__offload_at',
                    'schedule_status',
                    warehouse=F('container_number__order__retrieval_id__retrieval_destination_precise'),
                ).annotate(
                    fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
                    ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
                    shipping_marks=StringAgg("str_shipping_mark", delimiter=",", distinct=True, ordering="str_shipping_mark"),
                    ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField()
                        )
                    ),
                    total_cbm=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("cbm")),
                            default=F("pallet__cbm"),
                            output_field=FloatField()
                        )
                    ),
                    total_weight_lbs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("total_weight_lbs")),
                            default=F("pallet__weight_lbs"),
                            output_field=FloatField()
                        )
                    ),
                    total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField())/2,
                    label=Max(
                        Case(
                            When(pallet__isnull=True, then=Value("EST")),
                            default=Value("ACT"),
                            output_field=CharField()
                        )
                    ),
                ).distinct()
            )
            # for p in pl_list:
            #     try:
            #         pl = await sync_to_async(PoCheckEtaSeven.objects.get)(
            #             shipping_mark = p['shipping_marks'],
            #             fba_id = p['fba_ids'],
            #             ref_id = p['ref_ids']
            #         )
            #         if not pl.last_eta_checktime and not pl.last_retrieval_checktime:
            #             p['check'] = '未校验'
            #         elif pl.last_retrieval_checktime and not pl.last_retrieval_status:
            #             p['check'] = '失效'
            #         elif not pl.last_retrieval_checktime and pl.last_eta_checktime and not pl.last_eta_status:
            #             p['check'] = '失效'
            #         else:
            #             p['check'] = '有效'
            #     except PoCheckEtaSeven.DoesNotExist:
            #         # 如果没有找到对应的PoCheckEtaSeven记录，可以根据需求设置默认状态
            #         p['check'] = '未关联'
            #     except MultipleObjectsReturned:
            #         p['check'] = "不对应"
            data += pl_list
        else:
            await sync_to_async(print)("没有")
        return data
    
    def handle_select_get(self, request: HttpRequest) -> dict[str, Any]:
        request.POST = request.GET
        batch_number = request.GET.get("batch_number")
        context = self.handle_search_post(request)
        context['shipment_list'] = context['shipment_list'].filter(shipment_batch_number=batch_number)
        packling_list = PackingList.objects.select_related(
            "container_number", "container_number__order__customer_name"
        ).filter(
            shipment_batch_number__shipment_batch_number=batch_number
        ).values(
            'id', 'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
            'container_number__container_number',
            'container_number__order__customer_name__zem_name',
            'container_number__order__offload_id__offload_at',
        ).annotate(
            total_pcs=Sum("pallet__pcs", output_field=IntegerField()),
            total_cbm=Sum("pallet__cbm", output_field=FloatField()),
            total_weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
            total_n_pallet=Count('pallet__pallet_id', distinct=True),
        ).order_by(
            'container_number__container_number',
            '-total_weight_lbs'
        )
        context["packing_list"] = packling_list if packling_list else [None]
        return context
    
    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = request.POST.get("name")
        warehouse = None if warehouse=="N/A(直送)" else warehouse
        start_date = request.POST.get("start_date", None)
        end_date = request.POST.get("end_date", None)
        container_number = request.POST.get("container_number", None)
        criteria = models.Q(packinglist__container_number__order__warehouse__name=warehouse)
        if start_date:
            criteria &= models.Q(shipment_schduled_at__gte=start_date)
        if end_date:
            criteria &= models.Q(shipment_schduled_at__lte=end_date)
        if container_number:
            criteria &= (
                models.Q(packinglist__container_number__container_number=container_number) |
                models.Q(order__container_number__container_number=container_number)
            )
        shipment = Shipment.objects.prefetch_related(
            "packinglist", "packinglist__container_number", "packinglist__container_number__order",
            "packinglist__container_number__order__warehouse", "order"
        ).filter(criteria).distinct()
        warehouse_object = ZemWarehouse.objects.get(name=warehouse) if warehouse else None
        warehouse = warehouse if warehouse else "N/A(直送)"
        context = {
            "shipment_list": shipment,
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "name": warehouse,
            "warehouse": warehouse_object,
            "start_date": start_date,
            "end_date": end_date,
            "container_number": container_number,
        }
        return context
    
    def handle_bol_post(self, request: HttpRequest) -> dict[str, Any]:
        batch_number = request.POST.get("batch_number")
        warehouse = request.POST.get("warehouse")
        shipment = Shipment.objects.get(shipment_batch_number=batch_number)
        packing_list = list(PackingList.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
        ))
        for pl in packing_list:
            fba_ids = pl.fba_id
            if fba_ids:
                fba_ids = re.sub(r'[-,\s\/]+', '\n', fba_ids).strip()
                pl.fba_id = fba_ids

            ref_ids = pl.ref_id
            if ref_ids:
                ref_ids = re.sub(r'[-,\s\/]+', '\n', ref_ids).strip()
                pl.ref_id = ref_ids
        pallet = list(Pallet.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
            container_number__order__offload_id__offload_at__isnull=False,
        ).values(
            "container_number__container_number", "destination"
        ).annotate(
            total_cbm=Sum("cbm"),
            total_n_pallet=Count("pallet_id", distinct=True),
        ).order_by("container_number__container_number"))
        pallet += list(PackingList.objects.select_related("container_number").filter(
            shipment_batch_number__shipment_batch_number=batch_number,
            container_number__order__offload_id__offload_at__isnull=True,
        ).values(
            "container_number__container_number", "destination"
        ).annotate(
            total_cbm=Sum("cbm"),
            total_n_pallet=Sum("cbm")/2,
        ).order_by("container_number__container_number"))
        address_chinese_char = False if shipment.address.isascii() else True
        destination_chinese_char = False if shipment.destination.isascii() else True
        is_private_warehouse = True if re.search(r'([A-Z]{2})[-,\s]?(\d{5})', shipment.destination.upper()) else False
        try:
            note_chinese_char = False if shipment.note.isascii() else True
        except:
            note_chinese_char = False
        context = {
            "batch_number": batch_number,
            "warehouse": warehouse,
            "shipment": shipment,
            "packing_list": packing_list,
            "pallet": pallet,
            "address_chinese_char": address_chinese_char,
            "destination_chinese_char": destination_chinese_char,
            "note_chinese_char": note_chinese_char,
            "is_private_warehouse": is_private_warehouse,
        }
        return context
