from datetime import datetime, timedelta
from typing import Any

import pytz
import pandas as pd
from django.http import JsonResponse
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from asgiref.sync import sync_to_async,async_to_sync
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.utils import timezone
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count, Case, When, F, Max, CharField, Value
from django.db.models.functions import Cast
from django.contrib.postgres.aggregates import StringAgg

from warehouse.models.order import Order
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_po,export_po_check
from warehouse.forms.upload_file import UploadFileForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class PO(View):
    template_main = "po/po.html"
    template_po_check = "po/po_check.html"
    template_po_check = "po/po_invalid.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        print('GET',step)
        if step == "po_to_be_check":
            context = async_to_sync(self.handle_search_eta_seven)(request)
            return render(request, self.template_po_check, context)
        if step == "po_invalid":
            context = async_to_sync(self.handle_po_check_seven)(request)
            return render(request, self.template_po_check, context)
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
        step = request.POST.get("step")
        print('POST',step)
        if step == "search":
            return render(request, self.template_main, self.handle_search_post(request))
        elif step == "selection":
            return render(request, self.template_main, self.handle_selection_post(request))
        elif step == "export_po":
            return export_po(request)
        elif step == "export_po_full":
            return export_po(request, "FULL_TABLE")
        elif step == "selection_check_seven":
            return export_po_check(request)
        elif step == "upload_check_po":     
            context = async_to_sync(self.handle_upload_check_po_post)(request)
            return render(request, self.template_po_check, context)
    
    async def handle_upload_check_po_post(self, request: HttpRequest) -> tuple[Any]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_csv(file)
            if 'shipping_mark' in df.columns and 'is_valid' in df.columns:
                data_pairs = [(row['BOL'], row['shipping_mark'], row['PRO'], row['PO List (use , as separator) *'], row['is_valid']) for index, row in df.iterrows()]
            else:
                print("Either 'PRO' or 'is_valid' column is not present in the DataFrame.")
        for bol,mark,fba,ref,is_valid in data_pairs:
            try:
                if bol:
                    container = await sync_to_async(Container.objects.get)(container_number = bol)
                    query = models.Q(container_number=container)
                    if mark:
                        query &= models.Q(shipping_mark=mark)
                    elif fba:
                        query &= models.Q(fba_id=fba)
                    elif ref:
                        query &= models.Q(ref_id=ref)
                    pochecketaseven = await sync_to_async(PoCheckEtaSeven.objects.get)(query)
                    pochecketaseven.status = is_valid == 1
                    cn = pytz.timezone('Asia/Shanghai')
                    current_time_cn = datetime.now(cn)
                    pochecketaseven.last_checktime = current_time_cn
                    await sync_to_async(pochecketaseven.save)()
            except PoCheckEtaSeven.DoesNotExist:
                continue
        return await self.handle_po_check_seven(request)
            
    async def handle_po_check_seven(self, request: HttpRequest) -> dict[str, dict]:
        po_checks = await sync_to_async(PoCheckEtaSeven.objects.all)()
        context = {
            "po_check":po_checks,
            "upload_check_file": UploadFileForm(),
            }
        return context
    
    async def handle_search_po_invalid(self, request: HttpRequest)-> tuple[str, dict[str, Any]]:
        po_checks = await sync_to_async(PoCheckEtaSeven.objects.all)()
        context = {
            "po_check":po_checks,
            }
        return context

    async def handle_search_eta_seven(self, request: HttpRequest)-> tuple[str, dict[str, Any]]:
        seven_days_later = datetime.now().date() + timedelta(days=7)
        # 使用Q对象构建查询条件
        
        orders = await sync_to_async(list)(
            Order.objects.select_related(
                    'container_number', 'vessel_id','retrieval_id'
                    ).filter(   
                            models.Q(vessel_id__vessel_eta__lte = seven_days_later)&
                            models.Q(retrieval_id__target_retrieval_timestamp__isnull=True)&
                            models.Q(cancel_notification__isnull=False)
                    )
            )
        po_checks = []
        for order in orders:
            container_number = order.container_number
            #根据柜号查找pls，因为第一次查询完pls，可能会有预报新加pl的情况
            pls = await sync_to_async(list)(PackingList.objects.filter(container_number = order.container_number))
            for pl in pls:
                try:
                    # 直接在查询集中查找是否存在具有相同container_number的对象
                    existing_obj = await sync_to_async(PoCheckEtaSeven.objects.get)(packing_list = pl)
                except PoCheckEtaSeven.DoesNotExist:
                    
                    po_check_obj = PoCheckEtaSeven(container_number = container_number, packing_list = pl)
                    po_checks.append(po_check_obj)
        po_checks_dict = []
        for p in po_checks:
            if ("/" not in p.packing_list.fba_id) or ("/" not in p.packing_list.ref_id):
                valid_dict = {
                    'container_number': p.container_number,
                    "packing_list": p.packing_list,
                    "destination": p.packing_list.destination,
                    "fba_id": p.packing_list.fba_id,
                    "ref_id": p.packing_list.ref_id,
                    "shipping_mark": p.packing_list.shipping_mark,
                    'status': p.status,
                    'last_checktime': p.last_checktime,
                    'is_notified': p.is_notified,
                    'is_active': p.is_active,
                    'handling_method': p.handling_method
                }
                po_checks_dict.append(valid_dict)
        await sync_to_async(PoCheckEtaSeven.objects.bulk_create)(
                PoCheckEtaSeven(**p) for p in po_checks_dict
            )
        return await self.handle_po_check_seven(request)
        


    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = None if request.POST.get("name")=="N/A(直送)" else request.POST.get("name")
        try:
            warehouse_obj = ZemWarehouse.objects.get(name=warehouse)
        except:
            warehouse_obj = None
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        container_number = request.POST.get("container_number")
        container_list = container_number.split()
        criteria = models.Q(
            container_number__order__warehouse__name=warehouse,
            shipment_batch_number__isnull=True,
        )
        if container_list:
            criteria &= models.Q(container_number__container_number__in=container_list)
        if start_date:
            criteria &= (
                models.Q(container_number__order__eta__gte=start_date) |
                models.Q(container_number__order__vessel_id__vessel_eta__gte=start_date)
            )
        if end_date:
            criteria &= (
                models.Q(container_number__order__eta__lte=end_date) |
                models.Q(container_number__order__vessel_id__vessel_eta__lte=end_date)
            )
        packing_list = PackingList.objects.select_related(
            "container_number", "container_number__order", "container_number__order__warehouse"
        ).filter(criteria).annotate(
            str_id=Cast("id", CharField()),
        ).values(
            'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
            'container_number__container_number',
        ).annotate(
            ids=StringAgg("str_id", delimiter=",", distinct=True),
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
        ).distinct().order_by("destination", "container_number__container_number")
        context = {
            "packing_list": packing_list,
            "warehouse_form": ZemWarehouseForm(initial={"name": request.POST.get("name")}),
            "name": request.POST.get("name"),
            "warehouse": warehouse_obj,
            "start_date": start_date,
            "end_date": end_date,
            "container_number": container_number,
        }
        return context
    

    def handle_selection_post(self, request: HttpRequest) -> HttpResponse:
        warehouse = request.POST.get("warehouse")
        ids = request.POST.getlist("pl_ids")
        ids = [i.split(",") for i in ids]
        selections = request.POST.getlist("is_selected")
        selected = [int(i) for s, id in zip(selections, ids) for i in id if s == "on"]
        if selected:
            packing_list = PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(
                id__in=selected
            ).values(
                'fba_id', 'ref_id','address','zipcode','destination','delivery_method',
                'container_number__container_number',
            ).annotate(
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
            ).distinct().order_by("destination", "container_number__container_number")
            agg_pl = PackingList.objects.filter(
                id__in=selected
            ).annotate(
                label=Case(
                        When(pallet__isnull=True, then=Value("EST")),
                        default=Value("ACT"),
                        output_field=CharField()
                    )
            ).values("label").annotate(
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
                
            ).order_by("label")
            
            summary = self._get_pl_agg_summary(agg_pl)
            context = {
                "selected_packing_list": packing_list,
                "selected_pl_ids": selected,
                "warehouse": warehouse,
                "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
                "summary": summary,
            }
            return context
        else:
            mutable_post = request.POST.copy()
            mutable_post['name'] = warehouse
            request.POST = mutable_post
            return self.handle_search_post(request)
        
    def _get_pl_agg_summary(self, agg_pl: Any) -> dict[str, Any]:
        if len(agg_pl) == 2:
            est_pallet = agg_pl[1]["total_n_pallet_est"]//1 + (1 if agg_pl[1]["total_n_pallet_est"]%1 >= 0.45 else 0)
            return {
                "total_cbm": agg_pl[0]["total_cbm"] + agg_pl[1]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"] + agg_pl[1]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"] + agg_pl[1]["total_weight_lbs"],
                "act_pallet": agg_pl[0]["total_n_pallet_act"],
                "est_pallet": est_pallet,
                "total_pallet": agg_pl[0]["total_n_pallet_act"] + est_pallet,
            }
        elif agg_pl[0]["label"] == "EST":
            est_pallet = agg_pl[0]["total_n_pallet_est"]//1 + (1 if agg_pl[0]["total_n_pallet_est"]%1 >= 0.45 else 0)
            return {
                "total_cbm": agg_pl[0]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"],
                "act_pallet": 0,
                "est_pallet": est_pallet,
                "total_pallet": est_pallet,
            }
        else:
            return {
                "total_cbm": agg_pl[0]["total_cbm"],
                "total_pcs": agg_pl[0]["total_pcs"],
                "total_weight_lbs": agg_pl[0]["total_weight_lbs"],
                "act_pallet": agg_pl[0]["total_n_pallet_act"],
                "est_pallet": 0,
                "total_pallet": agg_pl[0]["total_n_pallet_act"],
            }
        