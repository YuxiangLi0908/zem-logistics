from datetime import datetime, timedelta
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Sum, FloatField, IntegerField, Count, Case, When, F, Max, CharField, Value
from django.db.models.functions import Cast
from django.contrib.postgres.aggregates import StringAgg

from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.views.export_file import export_po

@method_decorator(login_required(login_url='login'), name='dispatch')
class PO(View):
    template_main = "po.html"

    def get(self, request: HttpRequest) -> HttpResponse:
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
        if step == "search":
            return render(request, self.template_main, self.handle_search_post(request))
        elif step == "selection":
            return render(request, self.template_main, self.handle_selection_post(request))
        elif step == "export_po":
            return export_po(request)
        elif step == "export_po_full":
            return export_po(request, "FULL_TABLE")
        
    def handle_search_post(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = None if request.POST.get("name")=="N/A(直送)" else request.POST.get("name")
        try:
            warehouse_obj = ZemWarehouse.objects.get(name=warehouse)
        except:
            warehouse_obj = None 
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        criteria = models.Q(container_number__order__warehouse__name=warehouse)
        if start_date:
            criteria &= models.Q(container_number__order__eta__gte=start_date)
        if end_date:
            criteria &= models.Q(container_number__order__eta__lte=end_date)
        packing_list = PackingList.objects.select_related(
            "container_number", "container_number__order", "container_number__order__warehouse", "pallet"
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
        