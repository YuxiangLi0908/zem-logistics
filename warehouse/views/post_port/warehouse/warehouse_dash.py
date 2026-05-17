from django.views import View
from django.shortcuts import render, redirect
from django.http import HttpRequest, HttpResponse
from django.db.models import Sum, F, Func
from django.db import models
from django.utils.timezone import now
from collections import defaultdict
from statistics import mean, stdev

from warehouse.models.pallet import Pallet
from warehouse.models.packing_list import PackingList
from datetime import timedelta, date


class Week(Func):
    function = "EXTRACT"
    template = "%(function)s(WEEK FROM %(expressions)s)"

    def __init__(self, expression, **extra):
        super().__init__(expression, output_field=models.IntegerField(), **extra)


class WarehouseDashView(View):
    warehouse_dash_template = 'post_port/warehouse_dash/01_warehouse_dash.html'
    area = {"NJ": "NJ", "SAV": "SAV", "LA": "LA"}

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not self._user_authenticate(request):
            return redirect("login")
        return render(request, self.warehouse_dash_template, context={"area": self.area})
    
    def post(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not self._user_authenticate(request):
            return redirect("login")

        context = self.get_warehouse_dash_context(request)
        context["warehouse_filter"] = self.area
        return render(request, self.warehouse_dash_template, context)
    
    def get_warehouse_dash_context(self, request: HttpRequest) -> dict:
        warehouse = request.POST.get("warehouse_filter", None)
        historical_inventory = self._get_historical_inventory(warehouse)
        next_week_inventory = self._get_next_week_inventory(warehouse)

        historical_metrics = self._calculate_historical_metrics(historical_inventory)

        context = {
            "historical_metrics": historical_metrics,
            "next_week_inventory": next_week_inventory,
        }
        return context
    
    def _get_historical_inventory(self, warehouse: str) -> dict:
        today = now().date()
        nine_weeks_ago = today - timedelta(weeks=9)
        nine_weeks_ago_start = nine_weeks_ago - timedelta(days=nine_weeks_ago.weekday())
        data = Pallet.objects.prefetch_related(
                "container_number",
                "shipment_batch_number",
                "container_number__orders__offload_id",
            ).filter(
                location__startswith=warehouse,
                container_number__orders__offload_id__offload_at__gte=nine_weeks_ago_start
            ).annotate(
                week=Week("container_number__orders__offload_id__offload_at")
            ).values(
                "week",
                "destination"
            ).annotate(
                total_cbm=Sum("cbm")
            ).order_by("week")
        return list(data)
    
    def _get_next_week_inventory(self, warehouse: str) -> dict:
        today = now().date()
        next_week_start = today + timedelta(days=(7 - today.weekday()))
        next_week_end = next_week_start + timedelta(days=6)
        data = PackingList.objects.prefetch_related(
                "container_number__orders__vessel_id",
                "container_number__orders__retrieval_id",
                "container_number__orders__warehouse",
                "container_number__orders",
            ).filter(
                container_number__orders__retrieval_id__retrieval_destination_area=warehouse,
                container_number__orders__vessel_id__vessel_eta__gte=next_week_start,
                container_number__orders__vessel_id__vessel_eta__lte=next_week_end,
                container_number__orders__cancel_notification=False
            ).values(
                "destination"
            ).annotate(
                total_cbm=Sum("cbm")
            )
        return list(data)
    
    def _calculate_historical_metrics(self, historical_inventory: list) -> dict:
        today = now().date()
        current_week = today.isocalendar()[1]
        metrics = defaultdict(lambda: {"average_weekly_cbm": 0, "cv": 0, "active_week_ratio": 0, "stability_score": 0})

        # Group data by destination
        grouped_data = defaultdict(list)
        for entry in historical_inventory:
            if entry["week"] < current_week:  # Only consider weeks before the current week
                grouped_data[entry["destination"]].append(entry["total_cbm"])

        # Calculate metrics per destination
        all_avg_cbms = []
        all_cvs = []
        for destination, weekly_cbms in grouped_data.items():
            total_weeks = len(weekly_cbms)
            active_weeks = sum(1 for cbm in weekly_cbms if cbm > 0)
            avg_cbm = mean(weekly_cbms) if weekly_cbms else 0
            stddev_cbm = stdev(weekly_cbms) if len(weekly_cbms) > 1 else 0

            metrics[destination]["average_weekly_cbm"] = avg_cbm
            metrics[destination]["cv"] = stddev_cbm / avg_cbm if avg_cbm > 0 else 0
            metrics[destination]["active_week_ratio"] = active_weeks / total_weeks if total_weeks > 0 else 0

            all_avg_cbms.append(avg_cbm)
            all_cvs.append(metrics[destination]["cv"])

        # Normalize values
        min_avg_cbm, max_avg_cbm = min(all_avg_cbms, default=0), max(all_avg_cbms, default=1)
        min_cv, max_cv = min(all_cvs, default=0), max(all_cvs, default=1)

        def normalize(value, min_value, max_value):
            return (value - min_value) / (max_value - min_value) if max_value > min_value else 0

        for destination in metrics:
            normalized_cv = 1 - normalize(metrics[destination]["cv"], min_cv, max_cv)
            normalized_avg_cbm = normalize(metrics[destination]["average_weekly_cbm"], min_avg_cbm, max_avg_cbm)

            metrics[destination]["stability_score"] = (
                0.5 * metrics[destination]["active_week_ratio"] +
                0.3 * normalized_cv +
                0.2 * normalized_avg_cbm
            )

        return metrics

    def _user_authenticate(self, request: HttpRequest):
        if request.user.is_authenticated:
            return True
        return False
