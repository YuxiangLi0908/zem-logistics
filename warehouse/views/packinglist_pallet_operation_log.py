from datetime import datetime, timedelta
from typing import Any

from django.core.paginator import Paginator
from django.db.models import Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils import timezone
from django.views import View

from warehouse.models.packinglist_pallet_operation_log import (
    PackingListPalletOperationLog,
)
from warehouse.utils.packinglist_pallet_operation_log import BEIJING_TIMEZONE


class PackingListPalletOperationLogView(View):
    template_name = "operation_log/packinglist_pallet_operation_log.html"
    page_size = 50

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        context = self._build_context(request)
        return render(request, self.template_name, context)

    def _build_context(self, request: HttpRequest) -> dict[str, Any]:
        filters = {
            "target_type": request.GET.get("target_type", "").strip(),
            "action_type": request.GET.get("action_type", "").strip(),
            "operator": request.GET.get("operator", "").strip(),
            "operation_name": request.GET.get("operation_name", "").strip(),
            "operation_location": request.GET.get("operation_location", "").strip(),
            "container_number": request.GET.get("container_number", "").strip(),
            "po_id": request.GET.get("po_id", "").strip(),
            "keyword": request.GET.get("keyword", "").strip(),
            "start_date": request.GET.get("start_date", "").strip(),
            "end_date": request.GET.get("end_date", "").strip(),
        }

        if not filters["start_date"] and not filters["keyword"]:
            filters["start_date"] = (
                timezone.localtime(timezone.now(), BEIJING_TIMEZONE) - timedelta(days=30)
            ).strftime("%Y-%m-%d")
        if not filters["end_date"] and not filters["keyword"]:
            filters["end_date"] = timezone.localtime(
                timezone.now(), BEIJING_TIMEZONE
            ).strftime("%Y-%m-%d")

        criteria = Q()
        if filters["target_type"]:
            criteria &= Q(target_type=filters["target_type"])
        if filters["action_type"]:
            criteria &= Q(action_type=filters["action_type"])
        if filters["operator"]:
            criteria &= Q(operator_username__icontains=filters["operator"])
        if filters["operation_name"]:
            criteria &= Q(operation_name__icontains=filters["operation_name"])
        if filters["operation_location"]:
            criteria &= Q(operation_location__icontains=filters["operation_location"])
        if filters["container_number"]:
            criteria &= Q(container_number__icontains=filters["container_number"])
        if filters["po_id"]:
            criteria &= Q(po_id__icontains=filters["po_id"])
        if filters["keyword"]:
            keyword = filters["keyword"]
            criteria &= (
                Q(target_display__icontains=keyword)
                | Q(action_detail__icontains=keyword)
                | Q(operation_name__icontains=keyword)
                | Q(fba_id__icontains=keyword)
                | Q(ref_id__icontains=keyword)
                | Q(shipping_mark__icontains=keyword)
                | Q(destination__icontains=keyword)
                | Q(request_path__icontains=keyword)
            )

        if filters["start_date"]:
            start_datetime = self._parse_beijing_date(filters["start_date"])
            if start_datetime:
                criteria &= Q(operation_time_beijing__gte=start_datetime)
        if filters["end_date"]:
            end_datetime = self._parse_beijing_date(filters["end_date"])
            if end_datetime:
                criteria &= Q(operation_time_beijing__lt=end_datetime + timedelta(days=1))

        queryset = (
            PackingListPalletOperationLog.objects.select_related("operator")
            .filter(criteria)
            .order_by("-operation_time_utc")
        )
        paginator = Paginator(queryset, self.page_size)
        page_obj = paginator.get_page(request.GET.get("page"))
        query_params = request.GET.copy()
        query_params.pop("page", None)

        return {
            "logs": page_obj.object_list,
            "page_obj": page_obj,
            "total_count": paginator.count,
            "query_string": query_params.urlencode(),
            "filters": filters,
            "target_type_choices": PackingListPalletOperationLog.TARGET_TYPE_CHOICES,
            "action_type_choices": PackingListPalletOperationLog.ACTION_TYPE_CHOICES,
        }

    def _parse_beijing_date(self, value: str):
        try:
            naive_date = datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            return None
        return timezone.make_aware(naive_date, BEIJING_TIMEZONE)
