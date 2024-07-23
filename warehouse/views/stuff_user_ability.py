from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.db import models

from warehouse.models.order import Order
from warehouse.models.offload import Offload
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.packing_list import PackingList
from warehouse.models.shipment import Shipment

@method_decorator(login_required(login_url='login'), name='dispatch')
class StuffPower(View):
    template_1 = "stuff_user_clean_data.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_staff:
            return HttpResponseForbidden("You don't have permission to access this page.")
        context = {}
        return render(request, self.template_1, context)
    
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "update_pl_weight_kg":
            invalid_cases = self._update_pl_weight_kg_20240410()
            context = {
                "pl_update_success": True,
                "invalid_cases": invalid_cases,
            }
            return render(request, self.template_1, context)
        elif step == "update_delivery_method":
            cnt = self._update_delivery_method()
            context = {
                "delivery_update_success": True,
                "count": cnt,
            }
            return render(request, self.template_1, context)
        else:
            self._remove_offload()
            self._remove_clearance()
            self._remove_retrieval()
            self._remove_shipment()
            context = {"success": True}
            return render(request, self.template_1, context)
    
    def _remove_offload(self) -> None:
        Offload.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_clearance(self) -> None:
        Clearance.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_retrieval(self) -> None:
        Retrieval.objects.filter(models.Q(order__isnull=True)).delete()

    def _remove_shipment(self) -> None:
        Shipment.objects.filter(
            models.Q(order__isnull=True) &
            models.Q(packinglist__isnull=True)
        ).delete()

    def _update_pl_weight_kg_20240410(self):
        invalid_cases = []
        pl = PackingList.objects.all()
        for p in pl:
            try:
                p.total_weight_kg = round(p.total_weight_lbs / 2.20462, 2)
            except:
                p.total_weight_kg = 0
                invalid_cases.append(p)
        PackingList.objects.bulk_update(pl, ["total_weight_kg"])
        return invalid_cases
    
    def _update_delivery_method(self) -> int:
        pl = PackingList.objects.all()
        cnt = 0
        for p in pl:
            if p.delivery_method == "暂扣留仓":
                p.delivery_method = "暂扣留仓(HOLD)"
                cnt += 1
        PackingList.objects.bulk_update(pl, ["delivery_method"])
        return cnt
    
