from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render

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
        order_all = Order.objects.all()
        offload_all = Offload.objects.all()
        offload_used = [o.offload_id for o in order_all]
        offload_to_remove = [o for o in offload_all if o not in offload_used]
        for o in offload_to_remove:
            o.delete()

    def _remove_clearance(self) -> None:
        order_all = Order.objects.all()
        clearance_all = Clearance.objects.all()
        clearance_used = [o.clearance_id for o in order_all]
        clearance_to_remove = [o for o in clearance_all if o not in clearance_used]
        for o in clearance_to_remove:
            o.delete()

    def _remove_retrieval(self) -> None:
        order_all = Order.objects.all()
        retrieval_all = Retrieval.objects.all()
        retrieval_used = [o.retrieval_id for o in order_all]
        retrieval_to_remove = [o for o in retrieval_all if o not in retrieval_used]
        for o in retrieval_to_remove:
            o.delete()

    def _remove_shipment(self) -> None:
        order_all = Order.objects.all()
        packing_list_all = PackingList.objects.all()
        shipment_all = Shipment.objects.all()
        shipment_in_use = [p.shipment_id for p in order_all if p.shipment_id]
        shipment_in_use += [p.shipment_batch_number for p in packing_list_all if p.shipment_batch_number]
        shipment_in_use = set(shipment_in_use)
        shipment_to_remove = [s for s in shipment_all if s not in shipment_in_use]
        for s in shipment_to_remove:
            s.delete()

    def _update_pl_weight_kg_20240410(self):
        invalid_cases = []
        pl = PackingList.objects.all()
        for p in pl:
            try:
                p.total_weight_kg = round(p.total_weight_lbs / 2.20462, 2)
                p.save()
            except:
                p.total_weight_kg = 0
                p.save()
                invalid_cases.append(p)
        return invalid_cases
    
    def _update_delivery_method(self) -> int:
        pl = PackingList.objects.all()
        cnt = 0
        for p in pl:
            if p.delivery_method == "暂扣留仓":
                p.delivery_method = "暂扣留仓(HOLD)"
                p.save()
                cnt += 1
        return cnt
    
