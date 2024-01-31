import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models

from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class Palletization(View):
    template_main = "palletization.html"
    template_palletize = "palletization_packing_list.html"
    context: dict[str, Any] = {}
    warehouse_form = ZemWarehouseForm()
    order_not_palletized: Order | Any = None
    order_palletized: Order | Any = None
    order_packing_list: list[PackingList | Any] = []
    step: int | Any = None

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        self._set_context()
        pk = kwargs.get("pk", None)
        if pk:
            self.handle_packing_list_get(request, pk)
            return render(request, self.template_palletize, self.context)
        else:
            return render(request, self.template_main, self.context)
    
    def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        step = request.POST.get("step")
        if step == "warehouse":
            self.handle_warehouse_post(request)
        elif step == "palletization":
            pk = kwargs.get("pk")
            self.handle_packing_list_post(request, pk)
        else:
            raise ValueError(f"{request.POST}")
        return self.get(request)
    
    def handle_packing_list_get(self, request: HttpRequest, pk: int) -> None:
        order_selected = Order.objects.get(pk=pk)
        container = order_selected.container_number
        packing_list = PackingList.objects.select_related("container_number").filter(
            container_number__container_number=container.container_number
        ).order_by("-cbm")
        self.order_packing_list.clear()
        for pl in packing_list:
            pl_form = PackingListForm(instance=pl)
            self.order_packing_list.append((pl, pl_form))
    
    def handle_warehouse_post(self, request: HttpRequest) -> None:
        warehouse = request.POST.get("name")
        self.order_not_palletized = self._get_order_not_palletized(warehouse)
        self.order_palletized = self._get_order_palletized(warehouse)
        self.step = 1
        self.warehouse_form = ZemWarehouseForm(initial={"name": warehouse})

    def handle_packing_list_post(self, request: HttpRequest, pk: int) -> None:
        order_selected = Order.objects.get(pk=pk)
        offload = order_selected.offload_id
        ids = request.POST.getlist("id")
        n_pallet = request.POST.getlist("n_pallet")
        total_pallet = 0
        for id, n in zip(ids, n_pallet):
            n = int(n)
            PackingList.objects.filter(id=id).update(n_pallet=n)
            total_pallet += n
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn)
        offload.total_pallet = total_pallet
        offload.offload_at = current_time_cn
        offload.save()
        mutable_post = request.POST.copy()
        mutable_post['name'] = order_selected.warehouse.name
        request.POST = mutable_post
        self.handle_warehouse_post(request)

    def _get_order_not_palletized(self, warehouse: str) -> Order:
        return Order.objects.filter(
            models.Q(warehouse__name=warehouse) &
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=True) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
        ).order_by("retrieval_id__actual_retrieval_timestamp")
    
    def _get_order_palletized(self, warehouse: str) -> Order:
        return Order.objects.filter(
            models.Q(warehouse__name=warehouse) &
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=False) &
            models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False)
        ).order_by("offload_id__offload_at")
    
    def _set_context(self) -> None:
        self.context["step"] = self.step
        self.context["warehouse_form"] = self.warehouse_form
        self.context["order_not_palletized"] = self.order_not_palletized
        self.context["order_palletized"] = self.order_palletized
        self.context["order_packing_list"] = self.order_packing_list