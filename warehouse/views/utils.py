import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.forms import modelformset_factory

from warehouse.models.order import Order
from warehouse.models.container import Container
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.packing_list import PackingList
from warehouse.forms.order_form import OrderForm
from warehouse.forms.container_form import ContainerForm
from warehouse.forms.clearance_form import ClearanceSelectForm
from warehouse.forms.retrieval_form import RetrievalForm, RetrievalSelectForm
from warehouse.forms.packling_list_form import PackingListForm

@method_decorator(login_required(login_url='login'), name='dispatch')
class OrderManagement(View):
    template_main = 'order_list.html'
    context: dict[str, Any] = {}

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        if step == "all":
            orders = Order.objects.all()
            self.context = {
                'orders': orders,
            }
            return render(request, self.template_main, self.context)
        elif step == "query":
            container_number = request.GET.get("container_number")
            selected_order = Order.objects.filter(
                models.Q(container_number__container_number=container_number)
            )
            order_id = selected_order[0].order_id
            container = Container.objects.get(
                models.Q(container_number=container_number)
            )
            clearance = Clearance.objects.get(
                models.Q(clearance_id=selected_order[0].clearance_id)
            )
            retrieval = Retrieval.objects.get(
                models.Q(retrieval_id=selected_order[0].retrieval_id)
            )
            packing_list = PackingList.objects.filter(
                models.Q(container_number__container_number=container_number)
            )
            order_form = OrderForm(instance=selected_order[0])
            container_form = ContainerForm(instance=container)
            clearance_select_form = ClearanceSelectForm(initial={"clearance_option": self._get_clearance_option(clearance)})
            retrieval_select_form = RetrievalSelectForm(initial={"retrieval_option": self._get_retrieval_option(retrieval)})
            retrieval_form = RetrievalForm(instance=retrieval)
            if len(packing_list) > 0:
                packing_list_formset = modelformset_factory(PackingList, form=PackingListForm, extra=0)
            else:
                packing_list_formset = modelformset_factory(PackingList, form=PackingListForm, extra=1)
            packing_list_formset = packing_list_formset(queryset=packing_list)
            self.context = {
                "order_form": order_form,
                "container_form": container_form,
                "clearance_select_form": clearance_select_form,
                "retrieval_select_form": retrieval_select_form,
                "retrieval_form": retrieval_form,
                "packing_list_formset": packing_list_formset,
                "orders": selected_order,
                "order_id": order_id,
            }
            # raise ValueError(f"{packing_list}")
            return render(request, self.template_main, self.context)

    def _get_clearance_option(self, clearance: Clearance) -> str:
        if not clearance.is_clearance_required:
            return "N/A"
        elif clearance.clear_by_zem:
            return "代理清关"
        else:
            return "自理清关"
        
    def _get_retrieval_option(self, retrieval: Retrieval) -> str:
        if retrieval.retrive_by_zem:
            return "代理卡车"
        else:
            return "自理卡车"
            
