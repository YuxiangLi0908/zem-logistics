import pytz
from datetime import datetime
from typing import Any

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.forms import modelformset_factory, formset_factory

from warehouse.models.order import Order
from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
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
                "container_number": container_number,
            }
            return render(request, self.template_main, self.context)
        
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "update":
            order_id = request.POST.get("order_id")
            container_number = request.POST.get("container_number")
            order = Order.objects.get(models.Q(order_id=order_id))
            container = Container.objects.get(models.Q(container_number=container_number))
            customer = Customer.objects.get(models.Q(id=request.POST.get("customer_name")))
            clearance = Clearance.objects.get(models.Q(order__order_id=order_id))
            retrieval = Retrieval.objects.get(models.Q(order__order_id=order_id))
            try:
                warehoue = ZemWarehouse.objects.get(models.Q(id=request.POST.get("warehouse")))
            except:
                warehoue = None
            packing_list = PackingList.objects.filter(models.Q(container_number__order__order_id=order_id))
            self._update_order(
                request, order, container, retrieval, clearance, packing_list,
                customer, warehoue
            )
        else:
            raise ValueError(f"{request.POST}")
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = request.POST.getlist("container_number")[0]
        mutable_get["step"] = "query"
        request.GET = mutable_get
        return self.get(request)
    
    def _update_clearance(self, request: HttpRequest, obj: Clearance) -> Clearance:
        clearance_option = request.POST.get("clearance_option")
        if clearance_option == "N/A":
            obj.is_clearance_required = False
            obj.clear_by_zem = False
        elif clearance_option == "代理清关":
            obj.is_clearance_required = True
            obj.clear_by_zem = True
        else:
            obj.is_clearance_required = True
            obj.clear_by_zem = False
        obj.save()
        return obj

    def _update_retrieval(self, request: HttpRequest, obj: Retrieval) -> Retrieval:
        retrieval_option = request.POST.get("retrieval_option")
        if retrieval_option == "代理卡车":
            obj.retrive_by_zem = True
        else:
            obj.retrive_by_zem = False
        form = RetrievalForm(request.POST)
        form.is_valid()
        obj.shipping_order_number = form.cleaned_data.get("shipping_order_number")
        obj.origin = form.cleaned_data.get("origin")
        obj.destination = form.cleaned_data.get("destination")
        obj.retrieval_location = form.cleaned_data.get("retrieval_location")
        obj.shipping_line = form.cleaned_data.get("shipping_line")
        obj.target_retrieval_timestamp = form.cleaned_data.get("target_retrieval_timestamp")
        obj.actual_retrieval_timestamp = form.cleaned_data.get("actual_retrieval_timestamp")
        if obj.target_retrieval_timestamp or obj.actual_retrieval_timestamp:
            if not obj.scheduled_at:
                cn = pytz.timezone('Asia/Shanghai')
                current_time_cn = datetime.now(cn)
                obj.scheduled_at = current_time_cn
        else:
            obj.scheduled_at = None
        obj.save()
        return obj

    def _update_container(self, request: HttpRequest, obj: Container) -> Container:
        form = ContainerForm(request.POST)
        form.is_valid()
        obj.container_number = request.POST.getlist("container_number")[0]
        obj.container_type = form.cleaned_data.get("container_type")
        obj.save()
        return obj
    
    def _update_packing_list(
        self, request: HttpRequest, obj: list[PackingList], container: Container
    ) -> None:
        packing_list_formsets = formset_factory(PackingListForm, extra=1)
        packing_list_form = packing_list_formsets(request.POST)
        pl_valid = all([pl.is_valid() for pl in packing_list_form])
        if pl_valid:
            cleaned_data = [pl.cleaned_data for pl in packing_list_form]
            cleaned_data_bool = [any(d.values()) for d in cleaned_data]
            cleaned_data = [d for d, b in zip(cleaned_data, cleaned_data_bool) if b]
            for d in cleaned_data:
                d["container_number"] = container
                if d["destination"]:
                    d["destination"] = d["destination"].upper()
            n_pl_new = len(cleaned_data)
            n_pl_old = len(obj)
            i, j = 0, 0
            while (i < n_pl_new) and (j < n_pl_old):
                for k, v in cleaned_data[i].items():
                    setattr(obj[j], k, v)
                obj[j].save()
                i += 1
                j += 1
            while i < n_pl_new:
                new_obj = PackingList(**cleaned_data[i])
                new_obj.save()
                i += 1
            while j < n_pl_old:
                obj[j].delete()
                j += 1

    def _update_order(
        self,
        request: HttpRequest,
        order: Order,
        container: Container,
        retrieval: Retrieval,
        clearance: Clearance,
        packing_list: list[PackingList],
        customer: Customer,
        warehouse: ZemWarehouse,
    ) -> None:
        container = self._update_container(request, container)
        # order.container_number = container
        order.retrieval_id = self._update_retrieval(request, retrieval)
        order.clearance_id = self._update_clearance(request, clearance)
        order.customer_name = customer
        order.warehouse = warehouse
        form = OrderForm(request.POST)
        form.is_valid()
        order.eta = str(form.cleaned_data.get("eta"))
        order.order_type = form.cleaned_data.get("order_type")
        order.save()
        self._update_packing_list(request, packing_list, container)

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
            
