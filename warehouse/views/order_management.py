import os
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Any
from pathlib import Path

from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.forms import modelformset_factory, formset_factory
from django.forms.models import model_to_dict
from django.core.cache import cache

from warehouse.models.order import Order
from warehouse.models.customer import Customer
from warehouse.models.container import Container
from warehouse.models.clearance import Clearance
from warehouse.models.retrieval import Retrieval
from warehouse.models.packing_list import PackingList
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.pallet import Pallet
from warehouse.forms.order_form import OrderForm
from warehouse.forms.container_form import ContainerForm
from warehouse.forms.clearance_form import ClearanceSelectForm
from warehouse.forms.retrieval_form import RetrievalForm, RetrievalSelectForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.forms.upload_file import UploadFileForm
from warehouse.forms.shipment_form import ShipmentForm
from warehouse.utils.constants import PACKING_LIST_TEMP_COL_MAPPING
from warehouse.views.export_file import export_palletization_list

@method_decorator(login_required(login_url='login'), name='dispatch')
class OrderManagement(View):
    template_main = 'order_list.html'
    context: dict[str, Any] = {}

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step")
        current_date = datetime.now().date()
        start_date = (current_date + timedelta(days=-30)).strftime('%Y-%m-%d')
        end_date = (current_date + timedelta(days=30)).strftime('%Y-%m-%d')
        if step == "all":
            orders = Order.objects.filter(
                models.Q(eta__gte=start_date) & models.Q(eta__lte=end_date)
            )
            self.context = {
                'orders': orders,
                'order_detail': False,
                'start_date': start_date,
                'end_date': end_date,
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
            offload = selected_order[0].offload_id
            packing_list = PackingList.objects.filter(
                models.Q(container_number__container_number=container_number)
            ).order_by("id")
            pallet = Pallet.objects.filter(
                models.Q(packing_list__container_number__order__order_id=selected_order[0].order_id)
            )
            shipment = selected_order[0].shipment_id
            shipment_form = ShipmentForm(instance=shipment)
            order_form = OrderForm(instance=selected_order[0])
            container_form = ContainerForm(instance=container)
            clearance_select_form = ClearanceSelectForm(initial={"clearance_option": self._get_clearance_option(clearance)})
            retrieval_select_form = RetrievalSelectForm(initial={"retrieval_option": self._get_retrieval_option(retrieval)})
            retrieval_form = RetrievalForm(instance=retrieval)
            status = "palletized" if offload.offload_at else "non_palletized"
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
                "upload_file_form": UploadFileForm(),
                "status": status,
                "pallet": pallet,
                "packing_list": packing_list,
                "pl_pl_form_zip": zip(packing_list, packing_list_formset),
                "shipment": shipment,
                "shipment_form": shipment_form,
                "order_detail": True,
            }
            return render(request, self.template_main, self.context)
        elif step == "download_template":
            return self.handle_download_pl_template_get(request)
        
    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "update":
            if request.FILES:
                mutable_get = request.GET.copy()
                mutable_get["container_number"] = request.POST.getlist("container_number")[0]
                mutable_get["step"] = "query"
                request.GET = mutable_get
                self.get(request)
                self.handle_upload_pl_template_post(request)
                return render(request, self.template_main, self.context)
            else:
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
                packing_list = PackingList.objects.filter(models.Q(container_number__order__order_id=order_id)).order_by("id")
                if order.order_type == "直送":
                    shipment = order.shipment_id
                    shipment.destination = request.POST.getlist("destination")[0]
                    shipment.address = request.POST.getlist("address")[0]
                    shipment.save()
                self._update_order(
                    request, order, container, retrieval, clearance, packing_list,
                    customer, warehoue
                )
        elif step == "delete_order":
            self.handle_order_delete_post(request)
            return self.handle_search_post(request)
        elif step == "export_palletization_list":
            return export_palletization_list(request)
        elif step == "search":
            return self.handle_search_post(request)
        else:
            raise ValueError(f"{request.POST}")
        mutable_get = request.GET.copy()
        mutable_get["container_number"] = request.POST.getlist("container_number")[0]
        mutable_get["step"] = "query"
        request.GET = mutable_get
        return self.get(request)
    
    def handle_download_pl_template_get(self, request: HttpRequest) -> HttpResponse:
        file_path = Path(__file__).parent.parent.resolve().joinpath("templates/export_file/packing_list_template.xlsx")
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, 'rb') as file:
            response = HttpResponse(file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="zem_packing_list_template.xlsx"'
            return response
    
    def handle_upload_pl_template_post(self, request: HttpRequest) -> None:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            df = pd.read_excel(file)
            df = df.rename(columns=PACKING_LIST_TEMP_COL_MAPPING)
            df = df.dropna(how="all", subset=[c for c in df.columns if c != "delivery_method"])
            df = df.replace(np.nan, None)
            df = df.reset_index(drop=True)
            # if df[df[~"delivery_method"].isin(["客户自提", "UPS", "FEDEX"])]["destination"].isna().sum():
            #     raise ValueError(f"destination N/A error!")
            if df["cbm"].isna().sum():
                raise ValueError(f"cbm number N/A error!")
            if df["total_weight_lbs"].isna().sum() and df["total_weight_kg"].isna().sum():
                raise ValueError(f"weight number N/A error!")
            if df["pcs"].isna().sum():
                raise ValueError(f"boxes number N/A error!")
            for idx, row in df.iterrows():
                if row["unit_weight_kg"] and not row["unit_weight_lbs"]:
                    df.loc[idx, "unit_weight_lbs"] = round(df.loc[idx, "unit_weight_kg"] * 2.20462, 2)
                if row["total_weight_kg"] and not row["total_weight_lbs"]:
                    df.loc[idx, "total_weight_lbs"] = round(df.loc[idx, "total_weight_kg"] * 2.20462, 2)
            model_fields = [field.name for field in PackingList._meta.fields]
            col = [c for c in df.columns if c in model_fields]
            pl_data = df[col].to_dict("records")
            
            packing_list = [PackingList(**data) for data in pl_data]            
            packing_list_formset = formset_factory(PackingListForm, extra=0)
            packing_list_form = packing_list_formset(initial=[model_to_dict(obj) for obj in packing_list])
            self.context["packing_list_formset"] = packing_list_form
            cache.clear()
        else:
            raise ValueError(f"invalid file format!")
    
    def handle_order_delete_post(self, request: HttpRequest) -> None:
        order_ids = request.POST.getlist("order_id")
        selected = request.POST.getlist("is_order_selected")
        delete_orders = [id for s, id in zip(selected, order_ids) if s == "on"]
        orders = Order.objects.filter(order_id__in=delete_orders)
        for order in orders:
            self._delete_order(order)

    def handle_search_post(self, request: HttpRequest) -> HttpResponse:
        start_date = request.POST.get("start_date", None)
        end_date = request.POST.get("end_date", None)
        if start_date and end_date:
            criteria = models.Q(eta__gte=start_date) & models.Q(eta__lte=end_date)
        elif start_date:
            criteria = models.Q(eta__gte=start_date)
        elif end_date:
            criteria = models.Q(eta__lte=end_date)
        else:
            criteria = models.Q(eta__gte="2023-01-01")
        orders = Order.objects.filter(criteria)
        self.context = {
            'orders': orders,
            'order_detail': False,
            'start_date': start_date,
            'end_date': end_date,
        }
        return render(request, self.template_main, self.context)
        
    def _delete_order(self, order: Order) -> None:
        pallet = Pallet.objects.filter(packing_list__container_number__order__order_id=order.order_id)
        for p in pallet:
            p.delete()
        packing_list = PackingList.objects.filter(container_number__order__order_id=order.order_id)
        for pl in packing_list:
            pl.delete()
        order.container_number.delete()
        order.retrieval_id.delete()
        order.clearance_id.delete()
        order.offload_id.delete()
        if order.shipment_id:
            order.shipment_id.delete()
        order.delete()
        
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
    
    def _update_packing_list_non_palletized(
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
                    try:
                        d["destination"] = d["destination"].upper()
                    except:
                        pass
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
        else:
            raise ValueError(f"invaid packing list!")
        
    def _update_packing_list_palletized(
        self, request: HttpRequest, obj: list[PackingList], container: Container
    ) -> None:
        packing_list_formsets = formset_factory(PackingListForm, extra=1)
        packing_list_form = packing_list_formsets(request.POST)
        pl_valid = all([pl.is_valid() for pl in packing_list_form])
        if pl_valid:
            cleaned_data = [pl.cleaned_data for pl in packing_list_form]
            for pl, pl_form in zip(obj, cleaned_data):
                pl.product_name = pl_form["product_name"]
                pl.delivery_method = pl_form["delivery_method"]
                pl.shipping_mark = pl_form["shipping_mark"]
                pl.fba_id = pl_form["fba_id"]
                pl.ref_id = pl_form["ref_id"]
                pl.destination = pl_form["destination"]
                pl.contact_name = pl_form["contact_name"]
                pl.contact_method = pl_form["contact_method"]
                pl.address = pl_form["address"]
                pl.zipcode = pl_form["zipcode"]
                pl.note = pl_form["note"]
                pl.save()
        else:
            raise ValueError(f"invaid packing list!")
        
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
        status = request.POST.get("status")
        if status == "non_palletized":
            self._update_packing_list_non_palletized(request, packing_list, container)
        else:
            self._update_packing_list_palletized(request, packing_list, container)

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
