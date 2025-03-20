import ast
import time
import uuid
from datetime import datetime
from typing import Any

import pytz
import shortuuid
from django.contrib.auth.decorators import login_required
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Concat
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
from simple_history.utils import bulk_update_with_history

from warehouse.forms.shipment_form import ShipmentForm
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.models.packing_list import PackingList
from warehouse.models.shipment import Shipment
from warehouse.utils.constants import amazon_fba_locations
from warehouse.views.bol import BOL


@method_decorator(login_required(login_url="login"), name="dispatch")
class ScheduleShipment(View):
    template_main = "schedule_shipment/search.html"
    template_td = "schedule_shipment/td_shipment.html"
    template_dd = "schedule_shipment/dd_shipment.html"
    template_confirmation = "schedule_shipment/confirmation.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "destination":
            return render(
                request, self.template_main, self.handle_destination_get(request)
            )
        else:
            context = {"warehouse_form": ZemWarehouseForm()}
            return render(request, self.template_main, context)

    def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "selection":
            template, context = self.handle_selection_post(request)
            return render(request, template, context)
        elif step == "appointment":
            template, context = self.handle_appointment_post(request)
            return render(request, template, context)
        elif step == "cancel":
            template, context = self.handle_cancel_post(request)
            return render(request, template, context)
        else:
            raise ValueError(f"{request.POST}")

    def handle_destination_get(self, request: HttpRequest) -> dict[str, Any]:
        warehouse = request.GET.get("warehouse")
        destination = request.GET.get("destination")
        packing_list = PackingList.objects.select_related(
            "container_number",
            "container_number__order__warehouse",
            "shipment_batch_number",
            "container_number__order__customer_name",
        ).filter(
            container_number__order__warehouse__name=warehouse,
            destination=destination,
            n_pallet__isnull=False,
            shipment_batch_number__isnull=True,
        )
        packing_list = packing_list.values(
            "id",
            "delivery_method",
            "shipping_mark",
            "fba_id",
            "ref_id",
            "destination",
            "pcs",
            "total_weight_lbs",
            "cbm",
            "n_pallet",
            "container_number__container_number",
            customer_name=models.F("container_number__order__customer_name__zem_name"),
        ).order_by("-n_pallet")
        order_packing_list = []
        for pl in packing_list:
            order_packing_list.append((pl, ShipmentForm()))
        context = {
            "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
            "packing_list_not_scheduled": self._get_packing_list_not_scheduled(
                warehouse
            ),
            "order_packing_list": order_packing_list,
            "ids": [pl["id"] for pl in packing_list],
        }
        return context

    def handle_warehouse_post(self, request: HttpRequest) -> tuple[Any]:
        warehouse = request.POST.get("name")
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        is_direct = True if warehouse == "N/A(直送)" else False
        warehouse = None if warehouse == "N/A(直送)" else warehouse
        if is_direct:
            shipment = (
                Shipment.objects.select_related(
                    "order", "container_number", "customer_name"
                )
                .filter(order__order_type="直送")
                .values(
                    "shipment_batch_number",
                    "order__customer_name__zem_name",
                    "order__container_number__container_number",
                    "destination",
                    "appointment_id",
                    "shipment_appointment",
                    "carrier",
                    "load_type",
                    "total_pcs",
                    "total_weight",
                    "total_cbm",
                    "total_pallet",
                    "note",
                    "is_shipment_schduled",
                )
            )
            context = {
                "warehouse_form": warehouse_form,
                "warehouse": "N/A(直送)",
                "shipment": shipment,
                "shipment_form": ShipmentForm(),
            }
            return self.template_dd, context
        else:
            bol = BOL()
            context = bol.handle_search_post(request)
            packing_list_not_scheduled = self._get_packing_list_not_scheduled(warehouse)
            context.update(
                {
                    "warehouse_form": warehouse_form,
                    "packing_list_not_scheduled": packing_list_not_scheduled,
                    "warehouse": warehouse,
                }
            )
            return self.template_td, context

    def handle_selection_post(self, request: HttpRequest) -> tuple[Any]:
        warehouse = request.POST.get("warehouse")
        warehouse_form = ZemWarehouseForm(initial={"name": warehouse})
        selections = request.POST.getlist("is_shipment_schduled")
        ids = request.POST.getlist("pl_ids")
        ids = [i.split(",") for i in ids]
        selected = [int(i) for s, id in zip(selections, ids) for i in id if s == "on"]
        if selected:
            packling_list = (
                PackingList.objects.select_related(
                    "container_number",
                    "container_number__order__customer_name",
                    "container_number__order__offload_id",
                    "pallet",
                )
                .filter(id__in=selected)
                .values(
                    "id",
                    "fba_id",
                    "ref_id",
                    "address",
                    "zipcode",
                    "destination",
                    "delivery_method",
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "container_number__order__offload_id__offload_at",
                )
                .annotate(
                    total_pcs=Sum("pallet__pcs", output_field=IntegerField()),
                    total_cbm=Sum("pallet__cbm", output_field=FloatField()),
                    total_weight_lbs=Sum(
                        "pallet__weight_lbs", output_field=FloatField()
                    ),
                    total_n_pallet=Count("pallet__pallet_id", distinct=True),
                )
                .order_by("container_number__container_number")
            )

            total_pallet = (
                PackingList.objects.select_related("pallet")
                .filter(id__in=selected)
                .values("pallet__pallet_id")
                .distinct()
                .count()
            )
            total_weight, total_cbm, total_pcs = 0.0, 0.0, 0
            cbm_list = []
            for pl in packling_list:
                cbm_list.append(pl.get("total_cbm"))
                total_weight += (
                    pl.get("total_weight_lbs") if pl.get("total_weight_lbs") else 0
                )
                total_cbm += pl.get("total_cbm") if pl.get("total_cbm") else 0
                total_pcs += pl.get("total_pcs") if pl.get("total_pcs") else 0
            destination = (
                packling_list[0].get("destination")
                if packling_list[0].get("destination")
                else "null"
            )
            batch_id = uuid.uuid3(
                uuid.NAMESPACE_DNS,
                str(uuid.uuid4())
                + warehouse
                + destination
                + request.user.username
                + str(time.time()),
            )
            batch_id = shortuuid.encode(batch_id)
            if destination in amazon_fba_locations:
                fba = amazon_fba_locations[destination]
                address = (
                    f"{fba['location']}, {fba['city']} {fba['state']}, {fba['zipcode']}"
                )
            else:
                address, zipcode = str(packling_list[0].get("address")), str(
                    packling_list[0].get("zipcode")
                )
                if zipcode.lower() not in address.lower():
                    address += f", {zipcode}"
            shipment_data = {
                "shipment_batch_number": str(batch_id),
                "origin": warehouse,
                "destination": destination,
                "address": address,
                "total_weight": total_weight,
                "total_cbm": total_cbm,
                "total_pallet": total_pallet,
                "total_pcs": total_pcs,
            }
            shipment = Shipment(**shipment_data)
            shipment_form = ShipmentForm(instance=shipment)
            context = {
                "packling_list": packling_list,
                "shipment_data": shipment_data,
                "shipment_form": shipment_form,
                "shipment": shipment,
                "warehouse_form": warehouse_form,
                "pl_ids": selected,
            }
            return self.template_confirmation, context
        else:
            mutable_post = request.POST.copy()
            mutable_post["name"] = warehouse
            request.POST = mutable_post
            return self.handle_warehouse_post(request)

    def handle_appointment_post(self, request: HttpRequest) -> tuple[Any]:
        cn = pytz.timezone("Asia/Shanghai")
        current_time_cn = datetime.now(cn)
        appointment_type = request.POST.get("type")
        if appointment_type == "td":
            shipment_data = ast.literal_eval(request.POST.get("shipment_data"))
            if self._shipment_exist(shipment_data["shipment_batch_number"]):
                raise ValueError(
                    f"Shipment {shipment_data['shipment_batch_number']} already exists!"
                )
            shipment_data["appointment_id"] = request.POST.get("appointment_id", None)
            shipment_data["carrier"] = request.POST.get("carrier", None)
            shipment_data["third_party_address"] = request.POST.get(
                "third_party_address", None
            )
            try:
                shipment_data["third_party_address"] = shipment_data[
                    "third_party_address"
                ].strip()
            except:
                pass
            shipment_data["load_type"] = request.POST.get("load_type", None)
            shipment_data["note"] = request.POST.get("note", None)
            shipment_data["shipment_appointment"] = request.POST.get(
                "shipment_appointment", None
            )
            shipment_data["shipment_schduled_at"] = current_time_cn
            shipment_data["is_shipment_schduled"] = True
            shipment = Shipment(**shipment_data)
            shipment.save()

            pl_ids = request.POST.get("pl_ids").strip("][").split(", ")
            pl_ids = [int(i) for i in pl_ids]
            packing_list = PackingList.objects.filter(id__in=pl_ids)
            for pl in packing_list:
                pl.shipment_batch_number = shipment
            bulk_update_with_history(packing_list, PackingList, fields=["shipment_batch_number"])

            mutable_post = request.POST.copy()
            mutable_post["name"] = shipment_data.get("origin")
            request.POST = mutable_post
        else:
            batch_number = request.POST.get("batch_number")
            warehouse = request.POST.get("warehouse")
            shipment_appointment = request.POST.get("shipment_appointment")
            note = request.POST.get("note")
            shipment = Shipment.objects.get(shipment_batch_number=batch_number)
            shipment.shipment_appointment = shipment_appointment
            shipment.note = note
            shipment.is_shipment_schduled = True
            shipment.shipment_schduled_at = current_time_cn
            shipment.save()
            mutable_post = request.POST.copy()
            mutable_post["name"] = warehouse
            request.POST = mutable_post
        return self.handle_warehouse_post(request)

    def handle_cancel_post(self, request: HttpRequest) -> tuple[Any]:
        appointment_type = request.POST.get("type")
        if appointment_type == "td":
            shipment_batch_number = request.POST.get("shipment_batch_number")
            shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
            if shipment.is_shipped:
                raise RuntimeError(
                    f"Shipment with batch number {shipment} has been shipped!"
                )
            shipment.delete()
        else:
            shipment_batch_number = request.POST.get("batch_number")
            shipment = Shipment.objects.get(shipment_batch_number=shipment_batch_number)
            if shipment.is_shipped:
                raise RuntimeError(
                    f"Shipment with batch number {shipment} has been shipped!"
                )
            shipment.is_shipment_schduled = False
            shipment.shipment_appointment = None
            shipment.note = None
            shipment.shipment_schduled_at = None
            shipment.save()

        warehouse = request.POST.get("warehouse")
        mutable_post = request.POST.copy()
        mutable_post["name"] = warehouse
        request.POST = mutable_post
        return self.handle_warehouse_post(request)

    def _get_packing_list_not_scheduled(self, warehouse: str) -> PackingList:
        return (
            PackingList.objects.select_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
                "container_number__order__offload_id",
                "container_number__order__customer_name",
                "pallet",
            )
            .filter(
                models.Q(container_number__order__warehouse__name=warehouse)
                & models.Q(
                    container_number__order__offload_id__total_pallet__isnull=False
                )
                & models.Q(shipment_batch_number__isnull=True)
            )
            .annotate(
                custom_delivery_method=Case(
                    When(
                        Q(delivery_method="暂扣留仓(HOLD)")
                        | Q(delivery_method="暂扣留仓"),
                        then=Concat(
                            "delivery_method", Value("-"), "fba_id", Value("-"), "id"
                        ),
                    ),
                    default=F("delivery_method"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__order__customer_name__zem_name",
                "destination",
                "address",
                "custom_delivery_method",
                "container_number__order__offload_id__offload_at",
            )
            .annotate(
                fba_ids=StringAgg(
                    "str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"
                ),
                ref_ids=StringAgg(
                    "str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"
                ),
                ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                pcs=Sum("pallet__pcs", output_field=IntegerField()),
                cbm=Sum("pallet__cbm", output_field=FloatField()),
                weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
                n_pallet=Count("pallet__pallet_id", distinct=True),
            )
            .order_by("-n_pallet")
        )

    def _shipment_exist(self, batch_number: str) -> bool:
        if Shipment.objects.filter(shipment_batch_number=batch_number):
            return True
        else:
            return False
