import random
import re
import string
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import pytz
from django.contrib.auth.decorators import login_required
from django.db import models
from django.db.models import Count, FloatField, IntegerField, Sum
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from django.shortcuts import render
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from simple_history.utils import bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.clearance import Clearance
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.retrieval import Retrieval
from warehouse.models.shipment import Shipment
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import DELIVERY_METHOD_CODE


@method_decorator(login_required(login_url="login"), name="dispatch")
class StuffPower(View):
    template_1 = "stuff_user_clean_data.html"

    def get(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_staff:
            return HttpResponseForbidden(
                "You don't have permission to access this page."
            )
        context = {
            "pre_port_t49_tracking": UploadFileForm(),
        }
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
        elif step == "pre_port_update_vessel_pl_data":
            template, context = self.update_vessel_pl_data(request)
            return render(request, template, context)
        elif step == "update_pl_status":
            template, context = self.update_pl_status()
            return render(request, template, context)
        elif step == "update_warehouse":
            template, context = self.update_warehouse()
            return render(request, template, context)
        elif step == "update_order_eta":
            template, context = self.update_order_eta()
            return render(request, template, context)
        elif step == "update_shipment":
            template, context = self.update_shipment()
            return render(request, template, context)
        elif step == "update_pallet":
            template, context = self.update_pallet(request)
            return render(request, template, context)
        elif step == "update_shipment_stats":
            template, context = self.update_shipment_stats(request)
            return render(request, template, context)
        elif step == "update_inventory":
            template, context = self.update_inventory(request)
            return render(request, template, context)
        elif step == "po_id_batch_generation":
            template, context = self.po_id_batch_generation(request)
            return render(request, template, context)
        elif step == "update_shipment_ts_utc":
            template, context = self.update_shipment_ts_utc(request)
            return render(request, template, context)
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
            models.Q(order__isnull=True) & models.Q(packinglist__isnull=True)
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
        bulk_update_with_history(pl, PackingList, fields=["total_weight_kg"])
        return invalid_cases

    def _update_delivery_method(self) -> int:
        pl = PackingList.objects.all()
        cnt = 0
        for p in pl:
            if p.delivery_method == "暂扣留仓":
                p.delivery_method = "暂扣留仓(HOLD)"
                cnt += 1
        bulk_update_with_history(pl, PackingList, fields=["delivery_method"])
        return cnt

    def update_vessel_pl_data(self, request: HttpRequest) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        orders = Order.objects.select_related(
            "customer_name", "container_number", "vessel_id", "retrieval_id"
        ).filter(created_at__gte="2024-07-01")
        order_pl_count = (
            Order.objects.select_related("container_number")
            .filter(
                (models.Q(order_type="转运") | models.Q(order_type="转运组合")),
                models.Q(created_at__gte="2024-07-01"),
            )
            .values("container_number__container_number")
            .annotate(
                n_pl=models.Count("container_number__packinglist__id", distinct=True),
            )
        )
        order_pl_count = {
            o["container_number__container_number"]: o["n_pl"] for o in order_pl_count
        }
        context = {
            "vessel_pl_data_update_success": False,
            "orders_count": 0,
        }
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_csv(file)
            df = df.fillna("")
            t49_container_numbers = df["Container number"].to_list()
            updated_orders = []
            updated_retrievals = []
            cnt = 0
            for o in orders:
                if o.container_number.container_number in t49_container_numbers:
                    mbl = df.loc[
                        df["Container number"] == o.container_number.container_number,
                        "Shipment number",
                    ].values[0]
                    vessel_id = str(
                        uuid.uuid3(
                            uuid.NAMESPACE_DNS,
                            o.container_number.container_number + mbl,
                        )
                    )
                    if not o.vessel_id:
                        vessel = Vessel.objects.create(
                            vessel_id=vessel_id,
                            master_bill_of_lading=mbl,
                            origin_port=(
                                df.loc[
                                    df["Container number"]
                                    == o.container_number.container_number,
                                    "Port of Lading",
                                ].values[0]
                                if df.loc[
                                    df["Container number"]
                                    == o.container_number.container_number,
                                    "Port of Lading",
                                ].any()
                                else ""
                            ),
                            destination_port=o.retrieval_id.destination_port,
                            shipping_line=df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Carrier name",
                            ].values[0],
                            vessel=df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Vessel name",
                            ].values[0],
                            voyage=df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Voyage number",
                            ].values[0],
                            vessel_eta=(
                                self._format_string_datetime(
                                    df.loc[
                                        df["Container number"]
                                        == o.container_number.container_number,
                                        "Port of Discharge estimated time of arrival",
                                    ].values[0]
                                )
                                if df.loc[
                                    df["Container number"]
                                    == o.container_number.container_number,
                                    "Port of Discharge estimated time of arrival",
                                ].any()
                                else self._format_string_datetime(
                                    df.loc[
                                        df["Container number"]
                                        == o.container_number.container_number,
                                        "Port of Discharge actual time of arrival",
                                    ].values[0]
                                )
                            ),
                        )
                        vessel.save()
                        o.vessel_id = vessel
                        o.add_to_t49 = True
                    retrieval = o.retrieval_id
                    retrieval.temp_t49_lfd = (
                        self._format_string_datetime(
                            df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Current last free day at the POD terminal",
                            ].values[0]
                        )
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Current last free day at the POD terminal",
                        ].any()
                        else None
                    )
                    retrieval.temp_t49_available_for_pickup = (
                        True
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Available for pickup",
                        ].values[0]
                        == "Yes"
                        else False
                    )
                    retrieval.temp_t49_pod_arrive_at = (
                        self._format_string_datetime(
                            df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Port of Discharge arrival time",
                            ].values[0],
                            "datetime",
                        )
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Port of Discharge arrival time",
                        ].any()
                        else None
                    )
                    retrieval.temp_t49_pod_discharge_at = (
                        self._format_string_datetime(
                            df.loc[
                                df["Container number"]
                                == o.container_number.container_number,
                                "Port of Discharge discharged event",
                            ].values[0],
                            "datetime",
                        )
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Port of Discharge discharged event",
                        ].any()
                        else None
                    )
                    retrieval.temp_t49_hold_status = (
                        True
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Holds at POD status (0)",
                        ].values[0]
                        == "Hold"
                        else False
                    )
                    retrieval.master_bill_of_lading = (
                        df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Shipment number",
                        ].values[0]
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Shipment number",
                        ].any()
                        else None
                    )
                    retrieval.origin_port = (
                        df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Port of Lading",
                        ].values[0]
                        if df.loc[
                            df["Container number"]
                            == o.container_number.container_number,
                            "Port of Lading",
                        ].any()
                        else None
                    )
                    retrieval.shipping_line = o.vessel_id.shipping_line
                    updated_retrievals.append(retrieval)
                    cnt += 1
                if (
                    o.order_type == "直送"
                    or order_pl_count.get(o.container_number.container_number, 0) > 0
                ):
                    o.packing_list_updloaded = True
                updated_orders.append(o)
            bulk_update_with_history(
                updated_orders,
                Order,
                fields=["add_to_t49", "packing_list_updloaded", "vessel_id"],
            )
            bulk_update_with_history(
                updated_retrievals,
                Retrieval,
                fields=[
                    "temp_t49_lfd",
                    "temp_t49_available_for_pickup",
                    "temp_t49_pod_arrive_at",
                    "temp_t49_pod_discharge_at",
                    "temp_t49_hold_status",
                    "master_bill_of_lading",
                    "origin_port",
                    "shipping_line",
                ],
            )
            context["vessel_pl_data_update_success"] = True
            context["orders_count"] = cnt
        return self.template_1, context

    def update_pl_status(self) -> tuple[Any, Any]:
        orders = Order.objects.select_related(
            "customer_name", "container_number", "vessel_id", "retrieval_id"
        ).filter(created_at__gte="2024-07-01")

        order_pl_count = (
            Order.objects.select_related("container_number")
            .filter(models.Q(created_at__gte="2024-07-01"))
            .values("container_number__container_number")
            .annotate(
                n_pl=models.Count("container_number__packinglist__id", distinct=True),
            )
        )
        order_pl_count = {
            o["container_number__container_number"]: o["n_pl"] for o in order_pl_count
        }
        cnt = 0
        orders_updated = []
        for o in orders:
            cnt += 1
            if order_pl_count.get(o.container_number.container_number, 0) > 0:
                o.packing_list_updloaded = True
                orders_updated.append(o)
            else:
                o.packing_list_updloaded = False
        bulk_update_with_history(
            orders_updated, Order, fields=["packing_list_updloaded"]
        )
        context = {"order_packing_list_updloaded_updated": True, "count": cnt}
        return self.template_1, context

    def update_warehouse(self) -> tuple[Any, Any]:
        orders = Order.objects.select_related("warehouse").filter(
            models.Q(warehouse__name="SAV-31419")
        )
        warehouse = ZemWarehouse.objects.get(name="SAV-31326")
        orders_updated = []
        cnt = 0
        for o in orders:
            o.warehouse = warehouse
            orders_updated.append(o)
            cnt += 1
        bulk_update_with_history(orders_updated, Order, fields=["warehouse"])
        context = {
            "warehouse_updated": True,
            "count": cnt,
        }
        return self.template_1, context

    def update_order_eta(self) -> tuple[Any, Any]:
        orders = Order.objects.select_related("vessel_id").filter(
            models.Q(vessel_id__isnull=False, eta__isnull=True)
        )
        orders_updated = []
        cnt = 0
        for o in orders:
            o.eta = o.vessel_id.vessel_eta
            orders_updated.append(o)
            cnt += 1
        bulk_update_with_history(orders_updated, Order, fields=["eta"])
        context = {
            "order_eta_updated": True,
            "count": cnt,
        }
        return self.template_1, context

    def update_shipment(self) -> tuple[Any, Any]:
        shipment = Shipment.objects.all()
        shipment_updated = []
        cnt = 0
        for s in shipment:
            s.batch = 0
            if s.is_shipped:
                s.is_full_out = True
                s.pallet_dumpped = 0
            else:
                s.is_full_out = False
                s.pallet_dumpped = 0
            shipment_updated.append(s)
            cnt += 1
        bulk_update_with_history(
            shipment_updated,
            Shipment,
            fields=["is_full_out", "pallet_dumpped", "batch"],
        )
        context = {
            "shipment_updated": True,
            "count": cnt,
        }
        return self.template_1, context

    def update_pallet(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = Pallet.objects.select_related(
            "container_number",
            "shipment_batch_number",
        ).filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
        )
        packing_list = PackingList.objects.select_related(
            "shipment_batch_number", "container_number"
        ).filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
        )
        p_s_mapping = {
            f"{pl.container_number.container_number.strip()}-{pl.destination.strip()}-{pl.delivery_method.strip()}-{pl.note if pl.note else ''}": pl.shipment_batch_number
            for pl in packing_list
        }
        updated_pallet = []
        cnt = 0
        for p in pallet:
            k = f"{p.container_number.container_number.strip()}-{p.destination.strip()}-{p.delivery_method.strip()}-{p.note if p.note else ''}"
            if p_s_mapping.get(k):
                p.shipment_batch_number = p_s_mapping.get(k)
                cnt += 1
                updated_pallet.append(p)
        bulk_update_with_history(
            updated_pallet, Pallet, fields=["shipment_batch_number"]
        )
        context = {
            "pallet_updated": True,
            "count": cnt,
            "pallet_update_start_date": start_date,
            "pallet_update_end_date": end_date,
        }
        return self.template_1, context

    def update_shipment_stats(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        shipment = Shipment.objects.filter(
            shipment_schduled_at__gte=start_date,
            shipment_schduled_at__lte=end_date,
        )
        shipment_stats_1 = (
            Pallet.objects.select_related("shipment_batch_number", "container_number")
            .filter(
                shipment_batch_number__shipment_schduled_at__gte=start_date,
                shipment_batch_number__shipment_schduled_at__lte=end_date,
                container_number__order__offload_id__offload_at__isnull=False,
            )
            .values("shipment_batch_number__shipment_batch_number")
            .annotate(
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet=Count("pallet_id", distinct=True),
            )
        )
        shipment_stats_1 = {
            s.get("shipment_batch_number__shipment_batch_number"): [
                s.get("total_pcs"),
                s.get("total_cbm"),
                s.get("total_weight_lbs"),
                s.get("total_n_pallet"),
            ]
            for s in shipment_stats_1
        }
        shipment_stats_2 = (
            PackingList.objects.select_related(
                "shipment_batch_number", "container_number"
            )
            .filter(
                shipment_batch_number__shipment_schduled_at__gte=start_date,
                shipment_batch_number__shipment_schduled_at__lte=end_date,
                container_number__order__offload_id__offload_at__isnull=True,
            )
            .values("shipment_batch_number__shipment_batch_number")
            .annotate(
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                total_n_pallet=Sum("cbm", output_field=FloatField()) / 2,
            )
        )
        shipment_stats_2 = {
            s.get("shipment_batch_number__shipment_batch_number"): [
                s.get("total_pcs"),
                s.get("total_cbm"),
                s.get("total_weight_lbs"),
                (
                    1
                    if s.get("total_n_pallet") < 1
                    else (
                        s.get("total_n_pallet") // 1 + 1
                        if s.get("total_n_pallet") % 1 >= 0.45
                        else s.get("total_n_pallet") // 1
                    )
                ),
            ]
            for s in shipment_stats_2
        }
        cnt = 0
        updated_shipment = []
        for s in shipment:
            cnt += 1
            pcs, cbm, weight, n = 0, 0, 0, 0
            if s.shipment_batch_number in shipment_stats_1:
                pcs += shipment_stats_1[s.shipment_batch_number][0]
                cbm += shipment_stats_1[s.shipment_batch_number][1]
                weight += shipment_stats_1[s.shipment_batch_number][2]
                n += shipment_stats_1[s.shipment_batch_number][3]
            if s.shipment_batch_number in shipment_stats_2:
                pcs += shipment_stats_2[s.shipment_batch_number][0]
                cbm += shipment_stats_2[s.shipment_batch_number][1]
                weight += shipment_stats_2[s.shipment_batch_number][2]
                n += shipment_stats_2[s.shipment_batch_number][3]
            s.total_pcs = pcs
            s.total_cbm = cbm
            s.total_weight = weight
            s.total_pallet = n
            updated_shipment.append(s)
        bulk_update_with_history(
            updated_shipment,
            Shipment,
            fields=["total_pcs", "total_cbm", "total_weight", "total_pallet"],
        )
        context = {
            "shipment_stats_updated": True,
            "count": cnt,
            "shipment_start_date": start_date,
            "shipment_end_date": end_date,
        }
        return self.template_1, context

    def update_inventory(self, request: HttpRequest) -> tuple[Any, Any]:
        form = UploadFileForm(request.POST, request.FILES)
        warehouse = request.POST.get("warehouse")
        pallet_end_date = request.POST.get("pallet_end_date")
        current_datetime = datetime.now()
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_csv(file)
            container_number = df["container"].tolist()
            pallet = (
                Pallet.objects.select_related("shipment_batch_number")
                .filter(
                    models.Q(
                        models.Q(location=warehouse)
                        | models.Q(container_number__order__warehouse__name=warehouse)
                    ),
                    shipment_batch_number__isnull=True,
                    container_number__order__offload_id__offload_at__lte=pallet_end_date,
                )
                .exclude(container_number__container_number__in=container_number)
            )
            cnt = len(pallet)
            if pallet:
                shipment = Shipment(
                    shipment_batch_number=f"库存盘点-{warehouse}-{current_datetime}",
                    origin=warehouse,
                    is_shipped=True,
                    shipped_at=current_datetime,
                    is_full_out=True,
                    is_arrived=True,
                    arrived_at=current_datetime,
                    shipment_type="库存盘点",
                    in_use=False,
                    is_canceled=True,
                )
                shipment.save()
                pallet.update(shipment_batch_number=shipment)
        context = {
            "inventory_updated": True,
            "count": cnt,
            "pre_port_t49_tracking": UploadFileForm(),
        }
        return self.template_1, context

    def po_id_batch_generation(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        pallet = Pallet.objects.select_related("container_number").filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
            container_number__order__offload_id__offload_at__isnull=False,
        )
        packinglist = PackingList.objects.select_related("container_number").filter(
            container_number__order__created_at__gte=start_date,
            container_number__order__created_at__lte=end_date,
            # container_number__order__offload_id__offload_at__isnull=True,
        )
        po_id_hash = {}
        cnt = 0
        for p in pallet:
            po_id_seg = ""
            random_seg = "".join(
                random.choices(string.ascii_letters.upper() + string.digits, k=4)
            )
            container_number = p.container_number.container_number
            if p.delivery_method in ["暂扣留仓(HOLD)", "暂扣留仓"]:
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.fba_id}"
                po_id_seg = f"H{p.fba_id[-4:]}" if p.fba_id else f"H{random_seg}"
            elif p.delivery_method == "客户自提" or p.destination == "客户自提":
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.destination}-{p.shipping_mark}"
                po_id_seg = (
                    f"S{p.shipping_mark[-4:]}" if p.shipping_mark else f"S{random_seg}"
                )
            else:
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.destination}"
                po_id_seg = f"{DELIVERY_METHOD_CODE.get(p.delivery_method, 'UN')}{p.destination.replace(' ', '').split('-')[-1]}"

            if po_id_hkey in po_id_hash:
                po_id = po_id_hash.get(po_id_hkey)
            else:
                po_id = f"{container_number[-4:]}{po_id_seg}{''.join(random.choices(string.digits, k=2))}"
                po_id = re.sub(r"[\u4e00-\u9fff]", "", po_id)
                po_id_hash[po_id_hkey] = po_id
            p.PO_ID = po_id
            cnt += 1

        for p in packinglist:
            po_id_seg = ""
            random_seg = "".join(
                random.choices(string.ascii_letters.upper() + string.digits, k=4)
            )
            container_number = p.container_number.container_number
            if p.delivery_method in ["暂扣留仓(HOLD)", "暂扣留仓"]:
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.fba_id}"
                po_id_seg = f"H{p.fba_id[-4:]}" if p.fba_id else f"H{random_seg}"
            elif p.delivery_method == "客户自提" or p.destination == "客户自提":
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.destination}-{p.shipping_mark}"
                po_id_seg = (
                    f"S{p.shipping_mark[-4:]}" if p.shipping_mark else f"S{random_seg}"
                )
            else:
                po_id_hkey = f"{container_number}-{p.delivery_method}-{p.destination}"
                po_id_seg = f"{DELIVERY_METHOD_CODE.get(p.delivery_method, 'UN')}{p.destination.replace(' ', '').split('-')[-1]}"

            if po_id_hkey in po_id_hash:
                po_id = po_id_hash.get(po_id_hkey)
            else:
                po_id = f"{container_number[-4:]}{po_id_seg}{''.join(random.choices(string.digits, k=2))}"
                po_id = re.sub(r"[\u4e00-\u9fff]", "", po_id)
                po_id_hash[po_id_hkey] = po_id
            p.PO_ID = po_id
            cnt += 1
        bulk_update_with_history(pallet, Pallet, fields=["PO_ID"])
        bulk_update_with_history(packinglist, PackingList, fields=["PO_ID"])
        context = {
            "start_date": start_date,
            "end_date": end_date,
            "count": cnt,
            "po_id_updated": True,
        }
        return self.template_1, context

    def update_shipment_ts_utc(self, request: HttpRequest) -> tuple[Any, Any]:
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        cnt = 0
        shipment = Shipment.objects.filter(
            shipment_appointment__gte=start_date, shipment_appointment__lte=end_date
        )
        for s in shipment:
            tzinfo = self._parse_tzinfo(s.origin)
            if s.shipment_appointment:
                try:
                    s.shipment_appointment_utc = self._parse_ts(
                        s.shipment_appointment, tzinfo
                    )
                except Exception as e:
                    raise ValueError(
                        f"{e}: {s.shipment_appointment} - {tzinfo} - {s.origin}"
                    )
            if s.shipped_at:
                s.shipped_at_utc = self._parse_ts(s.shipped_at, tzinfo)
            if s.arrived_at:
                s.arrived_at_utc = self._parse_ts(s.arrived_at, tzinfo)
            cnt += 1
        bulk_update_with_history(
            shipment,
            Shipment,
            fields=["shipment_appointment_utc", "shipped_at_utc", "arrived_at_utc"],
        )
        context = {
            "start_date": start_date,
            "end_date": end_date,
            "count": cnt,
            "update_shipment_ts_utc_updated": True,
        }
        return self.template_1, context

    def _format_string_datetime(
        self, datetime_str: str, datetime_part: str = "date"
    ) -> str | None:
        if not datetime_str:
            return None
        datetime_obj = datetime.fromisoformat(datetime_str)
        if datetime_part == "date":
            return datetime_obj.strftime("%Y-%m-%d")
        else:
            return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    def _parse_tzinfo(self, s: str) -> str:
        if not s:
            return "America/New_York"
        if "NJ" in s.upper():
            return "America/New_York"
        elif "SAV" in s.upper():
            return "America/New_York"
        elif "LA" in s.upper():
            return "America/Los_Angeles"
        else:
            return "America/New_York"

    def _parse_ts(self, ts: Any, tzinfo: str) -> str:
        if isinstance(ts, str):
            ts_naive = datetime.fromisoformat(ts)
        else:
            ts_naive = ts.replace(tzinfo=None)
        tz = pytz.timezone(tzinfo)
        ts = tz.localize(ts_naive).astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
