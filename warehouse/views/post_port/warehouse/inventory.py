import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import CharField, Count, F, FloatField, IntegerField, Sum
from django.db.models.functions import Cast
from django.http import Http404, HttpRequest, HttpResponse, HttpResponseForbidden 
from django.shortcuts import redirect, render
from django.views import View
from django.contrib.auth.models import User
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.forms.upload_file import UploadFileForm
from warehouse.models.container import Container
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.transfer_location import TransferLocation
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS


class Inventory(View):
    template_inventory_management_main = (
        "post_port/inventory/01_inventory_management_main.html"
    )
    template_inventory_po_update = "post_port/inventory/02_inventory_po_update.html"
    template_counting_main = "post_port/inventory/01_inventory_count_main.html"
    template_inventory_list_and_upload = (
        "post_port/inventory/01_1_inventory_list_and_upload.html"
    )
    template_inventory_list_and_counting = (
        "post_port/inventory/01_2_inventory_list_and_counting.html"
    )
    warehouse_options = {
        "": "",
        "NJ-07001": "NJ-07001",
        "NJ-08817": "NJ-08817",
        "SAV-31326": "SAV-31326",
        "LA-91761": "LA-91761",
        "MO-62025": "MO-62025",
        "HX-77503": "HX-77503",
    }

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        if self.validate_user_staff(request.user):
            step = request.GET.get("step")
            if step == "counting":
                template, context = await self.handle_counting_get()
                return render(request, template, context)
            else:
                template, context = await self.handle_inventory_management_get()
                return render(request, template, context)
        else:
            return HttpResponseForbidden(
                "You are not authenticated to access this page!"
            )
        

    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "upload_counting_data":
            template, context = await self.handle_upload_counting_data_post(request)
            return render(request, template, context)
        elif step == "confirm_counting":
            template, context = await self.handle_confirm_counting_post(request)
            return render(request, template, context)
        elif step == "download_counting_template":
            return await self.handle_download_counting_template_post()
        elif step == "repalletize":
            template, context = await self.handle_repalletize_post(request)
            return render(request, template, context)
        elif step == "update_po_page":
            template, context = await self.handle_update_po_page_post(request)
            return render(request, template, context)
        elif step == "update_po":
            template, context = await self.handle_update_po_post(request)
            return render(request, template, context)
        elif step == "counting":
            template, context = await self.handle_counting_post(request)
            return render(request, template, context)
        elif step == "transfer_warehouse":
            template, context = await self.handle_transfer_location_post(request)
            return render(request, template, context)
        else:
            raise ValueError(f"Unknown step {request.POST.get('step')}")

    async def handle_counting_get(self) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_counting_main, context

    async def handle_inventory_management_get(self) -> tuple[str, dict[str, Any]]:
        context = {"warehouse_options": self.warehouse_options}
        return self.template_inventory_management_main, context

    async def handle_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        pallet = await self._get_inventory_pallet(warehouse)
        pallet_json = {}
        pallet_json = {
            p.get("plt_ids"): {
                k: (
                    round(v, 2)
                    if isinstance(v, float) or isinstance(v, int)
                    else (
                        re.sub(r'[\x00-\x1F\x7F\t"\']', " ", v)
                        if v != "None" and v
                        else ""
                    )
                )
                for k, v in p.items()
            }
            for p in pallet
        }
        total_cbm = sum([p.get("cbm") for p in pallet])
        total_pallet = sum([p.get("n_pallet") for p in pallet])
        context = {
            "warehouse": warehouse,
            "warehouse_options": self.warehouse_options,
            "delivery_method_options": DELIVERY_METHOD_OPTIONS,
            "pallet": pallet,
            "total_cbm": total_cbm,
            "total_pallet": total_pallet,
            "pallet_json": json.dumps(pallet_json, ensure_ascii=False),
        }
        return self.template_inventory_management_main, context

    async def handle_upload_counting_data_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            _, context = await self.handle_counting_warehouse_post(request)
            pallet = context["pallet"]
            df_sys_inv = pd.DataFrame(pallet)
            df_sys_inv = df_sys_inv.rename(
                columns={"container_number__container_number": "container_number"}
            )
            df = df.merge(
                df_sys_inv,
                on=["container_number", "destination"],
                how="outer",
                suffixes=["_act", "_sys"],
            )
            df = df.fillna(0)
            context["inventory_data"] = df.to_dict("records")
            context["total_pallet_cnt"] = df["n_pallet_act"].sum()
            return self.template_inventory_list_and_counting, context

    async def handle_confirm_counting_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        container_numbers = request.POST.get("container_number")
        destinations = request.POST.get("destination")
        n_pallet_act = request.POST.get("n_pallet_act")
        n_pallet_sys = request.POST.get("n_pallet_sys")
        pallet_ids = request.POST.get("pallet_ids")
        raise ValueError(request.POST)

    async def handle_download_counting_template_post(self) -> HttpResponse:
        file_path = (
            Path(__file__)
            .parent.parent.parent.parent.resolve()
            .joinpath("templates/export_file/inventory_counting_template.xlsx")
        )
        if not os.path.exists(file_path):
            raise Http404("File does not exist")
        with open(file_path, "rb") as file:
            response = HttpResponse(
                file.read(),
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = (
                f'attachment; filename="inventory_counting_template.xlsx"'
            )
            return response

    async def handle_repalletize_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        plt_ids = request.POST.get("plt_ids")
        plt_ids = [int(i) for i in plt_ids.split(",")]
        old_pallet = await sync_to_async(list)(Pallet.objects.filter(id__in=plt_ids))
        container_number = request.POST.get("container")
        container = await sync_to_async(Container.objects.get)(
            container_number=container_number
        )
        total_weight = float(request.POST.get("weight"))
        total_cbm = float(request.POST.get("cbm"))
        total_pcs = int(request.POST.get("pcs"))
        warehouse = request.POST.get("warehouse").upper().strip()
        # data of new pallets
        destinations = request.POST.getlist("destination_repalletize")
        delivery_methods = request.POST.getlist("delivery_method_repalletize")
        addresses = request.POST.getlist("address_repalletize")
        zipcodes = request.POST.getlist("zipcode_repalletize")
        shipping_marks = request.POST.getlist("shipping_mark_repalletize")
        fba_ids = request.POST.getlist("fba_id_repalletize")
        ref_ids = request.POST.getlist("ref_id_repalletize")
        pcses = request.POST.getlist("pcs_repalletize")
        n_pallets = request.POST.getlist("n_pallet_repalletize")
        notes = request.POST.getlist("note_repalletize")
        pcses = [int(i) for i in pcses]
        n_pallets = [int(i) for i in n_pallets]
        # create new pallets
        new_pallets = []
        old_po_id = old_pallet[0].PO_ID
        seq_num = 1
        for dest, dm, addr, zipcode, sm, fba, ref, p, n, note in zip(
            destinations,
            delivery_methods,
            addresses,
            zipcodes,
            shipping_marks,
            fba_ids,
            ref_ids,
            pcses,
            n_pallets,
            notes,
        ):
            # TODOs: find a better way to allocate cbm and weight
            new_pallets += [
                {
                    "pallet_id": str(
                        uuid.uuid3(
                            uuid.NAMESPACE_DNS, str(uuid.uuid4()) + dest + dm + str(i)
                        )
                    ),
                    "container_number": container,
                    "destination": dest,
                    "address": addr,
                    "zipcode": zipcode,
                    "delivery_method": dm,
                    "pcs": p,
                    "cbm": total_cbm * p / total_pcs,
                    "weight_lbs": total_weight * p / total_pcs,
                    "note": note,
                    "shipping_mark": sm if sm else "",
                    "fba_id": fba if fba else "",
                    "ref_id": ref if ref else "",
                    "location": old_pallet[0].location,
                    "PO_ID": f"{old_po_id}_{seq_num}",
                }
                for i in range(n)
            ]
            seq_num += 1
        instances = [Pallet(**p) for p in new_pallets]
        await sync_to_async(bulk_create_with_history)(instances, Pallet)
        # await sync_to_async(Pallet.objects.bulk_create)(
        #     Pallet(**p) for p in new_pallets
        # )
        # delete old pallets
        await sync_to_async(Pallet.objects.filter(id__in=plt_ids).delete)()
        return await self.handle_warehouse_post(request)

    async def handle_update_po_page_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        plt_ids = request.POST.get("plt_ids")
        plt_ids = [int(i) for i in plt_ids.split(",")]
        # pallet = await sync_to_async(list)(Pallet.objects.select_related("container_number").filter(id__in=plt_ids))
        pallet = await self._get_inventory_pallet(warehouse, models.Q(id__in=plt_ids))
        container_number = pallet[0].get("container")
        shipping_mark = pallet[0].get("shipping_mark")
        fba_id = pallet[0].get("fba_id")
        ref_id = pallet[0].get("ref_id")
        shipping_mark = shipping_mark.split(",") if shipping_mark else ""
        fba_id = fba_id.split(",") if fba_id else ""
        ref_id = ref_id.split(",") if ref_id else ""
        criteria = models.Q()
        if shipping_mark:
            criteria &= models.Q(shipping_mark__in=shipping_mark)
        if fba_id:
            criteria &= models.Q(fba_id__in=fba_id)
        if ref_id:
            criteria &= models.Q(ref_id__in=ref_id)
        packing_list = await sync_to_async(list)(PackingList.objects.filter(criteria))
        pl_ids = ",".join([str(pl.id) for pl in packing_list])
        context = {
            "packing_list": packing_list,
            "pallet": pallet[0],
            "warehouse": warehouse,
            "delivery_method_options": DELIVERY_METHOD_OPTIONS,
            "plt_ids": ",".join([str(i) for i in plt_ids]),
            # "pl_ids": pl_ids,
        }
        return self.template_inventory_po_update, context

    async def handle_update_po_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        plt_ids = request.POST.get("plt_ids")
        plt_ids = [int(i) for i in plt_ids.split(",")]
        pl_ids = request.POST.getlist("pl_ids")
        pl_ids = [int(i) for i in pl_ids]
        destination_new = request.POST.get("destination").strip()
        address_new = request.POST.get("address").strip()
        zipcode_new = request.POST.get("zipcode").strip()
        delivery_method_new = request.POST.get("delivery_method")
        location_new = request.POST.get("location")
        note_new = request.POST.get("note").strip()
        shipping_mark = request.POST.getlist("shipping_mark")
        fba_id = request.POST.getlist("fba_id")
        ref_id = request.POST.getlist("ref_id")
        shipping_mark_new = request.POST.getlist("shipping_mark_new")
        fba_id_new = request.POST.getlist("fba_id_new")
        ref_id_new = request.POST.getlist("ref_id_new")
        pallet = await sync_to_async(list)(Pallet.objects.filter(id__in=plt_ids))
        packing_list = await sync_to_async(list)(
            PackingList.objects.filter(id__in=pl_ids)
        )
        data_old = [
            pallet[0].destination,
            pallet[0].address,
            pallet[0].zipcode,
            pallet[0].delivery_method,
            pallet[0].location,
            pallet[0].note,
        ]
        data_new = [
            destination_new,
            address_new,
            zipcode_new,
            delivery_method_new,
            location_new,
            note_new,
        ]
        if any(old != new for old, new in zip(data_old, data_new)):

            for p in pallet:
                p.destination = destination_new
                p.address = address_new
                p.zipcode = zipcode_new
                p.delivery_method = delivery_method_new
                p.location = location_new
                p.note = note_new
            for pl in packing_list:
                pl.destination = destination_new
                pl.address = address_new
                pl.zipcode = zipcode_new
                pl.delivery_method = delivery_method_new

            # await sync_to_async(Pallet.objects.bulk_update)(
            #     pallet,
            #     [
            #         "destination",
            #         "address",
            #         "zipcode",
            #         "delivery_method",
            #         "location",
            #         "note",
            #     ],
            # )
            await sync_to_async(bulk_update_with_history)(
                pallet,
                Pallet,
                fields=[
                    "destination",
                    "address",
                    "zipcode",
                    "delivery_method",
                    "location",
                    "note",
                ],
            )
            await sync_to_async(bulk_update_with_history)(
                packing_list,
                PackingList,
                fields=["destination", "address", "zipcode", "delivery_method"],
            )
        for pl_id, sm, fba, ref, sm_new, fba_new, ref_new in zip(
            pl_ids,
            shipping_mark,
            fba_id,
            ref_id,
            shipping_mark_new,
            fba_id_new,
            ref_id_new,
        ):
            if sm != sm_new or fba != fba_new or ref != ref_new:
                packing_list = await sync_to_async(PackingList.objects.get)(id=pl_id)
                packing_list.shipping_mark = sm_new
                packing_list.fba_id = fba_new
                packing_list.ref_id = ref_new
                for p in pallet:
                    p.shipping_mark = p.shipping_mark.replace(sm, sm_new)
                    p.fba_id = p.fba_id.replace(fba, fba_new)
                    p.ref_id = p.ref_id.replace(ref, ref_new)
                await sync_to_async(packing_list.save)()
            await sync_to_async(bulk_update_with_history)(
                pallet,
                Pallet,
                fields=["shipping_mark", "fba_id", "ref_id"],
            )
        return await self.handle_warehouse_post(request)

    async def handle_counting_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        plt_ids = request.POST.getlist("plt_ids")
        n_pallet = [int(i) for i in request.POST.getlist("n_pallet")]
        counted_n_pallet = [int(i) for i in request.POST.getlist("counted_n_pallet")]
        current_datetime = datetime.now()
        shipment = Shipment(
            shipment_batch_number=f"库存盘点-{warehouse}-{current_datetime.date()}",
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
        await sync_to_async(shipment.save)()
        updated_pallets = []
        for ids, n, n_counted in zip(plt_ids, n_pallet, counted_n_pallet):
            if n > n_counted:
                pallet_ids = [int(i) for i in ids.split(",")]
                pallet = await sync_to_async(list)(
                    Pallet.objects.filter(id__in=pallet_ids)
                )
                diff = n - n_counted
                for p in pallet[:diff]:
                    p.shipment_batch_number = shipment
                    updated_pallets.append(p)
        if updated_pallets:
            await sync_to_async(bulk_update_with_history)(
                updated_pallets,
                Pallet,
                fields=["shipment_batch_number"],
            )
        else:
            await sync_to_async(shipment.delete)()
        return await self.handle_warehouse_post(request)

    async def handle_transfer_location_post(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        shipping_warehouse = request.POST.get("warehouse")
        receiving_warehouse = request.POST.get("receiving_warehouse")
        shipping_time = request.POST.get("shipping_time", None)
        eta = request.POST.get("ETA", None)
        selected_orders = json.loads(request.POST.get("selectedOrders", "[]"))
        selected_orders = list(set(selected_orders))
        selectedIds = json.loads(request.POST.get("selectedIds", "[]"))
        selectedIds = list(set(selectedIds))
        ids = []

        for plt_ids in selectedIds:
            plt_ids = plt_ids.split(",")
            plt_ids = [int(i) for i in plt_ids]
            ids.extend(plt_ids)
        # 查找板子
        pallets = await sync_to_async(list)(
            Pallet.objects.select_related("container_number").filter(id__in=ids)
        )
        total_weight, total_cbm, total_pcs = 0.0, 0.0, 0
        for plt in pallets:
            plt.location = receiving_warehouse
            total_weight += plt.weight_lbs
            total_pcs += plt.pcs
            total_cbm += plt.cbm
        await sync_to_async(bulk_update_with_history)(
            pallets,
            Pallet,
            fields=["location"],
        )
        # 然后新建transfer_warehouse新记录
        current_time = datetime.now()
        batch_id = (
            str(uuid.uuid4())[:2].upper()
            + "-"
            + current_time.strftime("%m%d")
            + "-"
            + shipping_warehouse
        )
        batch_id = batch_id.replace(" ", "").upper()
        transfer_location = TransferLocation(
            **{
                "shipping_warehouse": shipping_warehouse,
                "receiving_warehouse": receiving_warehouse,
                "shipping_time": shipping_time,
                "ETA": eta,
                "batch_number": batch_id,
                "container_number": selected_orders,
                "plt_ids": ids,
                "total_pallet": len(pallets),
                "total_pcs": total_pcs,
                "total_cbm": total_cbm,
                "total_weight": total_weight,
            }
        )
        await sync_to_async(transfer_location.save)()
        mutable_get = request.GET.copy()
        mutable_get["warehouse"] = request.POST.get("warehouse")
        mutable_get["step"] = "cancel_notification"
        request.GET = mutable_get
        return await self.handle_warehouse_post(request)

    async def _get_inventory_pallet(
        self, warehouse: str, criteria: models.Q | None = None
    ) -> list[Pallet]:
        if criteria:
            criteria &= models.Q(location=warehouse)
            criteria &= models.Q(
                models.Q(shipment_batch_number__isnull=True)
                | models.Q(shipment_batch_number__is_shipped=False)
            )
        else:
            criteria = models.Q(
                models.Q(location=warehouse)
                & models.Q(
                    models.Q(shipment_batch_number__isnull=True)
                    | models.Q(shipment_batch_number__is_shipped=False)
                )
            )
        return await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "shipment_batch_number",
                "container_number__order__customer_name",
            )
            .filter(criteria)
            .annotate(str_id=Cast("id", CharField()))
            .values(
                "destination",
                "delivery_method",
                "shipping_mark",
                "fba_id",
                "ref_id",
                "note",
                "address",
                "zipcode",
                "location",
                customer_name=F("container_number__order__customer_name__zem_name"),
                container=F("container_number__container_number"),
                shipment=F("shipment_batch_number__shipment_batch_number"),
                appointment_id=F("shipment_batch_number__appointment_id"),
            )
            .annotate(
                # shipping_marks=StringAgg("shipping_mark", delimiter=",", distinct=True, ordering="shipping_mark"),
                # fba_ids=StringAgg("fba_id", delimiter=",", distinct=True, ordering="fba_id"),
                # ref_ids=StringAgg("ref_id", delimiter=",", distinct=True, ordering="ref_id"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                weight=Sum("weight_lbs", output_field=FloatField()),
                n_pallet=Count("pallet_id", distinct=True),
            )
            .order_by("-n_pallet")
        )

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
    
    def validate_user_invoice_preport(self, user: User) -> bool:
        if user.is_staff:
            return True
        else:
            return False
