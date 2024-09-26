import pytz
import uuid
import asyncio
from asgiref.sync import sync_to_async
from datetime import datetime
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect
from django.views import View
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, Min, FloatField, IntegerField, When, Count, Q
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template

from warehouse.models.retrieval import Retrieval
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list


class Palletization(View):
    template_main = "post_port/palletization/palletization.html"
    template_palletize = "post_port/palletization/palletization_packing_list.html"
    template_pallet_label = "export_file/pallet_label_template.html"

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        pk = kwargs.get("pk", None)
        step = request.GET.get("step", None)
        if step == "container_palletization":
            template, context = await self.handle_container_palletization_get(request, pk)
            return render(request, template, context)
        else:
            template, context = await self.handle_all_get()
            return render(request, template, context)
    
    async def post(self, request: HttpRequest, **kwargs) -> HttpRequest:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "warehouse":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "palletize":
            pk = kwargs.get("pk")
            template, context = await self.handle_packing_list_post(request, pk)
            return render(request, template, context)
        elif step == "back":
            template, context = await self.handle_warehouse_post(request)
            return render(request, template, context)
        elif step == "export_palletization_list":
            return await export_palletization_list(request)
        elif step == "export_pallet_label":
            return await self._export_pallet_label(request)
        elif step == "cancel":
            template, context = await self.handle_cancel_post(request)
            return render(request, template, context)
        else:
            return await self.get(request)
    
    async def handle_all_get(self, warehouse: str = None) -> tuple[str, dict[str, Any]]:
        if warehouse:
            warehouse = None if warehouse == "Empty" else warehouse
            order_not_palletized, order_palletized, order_with_shipment = await asyncio.gather(
                self._get_order_not_palletized(warehouse),
                self._get_order_palletized(warehouse),
                self._get_order_shipment(warehouse)
            )
            
            order_with_shipment = {
                o.get("container_number__container_number"): o.get("shipment_scheduled_time")
                for o in order_with_shipment
            }
            order_not_palletized = [
                o for o in order_not_palletized if o.container_number.container_number in order_with_shipment
            ] + [
                o for o in order_not_palletized if o.container_number.container_number not in order_with_shipment
            ]
            context = {
                "order_not_palletized": order_not_palletized,
                "order_palletized": order_palletized,
                "order_with_shipment": order_with_shipment,
                "warehouse_form": ZemWarehouseForm(initial={"name": warehouse}),
                "warehouse": warehouse,
            }
        else:
            context = {
                "warehouse_form": ZemWarehouseForm()
            }
        return self.template_main, context
    
    async def handle_container_palletization_get(self, request: HttpRequest, pk: int) -> tuple[str, dict[str, Any]]:
        order_selected = await sync_to_async(Order.objects.select_related("container_number", "warehouse", "offload_id").get)(pk=pk)
        container = order_selected.container_number
        offload = order_selected.offload_id
        order_packing_list = []
        if request.GET.get("step", None) == "container_palletization" and offload.offload_at is None:
            packing_list = await self._get_packing_list(container_number=container.container_number, status="non_palletized")
            context = {
                "status": "non_palletized",
            }
        else:
            packing_list = await self._get_packing_list(container_number=container.container_number, status="palletized")
            context = {
                "status": "palletized",
            }
        for pl in packing_list:
            pl_form = PackingListForm(initial={"n_pallet": pl["n_pallet"]})
            order_packing_list.append((pl, pl_form))
        context["warehouse"] = request.GET.get("warehouse", None)
        context["order_packing_list"] = order_packing_list
        return self.template_palletize, context
    
    async def handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("name")
        template, context = await self.handle_all_get(warehouse)
        return template, context

    async def handle_packing_list_post(self, request: HttpRequest, pk: int) -> tuple[str, dict[str, Any]]:
        order_selected = await sync_to_async(Order.objects.select_related("offload_id", "warehouse").get)(pk=pk)
        offload = order_selected.offload_id
        if not offload.offload_at:
            ids = request.POST.getlist("ids")
            ids = [i.split(",") for i in ids]
            n_pallet = [int(n) for n in request.POST.getlist("n_pallet")]
            cbm = [float(c) for c in request.POST.getlist("cbms")]
            total_pallet = sum(n_pallet)
            for i, n, c in zip(ids, n_pallet, cbm):
                await self._split_pallet(i, n, c, pk)
            cn = pytz.timezone('Asia/Shanghai')
            current_time_cn = datetime.now(cn)
            offload.total_pallet = total_pallet
            offload.offload_at = current_time_cn
            await sync_to_async(offload.save)()
            self._update_shipment_stats(ids)
        mutable_post = request.POST.copy()
        mutable_post['name'] = order_selected.warehouse.name
        request.POST = mutable_post
        return await self.handle_warehouse_post(request)
        
    async def handle_cancel_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        container_number = request.POST.get("container_number")
        order = await sync_to_async(Order.objects.select_related("offload_id", "warehouse").get)(
            container_number__container_number=container_number
        )
        offload = order.offload_id
        offload.total_pallet = None
        offload.offload_at = None
        try:
            offload.devanning_company = None
            offload.devanning_fee = None
        except:
            pass
        await sync_to_async(Pallet.objects.filter(
            packing_list__container_number__container_number=container_number
        ).delete)()
        await sync_to_async(offload.save)()
        mutable_post = request.POST.copy()
        mutable_post['name'] = order.warehouse.name
        request.POST = mutable_post
        return await self.handle_warehouse_post(request)

    async def _export_pallet_label(self, request: HttpRequest) -> HttpResponse:
        container_number = request.POST.get("container_number")
        customer_name = request.POST.get("customer_name")
        status = request.POST.get("status")
        retrieval = await sync_to_async(Retrieval.objects.get)(
            order__container_number__container_number=container_number
        )
        retrieval_date = retrieval.target_retrieval_timestamp
        if retrieval_date:
            retrieval_date = retrieval_date.date()
        else:
            retrieval_date = datetime.now().date()
        retrieval_date = retrieval_date.strftime("%m/%d")
        packing_list = await self._get_packing_list(container_number=container_number, status=status)
        data = []
        for pl in packing_list:
            cbm = pl.get("cbm")
            remainder = cbm % 1
            cbm = int(cbm)
            if cbm%2:
                cbm += (cbm%2)
            elif remainder:
                cbm += 2
            data += [{
                "container_number": pl.get("container_number__container_number"),
                "destination": pl.get("destination"),
                "date": retrieval_date,
                "customer": customer_name,
                "hold": ("暂扣留仓" in pl.get("custom_delivery_method").split("-")[0]),
            }] * cbm
        context = {"data": data}
        template = get_template(self.template_pallet_label)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response['Content-Disposition'] = f'attachment; filename="pallet_label_{container_number}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response

    async def _split_pallet(self, ids: list[Any], n: int, c: float, pk: int) -> None:
        if n == 0 or n is None:
            return
        pallet_ids = [
            str(uuid.uuid3(uuid.NAMESPACE_DNS, str(uuid.uuid4()) + str(pk) + str(i))) for i in range(n)
        ]
        pallet_vol = [round(c / float(n), 2) for _ in range(n)]
        pallet_vol[-1] += (c - sum(pallet_vol))
        while (pallet_vol[-1] <= 0) & (len(pallet_vol) > 0):
            remaining = pallet_vol.pop()
            pallet_vol[-1] += remaining
        ids = [int(i) for i in ids]
        packing_list = await sync_to_async(list)(PackingList.objects.filter(id__in=ids))
        i = 0
        pallet_data = []
        for pl in packing_list:
            pcs_total = 0
            weight_total = 0
            pl_cbm = pl.cbm
            pl_total_weight = pl.total_weight_lbs if pl.total_weight_lbs else 0
            while pl_cbm > 1e-10:
                pcs_loaded = 0
                cbm_loaded = 0
                weight_loaded = 0
                if pallet_vol[i] == 0:
                    i += 1
                if pl_cbm - pallet_vol[i] <= 1e-10:
                    pallet_vol[i] -= pl_cbm
                    cbm_loaded += pl_cbm
                    pcs_loaded = int(pl.pcs * cbm_loaded / pl.cbm)
                    pcs_total += pcs_loaded
                    pcs_loaded += pl.pcs - pcs_total
                    weight_loaded = round(pl_total_weight * cbm_loaded / pl.cbm, 2)
                    weight_total += weight_loaded
                    weight_loaded += pl_total_weight - weight_total
                    pl_cbm = 0
                else:
                    pl_cbm -= pallet_vol[i]
                    cbm_loaded += pallet_vol[i]
                    pcs_loaded = int(pl.pcs * cbm_loaded / pl.cbm)
                    pcs_total += pcs_loaded
                    weight_loaded = round(pl_total_weight * cbm_loaded / pl.cbm, 2)
                    weight_total += weight_loaded
                    pallet_vol[i] = 0
                pallet_data.append({
                    "packing_list": pl,
                    "pallet_id": pallet_ids[i],
                    "pcs": pcs_loaded,
                    "cbm": cbm_loaded,
                    "weight_lbs": weight_loaded,
                })
        await sync_to_async(Pallet.objects.bulk_create)([
            Pallet(**d) for d in pallet_data
        ])

    async def _update_shipment_stats(self, ids: list[Any]) -> None:
        ids = [int(j) for i in ids for j in i]
        shipment_stats = await sync_to_async(list)(PackingList.objects.select_related(
            "shipment_batch_number", "pallet"
        ).filter(
            models.Q(id__in=ids) &
            models.Q(shipment_batch_number__isnull=False)
        ).values(
            "shipment_batch_number__shipment_batch_number"
        ).annotate(
            total_pcs=Sum("pallet__pcs", output_field=IntegerField()),
            total_cbm=Sum("pallet__cbm", output_field=FloatField()),
            weight_lbs=Sum("pallet__weight_lbs", output_field=FloatField()),
            total_n_pallet=Count('pallet__pallet_id', distinct=True, output_field=IntegerField()),
        ))
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related("shipment_batch_number").filter(id__in=ids)
        )
        shipment_list = set([pl.shipment_batch_number for pl in packing_list if pl.shipment_batch_number])
        shipment_stats = {
            s["shipment_batch_number__shipment_batch_number"]: {
                "total_pcs": s["total_pcs"],
                "total_cbm": s["total_cbm"],
                "weight_lbs": s["weight_lbs"],
                "total_n_pallet": s["total_n_pallet"],
            } for s in shipment_stats
        }
        for s in shipment_list:
            s.total_cbm = shipment_stats[s.shipment_batch_number]["total_cbm"]
            s.total_pallet = shipment_stats[s.shipment_batch_number]["total_n_pallet"]
            s.total_weight = shipment_stats[s.shipment_batch_number]["weight_lbs"]
            s.total_pcs = shipment_stats[s.shipment_batch_number]["total_pcs"]
        await sync_to_async(Shipment.objects.bulk_update)(shipment_list, ["total_cbm", "total_pallet", "total_weight", "total_pcs"])

    async def _get_packing_list(self, container_number:str, status: str) -> PackingList:
        if status == "non_palletized":
            return await sync_to_async(list)(PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(container_number__container_number=container_number).annotate(
                custom_delivery_method=Case(
                    When(Q(delivery_method='暂扣留仓(HOLD)') | Q(delivery_method='暂扣留仓'), then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                    default=F('delivery_method'),
                    output_field=CharField()
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
            ).values(
                "container_number__container_number", "destination", "address", "custom_delivery_method", "note"
            ).annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count('pallet__pallet_id', distinct=True)
            ).order_by("-cbm"))
        elif status == "palletized":
            return await sync_to_async(list)(PackingList.objects.select_related(
                "container_number", "pallet"
            ).filter(container_number__container_number=container_number).annotate(
                custom_delivery_method=Case(
                    When(Q(delivery_method='暂扣留仓(HOLD)') | Q(delivery_method='暂扣留仓'), then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                    default=F('delivery_method'),
                    output_field=CharField()
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
            ).values(
                "container_number__container_number", "destination", "address", "custom_delivery_method", "note"
            ).annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
                ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                pcs=Sum("pallet__pcs", output_field=IntegerField()),
                cbm=Sum("pallet__cbm", output_field=FloatField()),
                n_pallet=Count('pallet__pallet_id', distinct=True)
            ).order_by("-cbm"))
        else:
            raise ValueError(f"invalid status: {status}")

    async def _get_order_not_palletized(self, warehouse: str) -> Order:
        return await sync_to_async(list)(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "offload_id", "warehouse"
        ).filter(
            models.Q(
                warehouse__name=warehouse,
                offload_id__offload_required=True,
                offload_id__offload_at__isnull=True,
                created_at__gte='2024-07-01',
                cancel_notification=False,
            )
            # & (models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) | models.Q(retrieval_id__retrive_by_zem=False))
        ).order_by("retrieval_id__arrive_at"))
    
    async def _get_order_palletized(self, warehouse: str) -> Order:
        return await sync_to_async(list)(Order.objects.select_related(
            "customer_name", "container_number", "retrieval_id", "offload_id", "warehouse"
        ).filter(
            models.Q(
                warehouse__name=warehouse,
                offload_id__offload_required=True,
                offload_id__offload_at__isnull=False,
                cancel_notification=False,
                # retrieval_id__actual_retrieval_timestamp__isnull=False,
            )
        ).order_by("offload_id__offload_at"))
    
    async def _get_order_shipment(self, warehouse: str) -> Order:
        return await sync_to_async(list)(PackingList.objects.prefetch_related(
                "container_number", "container_number__order", "container_number__order__warehouse",
                "shipment_batch_number"
            ).filter(models.Q(
                container_number__order__warehouse__name=warehouse,
                shipment_batch_number__isnull=False
            )).values(
               "container_number__container_number" 
            ).annotate(
                shipment_scheduled_time=Min("shipment_batch_number__shipment_appointment")
            )
        )
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False