import pytz
import uuid
from datetime import datetime
from typing import Any
from xhtml2pdf import pisa

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views import View
from django.utils.decorators import method_decorator
from django.db import models
from django.db.models import Case, Value, CharField, F, Sum, FloatField, IntegerField, When, Count
from django.db.models.functions import Concat, Cast
from django.contrib.postgres.aggregates import StringAgg
from django.template.loader import get_template

from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list

@method_decorator(login_required(login_url='login'), name='dispatch')
class Palletization(View):
    template_main = "palletization.html"
    template_palletize = "palletization_packing_list.html"
    template_pallet_label = "export_file/pallet_label_template.html"
    context: dict[str, Any] = {}
    warehouse_form = ZemWarehouseForm()
    order_not_palletized: Order | Any = None
    order_palletized: Order | Any = None
    order_packing_list: list[PackingList | Any] = []
    step: int | Any = None

    def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        self._set_context()
        pk = kwargs.get("pk", None)
        step = request.GET.get("step", None)
        if pk:
            self.handle_packing_list_get(request, pk, step)
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
        elif step == "back":
            self.handle_warehouse_post(request)
        elif step == "export_palletization_list":
            return export_palletization_list(request)
        elif step == "export_pallet_label":
            return self._export_pallet_label(request)
        elif step == "cancel":
            self.handle_cancel_post(request)
        else:
            raise ValueError(f"{request.POST}")
        return self.get(request)
    
    def handle_packing_list_get(self, request: HttpRequest, pk: int, step: str) -> None:
        order_selected = Order.objects.get(pk=pk)
        container = order_selected.container_number
        if step == "new":
            packing_list = self._get_packing_list(container_number=container.container_number, status="non_palletized")
            # packing_list = PackingList.objects.filter(container_number__container_number=container.container_number).annotate(
            #     custom_delivery_method=Case(
            #         When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
            #         default=F('delivery_method'),
            #         output_field=CharField()
            #     ),
            #     str_id=Cast("id", CharField()),
            #     str_fba_id=Cast("fba_id", CharField()),
            #     str_ref_id=Cast("ref_id", CharField()),
            # ).values(
            #     "container_number__container_number", "destination", "address", "custom_delivery_method"
            # ).annotate(
            #     fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
            #     ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
            #     ids=StringAgg("str_id", delimiter=",", distinct=True),
            #     pcs=Sum("pcs", output_field=IntegerField()),
            #     cbm=Sum("cbm", output_field=FloatField()),
            #     n_pallet=Count('pallet__pallet_id', distinct=True)
            # ).order_by("-cbm")
        elif step == "complete":
            packing_list = self._get_packing_list(container_number=container.container_number, status="palletized")
            # packing_list = PackingList.objects.filter(container_number__container_number=container.container_number).annotate(
            #     custom_delivery_method=Case(
            #         When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
            #         default=F('delivery_method'),
            #         output_field=CharField()
            #     ),
            #     str_id=Cast("id", CharField()),
            #     str_fba_id=Cast("fba_id", CharField()),
            #     str_ref_id=Cast("ref_id", CharField()),
            # ).values(
            #     "container_number__container_number", "destination", "address", "custom_delivery_method"
            # ).annotate(
            #     fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
            #     ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
            #     ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
            #     pcs=Sum("pallet__pcs", output_field=IntegerField()),
            #     cbm=Sum("pallet__cbm", output_field=FloatField()),
            #     n_pallet=Count('pallet__pallet_id', distinct=True)
            # ).order_by("-cbm")
            self.context["step"] = "complete"
            self.context["name"] = order_selected.warehouse.name
        self.order_packing_list.clear()
        
        for pl in packing_list:
            pl_form = PackingListForm(initial={"n_pallet": pl["n_pallet"]})
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
        ids = request.POST.getlist("ids")
        ids = [i.split(",") for i in ids]
        n_pallet = request.POST.getlist("n_pallet")
        cbm = request.POST.getlist("cbms")
        total_pallet = 0
        for i, n, c in zip(ids, n_pallet, cbm):
            n = int(n)
            c = float(c)
            self._split_pallet(i, n, c, pk)
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

    def handle_cancel_post(self, request: HttpRequest) -> None:
        container_number = request.POST.get("container_number")
        shipment = Shipment.objects.filter(packinglist__container_number__container_number=container_number)
        if shipment:
            raise ValueError(f"Order {container_number} has scheduled shipment!")
        order = Order.objects.get(container_number__container_number=container_number)
        offload = Offload.objects.get(order__container_number__container_number=container_number)
        pallet = Pallet.objects.filter(packing_list__container_number__container_number=container_number)
        offload.total_pallet = None
        offload.offload_at = None
        offload.save()
        for p in pallet:
            p.delete()
        mutable_post = request.POST.copy()
        mutable_post['name'] = order.warehouse.name
        request.POST = mutable_post
        self.handle_warehouse_post(request)

    def _export_pallet_label(self, request: HttpRequest) -> HttpResponse:
        container_number = request.POST.get("container_number")
        status = request.POST.get("status")
        offload = Offload.objects.get(order__container_number__container_number=container_number)
        offload_date = offload.offload_at
        if offload_date:
            offload_date = offload_date.date()
            offload_date = offload_date.strftime("%m/%d")
        else:
            offload_date = datetime.now().date()
        packing_list = self._get_packing_list(container_number=container_number, status=status)
        # packing_list = PackingList.objects.filter(container_number__container_number=container_number).annotate(
        #     custom_delivery_method=Case(
        #         When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
        #         default=F('delivery_method'),
        #         output_field=CharField()
        #     ),
        #     str_id=Cast("id", CharField()),
        #     str_fba_id=Cast("fba_id", CharField()),
        #     str_ref_id=Cast("ref_id", CharField()),
        # ).values(
        #     "container_number__container_number", "destination", "address", "custom_delivery_method"
        # ).annotate(
        #     fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
        #     ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
        #     ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
        #     pcs=Sum("pallet__pcs", output_field=IntegerField()),
        #     cbm=Sum("pallet__cbm", output_field=FloatField()),
        #     n_pallet=Count('pallet__pallet_id', distinct=True)
        # ).order_by("-cbm")

        data = []
        for pl in packing_list:
            cbm = pl.get("cbm")
            remainder = cbm % 1
            cbm = int(cbm)
            if cbm%2:
                cbm += (cbm%2)
            elif remainder:
                cbm += 2
            for _ in range(cbm):
                data.append({
                    "container_number": pl.get("container_number__container_number"),
                    "destination": pl.get("destination"),
                    "date": offload_date,
                    "hold": (pl.get("custom_delivery_method").split("-")[0] == "暂扣留仓"),
                })
        context = {"data": data}
        template = get_template(self.template_pallet_label)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response['Content-Disposition'] = f'attachment; filename="pallet_label_{container_number}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response

    def _split_pallet(self, ids: list[Any], n: int, c: float, pk: int) -> None:
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
        packing_list = PackingList.objects.filter(id__in=ids)
        i = 0
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
                Pallet(**{
                    "packing_list": pl,
                    "pallet_id": pallet_ids[i],
                    "pcs": pcs_loaded,
                    "cbm": cbm_loaded,
                    "weight_lbs": weight_loaded,
                }).save()

    def _get_packing_list(self, container_number:str, status: str) -> PackingList:
        if status == "non_palletized":
            return PackingList.objects.filter(container_number__container_number=container_number).annotate(
                custom_delivery_method=Case(
                    When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                    default=F('delivery_method'),
                    output_field=CharField()
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
            ).values(
                "container_number__container_number", "destination", "address", "custom_delivery_method"
            ).annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count('pallet__pallet_id', distinct=True)
            ).order_by("-cbm")
        elif status == "palletized":
            return PackingList.objects.filter(container_number__container_number=container_number).annotate(
                custom_delivery_method=Case(
                    When(delivery_method='暂扣留仓', then=Concat('delivery_method', Value('-'), 'fba_id', Value('-'), 'id')),
                    default=F('delivery_method'),
                    output_field=CharField()
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
            ).values(
                "container_number__container_number", "destination", "address", "custom_delivery_method"
            ).annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True, ordering="str_fba_id"),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True, ordering="str_ref_id"),
                ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                pcs=Sum("pallet__pcs", output_field=IntegerField()),
                cbm=Sum("pallet__cbm", output_field=FloatField()),
                n_pallet=Count('pallet__pallet_id', distinct=True)
            ).order_by("-cbm")
        else:
            raise ValueError(f"invalid status: {status}")

    def _get_order_not_palletized(self, warehouse: str) -> Order:
        return Order.objects.filter(
            models.Q(warehouse__name=warehouse) &
            models.Q(offload_id__offload_required=True) &
            models.Q(offload_id__offload_at__isnull=True) &
            (models.Q(retrieval_id__actual_retrieval_timestamp__isnull=False) | models.Q(retrieval_id__retrive_by_zem=False))
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