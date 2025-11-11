from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db import models
from django.db.models import (
    Case,
    CharField,
    Count,
    F,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from django.db.models.functions import Cast, Concat
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.views import View

from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.views.post_port.shipment.fleet_management import FleetManagement


class PostportDash(View):
    template_main_dash = "post_port//01_summary_table.html"
    area_options = {"NJ": "NJ", "SAV": "SAV", "LA": "LA", "MO": "MO", "TX": "TX", "LA": "LA"}
    warehouse_mapping = {"NJ": "NJ-07001", "SAV": "SAV-31326", "LA": "LA-91761", "LA-91789": "LA-91789"}

    async def get(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.GET.get("step")
        if step == "summary_table":
            template, context = await self.handle_summary_table_get(request)
            return render(request, template, context)
        else:
            context = {"area_options": self.area_options}
            return render(request, self.template_main, context)

    async def post(self, request: HttpRequest) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        step = request.POST.get("step")
        if step == "summary_warehouse":
            template, context = await self.handle_summary_warehouse_post(request)
            return render(request, template, context)
        elif step == "export_report":
            return await self.handle_export_report_post(request)
        elif step == "export_bol":
            return await self.handle_bol_post(request)
        else:
            context = {"area_options": self.area_options}
            return render(request, self.template_main_dash, context)

    async def handle_bol_post(self, request: HttpRequest) -> HttpResponse:
        fm = FleetManagement()
        mutable_post = request.POST.copy()
        mutable_post["customerInfo"] = None
        mutable_post["pickupList"] = None
        area = mutable_post["area"]

        for key, code in self.warehouse_mapping.items():
            if key in area:
                mutable_post["warehouse"] = code

        shipment_batch_number = request.POST.get("shipment_batch_numbers")
        mutable_post["shipment_batch_number"] = shipment_batch_number
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related("fleet_number").get(
                shipment_batch_number=shipment_batch_number
            )
        )()
        if shipment.shipment_type == '客户自提':
            raise ValueError("该预约批次预约类型为客户自提，不支持客提的BOL下载！")
        if shipment.fleet_number:
            mutable_post["fleet_number"] = shipment.fleet_number
        else:
            raise ValueError("该预约批次尚未排车")
        request.POST = mutable_post
        return await fm.handle_export_bol_post(request)

    async def handle_summary_table_get(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        context = {"area_options": self.area_options}
        return self.template_main_dash, context

    async def handle_summary_warehouse_post(
        self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        area = request.POST.get("area")
        start_date = request.POST.get("start_date")
        end_date = request.POST.get("end_date")
        container_number = (
            request.POST.get("container_number").strip()
            if request.POST.get("container_number")
            else None
        )
        shipment_batch_number = (
            request.POST.get("shipment_batch_number").strip()
            if request.POST.get("shipment_batch_number")
            else None
        )
        appointment_id = (
            request.POST.get("appointment_id").strip()
            if request.POST.get("appointment_id")
            else None
        )
        destination = (
            request.POST.get("destination").strip()
            if request.POST.get("destination")
            else None
        )
        act_destination = (
            request.POST.get("act_destination").strip()
            if request.POST.get("act_destination")
            else None
        )
        shipping_marks = (
            request.POST.get("shipping_marks").strip()
            if request.POST.get("shipping_marks")
            else None
        )
        fba_ids = (
            request.POST.get("fba_ids").strip() if request.POST.get("fba_ids") else None
        )
        ref_ids = (
            request.POST.get("ref_ids").strip() if request.POST.get("ref_ids") else None
        )
        start_date = (
            (datetime.now().date() + timedelta(days=-4)).strftime("%Y-%m-%d")
            if not start_date
            else start_date
        )
        end_date = (
            (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")
            if not end_date
            else end_date
        )
        criteria = models.Q(
            (
                models.Q(container_number__order__order_type="转运")
                | models.Q(container_number__order__order_type="转运组合")
            ),
            container_number__order__packing_list_updloaded=True,
            container_number__order__created_at__gte="2024-09-01",
            container_number__order__cancel_notification=False,
        )
        if (
            shipment_batch_number
            or appointment_id
            or container_number
            or destination
            or act_destination
            or shipping_marks
            or fba_ids
            or ref_ids
        ):
            if shipment_batch_number:
                criteria &= models.Q(
                    shipment_batch_number__shipment_batch_number=shipment_batch_number
                )
            elif appointment_id:
                criteria &= models.Q(
                    shipment_batch_number__appointment_id=appointment_id
                )
            elif container_number:
                criteria &= models.Q(
                    container_number__container_number=container_number
                )
            elif destination:
                pl_criteria = models.Q(destination=destination)
                plt_criteria = models.Q(
                    container_number__order__offload_id__offload_at__isnull=True,  # 如果是查预报的仓点，就不看板子，所以这里加了一个不成立的条件
                    container_number__container_number="0",
                )
            elif act_destination:
                pl_criteria = models.Q(
                    destination=act_destination,
                    container_number__order__offload_id__offload_at__isnull=True,
                )
                plt_criteria = models.Q(
                    destination=act_destination,
                    container_number__order__offload_id__offload_at__isnull=False,
                )
            elif shipping_marks:
                pl_criteria = models.Q(
                    shipping_mark__contains=shipping_marks,
                    container_number__order__offload_id__offload_at__isnull=True,
                )
                plt_criteria = models.Q(
                    shipping_mark__contains=shipping_marks,
                    container_number__order__offload_id__offload_at__isnull=False,
                )
            elif fba_ids:
                pl_criteria = models.Q(
                    fba_id__contains=fba_ids,
                    container_number__order__offload_id__offload_at__isnull=True,
                )
                plt_criteria = models.Q(
                    fba_id__contains=fba_ids,
                    container_number__order__offload_id__offload_at__isnull=False,
                )
            elif ref_ids:
                pl_criteria = models.Q(
                    ref_id__contains=ref_ids,
                    container_number__order__offload_id__offload_at__isnull=True,
                )
                plt_criteria = models.Q(
                    ref_id__contains=ref_ids,
                    container_number__order__offload_id__offload_at__isnull=False,
                )
            if not destination and not shipping_marks and not fba_ids and not ref_ids and not act_destination:
                pl_criteria = criteria & models.Q(
                    container_number__order__offload_id__offload_at__isnull=True,
                )
                plt_criteria = criteria & models.Q(
                    container_number__order__offload_id__offload_at__isnull=False,
                )
            context = {
                "area_options": self.area_options,
            }
        else:
            pl_criteria = criteria & models.Q(
                container_number__order__vessel_id__vessel_eta__gte=start_date,
                container_number__order__vessel_id__vessel_eta__lte=end_date,
                container_number__order__offload_id__offload_at__isnull=True,
                container_number__order__retrieval_id__retrieval_destination_area=area,
            )
            plt_criteria = criteria & models.Q(
                container_number__order__vessel_id__vessel_eta__gte=start_date,
                container_number__order__vessel_id__vessel_eta__lte=end_date,
                container_number__order__offload_id__offload_at__isnull=False,
                location__startswith=area,
            )
            context = {
                "selected_area": area,
                "area_options": self.area_options,
                "start_date": start_date,
                "end_date": end_date,
            }

        packing_list = await self._get_packing_list(pl_criteria, plt_criteria)
        context["packing_list"] = packing_list
        cbm_act, cbm_est, pallet_act, pallet_est = 0, 0, 0, 0
        for pl in packing_list:
            if pl.get("label") == "ACT":
                cbm_act += pl.get("total_cbm")
                pallet_act += pl.get("total_n_pallet_act")
            else:
                cbm_est += pl.get("total_cbm")
                if pl.get("total_n_pallet_est") < 1:
                    pallet_est += 1
                elif pl.get("total_n_pallet_est") % 1 >= 0.45:
                    pallet_est += int(pl.get("total_n_pallet_est") // 1 + 1)
                else:
                    pallet_est += int(pl.get("total_n_pallet_est") // 1)
        if container_number:
            context["container_number"] = container_number
        if shipment_batch_number:
            context["shipment_batch_number"] = shipment_batch_number
        if destination:
            context["destination"] = destination
        if shipping_marks:
            context["shipping_marks"] = shipping_marks
        if fba_ids:
            context["fba_ids"] = fba_ids
        if ref_ids:
            context["ref_ids"] = ref_ids
        if act_destination:
            context["act_destination"] = act_destination
        if appointment_id:
            context["appointment_id"] = appointment_id
        return self.template_main_dash, context

    async def handle_export_report_post(self, request: HttpRequest) -> HttpResponse:
        selections = request.POST.getlist("is_selected")
        ids = request.POST.getlist("pl_ids")
        ids = [id for s, id in zip(selections, ids) if s == "on"]
        plt_ids = request.POST.getlist("plt_ids")
        plt_ids = [id for s, id in zip(selections, plt_ids) if s == "on"]
        selected = [int(i) for id in ids for i in id.split(",") if i]
        selected_plt = [int(i) for id in plt_ids for i in id.split(",") if i]
        if selected or selected_plt:
            packing_list_selected = await self._get_packing_list(
                models.Q(id__in=selected),
                models.Q(id__in=selected_plt),
            )
            data = []
            for pl in packing_list_selected:
                if pl.get("label") == "ACT":
                    n_pallet = pl.get("total_n_pallet_act")
                else:
                    n_pallet_est = pl.get("total_n_pallet_est")
                    if n_pallet_est < 1:
                        n_pallet = 1
                    elif n_pallet_est % 1 >= 0.45:
                        n_pallet = n_pallet_est // 1 + 1
                    else:
                        n_pallet = n_pallet_est // 1
                if pl.get(
                    "container_number__order__retrieval_id__actual_retrieval_timestamp"
                ):
                    retrieval_datetime = pl.get(
                        "container_number__order__retrieval_id__actual_retrieval_timestamp"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                elif pl.get(
                    "container_number__order__retrieval_id__target_retrieval_timestamp"
                ):
                    retrieval_datetime = pl.get(
                        "container_number__order__retrieval_id__target_retrieval_timestamp"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    retrieval_datetime = ""
                if "自提" in pl.get("custom_delivery_method"):
                    delivery_method = (
                        str(pl.get("custom_delivery_method"))
                        + "-"
                        + str(pl.get("shipping_marks"))
                    )
                else:
                    delivery_method = pl.get("custom_delivery_method").split("-")[0]
                data.append(
                    {
                        "所属仓库": pl.get("warehouse"),
                        "客户": pl.get(
                            "container_number__order__customer_name__zem_name"
                        ),
                        "货柜号": pl.get("container_number__container_number"),
                        "仓点": pl.get("destination"),
                        "派送方式": delivery_method,
                        "CBM": pl.get("total_cbm"),
                        "卡板数": n_pallet,
                        "箱数": pl.get("total_pcs"),
                        "总重lbs": pl.get("total_weight_lbs"),
                        "ETA": (
                            pl.get(
                                "container_number__order__vessel_id__vessel_eta"
                            ).replace(tzinfo=None)
                            if pl.get("container_number__order__vessel_id__vessel_eta")
                            else None
                        ),
                        "提柜时间": retrieval_datetime,
                        "入仓时间": (
                            pl.get(
                                "container_number__order__offload_id__offload_at"
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            if pl.get("container_number__order__offload_id__offload_at")
                            else ""
                        ),
                        "预约批次": pl.get(
                            "shipment_batch_number__shipment_batch_number"
                        ),
                        "预约号": pl.get("shipment_batch_number__appointment_id"),
                        "预约时间": (
                            pl.get(
                                "shipment_batch_number__shipment_appointment"
                            ).strftime("%Y-%m-%d %H:%M:%S")
                            if pl.get("shipment_batch_number__shipment_appointment")
                            else ""
                        ),
                        "发货时间": (
                            pl.get("shipment_batch_number__shipped_at").strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if pl.get("shipment_batch_number__shipped_at")
                            else ""
                        ),
                        "送达时间": (
                            pl.get("shipment_batch_number__arrived_at").strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            if pl.get("shipment_batch_number__arrived_at")
                            else ""
                        ),
                    }
                )
            df = pd.DataFrame(data)
            response = HttpResponse(
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            response["Content-Disposition"] = f"attachment; filename=PO.xlsx"
            df.to_excel(excel_writer=response, index=False, columns=df.columns)
            return response
        else:
            return self.handle_summary_table_get(request)

    async def _get_packing_list(
        self,
        pl_criteria: models.Q | None = None,
        plt_criteria: models.Q | None = None,
    ) -> list[Any]:
        data = []
        pal_list = await sync_to_async(list)(
            Pallet.objects.prefetch_related(
                "container_number",
                "container_number__order",
                "container_number__order__warehouse",
                "shipment_batch_number",
                "container_number__order__offload_id",
                "container_number__order__customer_name",
                "container_number__order__retrieval_id",
                "container_number__order__vessel_id",
            )
            .filter(plt_criteria)
            .annotate(
                schedule_status=Case(
                    When(
                        Q(
                            container_number__order__offload_id__offload_at__lte=datetime.now().date()
                            + timedelta(days=-7)
                        ),
                        then=Value("past_due"),
                    ),
                    default=Value("on_time"),
                    output_field=CharField(),
                ),
                str_id=Cast("id", CharField()),
            )
            .values(
                "container_number__container_number",
                "container_number__order__customer_name__zem_name",
                "container_number__order__warehouse__name",
                "destination",
                "PO_ID",
                "address",
                "delivery_method",
                "container_number__order__offload_id__offload_at",
                "container_number__order__retrieval_id__target_retrieval_timestamp",
                "container_number__order__retrieval_id__actual_retrieval_timestamp",
                "container_number__order__vessel_id__vessel_eta",
                "schedule_status",
                "abnormal_palletization",
                "po_expired",
                "shipment_batch_number__shipment_batch_number",
                "shipment_batch_number__appointment_id",
                "shipment_batch_number__shipment_appointment",
                "shipment_batch_number__shipped_at",
                "shipment_batch_number__arrived_at",
                "shipment_batch_number__pod_link",
                warehouse=F(
                    "container_number__order__retrieval_id__retrieval_destination_precise"
                ),
            )
            .annotate(
                custom_delivery_method=F("delivery_method"),
                fba_ids=F("fba_id"),
                ref_ids=F("ref_id"),
                shipping_marks=F("shipping_mark"),
                plt_ids=StringAgg(
                    "str_id", delimiter=",", distinct=True, ordering="str_id"
                ),
                total_pcs=Sum("pcs", output_field=IntegerField()),
                total_cbm=Sum("cbm", output_field=FloatField()),
                total_weight_lbs=Sum("weight_lbs", output_field=FloatField()),
                total_n_pallet_act=Count("pallet_id", distinct=True),
                label=Value("ACT"),
            )
            .order_by("container_number__order__offload_id__offload_at")
        )
        # for p in pal_list:
        #     try:
        #         po = await sync_to_async(PoCheckEtaSeven.objects.get)(
        #             shipping_mark = p['shipping_marks'],
        #             fba_id = p['fba_ids'],
        #             ref_id = p['ref_ids']
        #         )
        #         if not po.last_eta_checktime and not po.last_retrieval_checktime:
        #             p['check'] = '未校验'
        #         elif po.last_retrieval_checktime and not po.last_retrieval_status:
        #             p['check'] = '失效'
        #         elif not po.last_retrieval_checktime and po.last_eta_checktime and not po.last_eta_status:
        #             p['check'] = '失效'
        #         else:
        #             p['check'] = '有效'
        #     except PoCheckEtaSeven.DoesNotExist:
        #         # 如果没有找到对应的PoCheckEtaSeven记录，可以根据需求设置默认状态
        #         p['check'] = '未关联'
        #     except MultipleObjectsReturned:
        #             p['check'] = "不对应"
        data += pal_list
        if pl_criteria:
            pl_list = await sync_to_async(list)(
                PackingList.objects.prefetch_related(
                    "container_number",
                    "container_number__order",
                    "container_number__order__warehouse",
                    "shipment_batch_number",
                    "container_number__order__offload_id",
                    "container_number__order__customer_name",
                    "pallet",
                    "container_number__order__retrieval_id",
                    "container_number__order__vessel_id",
                )
                .filter(pl_criteria)
                .annotate(
                    custom_delivery_method=Case(
                        When(
                            Q(delivery_method="暂扣留仓(HOLD)")
                            | Q(delivery_method="暂扣留仓"),
                            then=Concat(
                                "delivery_method",
                                Value("-"),
                                "fba_id",
                                Value("-"),
                                "id",
                            ),
                        ),
                        default=F("delivery_method"),
                        output_field=CharField(),
                    ),
                    schedule_status=Case(
                        When(
                            Q(
                                container_number__order__offload_id__offload_at__lte=datetime.now().date()
                                + timedelta(days=-7)
                            ),
                            then=Value("past_due"),
                        ),
                        default=Value("on_time"),
                        output_field=CharField(),
                    ),
                    str_id=Cast("id", CharField()),
                    str_fba_id=Cast("fba_id", CharField()),
                    str_ref_id=Cast("ref_id", CharField()),
                    str_shipping_mark=Cast("shipping_mark", CharField()),
                )
                .values(
                    "container_number__container_number",
                    "container_number__order__customer_name__zem_name",
                    "container_number__order__warehouse__name",
                    "destination",
                    "PO_ID",
                    "address",
                    "custom_delivery_method",
                    "container_number__order__offload_id__offload_at",
                    "container_number__order__retrieval_id__target_retrieval_timestamp",
                    "container_number__order__retrieval_id__actual_retrieval_timestamp",
                    "container_number__order__vessel_id__vessel_eta",
                    "schedule_status",
                    "pcs",
                    "shipment_batch_number__shipment_batch_number",
                    "shipment_batch_number__appointment_id",
                    "shipment_batch_number__shipment_appointment",
                    warehouse=F(
                        "container_number__order__retrieval_id__retrieval_destination_precise"
                    ),
                )
                .annotate(
                    fba_ids=StringAgg(
                        "str_fba_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_fba_id",
                    ),
                    ref_ids=StringAgg(
                        "str_ref_id",
                        delimiter=",",
                        distinct=True,
                        ordering="str_ref_id",
                    ),
                    shipping_marks=StringAgg(
                        "str_shipping_mark",
                        delimiter=",",
                        distinct=True,
                        ordering="str_shipping_mark",
                    ),
                    ids=StringAgg(
                        "str_id", delimiter=",", distinct=True, ordering="str_id"
                    ),
                    total_pcs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("pcs")),
                            default=F("pallet__pcs"),
                            output_field=IntegerField(),
                        )
                    ),
                    total_cbm=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("cbm")),
                            default=F("pallet__cbm"),
                            output_field=FloatField(),
                        )
                    ),
                    total_weight_lbs=Sum(
                        Case(
                            When(pallet__isnull=True, then=F("total_weight_lbs")),
                            default=F("pallet__weight_lbs"),
                            output_field=FloatField(),
                        )
                    ),
                    total_n_pallet_act=Count("pallet__pallet_id", distinct=True),
                    total_n_pallet_est=Sum("cbm", output_field=FloatField()) / 2,
                    label=Max(
                        Case(
                            When(pallet__isnull=True, then=Value("EST")),
                            default=Value("ACT"),
                            output_field=CharField(),
                        )
                    ),
                )
                .distinct()
            )
            # for p in pl_list:
            #     try:
            #         pl = await sync_to_async(PoCheckEtaSeven.objects.get)(
            #             shipping_mark = p['shipping_marks'],
            #             fba_id = p['fba_ids'],
            #             ref_id = p['ref_ids']
            #         )
            #         if not pl.last_eta_checktime and not pl.last_retrieval_checktime:
            #             p['check'] = '未校验'
            #         elif pl.last_retrieval_checktime and not pl.last_retrieval_status:
            #             p['check'] = '失效'
            #         elif not pl.last_retrieval_checktime and pl.last_eta_checktime and not pl.last_eta_status:
            #             p['check'] = '失效'
            #         else:
            #             p['check'] = '有效'
            #     except PoCheckEtaSeven.DoesNotExist:
            #         # 如果没有找到对应的PoCheckEtaSeven记录，可以根据需求设置默认状态
            #         p['check'] = '未关联'
            #     except MultipleObjectsReturned:
            #         p['check'] = "不对应"
            data += pl_list
        return data

    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False
