import pytz,json
import uuid
import asyncio
import barcode
import io,base64
import sys
from PIL import Image
from barcode.writer import ImageWriter
from asgiref.sync import sync_to_async
from datetime import datetime
from typing import Any
from xhtml2pdf import pisa
from itertools import zip_longest
from django.db.models import OuterRef, Subquery

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
from warehouse.models.offload_status import AbnormalOffloadStatus
from warehouse.models.packing_list import PackingList
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.forms.warehouse_form import ZemWarehouseForm
from warehouse.forms.packling_list_form import PackingListForm
from warehouse.views.export_file import export_palletization_list
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS, WAREHOUSE_OPTIONS


class Palletization(View):
    template_main = "post_port/palletization/palletization.html"
    template_palletize = "post_port/palletization/palletization_packing_list.html"
    template_pallet_abnormal = "post_port/palletization/palletization_abnormal.html"
    template_pallet_label = "export_file/pallet_label_template.html"
    template_pallet_abnormal_records_search = "post_port/palletization/palletization_abnormal_records_search.html"
    template_pallet_abnormal_records_display = "post_port/palletization/palletization_abnormal_records_display.html"
    template_pallet_daily_operation = "post_port/palletization/daily_operation.html"

    async def get(self, request: HttpRequest, **kwargs) -> HttpResponse:
        if not await self._user_authenticate(request):
            return redirect("login")
        pk = kwargs.get("pk", None)
        step = request.GET.get("step", None)
        if step == "container_palletization":
            template, context = await self.handle_container_palletization_get(request, pk)
            return render(request, template, context)
        elif step == "abnormal":
            template, context = await self.handle_palletization_abnormal_get()
            return render(request, template, context)
        elif step == "abnormal_records":
            return render(request, self.template_pallet_abnormal_records_search, {})
        elif step == "daily_operation":
            template, context = await self.handle_daily_operation_get()
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
        elif step == "warehouse_abnormal":
            template, context = await self.handle_warehosue_abnormal_post(request)
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
        elif step == "amend_abnormal":
            template, context = await self.handle_amend_abnormal_post(request)
            return render(request, template, context)
        elif step == "abnormal_records":
            template, context = await self.handle_abnormal_records_post(request)
            return render(request, template, context)
        elif step == "warehouse_daily":
            template, context = await self.handle_warehouse_daily_post(request)
            return render(request, template, context)
        elif step == "edit_pallet":
            template, context = await self.handle_edit_pallet_post(request)
            return render(request, template, context)
        else:
            return await self.get(request)
    
    async def handle_palletization_abnormal_get(self, warehouse: str = None, include_all: bool = False) -> tuple[str, dict[str, Any]]:
        retrieval_precise_subquery = Subquery(
            Retrieval.objects.filter(
                id=OuterRef('container_number__order__retrieval_id')
            ).values('retrieval_destination_precise')[:1],
            output_field = CharField()
        )
        if include_all:
            all_status = await sync_to_async(list)(
                AbnormalOffloadStatus.objects
                .select_related('container_number', 'offload')
                .annotate(retrieval_destination_precise=Subquery(retrieval_precise_subquery))
                .all()
                .order_by('created_at')
            )
        else:
            all_status = await sync_to_async(list)(
                AbnormalOffloadStatus.objects
                .select_related('container_number', 'offload')
                .filter(is_resolved=False)
                .annotate(retrieval_destination_precise=Subquery(retrieval_precise_subquery))
                .all()
                .order_by('created_at')
            )
        abnormal = []
        for status in all_status:
            if warehouse :
                if warehouse in status.retrieval_destination_precise:
                    status_dict = {
                        "id": status.id,
                        "offload":status.offload.offload_id,
                        "container_number": status.container_number.container_number,
                        "created_at": status.created_at.strftime('%b-%d') if status.created_at else None,
                        "resolved_at": status.resolved_at,
                        "is_resolved": True if status.is_resolved else False,
                        "destination": status.destination,
                        "delivery_method": status.delivery_method,
                        "pcs_reported": status.pcs_reported,
                        "pcs_actual": status.pcs_actual,
                        "abnormal_reason": status.abnormal_reason,
                        "note": status.note,
                        "retrieval_destination_precise": status.retrieval_destination_precise,
                        "ddl_status": status.abnormal_status,
                    }
                    abnormal.append(status_dict)
            else:
                status_dict = {
                        "id": status.id,
                        "offload":status.offload.offload_id,
                        "container_number": status.container_number.container_number,
                        "created_at": status.created_at.strftime('%b-%d') if status.created_at else None,
                        "resolved_at": status.resolved_at,
                        "is_resolved": True if status.is_resolved else False,
                        "destination": status.destination,
                        "delivery_method": status.delivery_method,
                        "pcs_reported": status.pcs_reported,
                        "pcs_actual": status.pcs_actual,
                        "abnormal_reason": status.abnormal_reason,
                        "note": status.note,
                        "retrieval_destination_precise": status.retrieval_destination_precise,
                        "ddl_status": status.abnormal_status,
                    }
                abnormal.append(status_dict)
        context = {
            "abnormal":abnormal,
            "warehouse": warehouse,
            "warehouse_options": WAREHOUSE_OPTIONS,
        }
        if include_all:
            return self.template_pallet_abnormal_records_display, context
        else:
            return self.template_pallet_abnormal, context

    async def handle_daily_operation_get(self, warehouse: str = None, include_all: bool = False) -> tuple[str, dict[str, Any]]:
        #拆柜异常，客服已解决货物
        retrieval_precise_subquery = Subquery(
            Retrieval.objects.filter(
                id = OuterRef('container_number__order__retrieval_id')
            ).values('retrieval_destination_precise')[:1],
            output_field = CharField()
        )
        query = AbnormalOffloadStatus.objects.select_related('container_number').annotate(
            retrieval_destination_precise = Subquery(retrieval_precise_subquery)
        ).filter(is_resolved = True, confirmed_by_warehouse = False).order_by('created_at')
        if warehouse:
            query = query.filter(retrieval_destination_precise = warehouse)
        all_status = await sync_to_async(list)(query)
        abnormal = []
        for status in all_status:
            status_dict = {
                "id": status.id,
                "container_number": status.container_number.container_number,
                "created_at": status.created_at.strftime('%b-%d') if status.created_at else None,
                "resolved_at": status.resolved_at,
                "confirmed_by_warehouse": True if status.confirmed_by_warehouse else False,
                "destination": status.destination,
                "delivery_method": status.delivery_method,
                "pcs_reported": status.pcs_reported,
                "pcs_actual": status.pcs_actual,
                "abnormal_reason": status.abnormal_reason,
                "note": status.note,
                "ddl_status": status.abnormal_status,
            }
            abnormal.append(status_dict)
        
        cn = pytz.timezone('Asia/Shanghai')
        current_time_cn = datetime.now(cn)
        today = current_time_cn.date()

        #当日+下一天的预约信息
        query = Shipment.objects.prefetch_related(
                "packinglist", "packinglist__container_number", "packinglist__container_number__order",
                "packinglist__container_number__order__warehouse", "order"
            ).filter(
                    shipment_schduled_at__date=today              
            )
        if warehouse:
            query = query.filter(packinglist__container_number__order__retrieval_id__retrieval_destination_precise=warehouse).distinct()
        shipment = await sync_to_async(list)(query)

        #当日+下一天的预约信息
        query = Fleet.objects.prefetch_related(
            "shipment","shipment__packinglist","shipment__packinglist__container_number","shipment__packinglist__container_number__order",
            "shipment__packinglist__container_number__order__retrieval_id"
        ).filter(
                scheduled_at__date=today
        )
        if warehouse:
            query = query.filter(shipment__packinglist__container_number__order__retrieval_id__retrieval_destination_precise=warehouse).distinct()
        fleet = await sync_to_async(list)(query)
        
        #当日到港货柜
        query = Order.objects.select_related(
                "vessel_id", "container_number", "customer_name", "retrieval_id"
            ).filter(
                (
                    models.Q(retrieval_id__target_retrieval_timestamp__date=today) &
                    models.Q(cancel_notification=False)
                )
            )
        if warehouse:
            query = query.filter(retrieval_id__retrieval_destination_precise = warehouse)
        containers = await sync_to_async(list)(query)
        arrived_containers = []
        for o in containers:
            con_dict = {
                "id": o.id,
                "container_number": o.container_number.container_number,
                "order_type": o.order_type,
                "vessel_id":o.vessel_id,
                "temp_t49_pod_arrive_at": o.retrieval_id.temp_t49_pod_arrive_at,
            }
            arrived_containers.append(con_dict)
        context = {
            "abnormal":abnormal,
            "shipment":shipment,
            "fleet":fleet,
            "arrived_containers":arrived_containers,
            "warehouse_options": [("", ""), ("NJ-07001", "NJ-07001"), ("NJ-08817", "NJ-08817"),("SAV-31326", "SAV-31326"),("LA-91761", "LA-91761"),],
            "warehouse_filter": warehouse,
        }
        return self.template_pallet_daily_operation, context
           
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
        context["delivery_method_options"] = DELIVERY_METHOD_OPTIONS
        context["container_number"] = container.container_number
        context["pk"] = pk
        return self.template_palletize, context
    
    async def handle_warehouse_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("name")
        template, context = await self.handle_all_get(warehouse)
        return template, context

    async def handle_warehosue_abnormal_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        template, context = await self.handle_palletization_abnormal_get(warehouse)
        return template, context
    
    async def handle_warehouse_daily_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse_filter")
        template, context = await self.handle_daily_operation_get(warehouse)
        return template, context


    async def handle_edit_pallet_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        #将pallet信息存储，包括长宽高和件数
        plt_ids = request.POST.getlist("id")[0]
        plt_ids = plt_ids.split(',')
        length = request.POST.getlist("length")
        width = request.POST.getlist("width")
        height = request.POST.getlist("height")
        pcs = request.POST.getlist("pcs")
        weight = request.POST.getlist("weight")
        if request.POST.getlist("number"):
            number = request.POST.getlist("number")[0]
            number = number.split(',')
        pallets = []
        for i in range(len(plt_ids)):
            plt_id = int(plt_ids[i])
            pallet = await sync_to_async(Pallet.objects.get)(id=plt_id)
            pallet.length = length[i]
            pallet.width = width[i]
            pallet.height = height[i]
            pallet.pcs = pcs[i]
            pallet.weight_lbs = weight[i]
            pallet.cbm = round(float(length[i]) * float(width[i]) * float(height[i]) * 0.0254*0.0254*0.0254, 5)
            if request.POST.getlist("number"):
                pallet.sequence_number = number[i]
            pallets.append(pallet)
            
        await sync_to_async(Pallet.objects.bulk_update)(
            pallets,
            ["length","width","height","pcs","weight_lbs","cbm","sequence_number"]
        )
        request.GET.warehouse = request.POST.get("warehouse")
        request.GET.step = "container_palletization"
        request.GET.container_number = request.POST.get("container_number")
        pk = request.POST.get("pk")
        return await self.handle_container_palletization_get(request,pk)

    async def handle_packing_list_post(self, request: HttpRequest, pk: int) -> tuple[str, dict[str, Any]]:
        order_selected = await sync_to_async(Order.objects.select_related(
            "offload_id", "warehouse", "container_number"
        ).get)(pk=pk)
        offload = order_selected.offload_id
        container = order_selected.container_number
        newForcast = request.POST.getlist("new_destinations")    
        warehouse = order_selected.warehouse.name     
        if not offload.offload_at:
            cn = pytz.timezone('Asia/Shanghai')
            current_time_cn = datetime.now(cn)
            ids = request.POST.getlist("ids")
            ids = [i.split(",") for i in ids]
            n_pallet = [int(n) for n in request.POST.getlist("n_pallet")]
            pcs_actual = [int(n) for n in request.POST.getlist("pcs_actul")]
            pcs_reported = [int(d) for d in request.POST.getlist('pcs_reported')]
            cbm = [float(c) for c in request.POST.getlist("cbms")]
            weight = [float(c) for c in request.POST.getlist("weights")]
            destinations = [d for d in request.POST.getlist("destinations")]
            addresses = [d for d in request.POST.getlist("address")]
            zipcodes = [d for d in request.POST.getlist("zipcode")]
            delivery_method = [d for d in request.POST.getlist("delivery_method")]
            shipment_batch_number = [d for d in request.POST.getlist("shipment_batch_number")]
            shipping_marks = request.POST.getlist("shipping_marks")
            fba_ids = request.POST.getlist("fba_ids")
            ref_ids = request.POST.getlist("ref_ids")
            notes = [d for d in request.POST.getlist("note")]
            total_pallet = sum(n_pallet)
            abnormal_offloads = []
            for n, p_a, p_r, c, w, dest, d_m, note, shipment, shipping_mark, fba_id, ref_id, addr, zipcode in zip(
                n_pallet, pcs_actual, pcs_reported, cbm, weight, destinations, delivery_method, 
                notes, shipment_batch_number, shipping_marks, fba_ids, ref_ids, addresses, zipcodes
            ):
                await self._split_pallet(
                    order_selected, n, p_a, p_r, c, w, dest, d_m, note, shipment, 
                    shipping_mark, fba_id, ref_id, pk, addr, zipcode,
                )  #循环遍历每个汇总的板数
                if p_a != p_r:
                    abnormal_offloads.append({
                        "offload": offload,
                        "container_number": container,
                        "created_at": current_time_cn,
                        "is_resolved": False,
                        "destination": dest,
                        "delivery_method": d_m,
                        "pcs_reported": p_r,
                        "pcs_actual": p_a,
                    })
            if newForcast:
                #如果有多货的情况，因为前端目前新增行的时候通过clone id="palletization-row-empty"的行，所以会增加input，值为空，所以下面就进行了去重工作
                #计划是把多货的打板和正常预报的货一起做，但是因为多的input比较乱的插入在input中，不太好去重，所以就把新增的新命名了，然后直接去重
                new_destinations = request.POST.getlist("new_destinations")
                new_delivery_method = request.POST.getlist("new_delivery_method")
                new_pcs_actul = [int(value) for value in request.POST.getlist("new_pcs_actul")]
                new_pallets = [int(value) for value in request.POST.getlist("new_pallets")]
                shipping_marks = request.POST.getlist("new_shipping_marks")
                fba_ids = request.POST.getlist("new_fba_ids")
                ref_ids = request.POST.getlist("new_ref_ids")
                new_notes = request.POST.getlist("new_notes")
                new_cbm = [float(value) if value else 0 for value in request.POST.getlist("new_cbms")]
                #生成pallet
                for n, p_a, c, dest, d_m, note, shipping_mark, fba_id, ref_id in zip(
                    new_pallets, new_pcs_actul, new_cbm, new_destinations, new_delivery_method, new_notes,
                    shipping_marks, fba_ids, ref_ids, 
                ):
                    await self._split_pallet(
                        order_selected, n, p_a, 0, c, 0, dest, d_m, note, "None", 
                        shipping_mark, fba_id, ref_id, pk, seed=1
                    )  
                    #记录异常拆柜
                    abnormal_offloads.append({
                        "offload": offload,
                        "container_number": container,
                        "created_at": current_time_cn,
                        "is_resolved": False,
                        "destination": dest,
                        "delivery_method": d_m,
                        "pcs_reported": 0,
                        "pcs_actual": p_a,
                    })
            offload.total_pallet = total_pallet
            offload.offload_at = current_time_cn
            await sync_to_async(offload.save)()
            await self._update_shipment_stats(ids)
            await sync_to_async(AbnormalOffloadStatus.objects.bulk_create)(
                AbnormalOffloadStatus(**d) for d in abnormal_offloads
            )
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
        pallet = await sync_to_async(list)(
            Pallet.objects.select_related("shipment_batch_number").filter(container_number__container_number=container_number)
        )
        shipment = set()
        shipment.update([p.shipment_batch_number for p in pallet if p])
        await sync_to_async(Pallet.objects.filter(container_number__container_number=container_number).delete)()
        await sync_to_async(AbnormalOffloadStatus.objects.filter(
            container_number__container_number=container_number
        ).delete)()
        await sync_to_async(offload.save)()
        await self._update_shipment_abnormal_palletization(shipment)
        mutable_post = request.POST.copy()
        mutable_post['name'] = order.warehouse.name
        request.POST = mutable_post
        return await self.handle_warehouse_post(request)
    
    async def handle_amend_abnormal_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        selected = request.POST.getlist("is_case_selected")
        abnormal_pl_ids = request.POST.getlist("ids")    
        abnormal_reasons = request.POST.getlist("abnormal_reason")
        notes = request.POST.getlist("note")
        confirmed_by_warehouse = request.POST.getlist("confirmed_by_warehouse")   

        abnormal_pl_ids = [abnormal_pl_ids[i] for i in range(len(selected)) if selected[i] == "on"]
        abnormal_records = await sync_to_async(list)(
            AbnormalOffloadStatus.objects.select_related("container_number").filter(id__in=abnormal_pl_ids)
        )
        updated_records = []
        if confirmed_by_warehouse:
            shipment = set()
            for record in abnormal_records:
                pallet = await sync_to_async(list)(
                    Pallet.objects.select_related("container_number","shipment_batch_number").filter(
                        destination=record.destination,
                        container_number__container_number=record.container_number.container_number,
                        delivery_method=record.delivery_method,
                    )
                )
                shipment.update([p.shipment_batch_number for p in pallet])
                for p in pallet:
                    p.abnormal_palletization = False
                record.confirmed_by_warehouse = True
                updated_records.append(record)
                await sync_to_async(Pallet.objects.bulk_update)(
                    pallet, ["abnormal_palletization"]
                )
            await sync_to_async(AbnormalOffloadStatus.objects.bulk_update)(
                updated_records, ["confirmed_by_warehouse"]
            )
            
            await self._update_shipment_abnormal_palletization(shipment)
            return await self.handle_daily_operation_get()
        else:
            abnormal_reasons = [abnormal_reasons[i] for i in range(len(selected)) if selected[i] == "on"]
            notes = [notes[i] for i in range(len(selected)) if selected[i] == "on"]
            for record, reason, note in zip(abnormal_records, abnormal_reasons, notes):
                record.note = note
                record.abnormal_reason = reason
                record.is_resolved = True
                updated_records.append(record)
            await sync_to_async(AbnormalOffloadStatus.objects.bulk_update)(
                updated_records, ["is_resolved", "abnormal_reason", "note"]
            )
            if warehouse == 'None':
                warehouse = ''
            return await self.handle_palletization_abnormal_get(warehouse)
    
    async def handle_abnormal_records_post(self, request: HttpRequest) -> tuple[str, dict[str, Any]]:
        warehouse = request.POST.get("warehouse")
        return await self.handle_palletization_abnormal_get(warehouse, include_all=True)
        
    async def _export_pallet_label(self, request: HttpRequest) -> HttpResponse:
        data = []
        container_number = request.POST.get("container_number")
        customerInfo = request.POST.get("customerInfo")
        if customerInfo:
            customer_info = json.loads(customerInfo)
            packing_list = []
            for row in customer_info:
                date_str = row[7].strip()
                parts = date_str.split('-')
                month_day = f'{parts[1]}-{parts[2]}'

                destination = f"{row[4].strip()}"
                shipping_marks = row[2].strip()
                if "客户自提" in destination or "自提" in destination:
                    destination = "Self_PickUp"
                    marks = row[2].strip()
                    if marks:
                        array = marks.split(",")             
                        if len(array) > 2:
                            parts = []
                            for i in range(0, len(array), 2):
                                part = ",".join(array[i:i+2])
                                parts.append(part)
                            new_marks = "\n".join(parts)
                            newline_count = new_marks.count("\n")+1
                            new_marks = new_marks+str("TTT")+str(newline_count)
                        else:
                            new_marks = shipping_marks+str("TTT")+str(1)
                else:
                    destination = destination.replace("Walmart", "WM")
                    destination = destination.replace("沃尔玛", "WM")
                    destination = destination.replace("WALMART", "WM")
                    new_marks = None
                for num in range(int(row[6])):
                    num += 1
                    #生成条形码
                    barcode_type = 'code128'
                    barcode_class = barcode.get_barcode_class(barcode_type)
                    barcode_content = f"{row[0].strip()}|{destination}-{num}"
                    my_barcode = barcode_class(barcode_content, writer = ImageWriter()) #将条形码转换为图像形式
                    buffer = io.BytesIO()   #创建缓冲区
                    my_barcode.write(buffer, options={"dpi": 600})   #缓冲区存储图像
                    buffer.seek(0)               
                    barcode_base64 = base64.b64encode(buffer.read()).decode('utf-8')  #编码
                    if row[8].strip() == "是":
                        fba_ids = row[3].strip()
                    else:
                        fba_ids = None
                    new_data = {
                        "container_number": row[0].strip(),
                        "destination": f"{destination}-{num}",
                        "date": month_day,
                        "customer": row[1].strip(),
                        "hold": (row[8].strip() == "是"),
                        "fba_ids": fba_ids,
                        "barcode":barcode_base64,
                        "shipping_marks": new_marks,
                    }
                
                    for i in range(4):
                        data.append(new_data)
        else:
            customer_name = request.POST.get("customer_name")
            status = request.POST.get("status")
            n_label = int(request.POST.get("n_label"))
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
            for pl in packing_list:
                cbm = pl.get("cbm")
                remainder = cbm % 1
                cbm = int(cbm)
                if cbm%2:
                    cbm += (cbm%2)
                elif remainder:
                    cbm += 2
                cbm /= 2
                cbm *= n_label
                cbm = int(cbm)

                if "客户自提" in pl.get("destination") or "自提" in pl.get("destination"):
                    destination = "Self_PickUp"
                    marks = pl.get("shipping_marks")
                    if marks:
                        array = marks.split(",")             
                        if len(array) > 2:
                            parts = []
                            for i in range(0, len(array), 2):
                                part = ",".join(array[i:i+2])
                                parts.append(part)
                            new_marks = "\n".join(parts)  #换行符表示有几行
                            newline_count = new_marks.count("\n")+1
                            new_marks = new_marks+str("TTT")+str(newline_count)
                        else:
                            new_marks = pl.get("shipping_marks")+str("TTT")+str(1)
                else:
                    destination = pl.get("destination").replace("沃尔玛", "WM-")
                    new_marks = None
                if "暂扣留仓" in pl.get("custom_delivery_method").split("-")[0]:
                    fba_ids = pl.get("fba_ids")
                else:
                    fba_ids = None
                
                for num in range(cbm):
                    i = num // n_label + 1
                    barcode_type = 'code128'
                    barcode_class = barcode.get_barcode_class(barcode_type)
                    barcode_content = f"{pl.get('container_number__container_number')}|{destination}-{i}"
                    my_barcode = barcode_class(barcode_content, writer = ImageWriter()) #将条形码转换为图像形式
                    buffer = io.BytesIO()   #创建缓冲区
                    my_barcode.write(buffer, options={"dpi": 600})   #缓冲区存储图像
                    buffer.seek(0)               
                    barcode_base64 = base64.b64encode(buffer.read()).decode('utf-8')  #编码
                    
                    new_data = {
                        "container_number": pl.get("container_number__container_number"),
                        "destination": f"{destination}-{i}",
                        "date": retrieval_date,
                        "customer": customer_name,
                        "hold": ("暂扣留仓" in pl.get("custom_delivery_method").split("-")[0]),
                        "fba_ids": fba_ids,
                        "barcode":barcode_base64,
                        "shipping_marks":new_marks,
                        "pcs": pl.get("pcs")
                    }
                    
                    data.append(new_data)
        context = {"data": data}
        template = get_template(self.template_pallet_label)
        html = template.render(context)
        response = HttpResponse(content_type="application/pdf")
        response['Content-Disposition'] = f'attachment; filename="pallet_label_{container_number}.pdf"'
        pisa_status = pisa.CreatePDF(html, dest=response)
        if pisa_status.err:
            raise ValueError('Error during PDF generation: %s' % pisa_status.err, content_type='text/plain')
        return response

    async def _split_pallet(
        self,
        order: Order,
        n: int,
        p_a: int,
        p_r: int,
        c: float,
        w: float,
        destination: str,
        delivery_method: str,
        note: str,
        shipment_batch_number: str,
        shipping_mark: str, 
        fba_id: str, 
        ref_id: str,
        pk: int,
        address: str | None = None,
        zipcode: str | None = None,
        seed: int = 0
    ) -> None:
        if n == 0 or n is None:
            return
        pallet_ids = [
            str(uuid.uuid3(uuid.NAMESPACE_DNS, str(uuid.uuid4()) + str(pk) + str(i) + str(seed))) for i in range(n)
        ]
        if p_r == 0:  #多货的货物
            cbm_actual = c
            weight_actual = 0
        else:
            cbm_actual = c * p_a / p_r
            weight_actual = w * p_a / p_r
        if shipment_batch_number != "None":
            shipment = await sync_to_async(Shipment.objects.get)(shipment_batch_number=shipment_batch_number)
            shipment.abnormal_palletization = p_a != p_r
            await sync_to_async(shipment.save)()
        else:
            shipment = None
        pallet_data = []
        pallet_pcs = [p_a // n for _ in range(n)]
        for i in range(p_a % n):
            pallet_pcs[i] += 1
        for i in range(n):
            cbm_loaded = cbm_actual * pallet_pcs[i] / p_a
            weight_loaded = weight_actual * pallet_pcs[i] / p_a
            pallet_data.append({
                "container_number": order.container_number,
                "destination": destination,
                "address": address,
                "zipcode": zipcode,
                "delivery_method": delivery_method,
                "pallet_id": pallet_ids[i],
                "pcs": pallet_pcs[i],
                "cbm": cbm_loaded,
                "weight_lbs": weight_loaded,
                "shipment_batch_number": shipment,
                "note": note,
                "shipping_mark": shipping_mark if shipping_mark else "",
                "fba_id": fba_id if fba_id else "",
                "ref_id": ref_id if ref_id else "",
                "abnormal_palletization": p_a != p_r,
                "location": order.warehouse.name,
            })
        await sync_to_async(Pallet.objects.bulk_create)([Pallet(**d) for d in pallet_data])

    async def _update_shipment_stats(self, ids: list[Any]) -> None:
        ids = [int(j) for i in ids for j in i]
        packing_list = await sync_to_async(list)(
            PackingList.objects.select_related("shipment_batch_number").filter(id__in=ids)
        )
        shipment_list = set([pl.shipment_batch_number for pl in packing_list if pl.shipment_batch_number])
        shipment_stats = await sync_to_async(list)(Pallet.objects.select_related(
            "shipment_batch_number"
        ).filter(
            shipment_batch_number__shipment_batch_number__in=shipment_list
        ).values(
            "shipment_batch_number__shipment_batch_number"
        ).annotate(
            total_pcs=Sum("pcs", output_field=IntegerField()),
            total_cbm=Sum("cbm", output_field=FloatField()),
            weight_lbs=Sum("weight_lbs", output_field=FloatField()),
            total_n_pallet=Count('pallet_id', distinct=True, output_field=IntegerField()),
        ))
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
                    When(Q(delivery_method='客户自提') | Q(destination='客户自提'), then=Concat('delivery_method', Value('-'), 'destination',  Value('-'), 'shipping_mark')),
                    default=F('delivery_method'),
                    output_field=CharField()
                ),
                str_id=Cast("id", CharField()),
                str_fba_id=Cast("fba_id", CharField()),
                str_ref_id=Cast("ref_id", CharField()),
                str_shipping_mark=Cast("shipping_mark", CharField()),
            ).values(
                "container_number__container_number", "destination", "address", "zipcode",
                "custom_delivery_method", "note", "shipment_batch_number__shipment_batch_number"
            ).annotate(
                fba_ids=StringAgg("str_fba_id", delimiter=",", distinct=True),
                ref_ids=StringAgg("str_ref_id", delimiter=",", distinct=True),
                shipping_marks=StringAgg("str_shipping_mark", delimiter=",", distinct=True),
                ids=StringAgg("str_id", delimiter=",", distinct=True),
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count('pallet__pallet_id', distinct=True),
                weight_lbs=Sum("total_weight_lbs", output_field=FloatField()),
                plt_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
            ).order_by("-cbm"))
        elif status == "palletized":
            return await sync_to_async(list)(Pallet.objects.select_related(
                "container_number"
            ).filter(
                container_number__container_number=container_number
            ).annotate(
                str_id=Cast("id", CharField()),
                str_length=Cast("length", CharField()),
                str_width=Cast("width", CharField()),
                str_height=Cast("height", CharField()),
                str_pcs=Cast("pcs",CharField()),
                str_number=Cast("sequence_number",CharField()),
                str_weight=Cast("weight_lbs",CharField()),
            ).values(
                "container_number__container_number", "destination", "note",
                custom_delivery_method=F("delivery_method"),
            ).annotate(
                pcs=Sum("pcs", output_field=IntegerField()),
                cbm=Sum("cbm", output_field=FloatField()),
                n_pallet=Count('pallet_id', distinct=True),
                ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                length=StringAgg("str_length", delimiter=",", ordering="str_length"),
                width=StringAgg("str_width", delimiter=",", ordering="str_width"),
                height=StringAgg("str_height", delimiter=",", ordering="str_height"),
                n_pcs=StringAgg("str_pcs", delimiter=",", ordering="str_pcs"),
                number=StringAgg("str_number", delimiter=",", ordering="str_number"),
                weight=StringAgg("str_weight", delimiter=",", ordering="str_weight"),
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
    
    async def _update_shipment_abnormal_palletization(self, shipment: set[Shipment]) -> None:
        abnormal_shipment = await sync_to_async(list)(
            Shipment.objects.filter(
                shipment_batch_number__in=[p.shipment_batch_number for p in shipment if p],
                pallet__abnormal_palletization=True
            )
        )
        abnormal_shipment = set(s.shipment_batch_number for s in abnormal_shipment)
        updated_shipment = []
        for s in list(shipment-abnormal_shipment):
            if s:
                s.abnormal_palletization = False
                updated_shipment.append(s)
        await sync_to_async(Shipment.objects.bulk_update)(
            updated_shipment, ["abnormal_palletization"]
        )
    
    async def _user_authenticate(self, request: HttpRequest):
        if await sync_to_async(lambda: request.user.is_authenticated)():
            return True
        return False