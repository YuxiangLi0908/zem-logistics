from typing import Any, Coroutine
from django.db.models.functions import Trim
from collections import Counter
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
import pandas as pd
import re
from django.contrib import messages
from warehouse.forms.upload_file import UploadFileForm
from asgiref.sync import sync_to_async

from warehouse.models.order import Order
from warehouse.models.container import Container
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.invoice import Invoice
from warehouse.models.invoice_details import InvoiceDelivery

from warehouse.views.terminal49_webhook import T49Webhook

class ContainerTracking(View):
    t49_tracking_url = "https://api.terminal49.com/v2/tracking_requests"
    template_main = "container_tracking/main_page.html"
    template_po_sp_match = "container_tracking/po_sp_match.html"
    template_sp_operation = "container_tracking/sp_operation.html"
    template_find_table_id = "container_tracking/find_table_id.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "actual_match":
            return await sync_to_async(render)(request, self.template_po_sp_match)
        elif step == "sp_operation":
            return await sync_to_async(render)(request, self.template_sp_operation)
        elif step == "find_table_id":
            template, context = await self.handle_find_table_id_get(request)
            return await sync_to_async(render)(request, template, context)
        else:
            return await sync_to_async(render)(request, self.template_main)

    async def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "tracking_request":
            pass
        elif step == "upload_container_has_appointment":
            warehouse = request.POST.get('warehouse')
            if warehouse == "SAV" or warehouse == "NJ":
                template, context = await self.handle_upload_container_has_appointment_get(request)
                return await sync_to_async(render)(request, template, context)
            elif warehouse == "LA":
                template, context = await self.handle_upload_container_has_appointment_la(request)
                return await sync_to_async(render)(request, template, context)
            else:
                raise ValueError('仓库选择异常')
        elif step == "po_sp_match_search":
            warehouse = request.POST.get('warehouse')
            if warehouse == "SAV" or warehouse == "NJ":
                template, context = await self.handle_po_sp_match_search_get(request)
                return await sync_to_async(render)(request, template, context)
            if warehouse == "LA":
                template, context = await self.handle_po_sp_match_search_la(request)
                return await sync_to_async(render)(request, template, context)
        elif step == "sp_operation":
            warehouse = request.POST.get('warehouse')
            if warehouse == "SAV" or warehouse == "NJ":
                template, context = await self.handle_sp_operation_get(request)
                return await sync_to_async(render)(request, template, context)
            if warehouse == "LA":
                template, context = await self.handle_sp_operation_la(request)
                return await sync_to_async(render)(request, template, context)
        elif step == "Standardize_ISA":
            template, context = await self.handle_Standardize_ISA(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "search_invoice_delivery":
            template, context = await self.handle_search_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delete_pallet_invoice_delivery":
            template, context = await self.handle_delete_pallet_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context)
        elif step == "delete_container_invoice_deliveries":
            template, context = await self.handle_delete_container_invoice_deliveries(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_invoice_delivery":
            template, context = await self.handle_delete_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context) 
        elif step == "delete_all_invoice_delivery":
            template, context = await self.handle_delete_all_invoice_delivery(request)
            return await sync_to_async(render)(request, template, context) 
        else:
            return await sync_to_async(T49Webhook().post)(request)
        
    async def _send_tracking_request(self):
        pass
    
    async def handle_find_table_id_get(self,request):
        context = {}
        return self.template_find_table_id, context
    
    async def handle_search_invoice_delivery(self, request: HttpRequest):
        """查询 Invoice Delivery 记录"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        if not search_type or not search_value:
            messages.error(request, "请选择查询类型并输入查询值")
            return self.template_find_table_id, {"search_performed": False}

        context = {"search_type": search_type, "search_value": search_value}
        search_results = []

        try:
            # === 柜号查询 ===
            if search_type == "container":
                container = await sync_to_async(
                    lambda: Container.objects.filter(container_number=search_value).first()
                )()
                if container:
                    context["container_info"] = {"id": container.id, "number": container.container_number}
                    pallets = await sync_to_async(list)(
                        Pallet.objects.filter(container_number=container)
                        .select_related("invoice_delivery")
                        .values(
                            "id", "container_number__container_number", "destination", "delivery_method",
                            "cbm", "weight_lbs", "pcs", "location", "note",
                            "fba_id", "ref_id", "invoice_delivery_id"
                        )
                    )
                    search_results = pallets
                else:
                    messages.warning(request, f"未找到柜号: {search_value}")

            # === 发票查询 ===
            elif search_type == "invoice":
                invoice = None
                # 优先尝试查 invoice_number
                invoice = await sync_to_async(
                    lambda: Invoice.objects.filter(invoice_number=search_value).first()
                )()
                # 如果没找到，尝试按 container_number 查
                if not invoice:
                    container = await sync_to_async(
                        lambda: Container.objects.filter(container_number=search_value).first()
                    )()
                    if container:
                        invoice = await sync_to_async(
                            lambda: Invoice.objects.filter(container_number=container).first()
                        )()

                if invoice:
                    context["invoice_info"] = {"id": invoice.id, "number": invoice.invoice_number}

                    # 查该 Invoice 下的 InvoiceDelivery
                    invoice_deliveries = await sync_to_async(list)(
                        InvoiceDelivery.objects.filter(invoice_number=invoice)
                        .values(
                            "id", "invoice_delivery", "invoice_type", "delivery_type",
                            "destination", "zipcode", "total_pallet", "total_cbm",
                            "total_weight_lbs", "total_cost", "expense", "note"
                        )
                    )
                    context["invoice_deliveries"] = invoice_deliveries
                else:
                    messages.warning(request, f"未找到 Invoice 或 Container: {search_value}")

            # === Pallet 查询 ===
            elif search_type == "pallet":
                try:
                    pallet_id = int(search_value)
                    pallet = await sync_to_async(
                        lambda: Pallet.objects.filter(id=pallet_id)
                        .select_related("invoice_delivery", "container_number")
                        .values(
                            "id", "container_number__container_number", "destination", "delivery_method",
                            "cbm", "weight_lbs", "pcs", "location", "note",
                            "fba_id", "ref_id", "invoice_delivery_id"
                        ).first()
                    )()
                    if pallet:
                        search_results = [pallet]
                    else:
                        messages.warning(request, f"未找到 Pallet ID: {search_value}")
                except ValueError:
                    messages.error(request, "Pallet ID 必须是数字")

            context.update({
                "search_results": search_results,
                "search_performed": True
            })
            return self.template_find_table_id, context

        except Exception as e:
            messages.error(request, f"查询过程中发生错误: {str(e)}")
            return self.template_find_table_id, {"search_performed": True}

    async def handle_delete_invoice_delivery(self, request: HttpRequest):
        """删除单条 InvoiceDelivery"""
        invoice_delivery_id = request.POST.get("invoice_delivery_id")
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 先把对应 Pallet 的外键置空
            await sync_to_async(
                lambda: Pallet.objects.filter(invoice_delivery_id=invoice_delivery_id)
                .update(invoice_delivery=None)
            )()
            # 删除 InvoiceDelivery
            await sync_to_async(
                lambda: InvoiceDelivery.objects.filter(id=invoice_delivery_id).delete()
            )()
            messages.success(request, f"成功删除 InvoiceDelivery ID {invoice_delivery_id}")
        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)

    async def handle_delete_all_invoice_delivery(self, request: HttpRequest):
        """删除 Invoice/Container 下所有 InvoiceDelivery"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            invoice = None
            if search_type == "invoice":
                invoice = await sync_to_async(lambda: Invoice.objects.filter(invoice_number=search_value).first())()
                if not invoice:
                    container = await sync_to_async(lambda: Container.objects.filter(container_number=search_value).first())()
                    if container:
                        invoice = await sync_to_async(lambda: Invoice.objects.filter(container_number=container).first())()

            if invoice:
                # 清空对应 Pallet 外键
                await sync_to_async(
                    lambda: Pallet.objects.filter(
                        invoice_delivery__invoice_number=invoice
                    ).exclude(delivery_type='other')
                    .update(invoice_delivery=None)
                )()
                # 删除 InvoiceDelivery
                await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(invoice_number=invoice)
                    .exclude(delivery_type='other')
                    .delete()
                )()
                messages.success(request, f"成功删除 Invoice ID {invoice.id} 的所有 InvoiceDelivery")
            else:
                messages.warning(request, f"未找到对应的 Invoice 或 Container: {search_value}")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 重新查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    
    async def handle_delete_pallet_invoice_delivery(self, request: HttpRequest):
        """删除 Pallet 的 Invoice Delivery 外键，并删除对应的 InvoiceDelivery 记录"""
        pallet_id = request.POST.get("pallet_id")
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 先获取 pallet 对应的 invoice_delivery_id
            pallet = await sync_to_async(
                lambda: Pallet.objects.filter(id=pallet_id).first()
            )()

            if pallet and pallet.invoice_delivery_id:
                invoice_delivery_id = pallet.invoice_delivery_id

                # 先删除 Pallet 的外键
                await sync_to_async(
                    lambda: Pallet.objects.filter(id=pallet_id).update(invoice_delivery=None)
                )()

                # 再删除对应的 InvoiceDelivery 记录
                deleted_count = await sync_to_async(
                    lambda: InvoiceDelivery.objects.filter(id=invoice_delivery_id).delete()
                )()

                messages.success(request, f"成功删除 Pallet ID {pallet_id} 的 Invoice Delivery 及对应记录")
            else:
                messages.error(request, "Pallet 没有对应的 Invoice Delivery 记录")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 删除后重新执行查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)
    
    async def handle_delete_container_invoice_deliveries(self, request: HttpRequest):
        """删除整个柜子下的所有 Pallet 的 Invoice Delivery 外键，并删除对应的 InvoiceDelivery 记录"""
        container_id = request.POST.get("container_id")
        search_type = request.POST.get("search_type", "")
        search_value = request.POST.get("search_value", "").strip()

        try:
            # 获取这个柜子下所有 Pallet
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(container_number_id=container_id))
            )()

            if not pallets:
                messages.error(request, f"未找到 Container ID {container_id} 下的任何 Pallet")
            else:
                # 收集所有相关的 invoice_delivery_id
                invoice_delivery_ids = [p.invoice_delivery_id for p in pallets if p.invoice_delivery_id]

                # 将所有 Pallet 的外键置空
                await sync_to_async(
                    lambda: Pallet.objects.filter(container_number_id=container_id).update(invoice_delivery=None)
                )()

                # 删除对应的 InvoiceDelivery 记录
                if invoice_delivery_ids:
                    await sync_to_async(
                        lambda: InvoiceDelivery.objects.filter(id__in=invoice_delivery_ids).delete()
                    )()
                    messages.success(request, f"成功删除 Container ID {container_id} 下所有 Pallet 的 Invoice Delivery 及对应记录")
                else:
                    messages.info(request, "这个柜子下的 Pallet 没有任何 Invoice Delivery 记录")

        except Exception as e:
            messages.error(request, f"删除过程中发生错误: {str(e)}")

        # 删除后重新执行查询
        request.POST = request.POST.copy()
        request.POST["step"] = "search_invoice_delivery"
        request.POST["search_type"] = search_type
        request.POST["search_value"] = search_value
        return await self.handle_search_invoice_delivery(request)

    async def handle_Standardize_ISA(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        await sync_to_async(
            lambda: Shipment.objects.update(appointment_id=Trim("appointment_id"))
        )()

        context = {
            "message": "所有 Shipment 的 appointment_id 已经去掉前后空格"
        }
        return self.template_main, context
    
    async def handle_sp_operation_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        warehouse = request.POST.get('warehouse')
        if warehouse == "SAV":
            result = await self._sav_excel_normalization(request)        
        elif warehouse == "NJ":
            result = await self._nj_excel_normalization(request)   
        match_result = await self.detail_appointment_abnormalities(result['result'])
        context = {
            'result': match_result['result'],
            'processed_abnormalities': match_result['processed_abnormalities'],
            'processed_abnormalities': match_result['processed_abnormalities'],
            'processed_count': match_result['processed_count'],
            'remaining_count': match_result['remaining_count'],
        }
        return self.template_po_sp_match,context

    async def handle_sp_operation_la(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        result = await self._la_excel_normalization(request)
        match_result = await self.detail_appointment_abnormalities(result['result'])
        context = {
            'result': match_result['result'],
            'processed_abnormalities': match_result['processed_abnormalities'],
            'processed_abnormalities': match_result['processed_abnormalities'],
            'processed_count': match_result['processed_count'],
            'remaining_count': match_result['remaining_count'],
        }
        return self.template_po_sp_match,context

    async def handle_po_sp_match_search_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        warehouse = request.POST.get('warehouse')
        if warehouse == "SAV":
            result = await self._sav_excel_normalization(request)        
        elif warehouse == "NJ":
            result = await self._nj_excel_normalization(request)   
        match_result = await self.check_appointment_abnormalities(result['result'])
        shipment_table_rows = await self._process_format(match_result['result'])

        total_rows = len(shipment_table_rows)
        error_rows = sum(1 for row in shipment_table_rows if row.get("errors"))
        context = {
            'result': match_result['result'],
            'shipment_table_rows': shipment_table_rows,
            'global_errors': match_result['global_errors'],
            'summary': {
                'total_rows': total_rows,
                'error_rows': error_rows,
                'normal_rows': total_rows - error_rows,
                'error_type_summary': match_result['error_type_summary'], 
            },
        }
        return self.template_po_sp_match, context

    async def handle_po_sp_match_search_la(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        result = await self._la_excel_normalization(request)
        match_result = await self.check_appointment_abnormalities(result['result'])
        shipment_table_rows = await self._process_format(match_result['result'])

        total_rows = len(shipment_table_rows)
        error_rows = sum(1 for row in shipment_table_rows if row.get("errors"))
        context = {
            'result': match_result['result'],
            'shipment_table_rows': shipment_table_rows,
            'global_errors': match_result['global_errors'],
            'summary': {
                'total_rows': total_rows,
                'error_rows': error_rows,
                'normal_rows': total_rows - error_rows,
                'error_type_summary': match_result['error_type_summary'],
            },
        }
        return self.template_po_sp_match, context
    
    async def detail_appointment_abnormalities(self, result_dict):
        abnormalities = []
        processed_abnormalities = []  # 记录已处理的异常
        remaining_abnormalities = []  # 记录未处理的异常
        for pickup_number, pickup_data in result_dict.items():
            # 初始化该车次的异常列表
            group_abnormalities = []
            group_processed = []  # 该车次处理的异常
            group_remaining = []  # 该车次未处理的异常
            # 异常1: 检查车次是否存在
            fleets = [fleet async for fleet in Fleet.objects.filter(pickup_number=pickup_number)]
            if len(fleets) == 0:
                fleet = None
            elif len(fleets) > 1:
                # 查到多条记录
                fleet_numbers = [fleet.fleet_number for fleet in fleets]
                abnormality_msg = f'车 {pickup_number} 在系统中存在多条记录，fleet_number分别为: {", ".join(fleet_numbers)}'
                group_abnormalities.append(abnormality_msg)
                group_remaining.append(abnormality_msg)
                abnormalities.append({
                    'type': '车次重复',
                    'pickup_number': pickup_number,
                    'fleet_numbers': fleet_numbers,
                    'message': abnormality_msg
                })
            else:
                fleet = fleets[0]
            
            # 处理每个预约批次前，先检查一提两卸的车次匹配情况（只有多个批次时才检查）
            batch_fleet_ids = {}

            # 只有当有多个批次时才检查车次匹配
            if len(pickup_data['po']) > 1:
                # 收集所有批次的fleet_number_id
                for batch_num in pickup_data['po'].keys():
                    try:
                        shipment = await Shipment.objects.select_related('fleet_number').aget(shipment_batch_number=batch_num)
                        batch_fleet_ids[batch_num] = shipment.fleet_number_id
                    except Shipment.DoesNotExist:
                        # 如果批次不存在，跳过这个批次的检查
                        continue

                # 检查所有存在的批次是否关联到同一个车次
                if batch_fleet_ids and len(set(batch_fleet_ids.values())) > 1:
                    # 一提两卸的车次不匹配
                    fleet_info = []
                    for batch_num, fleet_id in batch_fleet_ids.items():
                        try:
                            fleet_obj = await Fleet.objects.aget(id=fleet_id)
                            fleet_info.append(f"{batch_num} -> {fleet_obj.fleet_number}")
                        except Fleet.DoesNotExist:
                            fleet_info.append(f"{batch_num} -> 车次不存在(ID: {fleet_id})")
                    
                    abnormality_msg = f'一提两卸车次不匹配: 批次关联到不同的车次 - {"; ".join(fleet_info)}'
                    group_abnormalities.append(abnormality_msg)
                    group_remaining.append(abnormality_msg)
                    abnormalities.append({
                        'type': '一提两卸车次不匹配',
                        'pickup_number': pickup_number,
                        'batch_fleet_mapping': fleet_info,
                        'message': abnormality_msg
                    })
            # 处理每个预约批次
            for batch_number, batch_data in pickup_data['po'].items():
                appointment_number = batch_data['预约号']
                detail = batch_data['detail']
                
                # 异常2: 检查预约批次是否存在
                shipment_by_batch = None
                shipment_by_appointment = None
                try:
                    shipment_by_batch = await Shipment.objects.select_related('fleet_number').aget(shipment_batch_number=batch_number)
                except Shipment.DoesNotExist:
                    # 如果通过batch_number找不到，尝试通过appointment_number查找
                    try:
                        shipment_by_appointment = await Shipment.objects.select_related('fleet_number').aget(appointment_id=str(appointment_number))
                        
                        # 找到了shipment，但batch_number不匹配
                        abnormality_msg = f'约不匹配: ISA {appointment_number} 对应的约应为 {shipment_by_appointment.shipment_batch_number}，而不是 {batch_number}'
                        group_abnormalities.append(abnormality_msg)
                        group_remaining.append(abnormality_msg)
                        abnormalities.append({
                            'type': '批次号不匹配',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'expected_batch': shipment_by_appointment.shipment_batch_number,
                            'appointment_number': appointment_number,
                            'message': abnormality_msg
                        })
                        shipment = shipment_by_appointment  # 使用找到的shipment继续后续检查
                        
                    except Shipment.DoesNotExist:
                        # 通过batch_number和appointment_number都找不到
                        abnormality_msg = f'约 {batch_number} (ISA: {appointment_number}) 在系统中不存在'
                        group_abnormalities.append(abnormality_msg)
                        group_remaining.append(abnormality_msg)
                        abnormalities.append({
                            'type': '预约批次不存在',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'appointment_number': appointment_number,
                            'message': abnormality_msg
                        })
                        #新创建一个shipment，把组内的po都加上
                        continue
                else:
                    shipment = shipment_by_batch
                
                # 异常3: 检查预约号是否匹配
                if shipment_by_batch and str(shipment.appointment_id) != str(appointment_number):
                    
                    repair_msg = f'批次 {batch_number} 的ISA已纠正: 从 {shipment.appointment_id} 改为 {appointment_number}'
                    group_abnormalities.append(repair_msg)
                    group_processed.append(repair_msg)
                    abnormalities.append({
                        'type': 'ISA纠正',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'old_appointment': shipment.appointment_id,
                        'new_appointment': appointment_number,
                        'message': repair_msg
                    })
                    #把预约表的ISA改到系统上
                    shipment.appointment_id = str(appointment_number)
                    await shipment.asave()
                
                # 异常4: 检查shipment是否关联到正确的车次
                if fleet and shipment.fleet_number_id != fleet.id:
                    abnormality_msg = f'批次 {batch_number} 未关联到车 {pickup_number}，实际关联车是 {shipment.fleet_number.fleet_number if shipment.fleet_number else "无"}'
                    group_abnormalities.append(abnormality_msg)
                    group_remaining.append(abnormality_msg)
                    abnormalities.append({
                        'type': '车次关联错误',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'actual_fleet': shipment.fleet_number.fleet_number if shipment.fleet_number else "无",
                        'message': abnormality_msg
                    })
                elif not fleet and shipment.fleet_number:                 
                    repair_msg = f'车次关联已修复: 将车次 {shipment.fleet_number.fleet_number} 的pickup_number设置为 {pickup_number}'
                    group_abnormalities.append(repair_msg)
                    group_processed.append(repair_msg)
                    abnormalities.append({
                        'type': '车次关联修复',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'fleet_number': shipment.fleet_number.fleet_number,
                        'message': repair_msg
                    })
                    # 将pickup_number赋值给shipment关联的fleet记录
                    shipment.fleet_number.pickup_number = pickup_number
                    await shipment.fleet_number.asave()
                    # 更新fleet变量
                    fleet = shipment.fleet_number
                if fleet:
                    #把费用更新到车次上
                    fleet.fleet_cost = pickup_data['fee']
                    await fleet.asave()
                    repair_msg = f'将车次 {shipment.fleet_number.fleet_number} 的费用设置为 {pickup_data["fee"]}'
                    group_abnormalities.append(repair_msg)
                    group_processed.append(repair_msg)
                # 检查每个柜号-仓点组合
                for container_no, expected_warehouse in detail.items():
                    # is_special_plt = False
                    # if '加甩' in original_container_no:
                    #     # 如果有"加甩"字样，按照"-"分组，取前面的部分
                    #     container_no = original_container_no.split('-')[0]
                    #     is_special_plt = True
                    # else:
                    #     container_no = original_container_no
    
                    # 直接在 Pallet 表中查询柜号和仓点的记录
                    pallets = await sync_to_async(list)(
                        Pallet.objects.select_related('container_number', 'shipment_batch_number','master_shipment_batch_number').filter(
                            container_number__container_number=container_no,
                            destination=expected_warehouse
                        )
                    )
                    
                    # 如异常5：检查是否关联到正确的预约批次
                    if not pallets:
                        abnormality_msg = f'柜号 {container_no} 下找不到仓点 {expected_warehouse} 的记录'
                        group_abnormalities.append(abnormality_msg)
                        group_remaining.append(abnormality_msg)
                        abnormalities.append({
                            'type': '柜号仓点不存在',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'container_number': container_no,
                            'expected_warehouse': expected_warehouse,
                            'message': abnormality_msg
                        })
                    else:
                        # 检查所有匹配的记录
                        unmatched_pallets = []
                        for pallet in pallets:
                            if not pallet.shipment_batch_number:
                                # 如果外键为空，归为异常
                                unmatched_pallets.append(pallet)
                        
                        repair_count = 0
                        if unmatched_pallets:
                            for plt in unmatched_pallets:
                                #如果有主约，就把实际约=主约
                                if plt.master_shipment_batch_number:
                                    plt.shipment_batch_number = plt.master_shipment_batch_number
                                else:
                                    #如果没有主约，让主约，实际约=这个约
                                    plt.shipment_batch_number = shipment
                                    plt.master_shipment_batch_number = shipment
                                await pallet.asave()

                            
                        if repair_count > 0:
                                repair_msg = f'{container_no} - {expected_warehouse} 已纠正 {repair_count} 个板的批次指向为 {batch_number}'
                                group_abnormalities.append(repair_msg)
                                group_processed.append(repair_msg)
                                abnormalities.append({
                                    'type': '板子批次指向纠正',
                                    'pickup_number': pickup_number,
                                    'batch_number': batch_number,
                                    'container_number': container_no,
                                    'expected_warehouse': expected_warehouse,
                                    'repair_count': repair_count,
                                    'message': repair_msg
                                })
            if group_abnormalities:
                new_errors = '；'.join(group_abnormalities)
                if pickup_data.get('errors'):
                    pickup_data['errors'] += f'；{new_errors}'
                else:
                    pickup_data['errors'] = new_errors

        error_type_counter = Counter([ab['type'] for ab in abnormalities])
        processed_count = len(processed_abnormalities)
        remaining_count = len(remaining_abnormalities)
        # 过滤result_dict，只保留有问题的记录
        filtered_result_dict = {}
        for pickup_number, pickup_data in result_dict.items():
            # 检查该车次是否有错误信息
            if pickup_data.get('errors'):
                filtered_result_dict[pickup_number] = pickup_data
        context = {
            'global_errors': abnormalities,
            'result': filtered_result_dict,
            'error_type_summary': dict(error_type_counter),
            'processed_count': processed_count,
            'remaining_count': remaining_count,
            'processed_abnormalities': processed_abnormalities,
            'remaining_abnormalities': remaining_abnormalities,
        }
        return context

    async def check_appointment_abnormalities(self, result_dict):
        abnormalities = []
        for pickup_number, pickup_data in result_dict.items():
            # 初始化该车次的异常列表
            group_abnormalities = []
            # 异常1: 检查车次是否存在
            fleets = [fleet async for fleet in Fleet.objects.filter(pickup_number=pickup_number)]
            if len(fleets) == 0:
                fleet = None
            elif len(fleets) > 1:
                # 查到多条记录
                fleet_numbers = [fleet.fleet_number for fleet in fleets]
                abnormality_msg = f'车 {pickup_number} 在系统中存在多条记录，fleet_number分别为: {", ".join(fleet_numbers)}'
                group_abnormalities.append(abnormality_msg)
                abnormalities.append({
                    'type': '车次重复',
                    'pickup_number': pickup_number,
                    'fleet_numbers': fleet_numbers,
                    'message': abnormality_msg
                })
            else:
                fleet = fleets[0]
            
            # 处理每个预约批次前，先检查一提两卸的车次匹配情况（只有多个批次时才检查）
            batch_fleet_ids = {}
            batch_fleet_mismatch = False

            # 只有当有多个批次时才检查车次匹配
            if len(pickup_data['po']) > 1:
                # 收集所有批次的fleet_number_id
                for batch_num in pickup_data['po'].keys():
                    try:
                        shipment = await Shipment.objects.select_related('fleet_number').aget(shipment_batch_number=batch_num)
                        batch_fleet_ids[batch_num] = shipment.fleet_number_id
                    except Shipment.DoesNotExist:
                        # 如果批次不存在，跳过这个批次的检查
                        continue

                # 检查所有存在的批次是否关联到同一个车次
                if batch_fleet_ids and len(set(batch_fleet_ids.values())) > 1:
                    # 一提两卸的车次不匹配
                    batch_fleet_mismatch = True
                    fleet_info = []
                    for batch_num, fleet_id in batch_fleet_ids.items():
                        try:
                            fleet_obj = await Fleet.objects.aget(id=fleet_id)
                            fleet_info.append(f"{batch_num} -> {fleet_obj.fleet_number}")
                        except Fleet.DoesNotExist:
                            fleet_info.append(f"{batch_num} -> 车次不存在(ID: {fleet_id})")
                    
                    abnormality_msg = f'一提两卸车次不匹配: 批次关联到不同的车次 - {"; ".join(fleet_info)}'
                    group_abnormalities.append(abnormality_msg)
                    abnormalities.append({
                        'type': '一提两卸车次不匹配',
                        'pickup_number': pickup_number,
                        'batch_fleet_mapping': fleet_info,
                        'message': abnormality_msg
                    })

            # 处理每个预约批次
            for batch_number, batch_data in pickup_data['po'].items():
                appointment_number = batch_data['预约号']
                detail = batch_data['detail']
                
                # 异常2: 检查预约批次是否存在
                shipment_by_batch = None
                shipment_by_appointment = None
                try:
                    shipment_by_batch = await Shipment.objects.select_related('fleet_number').aget(shipment_batch_number=batch_number)
                except Shipment.DoesNotExist:
                    # 如果通过batch_number找不到，尝试通过appointment_number查找
                    try:
                        shipment_by_appointment = await Shipment.objects.select_related('fleet_number').aget(appointment_id=str(appointment_number))
                        
                        # 找到了shipment，但batch_number不匹配
                        abnormality_msg = f'约不匹配: ISA {appointment_number} 对应的约应为 {shipment_by_appointment.shipment_batch_number}，而不是 {batch_number}'
                        group_abnormalities.append(abnormality_msg)
                        abnormalities.append({
                            'type': '批次号不匹配',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'expected_batch': shipment_by_appointment.shipment_batch_number,
                            'appointment_number': appointment_number,
                            'message': abnormality_msg
                        })
                        shipment = shipment_by_appointment  # 使用找到的shipment继续后续检查
                        
                    except Shipment.DoesNotExist:
                        # 通过batch_number和appointment_number都找不到
                        abnormality_msg = f'约 {batch_number} (ISA: {appointment_number}) 在系统中不存在'
                        group_abnormalities.append(abnormality_msg)
                        abnormalities.append({
                            'type': '预约批次不存在',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'appointment_number': appointment_number,
                            'message': abnormality_msg
                        })
                        #预约组还未在系统操作，系统不处理
                        continue
                else:
                    shipment = shipment_by_batch
                
                # 异常3: 检查预约号是否匹配
                if shipment_by_batch and str(shipment.appointment_id) != str(appointment_number):
                    abnormality_msg = f'批次 {batch_number} 的ISA不匹配: 期望 {appointment_number}, 实际 {shipment.appointment_id}'
                    group_abnormalities.append(abnormality_msg)
                    abnormalities.append({
                        'type': '预约号不匹配',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'expected_appointment': appointment_number,
                        'actual_appointment': shipment.appointment_id,
                        'message': abnormality_msg
                    })
                    #把系统的ISA改成和预约表的ISA一致
                
                # 异常4: 检查shipment是否关联到正确的车次
                if fleet and shipment.fleet_number_id != fleet.id:
                    abnormality_msg = f'批次 {batch_number} 未关联到车 {pickup_number}，实际关联车是 {shipment.fleet_number.fleet_number if shipment.fleet_number else "无"}'
                    abnormalities.append({
                        'type': '车次关联错误',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'actual_fleet': shipment.fleet_number.fleet_number if shipment.fleet_number else "无",
                        'message': abnormality_msg
                    })
                    
                elif not fleet and shipment.fleet_number:
                    #把pickupnumber赋值给shipment绑定的车
                    pass
                elif not fleet and not shipment.fleet_number:
                    abnormality_msg = f'车次 {pickup_number} 在系统中不存在'
                    group_abnormalities.append(abnormality_msg)
                    #加到总的报错信息里
                    abnormalities.append({
                        'type': 'pick找不到车次，这个约也没关联车',
                        'pickup_number': pickup_number,
                        'message': abnormality_msg
                    })
                    #

                
                # 检查每个柜号-仓点组合
                for original_container_no, expected_warehouse in detail.items():
                    if '加甩' in original_container_no:
                        # 如果有"加甩"字样，按照"-"分组，取前面的部分
                        container_no = original_container_no.split('-')[0]
                    else:
                        container_no = original_container_no
    
                    # 直接在 Pallet 表中查询柜号和仓点的记录
                    pallets = await sync_to_async(list)(
                        Pallet.objects.select_related('container_number', 'shipment_batch_number').filter(
                            container_number__container_number=container_no,
                            destination=expected_warehouse
                        )
                    )
                    
                    # 如异常6：检查是否关联到正确的预约批次
                    if not pallets:
                        abnormality_msg = f'柜号 {container_no} 下找不到仓点 {expected_warehouse} 的记录'
                        group_abnormalities.append(abnormality_msg)
                        abnormalities.append({
                            'type': '柜号仓点不存在',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'container_number': container_no,
                            'expected_warehouse': expected_warehouse,
                            'message': abnormality_msg
                        })
                    else:
                        # 检查所有匹配的记录
                        matched_pallets = []
                        unmatched_pallets = []
                        for pallet in pallets:
                            if not pallet.shipment_batch_number:
                                # 如果外键为空，归为异常
                                unmatched_pallets.append(pallet)
                            # elif pallet.shipment_batch_number.shipment_batch_number == batch_number:
                            #     matched_pallets.append(pallet)
                            # else:
                            #     unmatched_pallets.append(pallet)
                        if unmatched_count:
                            # 异常6：没有关联到正确的预约批次
                            total_count = len(pallets)
                            unmatched_count = len(unmatched_pallets)

                            actual_batches = []
                            for p in pallets:
                                if not p.shipment_batch_number:
                                    actual_batches.append("无关联批次")
                                else:
                                    actual_batches.append(p.shipment_batch_number.shipment_batch_number)
                            
                            actual_batches_unique = list(set(actual_batches))
                            abnormality_msg = f'{container_no} - {expected_warehouse} 未关联到约 {batch_number}，共{total_count}个板，{unmatched_count}个板的批次不匹配，实际关联情况: {", ".join(actual_batches_unique)}'
                            group_abnormalities.append(abnormality_msg)
                            abnormalities.append({
                                'type': '柜号批次不匹配',
                                'pickup_number': pickup_number,
                                'batch_number': batch_number,
                                'container_number': container_no,
                                'expected_warehouse': expected_warehouse,
                                'total_count': total_count,
                                'unmatched_count': unmatched_count, 
                                'actual_batches': actual_batches,
                                'message': abnormality_msg
                            })
            if group_abnormalities:
                new_errors = '；'.join(group_abnormalities)
                if pickup_data.get('errors'):
                    pickup_data['errors'] += f'；{new_errors}'
                else:
                    pickup_data['errors'] = new_errors
        # 过滤result_dict，只保留有问题的记录
        filtered_result_dict = {}
        for pickup_number, pickup_data in result_dict.items():
            # 检查该车次是否有错误信息
            if pickup_data.get('errors'):
                filtered_result_dict[pickup_number] = pickup_data
        error_type_counter = Counter([ab['type'] for ab in abnormalities])
        context = {
            'global_errors': abnormalities,
            'result': filtered_result_dict,
            'error_type_summary': dict(error_type_counter),
        }
        return context

    async def _nj_excel_normalization(self,request: HttpRequest) -> dict:
        form = UploadFileForm(request.POST, request.FILES)
        error_messages = [] #错误信息
        success_count = 0
        result = {}
        special_records = []  # 存储特殊记录（包含中文的,甩板什么的）

        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            required_columns = ['柜号', '仓点', '板数', 'CBM', '备注', '装柜顺序', '预约时间', 'ISA', 'PC号']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                error_messages.append(f"缺少必要的列: {', '.join(missing_columns)}")
            else:
                # 替换NaN值为空字符串
                df = df.fillna('')

                #第一步，按空行分割大组
                big_groups = []  # 所有大组
                # 第一步：按空行分割大组
                temp_group = []
                for index, row in df.iterrows():
                    # 检查是否为空行（所有主要列都为空）
                    if (row['柜号'] != '' and 
                        row['仓点'] == '' and 
                        row['预约时间'] == '' and 
                        row['ISA'] == '' and 
                        row['PC号'] == '' and 
                        row['备注'] == ''):
                        continue  # 跳过这一行
                    if (row['柜号'] == '' and row['仓点'] == '' and row['预约时间'] == '' and 
                        row['ISA'] == '' and row['PC号'] == '' and row['备注'] == ''):
                        if temp_group:  # 如果临时组不为空，则完成一个大组
                            big_groups.append(temp_group)
                            temp_group = []
                    else:
                        temp_group.append((index, row))
                #最后一个组，直接添加
                if temp_group:
                    big_groups.append(temp_group)
                
                # 第二步：处理每个大组，进一步按"一提两卸"分割小组
                for big_group in big_groups:                   
                    if not big_group:
                        continue
                    
                    group_errors = []  # 记录该大组内遇到的所有错误（会拼接）
                    small_groups = []  # 当前大组内的小组
                    temp_small_group = []
                    is_multiple = False
                    for i, (index, row) in enumerate(big_group):
                        temp_small_group.append((index, row))
                        
                        if ('一提两卸' in str(row['备注'])):
                            is_multiple = True
                        # 检查备注是否包含"一提两卸"，或者是否是最后一行
                        if ('一提两卸' in str(row['备注']) or i == len(big_group) - 1):
                            if temp_small_group:
                                small_groups.append(temp_small_group)
                                temp_small_group = []
                    # 如果还有剩余的行，添加到小组
                    if temp_small_group:
                        small_groups.append(temp_small_group)   

                    # 第三步：提取每个小组的数据
                    big_group_data = {
                        'fee': None,
                        'po': {}
                    }
                    # 提取车次号（从第一个小组的预约时间）
                    vehicle_number = None
                    for index, row in big_group:
                        appointment_time = str(row['ISA'])
                        if appointment_time.startswith('ZEM'):
                            # 取最后一个"-"前面的部分作为车次号
                            if is_multiple and '-' in appointment_time: #一提两卸的，两个预约批次都会写车次，一个是-1一个是-2.取-前面的
                                vehicle_number = appointment_time.rsplit('-', 1)[0]
                            else:
                                vehicle_number = appointment_time
                            break
                    
                    if not vehicle_number:
                        group_errors.append("未找到车次号（ZEM开头）")
                        vehicle_number = f"未知车次_行{big_group[0][0]}"
                    
                    # 提取费用（从ISA列，取第一个遇到的费用值）
                    fee_value = None
                    for index, row in big_group:
                        pc_number = str(row['PC号']).strip()
                        # 假设费用是数字格式
                        if pc_number and any('\u4e00' <= char <= '\u9fff' for char in pc_number) is False:
                            try:
                                # 尝试转换为数字
                                fv = float(pc_number)
                                if fv < 100000:
                                    fee_value = fv
                                    break  # 找到第一个小于10000的费用值就停止
                            except ValueError:
                                # 如果不是数字，继续寻找
                                continue
                    if fee_value is None:
                        group_errors.append("未找到费用")

                    big_group_data = {'fee': fee_value, 'po': {}, 'errors': ''}
                    # 处理每个小组
                    for sg_index, small_group in enumerate(small_groups):
                        batch_number = None  # 批次号
                        appointment_number = None  # 预约号
                        detail = {}  # 柜号:仓点映射
                        
                        # 提取批次号（从PC号列，非"BOL已做"的值）
                        for index, row in small_group:
                            pc_value = str(row['PC号']).strip()
                            #PC号列不包含BOL字样，光数字也不行，光数字的是费用
                            if pc_value and 'BOL' not in pc_value and any(char.isalpha() for char in pc_value):
                                batch_number = pc_value
                                break
                        if not batch_number:
                            group_errors.append(f"小组 {sg_index+1}：未找到批次号(PC号)")

                        # 提取预约号（从ISA列，ZEM开头的值）
                        for index, row in small_group:
                            isa_value = str(row['ISA']).strip()
                            
                            if isa_value:
                                try:
                                    # 尝试转换为数字
                                    isa_value = float(isa_value)
                                    if isa_value > 100000:
                                        
                                        appointment_number = int(isa_value)
                                        break  # 找到第一个小于10000的费用值就停止

                                except ValueError:
                                    # 如果不是数字，继续寻找
                                    continue
                        if not appointment_number:
                            group_errors.append(f"小组 {isa_value}：未找到预约号")

                        # 提取柜号和仓点详情，同时检查特殊记录
                        for index, row in small_group:
                            container_no = str(row['柜号']).strip()
                            if all('\u4e00' <= char <= '\u9fff' for char in container_no):
                                continue  # 如果全是中文就跳过当前循环
                            if 'NO' in container_no:
                                continue
                            container_no = re.sub(r"（.*?）|\(.*?\)|\(.*?\）|\（.*?\)", "", container_no).strip()

                            warehouse = str(row['仓点']).strip()
                            if '改' or '换标' in warehouse:
                                continue
                            warehouse = re.sub(r"（.*?）|\(.*?\)|\(.*?\）|\（.*?\)", "", warehouse).strip()
                            warehouse = re.sub(r'[\u4e00-\u9fff]', '', warehouse).strip()#把里面的中文去掉
                            loading_sequence = str(row['装柜顺序'])
                            cbm_value = str(row['CBM'])
                            remark = str(row['备注'])
                            # 判断是否包含甩板和加塞
                            has_special_keyword = False

                            # 检查三个列是否包含"甩板"或"加塞"
                            for text in [loading_sequence, cbm_value, remark]:
                                if '甩板' in text or '加塞' in text or '混送' in text:
                                    has_special_keyword = True

                            if container_no and warehouse:
                                if has_special_keyword:
                                    modified_container_no = container_no + '-加甩'
                                    detail[modified_container_no] = warehouse
                                else:
                                    detail[container_no] = warehouse
                            
                            # 检查是否包含中文
                            def contains_chinese_except_specific(text):
                                chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
                                return any(char not in ['甩', '板', '加', '塞', '满', '库', '存','混','送'] for char in chinese_chars)
                            
                            if contains_chinese_except_specific(loading_sequence) or contains_chinese_except_specific(cbm_value):
                                special_records.append({
                                    'index': index,
                                    '柜号': container_no,
                                    '仓点': warehouse,
                                    '装柜顺序': loading_sequence,
                                    'CBM': cbm_value,
                                    '备注': str(row['备注']),
                                    'PC号': batch_number
                                })
                        
                        if batch_number:
                            big_group_data['po'][batch_number] = {
                                '预约号': appointment_number,
                                'detail': detail
                            }
                    if group_errors:
                        big_group_data['errors'] = '；'.join(group_errors)
                    else:
                        big_group_data['errors'] = ''

                    # 只添加有数据的大组
                    if big_group_data['po']:
                        result[vehicle_number] = big_group_data
                        success_count += 1
                    else:
                        # 如果没有 po，但存在错误信息，也把错误放入全局错误（便于调试）
                        if big_group_data['errors']:
                            error_messages.append(f"车次 {vehicle_number}：{big_group_data['errors']}")
        context = {
            'result': result,
            'special_records': special_records,
            'error_messages': error_messages,
        }
        return context
    
    async def _sav_excel_normalization(self,request: HttpRequest) -> dict:
        form = UploadFileForm(request.POST, request.FILES)
        error_messages = [] #错误信息
        success_count = 0
        result = {}
        special_records = []  # 存储特殊记录（包含中文的,甩板什么的）

        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            required_columns = ['柜号', '仓库', '卡板', 'CBM', '备注', '装柜顺序', '正表同仓点', '预约时间', 'ISA', 'PC号', 'Note']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                error_messages.append(f"缺少必要的列: {', '.join(missing_columns)}")
            else:
                # 替换NaN值为空字符串
                df = df.fillna('')

                #第一步，按空行分割大组
                big_groups = []  # 所有大组
                # 第一步：按空行分割大组
                temp_group = []
                for index, row in df.iterrows():
                    # 检查是否为空行（所有主要列都为空）
                    if (row['柜号'] == '' and row['仓库'] == '' and row['预约时间'] == '' and 
                        row['ISA'] == '' and row['PC号'] == '' and row['备注'] == ''):
                        if temp_group:  # 如果临时组不为空，则完成一个大组
                            big_groups.append(temp_group)
                            temp_group = []
                    else:
                        temp_group.append((index, row))
                #最后一个组，直接添加
                if temp_group:
                    big_groups.append(temp_group)
                
                # 第二步：处理每个大组，进一步按"一提两卸"分割小组
                for big_group in big_groups:                   
                    if not big_group:
                        continue
                    
                    group_errors = []  # 记录该大组内遇到的所有错误（会拼接）
                    small_groups = []  # 当前大组内的小组
                    temp_small_group = []
                    is_multiple = False
                    for i, (index, row) in enumerate(big_group):
                        temp_small_group.append((index, row))
                        
                        if ('一提两卸' in str(row['备注'])):
                            is_multiple = True
                        # 检查备注是否包含"一提两卸"，或者是否是最后一行
                        if ('一提两卸' in str(row['备注']) or i == len(big_group) - 1):
                            if temp_small_group:
                                small_groups.append(temp_small_group)
                                temp_small_group = []
                    # 如果还有剩余的行，添加到小组
                    if temp_small_group:
                        small_groups.append(temp_small_group)   

                    # 第三步：提取每个小组的数据
                    big_group_data = {
                        'fee': None,
                        'po': {}
                    }
                    # 提取车次号（从第一个小组的预约时间）
                    vehicle_number = None
                    for index, row in big_group:
                        appointment_time = str(row['预约时间'])
                        if appointment_time.startswith('ZEM'):
                            # 取最后一个"-"前面的部分作为车次号
                            if is_multiple and '-' in appointment_time: #一提两卸的，两个预约批次都会写车次，一个是-1一个是-2.取-前面的
                                vehicle_number = appointment_time.rsplit('-', 1)[0]
                            else:
                                vehicle_number = appointment_time
                            break
                    
                    if not vehicle_number:
                        group_errors.append("未找到车次号（ZEM开头）")
                        vehicle_number = f"未知车次_行{big_group[0][0]}"
                    
                    # 提取费用（从ISA列，取第一个遇到的费用值）
                    fee_value = None
                    for index, row in big_group:
                        isa_value = str(row['ISA']).strip()
                        # 假设费用是数字格式
                        if isa_value:
                            try:
                                # 尝试转换为数字
                                fv = float(isa_value)
                                if fv < 100000:
                                    fee_value = fv
                                    break  # 找到第一个小于10000的费用值就停止
                            except ValueError:
                                # 如果不是数字，继续寻找
                                continue
                    if fee_value is None:
                        group_errors.append("未找到费用")

                    big_group_data = {'fee': fee_value, 'po': {}, 'errors': ''}
                    # 处理每个小组
                    for sg_index, small_group in enumerate(small_groups):
                        batch_number = None  # 批次号
                        appointment_number = None  # 预约号
                        detail = {}  # 柜号:仓库映射
                        
                        # 提取批次号（从PC号列，非"BOL已做"的值）
                        for index, row in small_group:
                            pc_value = str(row['PC号']).strip()
                            if pc_value and pc_value != 'BOL已做' and any('\u4e00' <= char <= '\u9fff' for char in pc_value) is False:
                                batch_number = pc_value
                                break
                        if not batch_number:
                            group_errors.append(f"小组 {sg_index+1}：未找到批次号(PC号)")

                        # 提取预约号（从ISA列，ZEM开头的值）
                        for index, row in small_group:
                            isa_value = str(row['ISA']).strip()
                            if isa_value:
                                try:
                                    # 尝试转换为数字
                                    isa_value = float(isa_value)
                                    if isa_value > 100000:
                                        appointment_number = int(isa_value)
                                        break  # 找到第一个小于10000的费用值就停止
                                except ValueError:
                                    # 如果不是数字，继续寻找
                                    continue
                        if not appointment_number:
                            group_errors.append(f"小组 {sg_index+1}：未找到预约号")

                        # 提取柜号和仓库详情，同时检查特殊记录
                        for index, row in small_group:
                            container_no = str(row['柜号']).strip()
                            if all('\u4e00' <= char <= '\u9fff' for char in container_no):
                                continue  # 如果全是中文就跳过当前循环
                            if 'NO' in container_no:
                                continue
                            container_no = re.sub(r"（.*?）|\(.*?\)|\(.*?\）|\（.*?\)", "", container_no).strip()

                            warehouse = str(row['仓库']).strip()
                            if '改' or '换标' in warehouse:
                                continue
                            warehouse = re.sub(r"（.*?）|\(.*?\)|\(.*?\）|\（.*?\)", "", warehouse).strip()
                            warehouse = re.sub(r'[\u4e00-\u9fff]', '', warehouse).strip()#把里面的中文去掉
                            loading_sequence = str(row['装柜顺序'])
                            cbm_value = str(row['CBM'])
                            remark = str(row['备注'])
                            # 判断是否包含甩板和加塞
                            has_special_keyword = False

                            # 检查三个列是否包含"甩板"或"加塞"
                            for text in [loading_sequence, cbm_value, remark]:
                                if '甩板' in text or '加塞' in text or '混送' in text:
                                    has_special_keyword = True

                            if container_no and warehouse:
                                if has_special_keyword:
                                    modified_container_no = container_no + '-加甩'
                                    detail[modified_container_no] = warehouse
                                else:
                                    detail[container_no] = warehouse
                            
                            # 检查是否包含中文
                            def contains_chinese_except_specific(text):
                                chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
                                return any(char not in ['甩', '板', '加', '塞', '满', '库', '存','混','送'] for char in chinese_chars)
                            
                            if contains_chinese_except_specific(loading_sequence) or contains_chinese_except_specific(cbm_value):
                                special_records.append({
                                    'index': index,
                                    '柜号': container_no,
                                    '仓库': warehouse,
                                    '装柜顺序': loading_sequence,
                                    'CBM': cbm_value,
                                    '备注': str(row['备注']),
                                    'PC号': batch_number
                                })
                        
                        if batch_number:
                            big_group_data['po'][batch_number] = {
                                '预约号': appointment_number,
                                'detail': detail
                            }
                    if group_errors:
                        big_group_data['errors'] = '；'.join(group_errors)
                    else:
                        big_group_data['errors'] = ''

                    # 只添加有数据的大组
                    if big_group_data['po']:
                        result[vehicle_number] = big_group_data
                        success_count += 1
                    else:
                        # 如果没有 po，但存在错误信息，也把错误放入全局错误（便于调试）
                        if big_group_data['errors']:
                            error_messages.append(f"车次 {vehicle_number}：{big_group_data['errors']}")
        context = {
            'result': result,
            'special_records': special_records,
            'error_messages': error_messages,
        }
        return context

    async def handle_upload_container_has_appointment_get(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        warehouse = request.POST.get('warehouse')
        if warehouse == "SAV":
            result = await self._sav_excel_normalization(request)        
        elif warehouse == "NJ":
            result = await self._nj_excel_normalization(request)            
        #把result按照合并格式去处理
        shipment_table_rows = await self._process_format(result['result'])
        #统计下整体情况
        total_rows = len(shipment_table_rows)
        error_rows = sum(1 for row in shipment_table_rows if row.get("errors"))
        normal_rows = total_rows - error_rows
        total_batches = len({row["batch_number"] for row in shipment_table_rows if row.get("batch_number")})
        total_vehicles = len({row["vehicle_number"] for row in shipment_table_rows if row.get("vehicle_number")})
        context = {
            'shipment_table_rows':shipment_table_rows,
            'shipment_details_raw': result, 
            'special_records': result['special_records'],
            'global_errors': result['error_messages'],
            'summary': {
                'total_rows': total_rows,
                'error_rows': error_rows,
                'normal_rows': normal_rows,
                'total_batches': total_batches,
                'total_vehicles': total_vehicles,
            },
            'warehouse': request.POST.get('warehouse')
        }
        return self.template_main, context

    async def handle_upload_container_has_appointment_la(
            self, request: HttpRequest
    ) -> tuple[Any, Any]:
        result = await self._la_excel_normalization(request)

        # 把result按照合并格式去处理
        shipment_table_rows = await self._process_format(result['result'])
        # 统计下整体情况
        total_rows = len(shipment_table_rows)
        error_rows = sum(1 for row in shipment_table_rows if row.get("errors"))
        normal_rows = total_rows - error_rows
        total_batches = len({row["batch_number"] for row in shipment_table_rows if row.get("batch_number")})
        total_vehicles = len({row["vehicle_number"] for row in shipment_table_rows if row.get("vehicle_number")})
        context = {
            'shipment_table_rows': shipment_table_rows,
            'shipment_details_raw': result,
            'special_records': result['special_records'],
            'global_errors': result['error_messages'],
            'summary': {
                'total_rows': total_rows,
                'error_rows': error_rows,
                'normal_rows': normal_rows,
                'total_batches': total_batches,
                'total_vehicles': total_vehicles,
            },
            'warehouse': request.POST.get('warehouse')
        }
        return self.template_main, context

    def clean_value(self, val):
        """统一清理Excel值，处理空值、数值、字符串，避免strip()报错"""
        if pd.isna(val):
            return ""
        if isinstance(val, (int, float)):
            return str(val)
        return str(val).strip()

    def contains_chinese(self, text):
        """判断文本是否含汉字（用于PC号/ISA字段检查）"""
        cleaned_text = self.clean_value(text)
        return bool(re.search(r'[\u4e00-\u9fff]', cleaned_text))

    async def _la_excel_normalization(
            self, request: HttpRequest
    ) -> dict[str, dict[Any, Any] | list[Any]]:
        form = UploadFileForm(request.POST, request.FILES)
        error_messages = []  # 错误信息
        success_count = 0
        result = {}
        special_records = []  # 存储特殊记录（包含中文的,甩板什么的）

        if form.is_valid():
            file = request.FILES["file"]
            df = pd.read_excel(file)
            required_columns = ['柜号', '仓库', '卡板', 'CBM', '装车备注', '装车顺序', '预约时间', 'ISA',
                                'PC号', 'Platform', 'Note']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                error_messages.append(f"缺少必要的列: {', '.join(missing_columns)}")
            else:
                # 替换NaN值为空字符串
                df = df.fillna('')

                # 第一步，按空行分割大组
                big_groups = []  # 所有大组
                # 第一步：按空行分割大组
                temp_group = []
                for index, row in df.iterrows():
                    # 检查是否为空行（所有主要列都为空）
                    if (row['柜号'] == '' and row['仓库'] == '' and row['预约时间'] == '' and
                            row['ISA'] == '' and row['PC号'] == '' and row['装车备注'] == ''):
                        if temp_group:  # 如果临时组不为空，则完成一个大组
                            big_groups.append(temp_group)
                            temp_group = []
                    else:
                        temp_group.append((index, row))
                # 最后一个组，直接添加
                if temp_group:
                    big_groups.append(temp_group)

                # 第二步：处理每个大组，进一步按"一提两卸"分割小组
                for big_group in big_groups:
                    if not big_group:
                        continue

                    group_errors = []  # 记录该大组内遇到的所有错误（会拼接）
                    small_groups = []  # 当前大组内的小组
                    temp_small_group = []
                    is_multiple = False
                    for i, (index, row) in enumerate(big_group):
                        temp_small_group.append((index, row))

                        if ('一提两卸' in str(row['装车备注'])):
                            is_multiple = True
                        # 检查备注是否包含"一提两卸"，或者是否是最后一行
                        if ('一提两卸' in str(row['装车备注']) or i == len(big_group) - 1):
                            if temp_small_group:
                                small_groups.append(temp_small_group)
                                temp_small_group = []
                    # 如果还有剩余的行，添加到小组
                    if temp_small_group:
                        small_groups.append(temp_small_group)

                        # 第三步：提取每个小组的数据
                    big_group_data = {
                        'fee': None,
                        'po': {}
                    }
                    # 提取车次号（从第一个小组的预约时间）
                    vehicle_number = None
                    for index, row in big_group:
                        appointment_time = str(row['预约时间'])
                        if appointment_time.startswith('ZEM'):
                            # 取最后一个"-"前面的部分作为车次号
                            if is_multiple and '-' in appointment_time:  # 一提两卸的，两个预约批次都会写车次，一个是-1一个是-2.取-前面的
                                vehicle_number = appointment_time.rsplit('-', 1)[0]
                            else:
                                vehicle_number = appointment_time
                            break

                    if not vehicle_number:
                        group_errors.append("未找到车次号（ZEM开头）")
                        vehicle_number = f"未知车次_行{big_group[0][0]}"

                    # 提取费用（从ISA列，取第一个遇到的费用值）
                    fee_value = None
                    for index, row in big_group:
                        isa_value = str(row['ISA']).strip()
                        platform_value = str(row['Platform']).strip()
                        # 假设费用是数字格式
                        if isa_value:
                            try:
                                # 尝试转换为数字
                                fv = float(isa_value)
                                if fv < 100000:
                                    fee_value = fv
                                    break  # 找到第一个小于10000的费用值就停止
                            except ValueError:
                                # 如果不是数字，继续寻找
                                continue
                        elif platform_value:
                            try:
                                # 尝试转换为数字
                                fv = float(platform_value)
                                if fv < 100000:
                                    fee_value = fv
                                    break  # 找到第一个小于10000的费用值就停止
                            except ValueError:
                                # 如果不是数字，继续寻找
                                continue
                    if fee_value is None:
                        group_errors.append("未找到费用")

                    big_group_data = {'fee': fee_value, 'po': {}, 'errors': ''}
                    # 处理每个小组
                    for sg_index, small_group in enumerate(small_groups):
                        batch_number = None  # 批次号
                        appointment_number = None  # 预约号
                        detail = {}  # 柜号:仓库映射

                        # 提取批次号（从PC号列，非"BOL已做"的值）
                        for index, row in small_group:
                            pc_value = str(row['PC号']).strip()
                            if pc_value and not self.contains_chinese(pc_value):
                                batch_number = pc_value
                                break
                        if not batch_number:
                            group_errors.append(f"小组 {sg_index + 1}：未找到批次号(PC号)")

                        # 提取预约号（从ISA列，ZEM开头的值）
                        for index, row in small_group:
                            isa_value = str(row['ISA']).strip()
                            if isa_value:
                                try:
                                    # 尝试转换为数字
                                    isa_value = float(isa_value)
                                    if isa_value > 100000:
                                        appointment_number = isa_value
                                        break  # 找到第一个小于10000的费用值就停止
                                except ValueError:
                                    # 如果不是数字，继续寻找
                                    continue
                        if not appointment_number:
                            group_errors.append(f"小组 {sg_index+1}：未找到预约号")

                        # 提取柜号和仓库详情，同时检查特殊记录
                        for index, row in small_group:
                            container_no = str(row['柜号']).strip()
                            warehouse = str(row['仓库']).strip()
                            # 检查是否为特殊记录（装车顺序或CBM包含中文）
                            loading_sequence = str(row['装车顺序'])
                            cbm_value = str(row['CBM'])
                            remark = str(row['装车备注'])
                            # 判断是否包含甩板和加塞
                            has_special_keyword = False

                            # 检查三个列是否包含"甩板"或"加塞"
                            for text in [loading_sequence, cbm_value, remark]:
                                if '甩板' in text or '加塞' in text or '混送' in text:
                                    has_special_keyword = True

                            if container_no and warehouse:
                                if has_special_keyword:
                                    modified_container_no = container_no + '-加甩'
                                    detail[modified_container_no] = warehouse
                                else:
                                    detail[container_no] = warehouse

                            # 检查是否包含中文
                            def contains_chinese_except_specific(text):
                                chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
                                return any(char not in ['甩', '板', '加', '塞', '满', '库', '存','混','送'] for char in chinese_chars)

                            if contains_chinese_except_specific(loading_sequence) or contains_chinese_except_specific(
                                    cbm_value):
                                special_records.append({
                                    'index': index,
                                    '柜号': container_no,
                                    '仓库': warehouse,
                                    '装柜顺序': loading_sequence,
                                    'CBM': cbm_value,
                                    '备注': str(row['装车备注']),
                                    'PC号': batch_number
                                })

                        if batch_number:
                            if not detail:
                                group_errors.append(f"批次 {batch_number}：没有有效的柜号/仓库明细")
                            big_group_data['po'][batch_number] = {
                                '预约号': appointment_number,
                                'detail': detail
                            }
                    if group_errors:
                        big_group_data['errors'] = '；'.join(group_errors)
                    else:
                        big_group_data['errors'] = ''

                    # 只添加有数据的大组
                    if big_group_data['po']:
                        result[vehicle_number] = big_group_data
                        success_count += 1
                    else:
                        # 如果没有 po，但存在错误信息，也把错误放入全局错误（便于调试）
                        if big_group_data['errors']:
                            error_messages.append(f"车次 {vehicle_number}：{big_group_data['errors']}")

        context = {
            'result': result,
            'special_records': special_records,
            'error_messages': error_messages,
        }
        return context
    
    async def _process_format(self,result:dict) -> dict:
        sorted_vehicles = sorted(result.items(), 
                           key=lambda x: (1 if x[1].get('errors') else 0, x[0]), 
                           reverse=True)
        shipment_table_rows = []
        for vehicle_number, big_group_data in sorted_vehicles :
            po = big_group_data.get('po', {})
            # 计算 vehicle 的总 detail 行数
            vehicle_total_rows = sum(len(batch_info.get('detail', {})) or 0 for batch_info in po.values())
            if vehicle_total_rows == 0:
                # 如果没有明细行也做一行占位（避免丢失车次）
                vehicle_total_rows = 1

            vehicle_row_rendered = False
            errors = big_group_data.get('errors', '')
            if errors:
                error_list = [error.strip() for error in errors.split('；') if error.strip()]
            else:
                error_list = []
            for batch_number, batch_info in po.items():
                details = batch_info.get('detail', {}) or {}
                batch_row_count = len(details)
                if batch_row_count == 0:
                    batch_row_count = 1  # 保证至少一行占位

                batch_row_rendered = False
                # 若 detail 有多行，每一对 container/warehouse 为一行
                if details:
                    def clean_text(value: str) -> str:
                        # 去掉中文和括号内容
                        value = re.sub(r'[\u4e00-\u9fff]', '', value)  # 去除中文
                        value = re.sub(r'[\(（].*?[\)）]', '', value)  # 去除括号及内容
                        return value.strip()
                    for i, (container_no, warehouse) in enumerate(details.items()):
                        row = {
                            'vehicle_number': vehicle_number,
                            'fee': big_group_data.get('fee', '') if big_group_data.get('fee', '') is not None else '',
                            'show_vehicle': False,
                            'vehicle_rowspan': vehicle_total_rows,
                            'batch_number': batch_number,
                            'appointment_number': int(batch_info.get('预约号', 0)) if batch_info.get('预约号') else '',
                            'show_batch': False,
                            'batch_rowspan': batch_row_count,
                            'container_no': clean_text(container_no),
                            'warehouse': clean_text(warehouse),
                            'errors': error_list,
                            'has_errors': bool(error_list) 
                        }
                        if not vehicle_row_rendered:
                            row['show_vehicle'] = True
                            vehicle_row_rendered = True
                        if not batch_row_rendered:
                            row['show_batch'] = True
                            batch_row_rendered = True
                        shipment_table_rows.append(row)
                else:
                    # 没有 detail 的占位行
                    row = {
                        'vehicle_number': vehicle_number,
                        'fee': big_group_data.get('fee', '') if big_group_data.get('fee', '') is not None else '',
                        'show_vehicle': False,
                        'vehicle_rowspan': vehicle_total_rows,
                        'batch_number': batch_number,
                        'appointment_number': int(batch_info.get('预约号', 0)) if batch_info.get('预约号') else '',
                        'show_batch': False,
                        'batch_rowspan': batch_row_count,
                        'container_no': '',
                        'warehouse': '',
                        'errors': error_list,
                        'has_errors': bool(error_list)
                    }
                    if not vehicle_row_rendered:
                        row['show_vehicle'] = True
                        vehicle_row_rendered = True
                    if not batch_row_rendered:
                        row['show_batch'] = True
                        batch_row_rendered = True
                    shipment_table_rows.append(row)
        return shipment_table_rows
    
    