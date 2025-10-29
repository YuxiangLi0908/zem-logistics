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
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.http import HttpRequest, HttpResponse, HttpResponseForbidden
from warehouse.models.order import Order
from django.contrib.auth.models import User
from warehouse.models.container import Container
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet
from warehouse.models.invoice import Invoice
from warehouse.models.invoice_details import InvoiceDelivery

from warehouse.views.terminal49_webhook import T49Webhook

class ExceptionHandling(View):
    template_container_pallet = "exception_handling/shipment_actual.html"
    template_post_port_status = "exception_handling/post_port_status.html"
    template_delivery_invoice = "exception_handling/delivery_invoice.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "post_port_status":
            if self._validate_user_exception_handling(request.user):
                return await sync_to_async(render)(request, self.template_post_port_status)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
            
        elif step == "delivery_invoice":
            return await sync_to_async(render)(request, self.template_delivery_invoice)     
        elif step == "shipment_actual":
            if self._validate_user_exception_handling(request.user):
                return await sync_to_async(render)(request, self.template_container_pallet)
            else:
                return HttpResponseForbidden(
                    "You are not authenticated to access this page!"
                )
        
    async def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        #修改约状态相关的
        if step == "search_shipment":
            template, context = await self.handle_search_shipment(request)
            return await sync_to_async(render)(request, template, context)          
        elif step == "update_shipment_status":
            template, context = await self.handle_update_shipment_status(request)
            return await sync_to_async(render)(request, template, context)
        #修改主约和实际约
        elif step == "search_container":
            template, context = await self.handle_search_container(request)
            return await sync_to_async(render)(request, template, context)          
        elif step == "update_pallet_master_shipment":
            template, context = await self.handle_update_pallet_master_shipment(request)
            return await sync_to_async(render)(request, template, context)
        #派送账单相关的
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
    
    async def handle_search_container(self, request: HttpRequest):
        """处理查询柜号请求"""
        context = {}
        container_number = request.POST.get('container_number', '').strip()
        
        if not container_number:
            messages.error(request, "请输入柜号")
            return self.template_container_pallet, context
        
        try:
            # 查询柜号
            container = await sync_to_async(
                lambda: Container.objects.filter(container_number=container_number).first()
            )()
            
            if not container:
                messages.error(request, f"未找到柜号 '{container_number}' 的相关数据")
                return self.template_container_pallet, context
            
            # 查询该柜号下的所有pallet记录，并关联shipment表
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(
                    container_number=container
                ).select_related(
                    'shipment_batch_number', 
                    'master_shipment_batch_number'
                ).order_by('PO_ID'))
            )()
            
            if not pallets:
                messages.error(request, f"柜号 '{container_number}' 下没有找到任何托盘记录")
                return self.template_container_pallet, context
            
            # 按照PO_ID、shipment_batch_number、master_shipment_batch_number分组
            grouped_pallets = await self.group_pallets_by_shipment(pallets)
            
            context['container'] = container
            context['pallets'] = pallets
            context['grouped_pallets'] = grouped_pallets
            context['container_number'] = container_number
                
        except Exception as e:
            messages.error(request, f"查询失败: {str(e)}")
        
        return self.template_container_pallet, context
    
    async def group_pallets_by_shipment(self, pallets):
        """按照PO_ID、shipment_batch_number、master_shipment_batch_number分组pallet记录"""
        groups = {}
        
        for pallet in pallets:
            # 构建分组键
            po_id = pallet.PO_ID or '未分类'
            shipment_key = pallet.shipment_batch_number_id or '未绑定'
            master_shipment_key = pallet.master_shipment_batch_number_id or '未绑定'
            
            group_key = f"{po_id}|{shipment_key}|{master_shipment_key}"
            
            if group_key not in groups:
                groups[group_key] = {
                    'PO_ID': po_id,
                    'destination': pallet.destination,
                    'delivery_method': pallet.delivery_method,
                    'shipment_batch_number_id': pallet.shipment_batch_number_id,
                    'master_shipment_batch_number_id': pallet.master_shipment_batch_number_id,
                    'shipment_batch_number_display': await self.get_shipment_display(pallet.shipment_batch_number),
                    'master_shipment_batch_number_display': await self.get_shipment_display(pallet.master_shipment_batch_number),
                    'pallets': [],
                    'pallet_count': 0,
                    'total_cbm': 0,
                    'total_weight': 0,
                    'total_pcs': 0
                }
            
            # 添加pallet到组
            groups[group_key]['pallets'].append(pallet)
            groups[group_key]['pallet_count'] += 1
            groups[group_key]['total_cbm'] += (pallet.cbm or 0)
            groups[group_key]['total_weight'] += (pallet.weight_lbs or 0)
            groups[group_key]['total_pcs'] += (pallet.pcs or 0)
        
        return list(groups.values())
    
    async def get_shipment_display(self, shipment):
        """获取shipment的显示字符串"""
        if not shipment:
            return "未绑定"
        
        display_info = await sync_to_async(
            lambda: f"{shipment.shipment_batch_number} ({shipment.destination or '无目的地'})"
        )()
        return display_info
    
    async def handle_update_pallet_master_shipment(self, request: HttpRequest):
        """处理更新pallet的主约批次号绑定"""
        context = {}
        try:
            container_number = request.POST.get('container_number')
            po_id = request.POST.get('po_id')
            current_shipment_id = request.POST.get('current_shipment_id')
            current_master_shipment_id = request.POST.get('current_master_shipment_id')
            new_master_shipment_batch = request.POST.get('new_master_shipment_batch', '').strip()
            
            if not container_number:
                messages.error(request, "缺少柜号信息")
                return self.template_container_pallet, context
            
            if not new_master_shipment_batch:
                messages.error(request, "请输入新的主约批次号")
                return self.template_container_pallet, context
            
            # 查询柜号
            container = await sync_to_async(
                lambda: Container.objects.filter(container_number=container_number).first()
            )()
            
            if not container:
                messages.error(request, f"未找到柜号 '{container_number}'")
                return self.template_container_pallet, context
            
            # 查找新的主约shipment记录
            try:
                new_master_shipment = await sync_to_async(
                    lambda: Shipment.objects.get(shipment_batch_number=new_master_shipment_batch)
                )()
            except ObjectDoesNotExist:
                messages.error(request, f"未找到主约批次号 '{new_master_shipment_batch}'")
                return self.template_container_pallet, context
            except MultipleObjectsReturned:
                messages.error(request, f"找到多个主约批次号 '{new_master_shipment_batch}'，请核实")
                return self.template_container_pallet, context
            
            # 构建查询条件
            query_filters = {'container_number': container}
            if po_id and po_id != '未分类':
                query_filters['PO_ID'] = po_id
            if current_shipment_id and current_shipment_id != '未绑定':
                query_filters['shipment_batch_number_id'] = current_shipment_id
            if current_master_shipment_id and current_master_shipment_id != '未绑定':
                query_filters['master_shipment_batch_number_id'] = current_master_shipment_id
            
            # 更新符合条件的pallet记录的主约批次号
            updated_count = await sync_to_async(
                lambda: Pallet.objects.filter(**query_filters).update(
                    master_shipment_batch_number=new_master_shipment
                )
            )()
            
            messages.success(request, f"成功更新 {updated_count} 条托盘记录的主约批次号绑定")
            
            # 重新查询数据以更新显示
            pallets = await sync_to_async(
                lambda: list(Pallet.objects.filter(
                    container_number=container
                ).select_related(
                    'shipment_batch_number', 
                    'master_shipment_batch_number'
                ).order_by('PO_ID'))
            )()
            
            grouped_pallets = await self.group_pallets_by_shipment(pallets)
            
            context['container'] = container
            context['pallets'] = pallets
            context['grouped_pallets'] = grouped_pallets
            context['container_number'] = container_number
            
        except Exception as e:
            messages.error(request, f"更新失败: {str(e)}")
        
        return self.template_container_pallet, context
    
    async def handle_find_table_id_get(self,request):
        context = {}
        return self.template_delivery_invoice, context
    
    async def handle_search_shipment(self, request: HttpRequest):
        """处理查询shipment请求"""
        context = {}
        search_value = request.POST.get('shipment_batch_number', '').strip()
        search_type = request.POST.get('search_type', 'batch')
        
        if not search_value:
            messages.error(request, "请输入查询内容")
            return self.template_post_port_status, context
        
        try:
            # 根据查询类型构建查询条件
            if search_type == 'batch':
                # 按批次号查询
                shipment = await sync_to_async(
                    lambda: Shipment.objects.get(shipment_batch_number=search_value)
                )()
                context['search_type'] = 'batch'
                context['search_value'] = search_value
            else:
                # 按预约号查询
                shipment = await sync_to_async(
                    lambda: Shipment.objects.get(appointment_id=search_value)
                )()
                context['search_type'] = 'appointment'
                context['search_value'] = search_value
            
            # 将单个shipment对象放入列表中，保持前端模板的一致性
            context['shipments'] = [shipment]
            context['search_batch_number'] = search_value
            
            # 计算状态和可用操作
            shipment.current_status = await self.get_shipment_status(shipment)
            shipment.status_display = await self.get_status_display_name(shipment.current_status)
            shipment.available_operations = await self.get_available_operations(shipment.current_status)
                
        except MultipleObjectsReturned:
            messages.error(request, f"找到多个匹配的记录，请核实查询条件：{search_value}")
        except ObjectDoesNotExist:
            if search_type == 'batch':
                messages.error(request, f"未找到批次号 '{search_value}' 的相关数据")
            else:
                messages.error(request, f"未找到预约号 '{search_value}' 的相关数据")
        except Exception as e:
            messages.error(request, f"查询失败: {str(e)}")
        
        return self.template_post_port_status, context
    
    async def handle_update_shipment_status(self, request: HttpRequest):
        """处理更新shipment状态请求"""
        context = {}
        
        shipment_id = request.POST.get('shipment_id')
        target_status = request.POST.get('target_status')
        search_batch_number = request.POST.get('search_batch_number')
        search_type = request.POST.get('search_type', 'batch')
        
        if not shipment_id or not target_status:
            messages.error(request, "缺少必要参数")
            return self.template_post_port_status, context
        
        # 异步获取shipment对象
        shipment = await sync_to_async(
            lambda: Shipment.objects.select_related('fleet_number').filter(id=shipment_id).first()
        )()
        
        if not shipment:
            messages.error(request, "未找到对应的shipment记录")
            return self.template_post_port_status, context
        fleet = shipment.fleet_number if shipment else None
        # 根据目标状态更新相应字段
        await self.update_shipment_status_fields(shipment, target_status, fleet)
        
        status_name = await self.get_status_display_name(target_status.replace('cancel_', ''))
        messages.success(request, f"状态已更新为: {status_name}")
        
        # 重新查询数据以更新显示
        if search_batch_number:
            try:
                if search_type == 'batch':
                    # 按批次号重新查询
                    shipment = await sync_to_async(
                        lambda: Shipment.objects.get(shipment_batch_number=search_batch_number)
                    )()
                else:
                    # 按预约号重新查询
                    shipment = await sync_to_async(
                        lambda: Shipment.objects.get(appointment_id=search_batch_number)
                    )()
                
                # 将单个shipment对象放入列表中
                context['shipments'] = [shipment]
                context['search_batch_number'] = search_batch_number
                context['search_type'] = search_type
                context['search_value'] = search_batch_number
                
                # 重新计算状态和可用操作
                shipment.current_status = await self.get_shipment_status(shipment)
                shipment.status_display = await self.get_status_display_name(shipment.current_status)
                shipment.available_operations = await self.get_available_operations(shipment.current_status)
                
            except MultipleObjectsReturned:
                messages.error(request, f"找到多个匹配的记录，请核实：{search_batch_number}")
            except ObjectDoesNotExist:
                if search_type == 'batch':
                    messages.error(request, f"未找到批次号 '{search_batch_number}' 的相关数据")
                else:
                    messages.error(request, f"未找到预约号 '{search_batch_number}' 的相关数据")
        
        return self.template_post_port_status, context
    
    async def update_shipment_status_fields(self, shipment, target_status, fleet):
        """根据目标状态更新shipment的各个字段"""
        #target_status就是想改为的状态        
        if target_status == 'cancel_shipped': 
            await self.cancel_shipped_status(shipment, fleet)
        elif target_status == 'cancel_arrived':
            await self.cancel_arrived_status(shipment, fleet)
        elif target_status == 'cancel_pod_uploaded':
            await self.cancel_pod_uploaded_status(shipment, fleet)

    async def cancel_shipped_status(self, shipment, fleet):
        """取消发货状态"""
        # 更新shipment状态
        await sync_to_async(self._cancel_shipped_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_shipped_fleet)(fleet)

    def _cancel_shipped_shipment(self, shipment):
        """取消发货状态 - shipment部分"""
        shipment.is_shipped = False
        shipment.shipped_at = None
        shipment.shipped_at_utc = None
        # 未送达
        shipment.is_arrived = False
        shipment.arrived_at = None
        shipment.arrived_at_utc = None
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_shipped_fleet(self, fleet):
        """取消发货状态 - fleet部分"""
        fleet.departured_at = None
        fleet.arrived_at = None
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()

    async def cancel_arrived_status(self, shipment, fleet):
        """取消送达状态（回退到已发货）"""
        # 更新shipment状态
        await sync_to_async(self._cancel_arrived_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_arrived_fleet)(fleet)

    def _cancel_arrived_shipment(self, shipment):
        """取消送达状态 - shipment部分"""
        shipment.is_arrived = False
        shipment.arrived_at = None
        shipment.arrived_at_utc = None
        # 同时取消POD状态
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_arrived_fleet(self, fleet):
        """取消送达状态 - fleet部分"""
        fleet.arrived_at = None
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()

    async def cancel_pod_uploaded_status(self, shipment, fleet):
        """取消POD状态（回退到已送达）"""
        # 更新shipment状态
        await sync_to_async(self._cancel_pod_uploaded_shipment)(shipment)
        
        # 如果存在fleet，更新fleet状态
        if fleet:
            await sync_to_async(self._cancel_pod_uploaded_fleet)(fleet)

    def _cancel_pod_uploaded_shipment(self, shipment):
        """取消POD状态 - shipment部分"""
        shipment.pod_uploaded_at = None
        shipment.pod_link = None
        shipment.save()

    def _cancel_pod_uploaded_fleet(self, fleet):
        """取消POD状态 - fleet部分"""
        fleet.pod_link = None
        fleet.pod_uploaded_at = None
        fleet.save()
    
    async def get_shipment_status(self, shipment):
        """
        获取shipment的当前状态
        按照优先级判断：POD上传 > 已送达 > 已发货 > 已预约
        """
        if shipment.pod_uploaded_at:
            return 'pod_uploaded'
        elif shipment.is_arrived:
            return 'arrived'
        elif shipment.is_shipped:
            return 'shipped'
        elif shipment.is_shipment_schduled:
            return 'scheduled'
        else:
            return 'unknown'
    
    async def get_status_display_name(self, status):
        """获取状态显示名称"""
        status_names = {
            'scheduled': '已预约',
            'shipped': '已发货', 
            'arrived': '已送达',
            'pod_uploaded': '已上传POD',
            'unknown': '未知'
        }
        return status_names.get(status, status)
    
    async def get_available_operations(self, current_status):
        """
        根据当前状态获取可用的操作
        """
        operations = []
        
        if current_status == 'pod_uploaded':
            # 已上传POD状态可以回退到已送达，或者取消POD状态
            operations.append(('cancel_pod_uploaded', '取消POD', 'btn-danger'))
            operations.append(('cancel_arrived', '取消送达', 'btn-secondary'))
            operations.append(('cancel_shipped', '取消发货', 'btn-secondary'))
        
        elif current_status == 'arrived':
            # 已送达状态可以回退到已发货，或者取消送达状态
            operations.append(('cancel_arrived', '取消送达', 'btn-danger'))
            operations.append(('cancel_shipped', '取消发货', 'btn-secondary'))
        
        elif current_status == 'shipped':
            # 已发货状态可以回退到已预约，或者取消发货状态
            operations.append(('cancel_shipped', '取消发货', 'btn-danger'))
        
        return operations
    
    async def handle_search_invoice_delivery(self, request: HttpRequest):
        """查询 Invoice Delivery 记录"""
        search_type = request.POST.get("search_type")
        search_value = request.POST.get("search_value", "").strip()

        if not search_type or not search_value:
            messages.error(request, "请选择查询类型并输入查询值")
            return self.template_delivery_invoice, {"search_performed": False}

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
            return self.template_delivery_invoice, context

        except Exception as e:
            messages.error(request, f"查询过程中发生错误: {str(e)}")
            return self.template_delivery_invoice, {"search_performed": True}

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
    
    async def _validate_user_exception_handling(self, user: User) -> bool:
        if user.is_staff:
            return True
        
        return await sync_to_async(
            lambda: user.groups.filter(name="exception_handling").exists()
        )()