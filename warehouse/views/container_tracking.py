from typing import Any
from django.db.models.functions import Trim

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.utils.decorators import method_decorator
from django.views import View
import pandas as pd
import re
from warehouse.forms.upload_file import UploadFileForm
from asgiref.sync import sync_to_async

from warehouse.models.order import Order
from warehouse.models.container import Container
from warehouse.models.shipment import Shipment
from warehouse.models.fleet import Fleet
from warehouse.models.pallet import Pallet

from warehouse.views.terminal49_webhook import T49Webhook

class ContainerTracking(View):
    t49_tracking_url = "https://api.terminal49.com/v2/tracking_requests"
    template_main = "container_tracking/main_page.html"
    template_po_sp_match = "container_tracking/po_sp_match.html"

    async def get(self, request: HttpRequest) -> HttpResponse:
        step = request.GET.get("step", None)
        if step == "actual_match":
            return await sync_to_async(render)(request, self.template_po_sp_match)
        else:
            return await sync_to_async(render)(request, self.template_main)

    async def post(self, request: HttpRequest) -> HttpResponse:
        step = request.POST.get("step", None)
        if step == "tracking_request":
            pass
        elif step == "upload_container_has_appointment":
            warehouse = request.POST.get('warehouse')
            if warehouse == "SAV":
                template, context = await self.handle_upload_container_has_appointment_sav(request)
                return await sync_to_async(render)(request, template, context)
        elif step == "po_sp_match_search":
            warehouse = request.POST.get('warehouse')
            if warehouse == "SAV":
                template, context = await self.handle_po_sp_match_search_sav(request)
                return await sync_to_async(render)(request, template, context)
        elif step == "Standardize_ISA":
            template, context = await self.handle_Standardize_ISA(request)
            return await sync_to_async(render)(request, template, context)
        else:
            return await sync_to_async(T49Webhook().post)(request)
        
    async def _send_tracking_request(self):
        pass
    
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

    async def handle_po_sp_match_search_sav(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        result = await self._sav_excel_normalization(request)
        match_result = await self.check_appointment_abnormalities(result['result'])
        shipment_table_rows = await self._process_format(match_result['result'])
        context = {
            'result': match_result['result'],
            'shipment_table_rows': shipment_table_rows,
            'global_errors': match_result['global_errors'],
        }
        return self.template_po_sp_match, context

    async def check_appointment_abnormalities(self, result_dict):
        abnormalities = []
        for pickup_number, pickup_data in result_dict.items():
            # 初始化该车次的异常列表
            group_abnormalities = []
            # 异常1: 检查车次是否存在
            try:
                fleet = await Fleet.objects.aget(pickup_number=pickup_number)
            except Fleet.DoesNotExist:
                abnormality_msg = f'车次 {pickup_number} 在系统中不存在'
                group_abnormalities.append(abnormality_msg)
                #加到总的报错信息里
                abnormalities.append({
                    'type': '车次不存在',
                    'pickup_number': pickup_number,
                    'message': abnormality_msg
                })
                # 将异常添加到该车次的errors中
                if pickup_data.get('errors'):
                    pickup_data['errors'] += f'；{abnormality_msg}'
                else:
                    pickup_data['errors'] = abnormality_msg
                continue
            # 处理每个预约批次
            for batch_number, batch_data in pickup_data['po'].items():
                appointment_number = batch_data['预约号']
                detail = batch_data['detail']
                
                # 异常2: 检查预约批次是否存在
                shipment_by_batch = None
                shipment_by_appointment = None
                try:
                    shipment_by_batch = await Shipment.objects.aget(shipment_batch_number=batch_number)
                except Shipment.DoesNotExist:
                    # 如果通过batch_number找不到，尝试通过appointment_number查找
                    try:
                        shipment_by_appointment = await Shipment.objects.aget(appointment_id=str(appointment_number))
                        
                        # 找到了shipment，但batch_number不匹配
                        abnormality_msg = f'预约批次号不匹配: 预约号 {appointment_number} 对应的批次号应为 {shipment_by_appointment.shipment_batch_number}，而不是 {batch_number}'
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
                        abnormality_msg = f'预约批次 {batch_number} (预约号: {appointment_number}) 在系统中不存在'
                        group_abnormalities.append(abnormality_msg)
                        abnormalities.append({
                            'type': '预约批次不存在',
                            'pickup_number': pickup_number,
                            'batch_number': batch_number,
                            'appointment_number': appointment_number,
                            'message': abnormality_msg
                        })
                        continue
                else:
                    shipment = shipment_by_batch
                
                # 异常3: 检查预约号是否匹配
                if shipment_by_batch and str(shipment.appointment_id) != str(appointment_number):
                    abnormality_msg = f'批次 {batch_number} 预约号不匹配: 期望 {appointment_number}, 实际 {shipment.appointment_id}'
                    group_abnormalities.append(abnormality_msg)
                    abnormalities.append({
                        'type': '预约号不匹配',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'expected_appointment': appointment_number,
                        'actual_appointment': shipment.appointment_id,
                        'message': abnormality_msg
                    })
                
                # 异常4: 检查shipment是否关联到正确的车次
                if shipment.fleet_number_id != fleet.id:
                    abnormality_msg = f'批次 {batch_number} 未关联到车次 {pickup_number}，实际关联到车次 {shipment.fleet_number.fleet_number if shipment.fleet_number else "无"}'
                    group_abnormalities.append(abnormality_msg)
                    abnormalities.append({
                        'type': '车次关联错误',
                        'pickup_number': pickup_number,
                        'batch_number': batch_number,
                        'actual_fleet': shipment.fleet_number.fleet_number if shipment.fleet_number else "无",
                        'message': abnormality_msg
                    })
                # 检查每个柜号-仓点组合
                for container_no, expected_warehouse in detail.items():
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
                            if pallet.shipment_batch_number.shipment_batch_number == batch_number:
                                matched_pallets.append(pallet)
                            else:
                                unmatched_pallets.append(pallet)
                        if not matched_pallets:
                            # 异常6：没有关联到正确的预约批次
                            total_count = len(pallets)
                            unmatched_count = len(unmatched_pallets)
                            actual_batches = list(set(p.shipment_batch_number.shipment_batch_number for p in pallets))
                            abnormality_msg = f'柜号 {container_no} 仓点 {expected_warehouse} 未关联到预约批次 {batch_number}，共{total_count}条记录，{unmatched_count}条批次不匹配，实际关联到批次: {", ".join(actual_batches)}'
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
        context = {
            'global_errors': abnormalities,
            'result': result_dict
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
                            if pc_value and pc_value != 'BOL已做':
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
                            group_errors.append(f"小组 {sg_index+1}：未找到预约号(ISA列中>100000的值)")

                        # 提取柜号和仓库详情，同时检查特殊记录
                        for index, row in small_group:
                            container_no = str(row['柜号']).strip()
                            warehouse = str(row['仓库']).strip()
                            
                            if container_no and warehouse:
                                detail[container_no] = warehouse
                            
                            # 检查是否为特殊记录（装柜顺序或CBM包含中文）
                            loading_sequence = str(row['装柜顺序'])
                            cbm_value = str(row['CBM'])
                            
                            # 检查是否包含中文
                            def contains_chinese_except_specific(text):
                                chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
                                return any(char not in ['满', '库', '存'] for char in chinese_chars)
                            
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
    
    async def handle_upload_container_has_appointment_sav(
        self, request: HttpRequest
    ) -> tuple[Any, Any]:
        result = await self._sav_excel_normalization(request)
                       
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
                            'errors': big_group_data.get('errors', '') or ''
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
                        'errors': big_group_data.get('errors', '') or ''
                    }
                    if not vehicle_row_rendered:
                        row['show_vehicle'] = True
                        vehicle_row_rendered = True
                    if not batch_row_rendered:
                        row['show_batch'] = True
                        batch_row_rendered = True
                    shipment_table_rows.append(row)
        return shipment_table_rows
    
    