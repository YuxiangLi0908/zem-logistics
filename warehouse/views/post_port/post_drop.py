import json
import os
import string
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import BytesIO
from itertools import zip_longest
from pathlib import Path
import random
from typing import Any, Coroutine
import re

import chardet
import numpy as np
import pandas as pd
from asgiref.sync import sync_to_async
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import Sum
from django.http import HttpRequest, HttpResponse, Http404
from django.shortcuts import render
from django.utils.dateparse import parse_date
from django.views import View
from simple_history.utils import bulk_update_with_history, bulk_create_with_history

from warehouse.forms.upload_file import UploadFileForm
from django.utils import timezone

from warehouse.models.container import Container
from warehouse.models.container_pickup_carrier import ContainerPickupCarrier
from warehouse.models.customer import Customer
from warehouse.models.fee_detail import FeeDetail
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.po_check_eta import PoCheckEtaSeven
from warehouse.models.quotation_master import QuotationMaster
from warehouse.models.retrieval import Retrieval
from warehouse.models.vessel import Vessel
from warehouse.models.warehouse import ZemWarehouse
from warehouse.models.system_parameter import SystemParameter
from warehouse.utils.constants import SHIPPING_LINE_OPTIONS, DELIVERY_METHOD_OPTIONS, ADDITIONAL_CONTAINER, \
    PACKING_LIST_TEMP_COL_MAPPING, DROPSHIPPING_PACKING_LIST_TEMP_COL_MAPPING, DELIVERY_METHOD_CODE
from warehouse.views.export_file import export_do

import json
from django.utils.safestring import mark_safe
from django.shortcuts import redirect, render
from django.db.models import Prefetch, F, Subquery, OuterRef, Exists, Min
from typing import Any, Dict, List, Tuple
from django.contrib.postgres.aggregates import StringAgg
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models import (
    Case,
    CharField,
    BooleanField,
    Count,
    F,
    Func,
    FloatField,
    IntegerField,
    Max,
    Q,
    Sum,
    Value,
    When,
)
from datetime import datetime, timedelta, time
from warehouse.utils.constants import (
    LOAD_TYPE_OPTIONS,
    DELIVERY_METHOD_OPTIONS, DELIVERY_METHOD_CODE
)
from warehouse.views.post_port.post_nsop import PostNsop

    

class PostDrop(View):
    template_ltl_pos_all = "post_port/new_sop/07_drop_shipping/07_ltl_main.html"
    template_account_rec = "post_port/new_sop/08_drop_ship_account/09_account_main.html"


    container_type = {
        "": "",
        "40HQ/GP": "40HQ/GP",
        "45HQ/GP": "45HQ/GP",
        "20GP": "20GP",
        "53HQ": "53HQ",
    }
    shipment_type_options = {
        "FTL": "FTL",
        "外配": "外配",
        # "LTL": "LTL",     
        # "快递": "快递",
        # "客户自提": "客户自提",
    }
    arm_account_options = {
        "": "",
        "Carrier Central ADWG": "Carrier Central ADWG",
        "Carrier Central1": "Carrier Central1",
        "Carrier Central2": "Carrier Central2",
        "ZEM-AMF": "ZEM-AMF",
        "ARM-AMF": "ARM-AMF",
        "walmart": "walmart",
    }
    abnormal_fleet_options = {
        "": "",
        "司机未按时提货": "司机未按时提货",
        "送仓被拒收": "送仓被拒收",
        "未送达": "未送达",
        "其它": "其它",
    }

    order_type = {"一件代发": "一件代发"}
    area = {"NJ": "NJ", "SAV": "SAV", "LA": "LA",}

    async def get(self, request: HttpRequest, **kwargs) -> Any | None:
        step = request.GET.get("step")
        pk = kwargs.get("pk", None)
        if step == "postport_delivery":
            # 获取所有客户
            customers = await sync_to_async(list)(Customer.objects.all())
            customers_dict = {c.zem_name: str(c.id) for c in customers}
            # 添加----选项
            customers_dict = {"----": None, **customers_dict}
            # 默认选中除了"new fortun"外的所有客户（使用字符串类型）
            customer_list = [customers_dict[k] for k in customers_dict.keys() if k != "----" and k.lower() != "new fortun"]
            context = {
                "customers": customers_dict,
                "customer_list": customer_list,
                "warehouse_options": await sync_to_async(list)(
                    ZemWarehouse.objects
                    .order_by("name")
                    .values_list("name", "name")
                )
            }
            return await sync_to_async(render)(request, self.template_ltl_pos_all, context)
        elif step =="account_rec":
            # 应收账单界面
            context = {
                "warehouse_options": await sync_to_async(list)(
                    ZemWarehouse.objects
                    .order_by("name")
                    .values_list("name", "name")
                )
            }
            return await sync_to_async(render)(request, self.template_account_rec, context)
        else:
            raise ValueError('wrong step',step)


    async def post(self, request: HttpRequest, **kwargs) -> None | HttpResponse | tuple[Any, Any] | Any:
        step = request.POST.get("step")
        if step == "ltl_post_warehouse":
            template, context = await self.handle_ltl_unscheduled_pos_post(request)
            return render(request, template, context)
        elif step == "verify_ltl_cargo":
            template, context = await self.handle_verify_ltl_cargo(request)
            return render(request, template, context)
        elif step == "export_ltl_unscheduled":
            return await self.export_ltl_unscheduled(request)
        elif step == "save_releaseCommand":
            template, context = await self.handle_save_releaseCommand(request)
            return render(request, template, context)
        else:
            raise ValueError('wrong step',step)
        
    async def handle_ltl_unscheduled_pos_post(
            self, request: HttpRequest, context: dict | None = None,
    ) -> tuple[str, dict[str, Any]]:
        '''LTL组的港后全流程'''
        warehouse = request.POST.get("warehouse")
        if not context:
            context = {}
        if warehouse:
            warehouse_name = warehouse
        else:
            context.update({'error_messages': "没选仓库！"})
            return self.template_ltl_pos_all, context

        # 获取客户筛选条件（保持字符串类型）
        customer_idlist = request.POST.getlist("customer")

        # 构建基础筛选条件
        pl_criteria = Q(
            container_number__orders__offload_id__offload_at__isnull=True,
            shipment_batch_number__shipment_batch_number__isnull=True,
            container_number__orders__retrieval_id__retrieval_destination_precise=warehouse,
            delivery_type="一件代发"
        )
        plt_criteria = Q(
            location=warehouse,
            shipment_batch_number__shipment_batch_number__isnull=True,
            container_number__orders__offload_id__offload_at__gt=datetime(2025, 12, 1),
            delivery_type="一件代发"
        )

        # 如果有客户筛选条件，添加到筛选条件中
        if customer_idlist:
            # 将字符串ID转换为整数
            customer_idlist_int = [int(id) for id in customer_idlist if id]
            # 获取选中客户的 zem_name
            customer_names = await sync_to_async(list)(
                Customer.objects.filter(id__in=customer_idlist_int).values_list("zem_name", flat=True)
            )
            # 添加客户筛选条件
            pl_criteria &= Q(container_number__orders__customer_name__zem_name__in=customer_names)
            plt_criteria &= Q(container_number__orders__customer_name__zem_name__in=customer_names)

        # 未放行、已放行-客提、已放行-自发
        pn = PostNsop()
        release_cargos, _, selfdel_cargos = await pn._get_classified_cargos(pl_criteria, plt_criteria)

        # 未排车
        unschedule_fleet = await pn._ltl_unscheduled_data(request, warehouse)
        # 待出库
        ready_to_ship_data = await pn._ltl_ready_to_ship_data(warehouse, request.user)
        # 待送达
        delivery_data_raw = await pn._fl_delivery_get(warehouse, None, 'ltl')
        delivery_data = delivery_data_raw['shipments']
        # #待传POD
        pod_data = await pn._ltl_pod_get(warehouse)
        # #待传出库单
        shipping_data = await pn._ltl_shipping_get(warehouse)
        pod_data = sorted(
            pod_data,
            key=lambda p: p.pod_to_customer is True
        )
        summary = {
            'release_count': len(release_cargos),
            'selfdel_count': len(selfdel_cargos),
            'ready_to_ship_count': len(ready_to_ship_data),
            'shipping_count': len(shipping_data),
            'ready_count': len(delivery_data),
            'pod_count': len(pod_data),
            'unfleet_count': len(unschedule_fleet),
        }
        if not context:
            context = {}
        supplier_mapping = await sync_to_async(SystemParameter.get_active_by_category)("私仓供应商")

        # 获取所有客户数据
        customers = await sync_to_async(list)(Customer.objects.all())
        customers_dict = {c.zem_name: str(c.id) for c in customers}
        customers_dict = {"----": None, **customers_dict}

        context.update({
            'warehouse': warehouse,
            'warehouse_options': await sync_to_async(list)(
                ZemWarehouse.objects
                .order_by("name")
                .values_list("name", "name")
            ),
            'account_options': self.arm_account_options,
            'load_type_options': LOAD_TYPE_OPTIONS,
            "release_cargos": release_cargos,
            "selfdel_cargos": selfdel_cargos,
            "ready_to_ship_data": ready_to_ship_data,
            "shipping_data": shipping_data,
            "delivery_data": delivery_data,
            "pod_data": pod_data,
            "summary": summary,
            'shipment_type_options': self.shipment_type_options,
            "carrier_options": await pn.get_carrier_other_options(),
            "abnormal_fleet_options": self.abnormal_fleet_options,
            "warehouse_name": warehouse_name,
            'unschedule_fleet': unschedule_fleet,
            "supplier_mapping_json": mark_safe(json.dumps(supplier_mapping)),
            "customers": customers_dict,
            "customer_list": customer_idlist
        })
        active_tab = request.POST.get('active_tab')
        if active_tab:
            context.update({'active_tab': active_tab})
        return self.template_ltl_pos_all, context

    async def handle_verify_ltl_cargo(
            self, request: HttpRequest, context: dict | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """LTL对po更改核实状态"""
        if not context:
            context = {}
        cargo_ids = request.POST.get('cargo_ids', '')
        ltl_verify = request.POST.get('ltl_verify', 'false').lower() == 'true'

        # 处理 PackingList 的核实（Pallet 没有 ltl_verify 字段，跳过 plt_ 前缀的ID）
        if cargo_ids:
            packinglist_ids = []
            for id_str in cargo_ids.split(','):
                id_str = id_str.strip()
                if id_str and not id_str.startswith('plt_'):
                    try:
                        packinglist_ids.append(int(id_str))
                    except ValueError:
                        pass
            if packinglist_ids:
                await sync_to_async(PackingList.objects.filter(
                    id__in=packinglist_ids
                ).update)(
                    ltl_verify=ltl_verify
                )
        return await self.handle_ltl_unscheduled_pos_post(request)

    async def handle_save_releaseCommand(
            self, request: HttpRequest
    ) -> tuple[str, dict[str, Any]]:
        '''单条或批量保存未放行的指令数据'''
        # 1. 尝试获取批量数据
        batch_commands_raw = request.POST.get('batch_commands')
        tasks = []

        if batch_commands_raw:
            try:
                commands_list = json.loads(batch_commands_raw)
            except json.JSONDecodeError:
                commands_list = []
        else:
            cargo_id = request.POST.get('cargo_id')
            release_command = request.POST.get('release_command')
            if cargo_id:
                commands_list = [{'cargo_id': cargo_id, 'command': release_command}]
            else:
                commands_list = []
        num = 0
        for item in commands_list:
            c_id = item.get('cargo_id')
            command_text = item.get('command')
            if not c_id or not command_text: continue

            if c_id.startswith('plt_'):
                target_ids = c_id.replace('plt_', '').split(',')
                model = Pallet

            else:
                target_ids = c_id.split(',')
                model = PackingList
            update_data = {'ltl_release_command': command_text}
            await sync_to_async(model.objects.filter(id__in=target_ids).update)(**update_data)
            num += 1
        context = {'success_messages': f'保存成功{num}组数据!'}
        return await self.handle_ltl_unscheduled_pos_post(request, context)

    async def export_ltl_unscheduled(
            self, request: HttpRequest
    ) -> HttpResponse:
        """导出未放行货物到Excel"""
        cargo_ids = request.POST.get('cargo_ids', '')

        # 构建筛选条件
        pl_criteria = Q()
        plt_criteria = models.Q(pk__isnull=True) & models.Q(pk__isnull=False)

        # 如果指定了 ID，则只导出选中的货物
        if cargo_ids:
            cargo_id_list = []
            for id_str in cargo_ids.split(','):
                id_str = id_str.strip()
                if id_str and not id_str.startswith('plt_'):
                    try:
                        cargo_id_list.append(int(id_str))
                    except ValueError:
                        pass
            if cargo_id_list:
                pl_criteria &= Q(id__in=cargo_id_list)

        # 获取数据
        pn = PostNsop()
        release_cargos = await pn._ltl_unscheduled_cargo(pl_criteria, plt_criteria)

        # 准备 Excel 数据
        excel_data = []
        for cargo in release_cargos:
            customer_name = cargo.get('customer_name', '-')
            container_numbers = cargo.get('container_numbers', '-')
            item_name = cargo.get('dropshipping_item_name', '-')
            item_model = cargo.get('dropshipping_item_model_number', '-')
            shipping_marks = cargo.get('shipping_marks', '-')
            address = cargo.get('address', '-')
            note = cargo.get('note', '-')

            # 格式化数字
            try:
                total_cbm = float(cargo.get('total_cbm', 0))
                total_cbm = round(total_cbm, 3)
            except (ValueError, TypeError):
                total_cbm = 0

            try:
                total_pcs = int(cargo.get('total_pcs', 0))
            except (ValueError, TypeError):
                total_pcs = 0

            try:
                weight_lbs = float(cargo.get('total_weight_lbs', 0))
                weight_lbs = round(weight_lbs, 2)
            except (ValueError, TypeError):
                weight_lbs = 0

            try:
                weight_kg = float(cargo.get('total_weight_kg', 0))
                weight_kg = round(weight_kg, 2)
            except (ValueError, TypeError):
                weight_kg = 0

            # 核实状态
            ltl_verify = cargo.get('ltl_verify', False)
            verify_status = '已核实' if ltl_verify else '未核实'

            row = {
                '客户': customer_name,
                '柜号': container_numbers,
                '商品型号': item_name,
                '商品类型': item_model,
                '唛头': shipping_marks,
                '详细地址': address,
                '备注': note,
                'CBM': total_cbm,
                '件数': total_pcs,
                '重量(lbs)': weight_lbs,
                '重量(kg)': weight_kg,
                '核实状态': verify_status,
            }

            excel_data.append(row)

        # 创建 DataFrame
        df = pd.DataFrame(excel_data)

        # 如果没有数据，创建一个空的DataFrame
        if df.empty:
            df = pd.DataFrame(columns=[
                '客户', '柜号', '商品型号', '商品类型', '唛头', '详细地址', '备注',
                'CBM', '件数', '重量(lbs)', '重量(kg)', '核实状态'
            ])

        # 创建 Excel 文件
        output = BytesIO()

        # 使用 ExcelWriter
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 主数据 sheet
            df.to_excel(writer, sheet_name='未放行货物', index=False)

            # 获取 worksheet 对象
            worksheet = writer.sheets['未放行货物']

            # 设置列宽
            column_widths = {
                '客户': 20,
                '柜号': 25,
                '商品型号': 20,
                '商品类型': 20,
                '唛头': 25,
                '详细地址': 40,
                '备注': 40,
                'CBM': 10,
                '件数': 10,
                '重量(lbs)': 12,
                '重量(kg)': 12,
                '核实状态': 12,
            }

            # 设置列宽
            from openpyxl.utils import get_column_letter

            for i, column in enumerate(df.columns, 1):
                col_letter = get_column_letter(i)
                width = column_widths.get(column, 15)
                worksheet.column_dimensions[col_letter].width = width

            # 设置数字格式
            from openpyxl.styles import numbers

            # 设置CBM列为3位小数格式
            if 'CBM' in df.columns:
                cbm_col_idx = df.columns.get_loc('CBM') + 1
                cbm_col_letter = get_column_letter(cbm_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{cbm_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00

            # 设置重量列为2位小数格式
            if '重量(lbs)' in df.columns:
                lbs_col_idx = df.columns.get_loc('重量(lbs)') + 1
                lbs_col_letter = get_column_letter(lbs_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{lbs_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00

            if '重量(kg)' in df.columns:
                kg_col_idx = df.columns.get_loc('重量(kg)') + 1
                kg_col_letter = get_column_letter(kg_col_idx)
                for row in range(2, len(df) + 2):
                    cell = worksheet[f"{kg_col_letter}{row}"]
                    cell.number_format = numbers.FORMAT_NUMBER_00

            # 设置样式：标题行加粗
            for cell in worksheet[1]:
                cell.font = cell.font.copy(bold=True)

            # 自动换行设置
            from openpyxl.styles import Alignment
            wrap_alignment = Alignment(wrap_text=True, vertical='top')

            # 对可能有多行内容的列设置自动换行
            wrap_columns = ['柜号', '详细地址', '备注', '唛头']
            for col_name in wrap_columns:
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name) + 1
                    col_letter = get_column_letter(col_idx)
                    for row in range(1, len(df) + 2):
                        cell = worksheet[f"{col_letter}{row}"]
                        cell.alignment = wrap_alignment

            # 添加筛选器
            worksheet.auto_filter.ref = f"A1:{get_column_letter(len(df.columns))}1"

            # 冻结标题行
            worksheet.freeze_panes = 'A2'

        output.seek(0)

        # 生成文件名
        timestamp = timezone.now().strftime('_%m%d')
        filename = f'未放行货物_{timestamp}.xlsx'

        # 对文件名进行 URL 编码，确保中文正确处理
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename)

        # 创建 HTTP 响应
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

        # 使用 RFC 6266 标准设置 Content-Disposition
        response['Content-Disposition'] = f"attachment; filename*=UTF-8''{encoded_filename}"

        # 备用方案：对于不支持 RFC 6266 的旧浏览器
        response['Content-Disposition'] = f"attachment; filename={encoded_filename}"

        return response
