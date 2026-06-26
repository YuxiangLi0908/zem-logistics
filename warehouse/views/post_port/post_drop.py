import json
import os
import string
import uuid
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
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
        else:
            raise ValueError('wrong step',step)


    async def post(self, request: HttpRequest, **kwargs) -> None | HttpResponse | tuple[Any, Any] | Any:
        step = request.POST.get("step")
        if step == "ltl_post_warehouse":
            template, context = await self.handle_ltl_unscheduled_pos_post(request)
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
