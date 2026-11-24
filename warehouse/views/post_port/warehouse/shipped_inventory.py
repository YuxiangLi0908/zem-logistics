import hashlib
import json
import logging
import uuid
from decimal import Decimal, InvalidOperation

from asgiref.sync import sync_to_async
from django.contrib.postgres.aggregates import StringAgg
from django.db import transaction
from django.db.models import CharField, Count, F, Q, Sum
from django.db.models.functions import Cast
from django.http import JsonResponse
from django.shortcuts import redirect, render
from simple_history.utils import bulk_create_with_history, bulk_update_with_history

from warehouse.models.container import Container
from warehouse.models.offload import Offload
from warehouse.models.order import Order
from warehouse.models.packing_list import PackingList
from warehouse.models.pallet import Pallet
from warehouse.models.shipment import Shipment
from warehouse.models.warehouse import ZemWarehouse
from warehouse.utils.constants import DELIVERY_METHOD_OPTIONS
from warehouse.views.post_port.warehouse.inventory import Inventory


logger = logging.getLogger(__name__)


class ShippedInventory(Inventory):
    template_name = "post_port/inventory/shipped_inventory.html"

    @staticmethod
    def shipped_criteria():
        return Q(shipment_batch_number__shipped_at__isnull=False) | Q(shipment_batch_number__is_shipped=True)

    async def get(self, request, **kwargs):
        if not await self._user_authenticate(request):
            return redirect("login")
        context = await sync_to_async(self.page_context)(request.GET)
        return await sync_to_async(render)(request, self.template_name, context)

    async def post(self, request, **kwargs):
        if not await self._user_authenticate(request):
            return JsonResponse({"success": False, "message": "登录已过期，请重新登录。"}, status=401)
        step = request.POST.get("step")
        if step == "edit_data":
            return await self.edit_data(request)
        if step == "save":
            return await self.save_row(request)
        return JsonResponse({"success": False, "message": "未知操作，请刷新页面。"}, status=400)

    def rows_query(self, warehouse, delivery_type=""):
        criteria = self.shipped_criteria() & Q(location=warehouse)
        if delivery_type:
            criteria &= Q(delivery_type=delivery_type)
        # 按库存管理的卡板属性汇总，并区分真实预约和订单，避免相同文本的不同记录混在一行。
        return (Pallet.objects.filter(criteria).annotate(str_id=Cast("id", CharField()))
                .values("destination", "delivery_method", "delivery_type", "shipping_mark", "fba_id", "ref_id",
                        "note", "PO_ID", "address", "zipcode", "location",
                        "container_number_id", "shipment_batch_number_id",
                        order_id=F("container_number__orders__id"),
                        customer_name=F("container_number__orders__customer_name__zem_name"),
                        container=F("container_number__container_number"),
                        offload_at=F("container_number__orders__offload_id__offload_at"),
                        shipped_at=F("shipment_batch_number__shipped_at"),
                        shipment=F("shipment_batch_number__shipment_batch_number"),
                        appointment_id=F("shipment_batch_number__appointment_id"))
                .annotate(plt_ids=StringAgg("str_id", delimiter=",", distinct=True, ordering="str_id"),
                          weight=Sum("weight_lbs"), pcs=Sum("pcs"), cbm=Sum("cbm"), n_pallet=Count("id", distinct=True))
                .order_by("-shipped_at", "container", "destination", "shipment_batch_number_id", "order_id"))

    def page_context(self, params):
        warehouse = params.get("warehouse", "")
        delivery_type = params.get("delivery_type", "")
        options = list(ZemWarehouse.objects.order_by("name").values_list("name", "name"))
        options = [("", "")] + [(name, value) for name, value in options if name]
        return {"warehouse": warehouse, "delivery_type": delivery_type, "warehouse_options": options,
                "delivery_type_options": self.delivery_type_options,
                "pallet": list(self.rows_query(warehouse, delivery_type)) if warehouse else [],
                "delivery_method_options": DELIVERY_METHOD_OPTIONS,
                "searched": bool(warehouse)}

    def selected_pallets(self, data, lock=False):
        raw_ids = data.get("plt_ids", "").split(",")
        if not all(item.strip().isascii() and item.strip().isdigit() for item in raw_ids):
            raise ValueError("卡板 ID 无效，请刷新页面后重试。")
        ids = sorted({int(item) for item in raw_ids})
        query = Pallet.objects.filter(pk__in=ids, location=data.get("warehouse", "")).filter(self.shipped_criteria())
        if lock:
            query = query.select_for_update(of=("self",))
        pallets = list(query.order_by("pk"))
        if len(pallets) != len(ids):
            raise ValueError("部分卡板已被修改、转仓或不再属于已出库数据，请刷新页面。")
        if len({(p.container_number_id, p.shipment_batch_number_id, p.PO_ID) for p in pallets}) != 1:
            raise ValueError("只能编辑同一行、同一柜号、同一 PO_ID 和同一预约的卡板。")
        group_fields = ("destination", "delivery_method", "delivery_type", "shipping_mark", "fba_id", "ref_id", "note", "address", "zipcode")
        if len({tuple(getattr(p, field) for field in group_fields) for p in pallets}) != 1:
            raise ValueError("这行卡板的汇总信息已发生变化，请刷新页面后重新编辑。")
        return pallets

    @staticmethod
    def row_order(data, container_id, lock=False):
        query = Order.objects.filter(container_number_id=container_id)
        if lock:
            query = query.select_for_update()
        order_id = data.get("order_id", "")
        if not order_id:
            if query.exists():
                raise ValueError("柜子的订单信息已变化，请刷新页面后重试。")
            return None
        if not order_id.isascii() or not order_id.isdigit():
            raise ValueError("订单 ID 无效。")
        order = query.filter(pk=int(order_id)).first()
        if not order:
            raise ValueError("订单与本行柜号不匹配，请刷新页面后重试。")
        return order

    @staticmethod
    def time_text(value):
        return value.strftime("%Y-%m-%dT%H:%M:%S") if value else ""

    def row_data(self, data):
        pallets = self.selected_pallets(data)
        first = pallets[0]
        order = self.row_order(data, first.container_number_id)
        shipment = Shipment.objects.get(pk=first.shipment_batch_number_id)
        offload = Offload.objects.filter(pk=order.offload_id_id).first() if order and order.offload_id_id else None
        result = {name: getattr(first, name) or "" for name in ("destination", "address", "zipcode", "delivery_method", "delivery_type", "location", "shipping_mark", "fba_id", "ref_id", "note")}
        result.update(plt_ids=",".join(str(p.pk) for p in pallets), order_id=str(order.pk) if order else "",
                      customer_name=order.customer_name.zem_name if order and order.customer_name_id else "",
                      container=first.container_number.container_number if first.container_number_id else "",
                      offload_at=self.time_text(offload.offload_at) if offload else "",
                      shipped_at=self.time_text(shipment.shipped_at), shipment=shipment.shipment_batch_number or "",
                      appointment_id=shipment.appointment_id or "", n_pallet=len(pallets),
                      weight=str(sum(Decimal(str(p.weight_lbs or 0)) for p in pallets).quantize(Decimal("0.0001"))),
                      pcs=sum(p.pcs or 0 for p in pallets), cbm=str(sum(Decimal(str(p.cbm or 0)) for p in pallets).quantize(Decimal("0.01"))),
                      has_order=bool(order), shared_shipment_count=Pallet.objects.filter(shipment_batch_number=shipment).count(),
                      shared_container_count=Pallet.objects.filter(container_number_id=first.container_number_id).count())
        result["revision"] = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        return result

    async def edit_data(self, request):
        try:
            data = await sync_to_async(self.row_data)(request.POST)
            return JsonResponse({"success": True, "data": data})
        except ValueError as exc:
            return JsonResponse({"success": False, "message": str(exc)}, status=400)
        except Exception as exc:
            logger.exception("读取已出库行数据失败")
            return JsonResponse({"success": False, "message": f"读取失败：{type(exc).__name__}: {exc}"}, status=500)

    @staticmethod
    def number(data, name, label, places=None, minimum=0):
        try:
            value = Decimal(data.get(name, ""))
        except (InvalidOperation, TypeError):
            raise ValueError(f"{label}必须填写有效数字。")
        if not value.is_finite() or value < minimum:
            raise ValueError(f"{label}必须大于等于 {minimum}。")
        if places is None:
            if value != value.to_integral_value():
                raise ValueError(f"{label}必须为整数。")
            return int(value)
        try:
            rounded = value.quantize(Decimal(1).scaleb(-places))
        except InvalidOperation:
            raise ValueError(f"{label}数值过大。")
        if rounded != value:
            raise ValueError(f"{label}最多保留 {places} 位小数。")
        return rounded

    @transaction.atomic
    def update_row(self, request):
        data = request.POST
        pallets = self.selected_pallets(data, lock=True)
        first = pallets[0]
        if not first.container_number_id:
            raise ValueError("卡板缺少柜号关联，请先补齐柜号信息。")
        container = Container.objects.select_for_update().get(pk=first.container_number_id)
        Shipment.objects.select_for_update().get(pk=first.shipment_batch_number_id)
        self.row_order(data, container.pk, lock=True)
        current = self.row_data(data)
        if not data.get("revision") or data["revision"] != current["revision"]:
            raise ValueError("数据已被修改，或上次保存已经成功，请刷新页面后重新编辑。")

        values = {}
        for name, label in (("destination", "目的地"), ("address", "地址"), ("zipcode", "邮编"),
                            ("delivery_method", "派送方式"), ("delivery_type", "派送类型"), ("location", "仓库"),
                            ("shipping_mark", "唛头"), ("fba_id", "FBA"), ("ref_id", "REF"), ("note", "备注")):
            if name not in data:
                raise ValueError(f"缺少{label}，请重新打开编辑框。")
            value = data.get(name, "").strip()
            if len(value) > Pallet._meta.get_field(name).max_length:
                raise ValueError(f"{label}内容过长。")
            values[name] = value
        if not values["destination"]:
            raise ValueError("目的地不能为空。")
        if values["delivery_method"] not in {value for _, value in DELIVERY_METHOD_OPTIONS} | {first.delivery_method or ""}:
            raise ValueError("派送方式无效，请从下拉框选择。")
        if values["delivery_type"] not in {"", "public", "other", first.delivery_type or ""}:
            raise ValueError("派送类型无效，请从下拉框选择。")
        if values["delivery_method"] == "客户自提":
            values["delivery_type"] = "other"
        weight = self.number(data, "weight", "总重量", 4)
        pcs = self.number(data, "pcs", "总箱数")
        cbm = self.number(data, "cbm", "总体积", 2)
        count = self.number(data, "n_pallet", "卡板数", minimum=1)
        if count > 10000:
            raise ValueError("卡板数不能超过 10000，请核对输入。")
        # 沿用库存管理：派送信息同步同柜号、同 PO_ID 的 PackingList，标记各自独立保存。
        packing_lists = list(PackingList.objects.select_for_update().filter(
            container_number=container, PO_ID=first.PO_ID,
        ).order_by("pk"))
        old_packing = [self._snapshot_packing_list(pl) for pl in packing_lists]

        old_snapshots = [self._snapshot_pallet(p) for p in pallets]
        old_count = len(pallets)
        kept = pallets[:count]
        if count < old_count:
            Pallet.objects.filter(pk__in=[p.pk for p in pallets[count:]]).delete()
        added = []
        for index in range(old_count, count):
            attributes = {field.attname: getattr(first, field.attname) for field in Pallet._meta.concrete_fields if not field.primary_key}
            attributes.update(pallet_id=str(uuid.uuid4()), sequence_number=str(index + 1))
            added.append(Pallet(**attributes))
        all_pallets = kept + added
        # 总数未改时保留各板原来的分配；需要重新分配时保证汇总值精确相等。
        totals = {"weight_lbs": weight, "pcs": pcs, "cbm": cbm}
        for field, total in totals.items():
            old_total = sum(Decimal(str(getattr(p, field) or 0)) for p in pallets)
            unit = Decimal(1) if field == "pcs" else Decimal("0.0001" if field == "weight_lbs" else "0.01")
            if count == old_count and old_total.quantize(unit) == total:
                continue
            average = (Decimal(total) / count).quantize(unit, rounding="ROUND_DOWN")
            remainder = Decimal(total) - average * count
            for index, pallet in enumerate(all_pallets):
                value = average + (remainder if index == 0 else 0)
                setattr(pallet, field, int(value) if field == "pcs" else float(value))
        for pallet in all_pallets:
            if pallet.delivery_method != values["delivery_method"]:
                pallet.released_at = None if self._is_hold_delivery_method(values["delivery_method"]) else pallet.created_at
            for name, value in values.items():
                setattr(pallet, name, value)
        fields = list(values) + ["weight_lbs", "pcs", "cbm", "released_at"]
        bulk_update_with_history(kept, Pallet, fields=fields, default_user=request.user)
        if added:
            bulk_create_with_history(added, Pallet, default_user=request.user)

        shared_fields = ["destination", "address", "zipcode", "delivery_method", "delivery_type"]
        for packing_list in packing_lists:
            for name in shared_fields:
                setattr(packing_list, name, values[name])
        if packing_lists:
            bulk_update_with_history(packing_lists, PackingList, fields=shared_fields, default_user=request.user)
        types = set(Pallet.objects.filter(container_number=container).exclude(delivery_type__isnull=True).exclude(delivery_type="").values_list("delivery_type", flat=True))
        if not types:
            raise ValueError("缺少派送类型，请选择派送类型后保存。")
        new_type = next(iter(types)) if len(types) == 1 else "mixed"
        if new_type != container.delivery_type:
            container.delivery_type = new_type
            container._history_user = request.user
            container.save(update_fields=["delivery_type"])
        new_snapshots = [self._snapshot_pallet(p) for p in all_pallets]
        return {"old": old_snapshots, "new": new_snapshots, "old_row": current,
                "ids": [p.pk for p in all_pallets], "container": container.container_number, "po_id": first.PO_ID,
                "fields": fields, "destination": values["destination"],
                "packing_ids": [pl.pk for pl in packing_lists], "old_packing": old_packing,
                "new_packing": [self._snapshot_packing_list(pl) for pl in packing_lists], "shared_fields": shared_fields}

    async def save_row(self, request):
        try:
            result = await sync_to_async(self.update_row)(request)
        except ValueError as exc:
            return JsonResponse({"success": False, "message": str(exc)}, status=400)
        except Exception as exc:
            logger.exception("保存已出库行失败，事务已回滚")
            return JsonResponse({"success": False, "message": f"保存失败：{type(exc).__name__}: {exc}。本次修改已回滚。"}, status=500)
        await self._record_pallet_pl_operation(
            request, target_type="pallet", target_ids=result["ids"], operation_name="已出库数据修改", action_type="update",
            po_id=result["po_id"], container_number=result["container"], destination=result["destination"],
            warehouse=request.POST.get("warehouse"),
            action_detail=self._build_change_summary(result["old"], result["new"], result["fields"]),
            metadata={"operation": "shipped_inventory_edit", **result},
        )
        if result["packing_ids"]:
            await self._record_pallet_pl_operation(
                request, target_type="packing_list", target_ids=result["packing_ids"], operation_name="已出库数据修改", action_type="update",
                po_id=result["po_id"], container_number=result["container"], destination=result["destination"],
                warehouse=request.POST.get("warehouse"),
                action_detail=self._build_change_summary(result["old_packing"], result["new_packing"], result["shared_fields"]),
                metadata={"operation": "shipped_inventory_edit", "old": result["old_packing"], "new": result["new_packing"]},
            )
        return JsonResponse({"success": True, "message": "已保存，正在刷新已出库数据。"})
