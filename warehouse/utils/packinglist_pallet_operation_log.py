from datetime import timedelta

from django.utils import timezone

from warehouse.models.packinglist_pallet_operation_log import (
    PackingListPalletOperationLog,
)


BEIJING_TIMEZONE = timezone.get_fixed_timezone(timedelta(hours=8))


def _safe_text(value, max_length=None):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if max_length and len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def _get_attr_text(obj, attr_name, max_length=None):
    return _safe_text(getattr(obj, attr_name, None), max_length=max_length)


def build_target_snapshot(target):
    return {
        "target_id": _safe_text(getattr(target, "id", None), 100),
        "target_display": _safe_text(target, 500),
        "container_number": _get_attr_text(target, "container_number", 255),
        "po_id": _get_attr_text(target, "PO_ID", 500),
        "fba_id": _get_attr_text(target, "fba_id", 500),
        "ref_id": _get_attr_text(target, "ref_id", 500),
        "shipping_mark": _get_attr_text(target, "shipping_mark", 1000),
        "destination": _get_attr_text(target, "destination", 1000),
        "warehouse": _get_attr_text(target, "location", 100),
    }


def record_packinglist_pallet_operation(
    *,
    request=None,
    target=None,
    target_type=None,
    action_type="other",
    action_detail=None,
    operation_name=None,
    operation_location=None,
    warehouse=None,
    metadata=None,
    **snapshot_overrides,
):
    if target and not target_type:
        model_name = target.__class__.__name__.lower()
        if model_name == "packinglist":
            target_type = "packing_list"
        elif model_name == "pallet":
            target_type = "pallet"

    if target_type not in {"packing_list", "pallet"}:
        raise ValueError("target_type must be 'packing_list' or 'pallet'")

    snapshot = build_target_snapshot(target) if target else {}
    snapshot.update({k: v for k, v in snapshot_overrides.items() if v is not None})

    user = getattr(request, "user", None)
    operator = user if getattr(user, "is_authenticated", False) else None
    operator_username = _safe_text(getattr(operator, "username", None), 150)

    if not operation_location and request:
        resolver_match = getattr(request, "resolver_match", None)
        operation_location = getattr(resolver_match, "view_name", None) or getattr(
            request, "path", None
        )

    now = timezone.now()
    warehouse_value = _safe_text(warehouse, 100) or snapshot.get("warehouse")
    return PackingListPalletOperationLog.objects.create(
        target_type=target_type,
        target_id=snapshot.get("target_id"),
        target_display=snapshot.get("target_display"),
        container_number=snapshot.get("container_number"),
        po_id=snapshot.get("po_id"),
        fba_id=snapshot.get("fba_id"),
        ref_id=snapshot.get("ref_id"),
        shipping_mark=snapshot.get("shipping_mark"),
        destination=snapshot.get("destination"),
        warehouse=warehouse_value,
        action_type=action_type,
        action_detail=_safe_text(action_detail),
        operator=operator,
        operator_username=operator_username,
        operation_location=_safe_text(operation_location, 255),
        operation_name=_safe_text(operation_name, 255)
        or _safe_text(operation_location, 255),
        request_path=_safe_text(getattr(request, "path", None), 500),
        operation_time_beijing=timezone.localtime(now, BEIJING_TIMEZONE),
        metadata=metadata or {},
    )
