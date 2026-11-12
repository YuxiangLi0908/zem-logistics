"""Saved AQ groups and atomic creation of one ordinary quote batch per member."""
import copy
import uuid

from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404

from warehouse.models.auto_quote import (
    AutoQuoteAddress, AutoQuoteBatch, AutoQuoteCargoGroup, AutoQuoteCargoGroupMember, AutoQuoteProfile,
)
from warehouse.utils.auto_quote import check_group, clean_text, create_batch, queue_lock, validate_parameters


def visible_groups(user):
    groups = AutoQuoteCargoGroup.objects.all()
    return groups if user.is_staff else groups.filter(operator=user)


def visible_profiles(user):
    batches = AutoQuoteBatch.objects.all()
    if not user.is_staff:
        batches = batches.filter(operator=user)
    # A saved group remains usable when its original quote history is deleted.
    return AutoQuoteProfile.objects.filter(
        Q(pk__in=batches.values("profile_id")) |
        Q(pk__in=AutoQuoteCargoGroupMember.objects.filter(group__in=visible_groups(user)).values("profile_id"))
    ).order_by("-id")


def cargo_options(user):
    return {
        "cargo_groups": [{"id": group.pk, "name": group.name, "updated_at": group.updated_at,
                          "profile_ids": [member.profile_id for member in group.members.all()]}
                         for group in visible_groups(user).prefetch_related("members")],
        "profiles": [{"id": profile.pk, "code": profile.code, "origin": profile.origin_label,
                      "configuration": profile.configuration} for profile in visible_profiles(user)],
    }


@transaction.atomic
def save_cargo_group(user, group_id, name, profile_ids):
    name = clean_text(name)
    if not name or len(name) > 100:
        raise ValueError("货物明细组名称不能为空，且不能超过100个字符")
    if not isinstance(profile_ids, list) or not 1 <= len(profile_ids) <= 50:
        raise ValueError("每组请选择1至50条 AQ 货物明细")
    if any(isinstance(value, bool) or not isinstance(value, (int, str)) for value in profile_ids):
        raise ValueError("AQ 编号格式错误")
    try:
        profile_ids = [int(value) for value in profile_ids]
    except (ValueError, TypeError):
        raise ValueError("AQ 编号格式错误")
    if not profile_ids or len(set(profile_ids)) != len(profile_ids):
        raise ValueError("请选择有效且不重复的 AQ 货物明细")
    group = get_object_or_404(visible_groups(user).select_for_update(), pk=group_id) if group_id else None
    allowed = set(visible_profiles(user).filter(pk__in=profile_ids).values_list("pk", flat=True))
    if allowed != set(profile_ids):
        raise ValueError("所选 AQ 货物明细不存在或无权使用，请刷新后重新选择")
    owner_id = group.operator_id if group else user.pk
    if AutoQuoteCargoGroup.objects.filter(operator_id=owner_id, name=name).exclude(pk=group.pk if group else None).exists():
        raise ValueError("已有同名货物明细组，请修改名称")
    if group:
        group.name = name
        group.save(update_fields=["name", "updated_at"])
        group.members.all().delete()
    else:
        group = AutoQuoteCargoGroup.objects.create(name=name, operator=user)
    AutoQuoteCargoGroupMember.objects.bulk_create([
        AutoQuoteCargoGroupMember(group=group, profile_id=profile_id, position=index)
        for index, profile_id in enumerate(profile_ids, 1)
    ])
    return group


@transaction.atomic
def delete_cargo_group(user, group_id):
    group = get_object_or_404(visible_groups(user).select_for_update(), pk=group_id)
    group.delete()


def profile_parameters(profile, pickup_date, warehouses):
    configuration = profile.configuration
    original = configuration.get("origin") or {}
    normalize = lambda value: clean_text(value).casefold()
    warehouse = next((warehouse for warehouse in warehouses if all(
        normalize(warehouse.get(key)) == normalize(original.get(key[0].upper() + key[1:]))
        for key in ("city", "state", "postCode", "detailAddress")
    )), None)
    if not warehouse:
        raise ValueError(f"{profile.code} 的发货地址已不在当前仓库列表，请先更新该货物明细")
    parameters = {key: copy.deepcopy(configuration[key]) for key in (
        "items", "quoteType", "carType", "declaredValue", "commodityUnit", "palletType", "destinationType", "needLiftgate"
    )}
    parameters.update(pickupDate=pickup_date, originWarehouse=warehouse["warehouse"],
                      originCity=warehouse.get("city"), originState=warehouse.get("state"),
                      originPostCode=warehouse.get("postCode"), originDetailAddress=warehouse.get("detailAddress", ""), originType=1)
    return parameters


@transaction.atomic
def start_cargo_group(user, cargo_group_id, address_group, pickup_date, submission_id, warehouses):
    check_group(address_group)
    token = uuid.UUID(str(submission_id))
    queue_lock()
    group = get_object_or_404(visible_groups(user).select_for_update(), pk=cargo_group_id)
    previous = list(AutoQuoteBatch.objects.filter(operator=user, cargo_submission_id=token).order_by("cargo_group_position"))
    if previous:
        return previous, True
    members = list(group.members.select_related("profile"))
    if not members:
        raise ValueError("该货物明细组没有可询价的 AQ 明细")
    snapshots = [address.snapshot() for address in AutoQuoteAddress.objects.filter(group=address_group)]
    if not snapshots:
        raise ValueError("所选地址组没有可询价的地址")
    parameters = []
    for member in members:
        try:
            value = profile_parameters(member.profile, pickup_date, warehouses)
            validate_parameters(value, snapshots[0])
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise ValueError(f"组内第{member.position}条（{member.profile.code}）：{exc}") from exc
        parameters.append(value)
    # Repeated clicks from different tabs reuse a complete, still-active group submission.
    active_tokens = AutoQuoteBatch.objects.filter(
        operator=user, cargo_group=group, group=address_group, parameters__pickupDate=pickup_date,
        status__in=("queued", "running"), cargo_submission_id__isnull=False,
    ).order_by().values_list("cargo_submission_id", flat=True).distinct()
    for active_token in active_tokens:
        active = list(AutoQuoteBatch.objects.filter(operator=user, cargo_submission_id=active_token).order_by("cargo_group_position"))
        if len(active) == len(parameters) and all(batch.parameters == value for batch, value in zip(active, parameters)):
            return active, True
    batches = [create_batch(
        address_group, value, user.pk, uuid.uuid5(token, f"{user.pk}:{member.profile_id}"), snapshots=snapshots,
        cargo_context={"cargo_group": group, "cargo_group_name": group.name,
                       "cargo_group_position": member.position, "cargo_submission_id": token},
    ) for member, value in zip(members, parameters)]
    return batches, False
