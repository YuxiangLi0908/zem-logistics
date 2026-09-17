from django.core.exceptions import PermissionDenied
from django.db import transaction
from simple_history.utils import bulk_create_with_history


def can_import_packing_list(offload):
    """整柜导入只适用于尚未产生拆柜数据的订单。"""
    return not offload or not any(
        getattr(offload, field, None)
        for field in (
            "offload_at",
            "offload_other_at",
            "offload_other_selfdelivery_at",
            "offload_other_selfpick_cargos_at",
            "offload_at_container",
        )
    )


def validate_packing_list_import(offload):
    if not can_import_packing_list(offload):
        raise PermissionDenied("该订单已拆柜，不能通过上传文件替换整柜 Packing List，请逐行修改。")


@transaction.atomic
def replace_packing_list(queryset, rows):
    """新清单写入失败时恢复原清单，避免文件数据错误导致旧货物丢失。"""
    if not rows:
        raise ValueError("导入清单不能为空")
    queryset.delete()
    bulk_create_with_history(rows, queryset.model)
