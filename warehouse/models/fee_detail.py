from django.db import models
from django.db.models import JSONField
import ast
import json
import re

from warehouse.models.quotation_master import QuotationMaster


class FeeDetail(models.Model):
    fee_detail_id = models.CharField(max_length=200, null=True)
    quotation_id = models.ForeignKey(
        QuotationMaster,
        on_delete=models.CASCADE,
        related_name="fee_details",
        db_index=True,
    )
    fee_type = models.CharField(max_length=255, null=True)
    warehouse = models.CharField(max_length=20, null=True, blank=True)
    details = JSONField(default=dict)
    niche_warehouse = models.CharField(max_length=2000, null=True, blank=True)
    hot_warehouse = JSONField(default=list, blank=True, verbose_name="热门仓点")

    def get_hot_warehouse_locations(self):
        """从本条报价的仓点列表中排除冷门仓点，返回去重后的热门仓点。"""
        def location_list(value):
            if value is None:
                return []
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return []
                if value.startswith(("[", "(")):
                    try:
                        value = ast.literal_eval(value)
                    except (ValueError, SyntaxError) as exc:
                        raise ValueError("仓点列表字符串格式无效") from exc
                else:
                    value = re.split(r"[,，;；\n]+", value)
            if not isinstance(value, (list, tuple, set)):
                raise ValueError("仓点列表必须是列表或文本")
            return [str(item).strip() for item in value if item is not None and str(item).strip()]

        def collect_locations(value, allow_price_lists):
            result = []
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in ("location", "locations"):
                        result.extend(location_list(child))
                    else:
                        result.extend(collect_locations(child, allow_price_lists))
            elif isinstance(value, list):
                if allow_price_lists and all(isinstance(item, str) for item in value):
                    result.extend(location_list(value))
                else:
                    for child in value:
                        if isinstance(child, (dict, list)):
                            result.extend(collect_locations(child, allow_price_lists))
            return result

        details = json.loads(self.details) if isinstance(self.details, str) else self.details
        locations = collect_locations(details, (self.fee_type or "").endswith("_PUBLIC"))
        cold = {name.upper() for name in location_list(self.niche_warehouse)}
        hot = {}
        for name in locations:
            if name.upper() not in cold:
                hot.setdefault(name.upper(), name)
        return sorted(hot.values(), key=str.upper)

    def __str__(self) -> str:
        return f"{self.fee_type} ({self.quotation_id})"
