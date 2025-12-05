import datetime
from typing import Any

from django import template
from django.utils.safestring import mark_safe
import re

register = template.Library()


@register.filter
def split_and_get_first(value: str) -> str:
    return value.split("-")[0]


@register.filter(name="get")
def get(dictionary: dict, key: Any) -> Any:
    return dictionary.get(key)


@register.filter
def slice_string(value, mode="front"):
    if isinstance(value, str) and len(value) > 4:
        if mode == "front":
            return value[:-4]
        elif mode == "back":
            return value[-4:]
    return value


@register.filter(name="slice")
# 历史记录中，变更内容那列数据太多，就分成两列展示，half是第一列，:half是第二列
def slice_filter(value, arg):
    try:
        if arg == "half":
            return value[: len(value) // 2]
        elif arg == ":half":
            return value[len(value) // 2 :]
        parts = arg.split(":")
        if len(parts) == 2:
            start = int(parts[0]) if parts[0] else None
            end = int(parts[1]) if parts[1] else None
            return value[start:end]
        return value
    except (ValueError, TypeError, AttributeError):
        return value


@register.filter
def filter_by_fleet(queryset, fleet_id):
    return [item for item in queryset if item.fleet_number_id == fleet_id]


@register.filter
def filter_by_pickup(queryset, pickup_number):
    return [item for item in queryset if item.pickup_number == pickup_number]


@register.filter
def dict_values(dictionary):
    """返回字典的值列表"""
    return list(dictionary.values())


@register.filter
def sum_attr(values_list, attr_path):
    """计算对象列表中指定属性的总和"""
    total = 0
    for item in values_list:
        # 处理属性路径（如 'orders|length'）
        attrs = attr_path.split("|")
        obj = item
        for attr in attrs:
            if hasattr(obj, attr):
                obj = getattr(obj, attr)
            elif isinstance(obj, dict) and attr in obj:
                obj = obj[attr]
            else:
                obj = None
                break

        # 如果是可计算长度的对象
        if hasattr(obj, "__len__"):
            total += len(obj)
    return total


@register.filter
# 给月份加1
def add_month(date):
    try:
        return date.replace(month=date.month + 1)
    except ValueError:  # 处理12月+1的情况
        return date.replace(year=date.year + 1, month=1)

@register.filter
def dict_get(d, key):
    if d is None:
        return None
    return d.get(key)

@register.filter
def sum_subgroup_attr(suggestions, attr):
    """计算子组某个属性的总和"""
    total = 0
    for suggestion in suggestions:
        if hasattr(suggestion, 'subgroup') and hasattr(suggestion.subgroup, attr):
            total += getattr(suggestion.subgroup, attr, 0)
        elif isinstance(suggestion, dict) and 'subgroup' in suggestion:
            total += suggestion['subgroup'].get(attr, 0)
    return total

@register.filter
def get_item(obj, key):
    """从字典或对象中获取属性"""
    if isinstance(obj, dict):
        return obj.get(key)
    else:
        return getattr(obj, key, None)

@register.filter
def linebreaks_container(value):
    if not value:
        return ""
    
    # 直接按逗号分割，然后清理每个项目
    items = [item.strip() for item in str(value).split(',') if item.strip()]
    # 简单用换行符连接
    result = ',\n'.join(items)
    
    # 移除开头可能的多余换行
    result = result.lstrip('\n')
    
    return mark_safe(result)

@register.filter
def linebreaks_cn(value):
    if not value:
        return ""
    
    # 直接按逗号分割，然后清理每个项目
    items = [item.strip() for item in str(value).split(',') if item.strip()]
    # 用换行符连接，不加逗号
    result = '\n'.join(items)
    
    return mark_safe(result)

@register.filter
def linebreaks_comma(value):
    """将逗号分隔的内容转换为换行显示"""
    if not value:
        return value
    # 按逗号分割，然后每个元素用<br>连接
    parts = [part.strip() for part in str(value).split(',') if part.strip()]
    return '<br>'.join(parts)

register.filter
def get_item(dictionary, key):
    """从字典中获取值，如果不存在返回空字符串"""
    if isinstance(dictionary, dict):
        return dictionary.get(key, '')
    return ''

@register.filter
def add_str(str1, str2):
    """字符串连接"""
    return str(str1) + str(str2)

@register.filter(name='multiply')
def multiply(value, arg):
    """将值乘以参数"""
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return ''