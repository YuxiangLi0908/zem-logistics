from typing import Any

from django import template

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

@register.filter(name='slice')
#历史记录中，变更内容那列数据太多，就分成两列展示，half是第一列，:half是第二列
def slice_filter(value, arg):
    try:
        if arg == "half":
            return value[:len(value)//2]
        elif arg == ":half":
            return value[len(value)//2:]
        parts = arg.split(':')
        if len(parts) == 2:
            start = int(parts[0]) if parts[0] else None
            end = int(parts[1]) if parts[1] else None
            return value[start:end]
        return value
    except (ValueError, TypeError, AttributeError):
        return value
