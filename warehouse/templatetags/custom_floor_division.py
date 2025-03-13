from typing import Any

from django import template

register = template.Library()


@register.filter
def floor_division(value: Any | int, n: int) -> int:
    try:
        return value // n
    except:
        return 0
