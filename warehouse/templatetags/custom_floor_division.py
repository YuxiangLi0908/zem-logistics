from django import template
from typing import Any

register = template.Library()

@register.filter
def floor_division(value: Any | int, n: int) -> int:
    try: 
        return value // n
    except:
        return 0