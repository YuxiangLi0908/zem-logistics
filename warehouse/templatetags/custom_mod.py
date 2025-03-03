from django import template

register = template.Library()


@register.filter
def modulo(value: float | int, n: int) -> float | int:
    return value % n
