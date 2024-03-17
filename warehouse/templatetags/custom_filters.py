from django import template

register = template.Library()

@register.filter
def split_and_get_first(value: str) -> str:
    return value.split("-")[0]