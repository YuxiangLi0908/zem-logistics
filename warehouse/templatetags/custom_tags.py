from django import template

register = template.Library()

@register.inclusion_tag('navbar.html', takes_context=True)
def navbar(context):
    return {
        'user': context['user'],
    }
