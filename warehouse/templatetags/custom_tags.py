from django import template
from django.contrib.auth.models import Group


register = template.Library()


@register.inclusion_tag("navbar.html", takes_context=True)
def navbar(context):
    user = context["user"]
    generous_and_wide = False

    if user and user.is_authenticated:
        # 使用线程池执行同步查询
        try:
            # 在新线程中执行同步查询
            generous_and_wide = _run_in_thread(
                lambda: Group.objects.filter(
                    name="generous_and_wide",
                    user=user
                ).exists()
            )
        except:
            generous_and_wide = False

    return {
        "user": user,
        "generous_and_wide": generous_and_wide,
    }


def _run_in_thread(sync_func):
    """在新线程中运行同步函数"""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(sync_func)
        return future.result()