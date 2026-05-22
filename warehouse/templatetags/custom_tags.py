from django import template
from django.contrib.auth.models import Group
from django.utils.safestring import mark_safe
import json


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


@register.simple_tag
def render_fee_details(details, fee_type):
    """渲染费用详情为表格形式"""
    html = ""
    
    if fee_type in ["NJ_AMAZON", "LA_AMAZON", "SAV_AMAZON"]:
        # 亚马逊派送类型
        html += _render_amazon_details(details)
    elif fee_type in ["NJ_COMBINA", "LA_COMBINA", "SAV_COMBINA"]:
        # 组合柜类型
        html += _render_combina_details(details)
    elif fee_type == "NJ_LOCAL":
        # NJ本地派送
        html += _render_local_details(details)
    elif fee_type in ["preport", "direct"]:
        # 码头费用或直送
        html += _render_key_value_details(details)
    elif fee_type == "warehouse":
        # 仓库库内操作费
        html += _render_warehouse_details(details)
    elif fee_type == "COMBINA_STIPULATE":
        # 组合柜规则
        html += _render_stipulate_details(details)
    else:
        # 默认渲染方式
        html += _render_default_details(details)
    
    return mark_safe(html)


def _render_amazon_details(details):
    """渲染亚马逊派送详情"""
    html = '<table class="table" style="width: 100%; border-collapse: collapse; font-size: 15px;">'
    html += '<thead style="background: #e9d5ff;"><tr><th style="padding: 12px; border: 1px solid #c4b5fd; width: 25%;">类型</th><th style="padding: 12px; border: 1px solid #c4b5fd; width: 35%;">价格</th><th style="padding: 12px; border: 1px solid #c4b5fd; width: 40%;">仓点</th></tr></thead>'
    html += '<tbody>'
    
    for type_name, price_groups in details.items():
        type_display = {
            "NJ_AMAZON": "NJ亚马逊",
            "NJ_WALMART": "NJ沃尔玛",
            "LA_AMAZON": "LA亚马逊",
            "SAV_AMAZON": "SAV亚马逊",
            "SAV_WALMART": "SAV沃尔玛"
        }.get(type_name, type_name)
        
        first_row = True
        for price, locations in price_groups.items():
            html += '<tr>'
            if first_row:
                html += f'<td style="padding: 10px; border: 1px solid #e9d5ff; vertical-align: top; word-wrap: break-word;" rowspan="{len(price_groups)}"><strong>{type_display}</strong></td>'
                first_row = False
            html += f'<td style="padding: 10px; border: 1px solid #e9d5ff; word-wrap: break-word;">{price if price is not None else ""}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #e9d5ff; word-wrap: break-word;">{", ".join(locations) if locations else ""}</td>'
            html += '</tr>'
    
    html += '</tbody></table>'
    return html


def _render_combina_details(details):
    """渲染组合柜详情"""
    html = '<table class="table" style="width: 100%; border-collapse: collapse; font-size: 15px;">'
    html += '<thead style="background: #bbf7d0;"><tr><th style="padding: 12px; border: 1px solid #86efac; width: 25%;">区域</th><th style="padding: 12px; border: 1px solid #86efac; width: 35%;">价格</th><th style="padding: 12px; border: 1px solid #86efac; width: 40%;">仓点</th></tr></thead>'
    html += '<tbody>'
    
    for region, groups in details.items():
        first_row = True
        for group in groups:
            prices = group.get("prices", [])
            locations = group.get("location", [])
            price_str = ", ".join(str(p) for p in prices) if prices else ""
            location_str = ", ".join(locations) if locations else ""
            
            html += '<tr>'
            if first_row:
                html += f'<td style="padding: 10px; border: 1px solid #bbf7d0; vertical-align: top; word-wrap: break-word;" rowspan="{len(groups)}"><strong>{region}</strong></td>'
                first_row = False
            html += f'<td style="padding: 10px; border: 1px solid #bbf7d0; word-wrap: break-word;">{price_str}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bbf7d0; word-wrap: break-word;">{location_str}</td>'
            html += '</tr>'
    
    html += '</tbody></table>'
    return html


def _render_local_details(details):
    """渲染NJ本地派送详情"""
    html = '<table class="table" style="width: 100%; border-collapse: collapse; font-size: 15px;">'
    html += '<thead style="background: #a7f3d0;"><tr><th style="padding: 12px; border: 1px solid #6ee7b7; width: 20%;">索引</th><th style="padding: 12px; border: 1px solid #6ee7b7; width: 30%;">价格</th><th style="padding: 12px; border: 1px solid #6ee7b7; width: 50%;">邮编</th></tr></thead>'
    html += '<tbody>'
    
    for idx, data in details.items():
        prices = data.get("prices", [])
        zipcodes = data.get("zipcodes", [])
        price_str = ", ".join(str(p) for p in prices) if prices else ""
        zip_str = ", ".join(str(z) for z in zipcodes) if zipcodes else ""
        
        html += '<tr>'
        html += f'<td style="padding: 10px; border: 1px solid #a7f3d0; word-wrap: break-word;">{idx if idx is not None else ""}</td>'
        html += f'<td style="padding: 10px; border: 1px solid #a7f3d0; word-wrap: break-word;">{price_str}</td>'
        html += f'<td style="padding: 10px; border: 1px solid #a7f3d0; word-wrap: break-word;">{zip_str}</td>'
        html += '</tr>'
    
    html += '</tbody></table>'
    return html


def _render_key_value_details(details):
    """渲染键值对详情（码头费用、直送等）"""
    html = '<table class="table" style="width: 100%; border-collapse: collapse; font-size: 15px;">'
    html += '<thead style="background: #fef3c7;"><tr><th style="padding: 12px; border: 1px solid #fcd34d; width: 25%;">服务名称</th><th style="padding: 12px; border: 1px solid #fcd34d; width: 45%;">服务内容</th><th style="padding: 12px; border: 1px solid #fcd34d; width: 30%;">提拆费</th></tr></thead>'
    html += '<tbody>'
    
    # 上半部分 - 提拆费相关三列数据
    for key, value in details.items():
        if key in ["提拆费", "拆提费", "Terminal Handling", "THC", "提柜费", "还柜费"] and isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    html += '<tr>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item.get("服务名称", item.get("name", ""))}</td>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item.get("服务内容", item.get("content", ""))}</td>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item.get("提拆费", item.get("fee", ""))}</td>'
                    html += '</tr>'
                elif isinstance(item, list) and len(item) >= 3:
                    html += '<tr>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item[0]}</td>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item[1]}</td>'
                    html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{item[2]}</td>'
                    html += '</tr>'
            break
    
    # 下半部分 - 提柜杂费，三列布局，第一列合并
    # 先统计有多少行需要合并
    misc_items = []
    for key, value in details.items():
        if key not in ["提拆费", "拆提费", "Terminal Handling", "THC", "提柜费", "还柜费"]:
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, list):
                        sub_value = ", ".join(sub_value)
                    misc_items.append((sub_key, sub_value))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        misc_items.append((item.get("name", item.get("服务名称", "")), item.get("fee", item.get("提拆费", ""))))
                    elif isinstance(item, list) and len(item) >= 2:
                        misc_items.append((item[0], item[1]))
            else:
                misc_items.append((key, value))
    
    if misc_items:
        html += f'<tr><td style="padding: 10px; border: 1px solid #fef3c7; font-weight: bold; vertical-align: middle; background: #fffbeb;" rowspan="{len(misc_items)}">提柜杂费</td>'
        first_item = True
        for name, fee in misc_items:
            if first_item:
                first_item = False
            else:
                html += '<tr>'
            html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{name if name is not None else ""}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #fef3c7; word-wrap: break-word;">{fee if fee is not None else ""}</td>'
            html += '</tr>'
    
    html += '</tbody></table>'
    return html


def _render_warehouse_details(details):
    """渲染仓库库内操作费详情"""
    html = '<table class="table" style="width: 100%; border-collapse: collapse; font-size: 15px;">'
    html += '<thead style="background: #ffeaa7;"><tr><th style="padding: 12px; border: 1px solid #fcd34d; width: 50%;">项目</th><th style="padding: 12px; border: 1px solid #fcd34d; width: 50%;">价格</th></tr></thead>'
    html += '<tbody>'
    
    for key, value in details.items():
        html += '<tr>'
        html += f'<td style="padding: 10px; border: 1px solid #ffeaa7; word-wrap: break-word;">{key if key is not None else ""}</td>'
        html += f'<td style="padding: 10px; border: 1px solid #ffeaa7; word-wrap: break-word;">{value if value is not None else ""}</td>'
        html += '</tr>'
    
    html += '</tbody></table>'
    return html


def _render_stipulate_details(details):
    """渲染组合柜规则详情"""
    html = '<div style="font-size: 15px;">'
    
    global_rules = details.get("global_rules", {})
    warehouse_pricing = details.get("warehouse_pricing", {})
    special_warehouse = details.get("special_warehouse", {})
    tiered_pricing = details.get("tiered_pricing", {})
    
    if global_rules:
        html += '<div style="margin-bottom: 20px;">'
        html += '<h4 style="margin-bottom: 10px; color: #495057;">全局规则</h4>'
        html += '<table class="table" style="width: 100%; border-collapse: collapse;">'
        html += '<thead style="background: #bfdbfe;"><tr><th style="padding: 12px; border: 1px solid #93c5fd;">参数</th><th style="padding: 12px; border: 1px solid #93c5fd;">默认值</th><th style="padding: 12px; border: 1px solid #93c5fd;">例外</th></tr></thead>'
        html += '<tbody>'
        for rule_name, rule_data in global_rules.items():
            exceptions = rule_data.get("exceptions", {})
            exception_str = ""
            if exceptions:
                exception_str = "; ".join([f"{k}: {v}" for k, v in exceptions.items()])
            html += '<tr>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule_name if rule_name is not None else ""}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule_data.get("default", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{exception_str if exception_str else ""}</td>'
            html += '</tr>'
        html += '</tbody></table></div>'
    
    if warehouse_pricing:
        html += '<div style="margin-bottom: 20px;">'
        html += '<h4 style="margin-bottom: 10px; color: #495057;">仓库价格</h4>'
        html += '<table class="table" style="width: 100%; border-collapse: collapse;">'
        html += '<thead style="background: #bfdbfe;"><tr><th style="padding: 12px; border: 1px solid #93c5fd;">仓库</th><th style="padding: 12px; border: 1px solid #93c5fd;">40尺基础</th><th style="padding: 12px; border: 1px solid #93c5fd;">45尺基础</th><th style="padding: 12px; border: 1px solid #93c5fd;">40尺不混装</th><th style="padding: 12px; border: 1px solid #93c5fd;">45尺不混装</th><th style="padding: 12px; border: 1px solid #93c5fd;">提货最小</th><th style="padding: 12px; border: 1px solid #93c5fd;">提货最大</th><th style="padding: 12px; border: 1px solid #93c5fd;">打板</th><th style="padding: 12px; border: 1px solid #93c5fd;">53尺不混装</th></tr></thead>'
        html += '<tbody>'
        for wh, data in warehouse_pricing.items():
            html += '<tr>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{wh if wh is not None else ""}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("base_40ft", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("base_45ft", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("nonmix_40ft", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("nonmix_45ft", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("pickup_min", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("pickup_max", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("palletizing", "")}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{data.get("nonmix_53ft", "")}</td>'
            html += '</tr>'
        html += '</tbody></table></div>'
    
    if special_warehouse:
        html += '<div style="margin-bottom: 20px;">'
        html += '<h4 style="margin-bottom: 10px; color: #495057;">特殊仓库</h4>'
        html += '<table class="table" style="width: 100%; border-collapse: collapse;">'
        html += '<thead style="background: #bfdbfe;"><tr><th style="padding: 12px; border: 1px solid #93c5fd;">仓库</th><th style="padding: 12px; border: 1px solid #93c5fd;">目的地</th><th style="padding: 12px; border: 1px solid #93c5fd;">倍数</th></tr></thead>'
        html += '<tbody>'
        for wh, data in special_warehouse.items():
            destinations = data.get("destination", [])
            multiplier = data.get("multiplier", "")
            dest_str = ", ".join(destinations) if destinations else ""
            html += '<tr>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{wh if wh is not None else ""}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{dest_str}</td>'
            html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{multiplier if multiplier is not None else ""}</td>'
            html += '</tr>'
        html += '</tbody></table></div>'
    
    if tiered_pricing:
        html += '<div style="margin-bottom: 20px;">'
        html += '<h4 style="margin-bottom: 10px; color: #495057;">阶梯定价</h4>'
        html += '<table class="table" style="width: 100%; border-collapse: collapse;">'
        html += '<thead style="background: #bfdbfe;"><tr><th style="padding: 12px; border: 1px solid #93c5fd;">仓库</th><th style="padding: 12px; border: 1px solid #93c5fd;">最小点</th><th style="padding: 12px; border: 1px solid #93c5fd;">最大点</th><th style="padding: 12px; border: 1px solid #93c5fd;">费用</th><th style="padding: 12px; border: 1px solid #93c5fd;">备注</th></tr></thead>'
        html += '<tbody>'
        for wh, rules in tiered_pricing.items():
            first_row = True
            for rule in rules:
                html += '<tr>'
                if first_row:
                    html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; vertical-align: top; word-wrap: break-word;" rowspan="{len(rules)}">{wh if wh is not None else ""}</td>'
                    first_row = False
                html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule.get("min_points", "")}</td>'
                html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule.get("max_points", "")}</td>'
                html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule.get("fee", "")}</td>'
                html += f'<td style="padding: 10px; border: 1px solid #bfdbfe; word-wrap: break-word;">{rule.get("note", "")}</td>'
                html += '</tr>'
        html += '</tbody></table></div>'
    
    html += '</div>'
    return html


def _render_default_details(details):
    """默认渲染方式"""
    if isinstance(details, dict):
        return _render_key_value_details(details)
    else:
        return f'<div style="padding: 10px; background: #f8f9fa; border-radius: 4px;">{details if details is not None else ""}</div>'