"""Read-only inventory of explicit permissions in application code and templates.

This inventory documents existing checks; it never grants or changes access.
"""
import ast
import re
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERMISSION_CATEGORY = "权限管理"
AREAS = {
    "accounting": "应付账单", "receivable_accounting": "应收账单",
    "dropshipping": "一件代发", "order_creation": "港前订单",
    "order_management": "港前订单", "post_nsop": "港后新SOP",
    "fleet_management": "车次管理", "shipping_management": "派送管理",
    "palletization": "拆柜打板", "shipment_schedule": "派送排程",
    "order_quantity": "统计报表", "customer_management": "客户管理",
    "exception_handling": "异常处理", "terminal_dispatch": "码头调度",
    "stuff_user_ability": "员工能力管理", "db_query": "数据库查询",
    "auto_quote": "自动报价", "admin": "后台管理", "navbar": "导航菜单",
    "shipment_binding_utils": "PO绑定权限", "custom_tags": "导航菜单",
    "inventory": "库存管理", "shipment": "派送管理", "pre_port": "港前操作",
    "statistics": "统计报表", "po": "PO查询",
}
OPERATIONS = {
    "_validate_user_invoice_direct": "直送账单权限",
    "_validate_user_invoice_combina": "组合柜账单权限",
    "_validate_user_invoice_preport": "港前账单录入/组长权限",
    "_validate_user_invoice_warehouse": "仓库账单权限",
    "_validate_user_invoice_delivery": "派送账单权限",
    "_validate_user_invoice_confirm": "账单确认权限",
    "_validate_user_invoice_payable_check": "应付账单审核权限",
    "_validate_user_exception_handling": "异常处理权限",
    "_validate_super_user": "管理员操作（实际检查 is_staff）",
    "visible_batches": "报价任务/历史/分析数据范围：工作人员看全部，其他人仅看本人",
    "_get_delivery_type_filter": "公仓/私仓派送数据可见范围",
    "has_public_permission": "公仓PO绑定及公仓供应商、FBA仓点、州仓点、ZEM仓库地址",
    "has_other_permission": "私仓PO绑定及私仓供应商",
    "has_delete_permission": "删除操作日志",
    "_get_system_parameter_context": "系统参数分类可见范围",
    "handle_cancel_notification": "取消预报",
}
ALIASES = {
    "has_public_permission": {"shipment_bind_public"},
    "has_other_permission": {"shipment_bind_other"},
    "has_permission": {"shipment_bind_public", "shipment_bind_other"},
}


def display_permissions(permissions):
    values = set(permissions)
    if len(values) > 1:
        values.discard("superuser")
    return "；".join(sorted(values))


def _direct_permissions(node, allowed_group):
    result = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Attribute) and child.attr in ("is_staff", "is_superuser"):
            result.add("superuser" if child.attr == "is_superuser" else "is_staff")
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
            name = ast.unparse(child.func)
            if ".groups.filter" in name or name == "Group.objects.filter":
                for keyword in child.keywords:
                    if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
                        result.add(str(keyword.value.value))
                    elif keyword.arg == "name" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "allowed_group" and allowed_group:
                        result.add(allowed_group)
        if isinstance(child, ast.Compare):
            operands = [child.left, *child.comparators]
            if any(isinstance(value, ast.Name) and value.id == "groups" for value in operands):
                result.update(value.value for value in operands if isinstance(value, ast.Constant) and isinstance(value.value, str))
    return result


@lru_cache(maxsize=1)
def get_permission_catalog():
    functions = []
    for directory in (ROOT / "views", ROOT / "utils", ROOT / "templatetags"):
        for path in sorted(directory.rglob("*.py")):
            if path.name == Path(__file__).name:
                continue
            source = path.read_text(encoding="utf-8-sig")
            tree = ast.parse(source)
            for parent in [tree, *(item for item in ast.walk(tree) if isinstance(item, ast.ClassDef))]:
                allowed_group = next((item.value.value for item in parent.body if isinstance(item, ast.Assign)
                                      and any(isinstance(t, ast.Name) and t.id == "allowed_group" for t in item.targets)
                                      and isinstance(item.value, ast.Constant)), None)
                for node in parent.body:
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        functions.append((path, node, allowed_group, getattr(parent, "name", "")))
    # Admin model permissions are explicit too.
    path = ROOT / "admin.py"
    for parent in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
        if isinstance(parent, ast.ClassDef):
            functions.extend((path, node, None, parent.name) for node in parent.body if isinstance(node, ast.FunctionDef))

    permissions = {(str(path), owner, node.name): _direct_permissions(node, group)
                   for path, node, group, owner in functions}
    # Resolve local permission helpers and the shared public/private binding helpers.
    for _ in range(4):
        for path, node, _, owner in functions:
            values = permissions[str(path), owner, node.name]
            for child in ast.walk(node):
                if isinstance(child, ast.Attribute):
                    values.update(ALIASES.get(child.attr, set()))
                    if isinstance(child.value, ast.Name) and child.value.id == "self":
                        values.update(permissions.get((str(path), owner, child.attr), set()))

    rows = []
    for path, node, _, owner in functions:
        values = permissions[str(path), owner, node.name]
        if not values:
            continue
        doc = ast.get_docstring(node)
        title = OPERATIONS.get(node.name) or (doc.splitlines()[0][:100] if doc else node.name)
        if path.stem == "order_quantity":
            title = {"_validate_user_group": "统计总览", "_validate_user_manage": "历史查询", "_validate_user_profit": "订单利润"}.get(node.name, title)
        rows.append({"area": AREAS.get(path.stem, path.stem), "name": title,
                     "permission": display_permissions(values), "kind": "后端/数据范围",
                     "source": str(path.relative_to(ROOT.parent)).replace("\\", "/"),
                     "line": node.lineno, "operation": node.name,
                     "note": "该入口涉及的账号标记/用户组；不同业务分支可能分别校验。"})

    for path in sorted((ROOT / "templates").rglob("*.html")):
        if path.name in ("system_parameter_add.html", "permission_catalog.html"):
            continue
        source = path.read_text(encoding="utf-8-sig")
        for match in re.finditer(r"{%\s*(?:if|elif)\s+(.+?)\s*%}", source):
            condition = match.group(1)
            values = set()
            if "is_staff" in condition:
                values.add("is_staff")
            if "is_superuser" in condition:
                values.add("superuser")
            for flag, groups in {"cancel_access": {"create_order_dropshipping" if "dropshipping" in path.parts else "create_order"},
                                 "unpacking_personnel": {"unpacking_personnel"},
                                 "modify_shipped_shipment": {"shipment_leader"}}.items():
                if flag in condition:
                    values.update(groups)
            if "permission ==" in condition:
                values.update({"shipment_bind_public", "shipment_bind_other"} if '"all"' in condition else
                              {"shipment_bind_public"} if '"public"' in condition else {"shipment_bind_other"})
            if not values:
                continue
            # List each gated button's actual label, not just its template filename.
            fragment = source[match.end():]
            depth, end = 1, len(fragment)
            for tag in re.finditer(r"{%\s*(if\b|endif\b).*?%}", fragment):
                depth += 1 if tag.group(1) == "if" else -1
                if depth == 0:
                    end = tag.start()
                    break
            labels = []
            for button in re.finditer(r"<button\b[^>]*>(.*?)</button>", fragment[:end], re.S):
                label = re.sub(r"<[^>]*>|{%.*?%}|{{.*?}}", "", button.group(1), flags=re.S)
                label = " ".join(label.split())
                if label and label not in labels:
                    labels.append(label)
            for label in labels or [path.stem + "：受限区域"]:
                rows.append({"area": AREAS.get(path.parent.name, path.parent.name), "name": label,
                             "permission": display_permissions(values), "kind": "页面/按钮显示",
                             "source": str(path.relative_to(ROOT.parent)).replace("\\", "/"),
                             "line": source.count("\n", 0, match.start()) + 1,
                             "operation": condition, "note": "模板显示条件，不代表后端已执行同样的权限校验。"})
    rows.extend([
        {"area": "导航菜单", "name": "系统管理导航入口", "permission": "is_staff", "kind": "页面/按钮显示",
         "source": "warehouse/templates/navbar.html", "line": 660, "operation": "if (isStaff)",
         "note": "工作人员显示系统管理菜单；导航可见性不等于各接口的权限。"},
        {"area": "通用", "name": "需要登录的页面（login_required / is_authenticated）", "permission": "已登录账号",
         "kind": "登录校验", "source": "warehouse/views/user_login.py", "line": 29,
         "operation": "login_required / is_authenticated", "note": "登录校验统一列示；未将每个登录入口重复列入。"},
        {"area": "后台管理", "name": "Django管理后台 / 模型增删改查", "permission": "is_staff + 对应模型权限",
         "kind": "后端/数据范围", "source": "warehouse/admin.py", "line": 1,
         "operation": "Django Admin 默认权限机制", "note": "后台登录要求有效工作人员账号，模型操作还需要对应权限；单独重写的日志删除权限见清单。"},
    ])
    rows.sort(key=lambda row: (row["area"], row["source"], row["line"], row["name"]))
    return rows
