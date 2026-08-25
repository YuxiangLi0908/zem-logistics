# Frontend Separation Plan

Branch: `qyj_0825_frontend`

## Current Findings

- The current app is a Django template application with `warehouse/templates` as the frontend surface.
- `warehouse/templates/navbar.html` is the best first extraction target because it defines the product navigation and module map.
- There are 340 HTML templates under `warehouse/templates` after cleanup.
- A literal/template-tag scan now reports 0 possibly unused templates.

Removed unused templates:

- `po.html`
- `post_port/01_summary_table.html`
- `post_port/new_sop/06_ltl_history_pos/delivery_section.html`
- `post_port/new_sop/06_ltl_history_pos/pod_section.html`
- `post_port/new_sop/06_ltl_history_pos/ready_section.html`
- `post_port/new_sop/06_ltl_history_pos/release_cargos.html`
- `post_port/new_sop/07_drop_shipping/modal/maersk_schedule_modal.html`
- `post_port/new_sop/08_drop_ship_account/modal/piece_detail_modal.html`

Run the scan again with:

```bash
python tools/audit_frontend_templates.py
```

## Target Architecture

```text
frontend/                  Vue 3 + Vite SPA
  src/data/navigation.js   Extracted module map from Django navbar
  src/views/               New Vue pages
  src/styles.css           Modern operations UI

backend_fastapi/           Future API service
  app/main.py              FastAPI app with CORS
  app/routers/             API routers

warehouse/                 Existing Django app kept as legacy fallback
```

## Migration Strategy

1. Vue owns the shell: navigation, module layout, search, migration map.
2. Existing Django pages stay reachable through `/legacy/*` during transition.
3. FastAPI starts with `/api/health` and `/api/navigation`, then grows by module.
4. Migrate low-risk read-only pages first: dashboards, status search, reports.
5. Migrate write-heavy workflows after API contracts and tests exist.
6. Remove more Django templates only after route usage and logs prove they are unused.

## API Shape

Recommended conventions:

- `GET /api/<domain>/<resource>` for lists with `page`, `page_size`, `sort`, and filter params.
- `GET /api/<domain>/<resource>/{id}` for detail views.
- `POST /api/<domain>/<resource>` for create.
- `PATCH /api/<domain>/<resource>/{id}` for partial updates.
- `POST /api/tasks/<task_type>` for exports, labels, PDFs, bulk mutations.

## First Modules To Migrate

- Pre-port tracking: high value and mostly read/query oriented.
- Container status summary: dashboard-friendly and easy to validate.
- Warehouse inventory: needs shared table/filter components.
- Dropshipping order list: good candidate after table and status components exist.

## Notes

- The current Django `warehouse.urls` already labels several legacy routes. Keep them until users stop depending on them.
- Finance templates should be migrated late because billing state has the highest regression cost.
