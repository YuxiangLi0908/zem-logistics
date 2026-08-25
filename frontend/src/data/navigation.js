export const navSections = [
  {
    id: "customer",
    label: "客户管理",
    description: "客户资料、账户余额、报价关系",
    items: [
      { id: "customer-new", label: "添加客户", description: "维护客户基础资料", legacyUrl: "/customer_management/" },
      { id: "customer-balance", label: "余额管理", description: "客户账户余额与授信", legacyUrl: "/customer_management/?step=customer_balance" }
    ]
  },
  {
    id: "pre_port",
    label: "港前操作",
    description: "订单创建、港口调度、柜况追踪",
    items: [
      {
        id: "quote",
        label: "询价",
        description: "新建询价和历史询价",
        children: [
          { label: "新建询价", legacyUrl: "/quote/?step=new" },
          { label: "历史询价", legacyUrl: "/quote/?step=history" }
        ]
      },
      { id: "pre-order", label: "港前订单管理", description: "港前订单总表", legacyUrl: "/create_order/?step=all" },
      { id: "repeat-order", label: "订单删除", description: "重复订单和异常删除", legacyUrl: "/create_order/?step=repeat_all" },
      { id: "t49", label: "T49待追踪", description: "Terminal49 待追踪任务", legacyUrl: "/create_order/?step=repeat_t49_all" },
      { id: "vendor", label: "新增仓库与供应商", description: "基础资料维护", legacyUrl: "/create_order/?step=create_warehouse_get" },
      { id: "pre-tracking", label: "港前订单追踪", description: "订单轨迹追踪", legacyUrl: "/pre_port_tracking/?step=all" },
      { id: "terminal", label: "港口调度", description: "预约提柜和调度任务", legacyUrl: "/terminal_dispatch/?step=all" },
      { id: "container-status", label: "货柜状态追踪", description: "柜况、可提、滞箱风险", legacyUrl: "/contaier_pickup_status/?step=all" },
      { id: "oct", label: "OCT汇总", description: "OCT 操作汇总", legacyUrl: "/oct_summary/?step=all" },
      { id: "pre-summary", label: "货柜进度汇总", description: "港前进度看板", legacyUrl: "/contaier_pre_port_summary_dash/?step=all" },
      { id: "info-update", label: "信息更新", description: "批量更新提柜和订单信息", legacyUrl: "/information_update/?step=all" }
    ]
  },
  {
    id: "warehouse",
    label: "仓库操作",
    description: "拆柜入库、盘点、出库和库存",
    items: [
      { id: "palletize", label: "拆柜入库", description: "仓库入库作业", legacyUrl: "/palletize/" },
      {
        id: "warehouse-manage",
        label: "仓库管理",
        description: "入库、盘点、出库、LTL",
        children: [
          { label: "入库", legacyUrl: "/warehouse_operations/?step=warehousing_operation" },
          { label: "盘点及特操", legacyUrl: "/warehouse_operations/?step=counting_pallet" },
          { label: "出库", legacyUrl: "/warehouse_operations/?step=upcoming_fleet" },
          { label: "LTL出库", legacyUrl: "/warehouse_operations/?step=upcoming_fleet_ltl" }
        ]
      },
      { id: "warehouse-zone", label: "库位管理", description: "库位规划与调整", legacyUrl: "/palletize/?step=warehouse_zone" },
      { id: "merge-pallet", label: "合板管理", description: "合板、拆板、库存合并", legacyUrl: "/inventory/?step=merge_pallet" },
      { id: "daily-operation", label: "仓库待处理", description: "仓库任务队列", legacyUrl: "/palletize/?step=daily_operation" },
      { id: "inventory", label: "库存管理", description: "库存查询和批次状态", legacyUrl: "/inventory/" }
    ]
  },
  {
    id: "work",
    label: "工作一览",
    description: "操作组、批次核对、便捷工具",
    items: [
      { id: "ops-unscheduled", label: "四大仓备约", description: "四大仓待备约", legacyUrl: "/post_nsop/?step=unscheduled_pos_all" },
      { id: "appointment", label: "备约", description: "预约管理", legacyUrl: "/post_nsop/?step=appointment_management" },
      { id: "schedule", label: "库存排约", description: "库存排车排约", legacyUrl: "/post_nsop/?step=schedule_shipment" },
      { id: "fleet", label: "车次操作", description: "车次管理和异常", legacyUrl: "/post_nsop/?step=fleet_management" },
      { id: "history-shipment", label: "历史排约", description: "历史排约查询", legacyUrl: "/post_nsop/?step=history_shipment" },
      { id: "easy-action", label: "便捷操作", description: "常用表格和快速任务", legacyUrl: "/post_nsop/?step=easy_action" }
    ]
  },
  {
    id: "dropshipping",
    label: "一件代发",
    description: "代发订单、派送、退货和账单",
    items: [
      { id: "drop-order", label: "创建订单", description: "一件代发订单创建", legacyUrl: "/dropshipping/?step=all" },
      { id: "drop-terminal", label: "港口调度", description: "代发港口调度", legacyUrl: "/dropshipping/?step=terminal_all" },
      { id: "drop-list", label: "订单列表", description: "代发订单列表", legacyUrl: "/dropshipping/?step=order_management_list" },
      { id: "drop-inventory", label: "库存管理", description: "代发库存", legacyUrl: "/dropshipping/?step=inventory_all" },
      { id: "post-delivery", label: "港后派送", description: "派送和 POD", legacyUrl: "/post_drop/?step=postport_delivery" },
      { id: "return-process", label: "退货处理", description: "退货流程", legacyUrl: "/post_drop/?step=return_process" }
    ]
  },
  {
    id: "finance",
    label: "新版财务",
    description: "新版应收应付、核销和账单进度",
    items: [
      { id: "invoice-search", label: "账单录入进度", description: "应收应付进度看板", legacyUrl: "/receivable_accounting/?step=invoice_search" },
      { id: "receivable-preport", label: "提拆账单录入", description: "应收提拆", legacyUrl: "/receivable_accounting/?step=preport" },
      { id: "receivable-warehouse", label: "库内账单录入", description: "应收库内", legacyUrl: "/receivable_accounting/?step=warehouse" },
      { id: "receivable-delivery", label: "派送账单录入", description: "应收派送", legacyUrl: "/receivable_accounting/?step=delivery" },
      { id: "payable-preport", label: "应付提拆账单", description: "应付提拆确认", legacyUrl: "/accounting/?step=invoice_payable_v1" }
    ]
  },
  {
    id: "old_finance",
    label: "旧版财务",
    description: "旧版账单、托盘数据和报价表",
    items: [
      { id: "old-pallet-data", label: "托盘数据", description: "旧版财务托盘数据", legacyUrl: "/accounting/?step=pallet_data" },
      { id: "old-pl-data", label: "派送清单", description: "旧版派送清单", legacyUrl: "/accounting/?step=pl_data" },
      {
        id: "old-invoice-entry",
        label: "账单录入",
        description: "旧版整柜、提拆、库内、派送账单",
        children: [
          { label: "旧版货柜账单", legacyUrl: "/accounting/?step=invoice" },
          { label: "整柜直送", legacyUrl: "/accounting/?step=invoice_direct" },
          { label: "提拆柜", legacyUrl: "/accounting/?step=invoice_preport" },
          { label: "库内操作", legacyUrl: "/accounting/?step=invoice_warehouse" },
          { label: "派送", legacyUrl: "/accounting/?step=invoice_delivery" },
          { label: "账单确认", legacyUrl: "/accounting/?step=invoice_confirm" },
          { label: "应付账单", legacyUrl: "/accounting/?step=invoice_payable" },
          { label: "账单录入进度", legacyUrl: "/accounting/?step=invoice_search" }
        ]
      },
      {
        id: "old-quote-master",
        label: "报价表",
        description: "旧版应收/应付报价表",
        children: [
          { label: "应收报价表管理", legacyUrl: "/quote/?step=quote_master" },
          { label: "应付报价表管理", legacyUrl: "/quote/?step=payable_quote_master" }
        ]
      }
    ]
  },
  {
    id: "report",
    label: "报表汇总",
    description: "统计、利润、时效和历史查询",
    items: [
      { id: "order-stat", label: "订单量统计", description: "订单量趋势", legacyUrl: "/order_statistics" },
      { id: "profit", label: "订单利润统计", description: "利润分析", legacyUrl: "/order_statistics?step=profit_analysis" },
      { id: "cbm", label: "CBM统计", description: "CBM 分析", legacyUrl: "/order_statistics?step=cbm_analysis" },
      { id: "timeliness", label: "时效统计", description: "派送时效", legacyUrl: "/order_statistics?step=delivery_timeliness_analysis" },
      { id: "history", label: "历史操作查询", description: "历史动作检索", legacyUrl: "/order_statistics?step=historical_query" }
    ]
  },
  {
    id: "exception",
    label: "异常处理",
    description: "异常修复、数据工具和历史修复",
    items: [
      { id: "excel-tool", label: "表格便捷操作", description: "Excel 辅助工具", legacyUrl: "/exception_handling/?step=excel_formula_tool" },
      { id: "post-port-status", label: "港后状态异常", description: "状态异常处理", legacyUrl: "/exception_handling/?step=post_port_status" },
      { id: "shipment-actual", label: "实际约主约修改", description: "主约修复", legacyUrl: "/exception_handling/?step=shipment_actual" },
      { id: "find-table", label: "查询各表详情", description: "数据库记录定位", legacyUrl: "/exception_handling/?step=find_table_id" }
    ]
  },
  {
    id: "admin",
    label: "后台管理",
    description: "SQL、清理数据、系统特殊操作",
    items: [
      { id: "admin-sql", label: "SQL Query", description: "数据库查询工具", legacyUrl: "/dbconn" },
      { id: "admin-clean", label: "清理数据", description: "后台清理任务", legacyUrl: "/stuff_user/?step=clean_data" },
      {
        id: "admin-system-tools",
        label: "系统各种特殊操作",
        description: "预约表、系统匹配、数据落库",
        children: [
          { label: "上传预约表并展示", legacyUrl: "/container_tracking/" },
          { label: "预约表与系统匹配情况", legacyUrl: "/container_tracking/?step=actual_match" },
          { label: "将预约表落实到系统", legacyUrl: "/container_tracking/?step=sp_operation" },
          { label: "查询记录id", legacyUrl: "/container_tracking/?step=find_table_id" }
        ]
      },
      {
        id: "admin-async",
        label: "Test Async",
        description: "异步测试工具",
        children: [
          { label: "async", legacyUrl: "/async_view?step=async" },
          { label: "sync", legacyUrl: "/async_view?step=sync" }
        ]
      },
      {
        id: "admin-pre-legacy",
        label: "港前(legacy)",
        description: "旧版港前创建订单和预约提柜",
        children: [
          { label: "直送", legacyUrl: "/create_order_legacy/?type=DD" },
          { label: "转运", legacyUrl: "/create_order_legacy/?type=TD" },
          { label: "预约提柜", legacyUrl: "/container_pickup/" }
        ]
      }
    ]
  }
];

export function findModule(moduleId) {
  for (const section of navSections) {
    const item = section.items.find((entry) => entry.id === moduleId);
    if (item) return { section, item };
  }
  return null;
}
