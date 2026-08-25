<script setup>
import { Compass, Delete, Guide, Warning } from "@element-plus/icons-vue";

const phases = [
  { title: "外壳抽离", text: "Vue 负责导航、布局和登录后工作台，Django 页面保留为 legacy 回退。" },
  { title: "API 切片", text: "从只读列表开始，把 Django 查询逻辑整理为 FastAPI endpoint。" },
  { title: "交互迁移", text: "批量操作、弹窗、导出任务拆成 Vue 组件和 API action。" },
  { title: "Django 收缩", text: "模板不再承载页面，只保留后台管理、过渡接口或最终下线。" }
];

const removedTemplates = [
  "po.html",
  "post_port/01_summary_table.html",
  "post_port/new_sop/06_ltl_history_pos/delivery_section.html",
  "post_port/new_sop/06_ltl_history_pos/pod_section.html",
  "post_port/new_sop/06_ltl_history_pos/ready_section.html",
  "post_port/new_sop/06_ltl_history_pos/release_cargos.html",
  "post_port/new_sop/07_drop_shipping/modal/maersk_schedule_modal.html",
  "post_port/new_sop/08_drop_ship_account/modal/piece_detail_modal.html"
];
</script>

<template>
  <div class="page-stack">
    <el-card shadow="never">
      <template #header>
        <div class="card-header">
          <span><el-icon><Compass /></el-icon>迁移路线</span>
          <el-tag type="success" effect="light">Vue + FastAPI</el-tag>
        </div>
      </template>
      <el-steps :active="1" finish-status="success" align-center>
        <el-step v-for="phase in phases" :key="phase.title" :title="phase.title" :description="phase.text" />
      </el-steps>
    </el-card>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="12">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><Delete /></el-icon>已删除废弃模板</span>
              <el-tag type="info">{{ removedTemplates.length }} files</el-tag>
            </div>
          </template>
          <div class="removed-list">
            <el-tag v-for="template in removedTemplates" :key="template" effect="plain">
              {{ template }}
            </el-tag>
          </div>
        </el-card>
      </el-col>

      <el-col :xs="24" :lg="12">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><Guide /></el-icon>下一步</span>
            </div>
          </template>
          <el-timeline>
            <el-timeline-item type="primary" timestamp="Phase 1">
              港前追踪、柜况和调度看板迁移为只读 API。
            </el-timeline-item>
            <el-timeline-item type="success" timestamp="Phase 2">
              建立通用 Table、FilterBar、BatchAction 组件。
            </el-timeline-item>
            <el-timeline-item type="warning" timestamp="Phase 3">
              财务模块先补审计和权限，再迁移写操作。
            </el-timeline-item>
          </el-timeline>
        </el-card>
      </el-col>
    </el-row>

    <el-alert
      :closable="false"
      type="warning"
      show-icon
      title="删除说明"
      description="本次删除基于模板引用扫描结果；后续仍建议结合线上访问日志继续清理遗留 URL 和 view。"
    >
      <template #icon><Warning /></template>
    </el-alert>
  </div>
</template>
