<script setup>
import { computed } from "vue";
import {
  Aim,
  Connection,
  DataAnalysis,
  Finished,
  Monitor,
  TrendCharts,
  Warning
} from "@element-plus/icons-vue";
import { navSections } from "../data/navigation";

const totalEntrypoints = computed(() =>
  navSections.reduce((sum, section) => sum + section.items.length, 0)
);

const overview = computed(() => [
  { label: "业务域", value: navSections.length, icon: Monitor, type: "primary" },
  { label: "菜单入口", value: totalEntrypoints.value, icon: Finished, type: "success" },
  { label: "API 服务", value: "FastAPI", icon: Connection, type: "warning" },
  { label: "待确认废弃", value: 0, icon: Warning, type: "info" }
]);

const migrationRows = [
  { module: "港前操作", phase: "Phase 1", risk: "低", progress: 72, owner: "API + Table" },
  { module: "仓库操作", phase: "Phase 2", risk: "中", progress: 48, owner: "Inventory domain" },
  { module: "一件代发", phase: "Phase 3", risk: "中", progress: 36, owner: "Order workflow" },
  { module: "新版财务", phase: "Hold", risk: "高", progress: 18, owner: "Audit first" },
  { module: "旧版财务", phase: "Legacy", risk: "高", progress: 12, owner: "Keep fallback" },
  { module: "后台管理", phase: "Protected", risk: "中", progress: 28, owner: "Permission first" }
];

const systemCards = [
  { label: "Vue Shell", status: "已升级", detail: "Element Plus 管理后台结构" },
  { label: "Legacy Proxy", status: "可回退", detail: "/legacy/* -> Django" },
  { label: "FastAPI", status: "骨架", detail: "/api/health /api/navigation" },
  { label: "Auth", status: "待设计", detail: "Cookie 共享或 JWT" }
];

function riskType(risk) {
  return risk === "高" ? "danger" : risk === "中" ? "warning" : "success";
}
</script>

<template>
  <div class="page-stack">
    <el-card class="hero-card" shadow="never">
      <div class="hero-layout">
        <div>
          <el-tag type="primary" effect="light">Element Plus Admin</el-tag>
          <h2>前后端分离运营控制台</h2>
          <p>
            参照主流 Vue3 后台模板的结构，将导航、指标、模块入口、迁移状态集中到统一后台工作台。
          </p>
        </div>
        <div class="hero-actions">
          <el-button type="primary" size="large" @click="$router.push('/migration')">查看迁移地图</el-button>
          <el-button size="large" tag="a" href="/legacy/" target="_blank">打开旧系统</el-button>
        </div>
      </div>
    </el-card>

    <el-row :gutter="16">
      <el-col v-for="item in overview" :key="item.label" :xs="24" :sm="12" :lg="6">
        <el-card class="stat-card" shadow="never">
          <div class="stat-icon" :class="item.type">
            <el-icon><component :is="item.icon" /></el-icon>
          </div>
          <div>
            <span>{{ item.label }}</span>
            <strong>{{ item.value }}</strong>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="16">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><TrendCharts /></el-icon>迁移优先级</span>
              <el-tag effect="plain">Roadmap</el-tag>
            </div>
          </template>
          <el-table :data="migrationRows" stripe>
            <el-table-column prop="module" label="模块" min-width="120" />
            <el-table-column prop="phase" label="阶段" width="110" />
            <el-table-column label="风险" width="90">
              <template #default="{ row }">
                <el-tag :type="riskType(row.risk)" effect="light">{{ row.risk }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="进度" min-width="180">
              <template #default="{ row }">
                <el-progress :percentage="row.progress" :stroke-width="8" />
              </template>
            </el-table-column>
            <el-table-column prop="owner" label="迁移重点" min-width="150" />
          </el-table>
        </el-card>
      </el-col>

      <el-col :xs="24" :lg="8">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><Aim /></el-icon>系统状态</span>
            </div>
          </template>
          <div class="status-list">
            <div v-for="item in systemCards" :key="item.label" class="status-row">
              <div>
                <strong>{{ item.label }}</strong>
                <small>{{ item.detail }}</small>
              </div>
              <el-tag>{{ item.status }}</el-tag>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never">
      <template #header>
        <div class="card-header">
          <span><el-icon><DataAnalysis /></el-icon>业务模块矩阵</span>
          <el-tag type="success" effect="light">{{ totalEntrypoints }} entries</el-tag>
        </div>
      </template>
      <el-row :gutter="12">
        <el-col v-for="section in navSections" :key="section.id" :xs="24" :sm="12" :lg="6">
          <div class="module-card" @click="$router.push(`/modules/${section.items[0]?.id}`)">
            <strong>{{ section.label }}</strong>
            <p>{{ section.description }}</p>
            <el-tag effect="plain">{{ section.items.length }} 个入口</el-tag>
          </div>
        </el-col>
      </el-row>
    </el-card>
  </div>
</template>
