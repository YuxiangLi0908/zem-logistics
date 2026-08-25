<script setup>
import { computed } from "vue";
import { useRoute } from "vue-router";
import { Connection, DataLine, Link, Operation } from "@element-plus/icons-vue";
import { findModule } from "../data/navigation";

const route = useRoute();
const current = computed(() => findModule(route.params.moduleId));

const apiContracts = [
  { method: "GET", path: "/api/{domain}/{resource}", note: "列表、分页、排序、筛选" },
  { method: "GET", path: "/api/{domain}/{resource}/{id}", note: "详情和审计上下文" },
  { method: "PATCH", path: "/api/{domain}/{resource}/{id}", note: "状态更新和局部编辑" },
  { method: "POST", path: "/api/tasks/{type}", note: "导出、标签、PDF、批量动作" }
];
</script>

<template>
  <div v-if="current" class="page-stack">
    <el-card class="hero-card" shadow="never">
      <div class="hero-layout">
        <div>
          <el-tag type="primary" effect="light">{{ current.section.label }}</el-tag>
          <h2>{{ current.item.label }}</h2>
          <p>{{ current.item.description }}</p>
        </div>
        <div class="hero-actions">
          <el-button v-if="current.item.legacyUrl" type="primary" tag="a" :href="`/legacy${current.item.legacyUrl}`" target="_blank">
            打开旧页面
          </el-button>
          <el-button @click="$router.push('/migration')">迁移计划</el-button>
        </div>
      </div>
    </el-card>

    <el-row :gutter="16">
      <el-col :xs="24" :lg="14">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><Connection /></el-icon>API Contract</span>
            </div>
          </template>
          <el-table :data="apiContracts" border>
            <el-table-column prop="method" label="Method" width="100">
              <template #default="{ row }">
                <el-tag :type="row.method === 'GET' ? 'success' : row.method === 'POST' ? 'warning' : 'primary'">
                  {{ row.method }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="path" label="Endpoint" min-width="240" />
            <el-table-column prop="note" label="说明" min-width="180" />
          </el-table>
        </el-card>
      </el-col>

      <el-col :xs="24" :lg="10">
        <el-card shadow="never">
          <template #header>
            <div class="card-header">
              <span><el-icon><DataLine /></el-icon>迁移状态</span>
            </div>
          </template>
          <el-steps direction="vertical" :active="1" finish-status="success">
            <el-step title="Legacy linked" description="旧页面可回退" />
            <el-step title="Vue route scaffolded" description="前端入口已创建" />
            <el-step title="API pending" description="等待拆 Django view 查询层" />
          </el-steps>
        </el-card>
      </el-col>
    </el-row>

    <el-card v-if="current.item.children?.length" shadow="never">
      <template #header>
        <div class="card-header">
          <span><el-icon><Operation /></el-icon>子入口</span>
        </div>
      </template>
      <div class="entry-grid">
        <el-button
          v-for="child in current.item.children"
          :key="child.label"
          tag="a"
          :href="`/legacy${child.legacyUrl}`"
          target="_blank"
        >
          <el-icon><Link /></el-icon>
          {{ child.label }}
        </el-button>
      </div>
    </el-card>
  </div>

  <el-empty v-else description="没有找到模块">
    <el-button type="primary" @click="$router.push('/')">返回工作台</el-button>
  </el-empty>
</template>
