<script setup>
import { computed, ref, watch } from "vue";
import { RouterView, useRoute, useRouter } from "vue-router";
import {
  ArrowRight,
  Connection,
  DataBoard,
  Fold,
  Grid,
  House,
  Menu as MenuIcon,
  Operation,
  Search,
  Setting,
  SwitchButton
} from "@element-plus/icons-vue";
import { navSections } from "./data/navigation";

const route = useRoute();
const router = useRouter();
const collapsed = ref(false);
const keyword = ref("");
const activeSectionId = ref("pre_port");

const sectionIcons = {
  customer: Grid,
  pre_port: Operation,
  warehouse: House,
  work: DataBoard,
  dropshipping: Connection,
  finance: Setting,
  old_finance: DataBoard,
  report: DataBoard,
  exception: SwitchButton,
  admin: Setting
};

const totalEntrypoints = computed(() =>
  navSections.reduce((sum, section) => sum + section.items.length, 0)
);

const currentSection = computed(() =>
  navSections.find((section) => section.id === activeSectionId.value) ?? navSections[0]
);

const visibleSections = computed(() => {
  const term = keyword.value.trim().toLowerCase();
  if (!term) return navSections;

  return navSections
    .map((section) => ({
      ...section,
      items: section.items.filter((item) =>
        `${section.label} ${item.label} ${item.description}`.toLowerCase().includes(term)
      )
    }))
    .filter((section) => section.items.length > 0 || section.label.toLowerCase().includes(term));
});

const activeMenu = computed(() => {
  if (route.name === "dashboard") return "dashboard";
  if (route.name === "migration") return "migration";
  return activeSectionId.value;
});

function selectMenu(index) {
  if (index === "dashboard") {
    router.push("/");
    return;
  }
  if (index === "migration") {
    router.push("/migration");
    return;
  }
  activeSectionId.value = index;
}

function openModule(item) {
  router.push({ name: "module", params: { moduleId: item.id } });
}

watch(
  () => route.params.moduleId,
  (moduleId) => {
    if (!moduleId) return;
    const section = navSections.find((entry) => entry.items.some((item) => item.id === moduleId));
    if (section) activeSectionId.value = section.id;
  },
  { immediate: true }
);
</script>

<template>
  <el-container class="admin-shell">
    <el-aside class="admin-aside" :width="collapsed ? '72px' : '268px'">
      <div class="brand-bar" :class="{ collapsed }">
        <div class="brand-logo">Z</div>
        <div v-if="!collapsed" class="brand-copy">
          <strong>ZEM Logistics</strong>
          <span>Vue Admin Console</span>
        </div>
      </div>

      <el-menu
        :collapse="collapsed"
        :default-active="activeMenu"
        class="admin-menu"
        background-color="#ffffff"
        text-color="#303133"
        active-text-color="#1677ff"
        @select="selectMenu"
      >
        <el-menu-item index="dashboard">
          <el-icon><House /></el-icon>
          <template #title>工作台</template>
        </el-menu-item>

        <el-menu-item v-for="section in visibleSections" :key="section.id" :index="section.id">
          <el-icon>
            <component :is="sectionIcons[section.id] || MenuIcon" />
          </el-icon>
          <template #title>{{ section.label }}</template>
        </el-menu-item>

        <el-menu-item index="migration">
          <el-icon><Connection /></el-icon>
          <template #title>迁移地图</template>
        </el-menu-item>
      </el-menu>
    </el-aside>

    <el-container>
      <el-header class="admin-header">
        <div class="header-left">
          <el-button :icon="Fold" circle @click="collapsed = !collapsed" />
          <el-breadcrumb separator="/">
            <el-breadcrumb-item>ZEM</el-breadcrumb-item>
            <el-breadcrumb-item>{{ currentSection.label }}</el-breadcrumb-item>
            <el-breadcrumb-item>Console</el-breadcrumb-item>
          </el-breadcrumb>
        </div>

        <div class="header-right">
          <el-input
            v-model="keyword"
            class="global-search"
            :prefix-icon="Search"
            clearable
            placeholder="搜索模块"
          />
          <el-tag type="success" effect="light">Vue Ready</el-tag>
          <el-tag effect="plain">{{ totalEntrypoints }} Legacy</el-tag>
        </div>
      </el-header>

      <el-main class="admin-main">
        <section class="module-tabs">
          <el-scrollbar>
            <div class="module-tab-inner">
              <el-button
                v-for="item in currentSection.items"
                :key="item.id"
                class="module-tab"
                :class="{ active: route.params.moduleId === item.id }"
                plain
                @click="openModule(item)"
              >
                {{ item.label }}
                <el-icon><ArrowRight /></el-icon>
              </el-button>
            </div>
          </el-scrollbar>
        </section>

        <RouterView />
      </el-main>
    </el-container>
  </el-container>
</template>
