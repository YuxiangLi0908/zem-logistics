import { createApp } from "vue";
import { createRouter, createWebHistory } from "vue-router";
import App from "./App.vue";
import Dashboard from "./views/Dashboard.vue";
import ModuleView from "./views/ModuleView.vue";
import MigrationMap from "./views/MigrationMap.vue";
import "./styles.css";

const routes = [
  { path: "/", name: "dashboard", component: Dashboard },
  { path: "/modules/:moduleId", name: "module", component: ModuleView },
  { path: "/migration", name: "migration", component: MigrationMap }
];

const router = createRouter({
  history: createWebHistory(),
  routes
});

createApp(App).use(router).mount("#app");
