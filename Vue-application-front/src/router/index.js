import { createRouter, createWebHistory } from 'vue-router'
import uploadFile from '../views/uploadFile.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'home',
      component: uploadFile
    },
  ]
})

export default router
