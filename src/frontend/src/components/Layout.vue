<template>
    <div class="layout">
      <AppSidebar />
      <div class="main-content">
        <router-view />
      </div>
    </div>
  </template>
  
  <script>
  import AppSidebar from "@/components/Sidebar.vue";
  
  export default {
    name: "MainLayout",
    components: {
      AppSidebar,
    },
  };
  </script>
  
  <style scoped>
  .layout {
    display: flex;
    height: 100vh; /* 화면 전체 높이를 사용 */
  }
  
  .main-content {
    flex-grow: 1; /* 메인 콘텐츠가 사이드바 옆에 위치하도록 설정 */
    overflow-y: auto; /* 스크롤 가능 */
    padding: 20px;
    background-color: #121212; /* 일관된 배경색 */
  }
  
  .sidebar {
    width: 250px; /* 사이드바의 고정된 너비 */
    min-width: 250px; /* 최소 너비를 설정하여 크기가 변하지 않도록 */
    background-color: #1e1e1e; /* 사이드바 배경색 */
    height: 100%; /* 화면 전체 높이를 사용 */
    box-shadow: 2px 0 5px rgba(0, 0, 0, 0.1);
  }
  </style>
  