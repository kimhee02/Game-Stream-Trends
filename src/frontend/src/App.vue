<template>
  <div id="app">
    <router-view />
  </div>
</template>

<script>
export default {
  name: 'App',
};
</script>

<style>
@import '@/assets/css/main.css';

#app {
  display: flex;
  flex-direction: row;
  height: 100vh;
}
</style>
