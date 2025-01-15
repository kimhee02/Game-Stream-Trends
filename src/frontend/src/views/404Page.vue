<template>
    <div class="not-found">
      <h1 class="error-code">404</h1>
      <p class="error-message">
        Whoops! <br />
        We couldn't find that page.
      </p>
      <router-link to="/home" class="home-link">Main page</router-link>
    </div>
  </template>
  
  <script>
  export default {
    name: "NotFoundPage",
  };
  </script>
  
  <style scoped>
  /* 전체 화면 중앙 정렬 */
  .not-found {
    position: absolute; /* 부모의 영향을 무시하고 위치 고정 */
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    height: auto;
    text-align: center;
    background-color: #121212;
    color: #ffffff;
    width: 100%;
    padding: 0;
    margin: 0;
    box-sizing: border-box;
  }
  
  /* 404 에러 코드 스타일 */
  .error-code {
    font-size: 8rem;
    font-weight: bold;
    margin: 0;
    color: #ffffff;
  }
  
  /* 에러 메시지 스타일 */
  .error-message {
    font-size: 1.5rem;
    margin: 20px 0;
    color: #cccccc;
    line-height: 1.5;
  }
  
  /* 홈 버튼 스타일 */
  .home-link {
    text-decoration: none;
    background-color: #ffffff;
    color: #000000;
    padding: 15px 30px;
    border-radius: 8px;
    font-size: 1rem;
    font-weight: bold;
    transition: background-color 0.3s, color 0.3s;
  }
  
  .home-link:hover {
    background-color: #f5f5f5;
    color: #333333;
  }
  </style>
  