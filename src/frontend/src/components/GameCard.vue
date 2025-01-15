<template>
    <div class="game-card">
      <img :src="image" :alt="title" class="game-image" />
      <div class="game-info">
        <h2 class="game-title">{{ title }}</h2>
        <div class="game-likes">
          <span class="like-icon">+</span>
          <span class="like-count">{{ likes }}</span>
        </div>
      </div>
    </div>
  </template>
  
  <script>
  export default {
    name: "GameCard",
    props: {
      title: {
        type: String,
        required: true,
      },
      image: {
        type: String,
        required: true,
      },
      likes: {
        type: Number,
        required: true,
      },
    },
  };
  </script>
  
  <style scoped>
  /* 게임 카드 스타일 */
  .game-card {
    background-color: #1e1e1e;
    border-radius: 12px;
    padding: 15px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    text-align: left;
    gap: 10px;
    box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.2);
  }
  
  .game-image {
    width: 100%;
    height: auto;
    border-radius: 8px;
    margin-bottom: 10px;
  }
  
  .game-info {
    display: flex;
    flex-direction: column;
    gap: 10px;
    width: 100%;
  }
  
  .game-title {
    font-size: 1.2rem;
    font-weight: bold;
    color: #ffffff;
    margin: 0;
    display: flex;
    align-items: center;
    gap: 5px;
  }
  
  .game-likes {
    display: flex;
    align-items: center;
    gap: 5px;
    background-color: #333;
    border-radius: 8px;
    padding: 5px 10px;
    font-size: 0.9rem;
    color: #fff;
  }
  
  .like-icon {
    font-weight: bold;
    color: #fff;
  }
  
  .like-count {
    font-size: 0.9rem;
    color: #fff;
  }
  </style>
  