<template>
  <div class="home-container">
    <header class="home-header">
      <h1 class="title">Game & Stream Stats Overview</h1>
      <p class="subtitle">Check out the most popular games on various platforms!</p>
    </header>
    <main class="home-content">
      <div class="stats-tables">
        <!-- Steam Games -->
        <div class="stats-card">
          <h3 class="stats-title">Steam</h3>
          <table class="stats-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Game Name</th>
                <th>Total Hours</th>
                <th>Change</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(game, index) in steamGames" :key="index">
                <td>{{ index + 1 }}</td>
                <td class="game-name">{{ truncateText(game.name) }}</td>
                <td>{{ formatNumber(game.absolute) }}</td>
                
                <td :class="getChangeClass(game.change)">
                  {{ game.change > 0 ? '+' : '' }}{{ formatNumber(game.change) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- YouTube Games -->
        <div class="stats-card">
          <h3 class="stats-title">Trending YouTube Games</h3>
          <table class="stats-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Game Name</th>
                <th>Total Hours</th>
                <th>Change</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(game, index) in youtubeGames" :key="index">
                <td>{{ index + 1 }}</td>
                <td class="game-name">{{ truncateText(game.name) }}</td>
                <td>{{ formatNumber(game.absolute) }}</td>
                <td :class="getChangeClass(game.change)">
                  {{ game.change > 0 ? '+' : '' }}{{ formatNumber(game.change) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Twitch Games -->
        <div class="stats-card">
          <h3 class="stats-title">Trending Twitch Games</h3>
          <table class="stats-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Game Name</th>
                <th>Total Hours</th>
                <th>Change</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(game, index) in twitchGames" :key="index">
                <td>{{ index + 1 }}</td>
                <td class="game-name">{{ truncateText(game.name) }}</td>
                <td>{{ formatNumber(game.absolute) }}</td>
                <td :class="getChangeClass(game.change)">
                  {{ game.change > 0 ? '+' : '' }}{{ formatNumber(game.change) }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </main>
  </div>
</template>

<script>
import axios from "axios";

export default {
  name: "HomeView",
  data() {
    return {
      steamGames: [],
      youtubeGames: [],
      twitchGames: [],
    };
  },
  methods: {
    async fetchData() {
      try {
        const steamResponse = await axios.get("http://43.202.154.13:8000/api/steam/overview");
        this.steamGames = steamResponse.data.result.map((game) => ({
          name: game[0],
          absolute: game[1],
          change: game[2],
        }));


        const youtubeResponse = await axios.get("http://43.202.154.13:8000/api/youtube/overview");
        this.youtubeGames = youtubeResponse.data.result.map((game) => ({
          name: game[0],
          absolute: game[2],
          change: game[3],
        }));

        const twitchResponse = await axios.get("http://43.202.154.13:8000/api/twitch/overview");
        this.twitchGames = twitchResponse.data.result.map((game) => ({
          name: game[0],
          absolute: game[1],
          change: game[2],
        }));
      } catch (error) {
        console.error("Failed to fetch data:", error);
      }
    },
    getChangeClass(change) {
      if (change > 0) {
        return "positive-change"; // 초록색
      } else if (change < 0) {
        return "negative-change"; // 빨간색
      }
      return ""; // 기본값 (색상 없음)
    },
    truncateText(text) {
      return text.length > 10 ? text.slice(0, 10) + "..." : text;
    },
    formatNumber(num) {
      return num.toLocaleString();
    },
  },
  created() {
    this.fetchData();
  },
};
</script>

<style scoped>
.home-container {
  padding: 20px;
  background-color: #121212;
  color: #ffffff;
}

.home-header {
  text-align: center;
  margin-bottom: 30px;
}

.title {
  font-size: 3.5rem;
  font-weight: bold;
}

.subtitle {
  font-size: 1rem;
  color: #cccccc;
}

/* Stats Tables Container */
.stats-tables {
  display: flex;
  justify-content: space-between; /* Spread tables horizontally */
  gap: 20px;
}

.stats-card {
  background-color: #1e1e1e;
  border-radius: 8px;
  padding: 20px;
  width: 32%; /* Three tables evenly distributed */
}

.stats-title {
  font-size: 1.2rem;
  font-weight: bold;
  margin-bottom: 15px;
}

.stats-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 10px;
}

.stats-table th,
.stats-table td {
  text-align: center;
  padding: 10px;
  border-bottom: 1px solid #333;
  white-space: nowrap;
  color: #ffffff;
}

.stats-table td.positive-change {
  color: #4caf50; /* 초록색 */
  font-weight: bold;
}

.stats-table td.negative-change {
  color: #f44336; /* 빨간색 */
  font-weight: bold;
}


.stats-table th {
  background-color: #333;
  cursor: pointer;
}

.stats-table tbody tr:hover {
  background-color: #2a2a2a;
}

.game-name {
  font-weight: bold;
  color: #4caf50;
}
</style>
