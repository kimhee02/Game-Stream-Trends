<template>
  <div class="twitch-container">
    <header class="twitch-header">
      <h1 class="title">Twitch Game Summary</h1>
      <p class="subtitle">Explore the latest game trends on Twitch</p>
    </header>
    <main class="twitch-content">
      <table class="stats-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Game Name</th>
            <th>Total Viewers</th>
            <th>Total Streams</th>
            <th>Top Streamer</th>
            <th>Viewer Increase</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(game, index) in sortedGames" :key="game.rank">
            <td>{{ index + 1 }}</td>
            <td class="game-name">{{ game.gameName }}</td>
            <td class="total-viewers">{{ formatNumber(game.totalViewers) }}</td>
            <td class="total-streams">{{ formatNumber(game.totalStreams) }}</td>
            <td class="top-streamer">{{ game.topStreamer }}</td>
            <td :class="getViewerChangeClass(game.viewerIncrease)">
              {{ formatNumber(game.viewerIncrease) }}
            </td>
          </tr>
        </tbody>
      </table>
    </main>
  </div>
</template>

<script>
import axios from "axios";

export default {
  name: "TwitchGamesummaryView",
  data() {
    return {
      games: [], // Original games data
      sortKey: "totalViewers", // Default column for sorting
      sortOrder: -1, // Default sort order (-1 for descending)
    };
  },
  computed: {
    sortedGames() {
      if (this.sortKey) {
        return [...this.games].sort((a, b) => {
          if (typeof a[this.sortKey] === "string") {
            return (
              this.sortOrder *
              a[this.sortKey].localeCompare(b[this.sortKey], undefined, {
                numeric: true,
              })
            );
          } else {
            return this.sortOrder * (a[this.sortKey] - b[this.sortKey]);
          }
        });
      }
      return this.games;
    },
  },
  methods: {
    async fetchData() {
      try {
        const response = await axios.get("http://43.202.154.13:8000/api/twitch/game");
        const rawData = response.data.result;

        this.games = rawData.map((game, index) => ({
          rank: index + 1,
          gameName: game[0],
          image: game[5], // Assuming the API provides a game thumbnail URL
          totalViewers: game[3],
          totalStreams: game[2],
          topStreamer: game[1],
          viewerIncrease: game[4],
        }));
      } catch (error) {
        console.error("Failed to fetch data:", error);
      }
    },
    getViewerChangeClass(change) {
      if (change > 0) {
        return "viewer-positive";
      } else if (change < 0) {
        return "viewer-negative";
      } else {
        return "viewer-neutral";
      }
    },
    formatNumber(num) {
      return num ? num.toLocaleString() : "0";
    },
  },
  created() {
    this.fetchData();
  },
};
</script>

<style scoped>
.title {
  font-size: 4rem;
}

.subtitle {
  font-size: 1rem;
}

/* Table Styles */
.stats-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  color: #e5e5e5;
}

.stats-table th,
.stats-table td {
  text-align: center;
  padding: 15px;
  border-bottom: 1px solid #444;
}

.stats-table th {
  font-weight: bold;
}

.stats-table tbody tr:hover {
  background-color: #333;
}

/* Game Info Styles */
.game-info {
  display: flex;
  align-items: center;
  gap: 10px;
}

.game-thumbnail {
  width: 40px;
  height: 40px;
  border-radius: 5px;
}

/* Viewer Change Classes */
.viewer-positive {
  color: #4caf50;
}

.viewer-negative {
  color: #f44336;
}

.viewer-neutral {
  color: #ffeb3b;
}
</style>