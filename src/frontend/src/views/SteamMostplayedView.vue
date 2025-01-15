<template>
  <div class="steam-container">
    <header class="steam-header">
      <h1 class="title">Steam Most Played Games</h1>
      <p class="subtitle">Explore the latest trends in Current Players</p>
    </header>
    <main class="steam-content">
      <table class="stats-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th> </th>
            <th @click="sortTable('gameName')">
              Game Name
              <span class="sort-icon" :class="getSortIcon('gameName')"></span>
            </th>
            <th @click="sortTable('players')">
              Players
              <span class="sort-icon" :class="getSortIcon('players')"></span>
            </th>
            <th @click="sortTable('playerChange')">
              Player Change
              <span class="sort-icon" :class="getSortIcon('playerChange')"></span>
            </th>
            <th @click="sortTable('popularityScore')">
              Game-Quality Score
              <span class="sort-icon" :class="getSortIcon('popularityScore')"></span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(game, index) in sortedGames" :key="game.rank">
            <td>{{ index + 1 }}</td>
            <td>
              <a
                :href="'https://store.steampowered.com/app/' + game.gameId"
                target="_blank"
                rel="noopener noreferrer"
              >
                <img :src="game.imageLink" class="preview-image" alt="Game Preview" />
              </a>
            </td>
            <td class="game-name">{{ game.gameName }}</td>
            <td class="players">{{ formatNumber(game.players) }}</td>
            <td :class="getChangeClass(game.playerChange)">
              {{ game.playerChange > 0 ? '+' : '' }}{{ formatNumber(game.playerChange) }}
            </td>
            <td class="popularity-score">
              {{ game.popularityScore }} {{ getPopularityEmoji(game.popularityScore) }}
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
  name: "SteamMostplayedView",
  data() {
    return {
      games: [], // Original games data
      sortKey: "players", // Default column for sorting
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
    async fetchSteamData() {
      try {
        const response = await axios.get("http://43.202.154.13:8000/api/steam/popular");
        const rawData = response.data.result;

        this.games = rawData.map((game, index) => ({
          rank: index + 1,
          gameName: game[0],
          gameId: game[1],
          imageLink: game[2],
          players: game[3],
          playerChange: game[4],
          popularityScore: game[5],
        }));
      } catch (error) {
        console.error("Failed to fetch data:", error);
      }
    },
    sortTable(column) {
      if (this.sortKey === column) {
        this.sortOrder *= -1; // Reverse sort order
      } else {
        this.sortKey = column;
        this.sortOrder = 1; // Default to ascending
      }
    },
    getSortIcon(column) {
      if (this.sortKey === column) {
        return this.sortOrder === 1 ? "sort-asc" : "sort-desc";
      }
      return "sort-default";
    },
    getChangeClass(change) {
      return change > 0 ? "positive" : "negative";
    },
    formatNumber(num) {
      // Format number with commas
      return num.toLocaleString();
    },
    getPopularityEmoji(score) {
      if (score >= 9) {
        return "😎"; // High popularity
      } else if (score >= 7) {
        return "😍"; // Medium popularity
      } else if (score >= 4) {
        return "😀"; // Low-medium popularity
      } else {
        return "😢"; // Low popularity
      }
    },
  },
  created() {
    this.fetchSteamData();
  },
};
</script>

<style scoped>
.title {
  font-size: 4rem;
}

/* Table styles */
.stats-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
}

.stats-table th,
.stats-table td {
  text-align: center;
  padding: 10px 15px;
  border-bottom: 1px solid #333;
  white-space: nowrap;
  vertical-align: middle;
}

.stats-table th {
  cursor: pointer;
}

.stats-table th:hover {
  background-color: #1e1e1e;
}

.stats-table tbody tr:hover {
  background-color: #2a2a2a;
}

.preview-image {
  width: 200px; /* 원하는 너비로 설정 */
  height: auto;
  border-radius: 5px; /* 둥근 모서리 */
  display: block;
  margin: 0 auto;
}

/* Sort icons */
.sort-icon {
  margin-left: 8px; /* Adds spacing between column name and icon */
  font-size: 0.8rem;
  vertical-align: middle;
  display: inline-block; /* Ensures proper alignment */
}

.sort-asc::after {
  content: "▲";
  color: #ffffff;
}

.sort-desc::after {
  content: "▼";
  color: #ffffff;
}

.sort-default::after {
  content: "⇅";
  color: #666666;
}

/* Player Change styles */
.positive {
  color: #4caf50; /* Green for positive change */
}

.negative {
  color: #f44336; /* Red for negative change */
}

/* Popularity Score styles */
.popularity-score {
  text-align: center; /* 텍스트 중앙 정렬 */
  line-height: 1.5; /* 줄 간격 */
}

.popularity-score span {
  display: block; /* 텍스트와 이모지를 별도 블록으로 분리 */
}
</style>