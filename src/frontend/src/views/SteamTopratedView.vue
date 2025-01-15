<template>
  <div class="steam-container">
    <header class="steam-header">
      <h1 class="title">Steam Top Rated Games</h1>
      <p class="subtitle">Explore the latest trends in Game Reviews</p>
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
            <th @click="sortTable('positiveRatio')">
              Positive Review Ratio
              <span class="sort-icon" :class="getSortIcon('positiveRatio')"></span>
            </th>
            <th @click="sortTable('positive')">
              Positives
              <span class="sort-icon" :class="getSortIcon('positive')"></span>
            </th>
            <th @click="sortTable('negative')">
              Negatives
              <span class="sort-icon" :class="getSortIcon('negative')"></span>
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
            <td class="positive-ratio-cell">
              <div class="positive-ratio-wrapper">
                <div class="positive-ratio-container">
                  <div
                    class="positive-ratio-bar"
                    :style="{ 
                      width: game.positiveRatio + '%',
                      backgroundColor: getBarColor(game.positiveRatio),
                    }"
                  ></div>
                </div>
                <span class="positive-ratio-text">{{ game.positiveRatio }}%</span>
              </div>
            </td>
            <td class="positive">
              {{ formatNumber(game.positive) }}
              <span class="getChangeClassForPositive(game.changePositive)">
                ({{ game.changePositive > 0 ? '+' : '' }}{{ game.changePositive }})
              </span>
            </td>
            <td class="negative">
              {{ formatNumber(game.negative) }}
              <span :class="getChangeClassForNegative(game.changeNegative)">
                ({{ game.changeNegative > 0 ? '+' : '' }}{{ game.changeNegative }})
              </span>
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
  name: "SteamTopratedView",
  data() {
    return {
      games: [], // Original games data
      sortKey: "positiveRatio", // Default column for sorting
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
    getBarColor(positiveRatio) {
      if (positiveRatio >= 85) {
        return "#2196f3"; // Blue
      } else if (positiveRatio >= 75) {
        return "#4caf50"; // Green
      } else if (positiveRatio >= 50) {
        return "#ffeb3b"; // Yellow
      } else {
        return "#D0312D"; // Red
      }
    },
    getChangeClassForPositive(change) {
      if (change > 0) {
        return "positive"; // Green
      } else if (change < 0) {
        return "neutral"; // Yellow
      } else {
        return "";
      }
    },
    getChangeClassForNegative(change) {
      if (change > 0) {
        return "negative"; // Red
      } else if (change < 0) {
        return "neutral"; // Yellow
      } else {
        return "";
      }
    },
    async fetchSteamData() {
      try {
        const response = await axios.get("http://43.202.154.13:8000/api/steam/recommend");
        const rawData = response.data.result;

        this.games = rawData.map((game, index) => {
          const imageLink = game[1];
          const gameIdMatch = imageLink.match(/apps\/(\d+)/);
          const gameId = gameIdMatch ? gameIdMatch[1] : null;

          return {
            rank: index + 1,
            gameName: game[0],
            imageLink: imageLink,
            gameId: gameId, // 추출한 gameId 추가
            positiveRatio: parseFloat(game[2]).toFixed(2),
            changePositive: game[3],
            changeNegative: game[4],
            positive: game[5],
            negative: game[6],
          };
        });

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
    formatNumber(num) {
        return num.toLocaleString();
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

.preview-image {
  width: 200px; /* 원하는 너비로 설정 */
  height: auto;
  border-radius: 5px; /* 둥근 모서리 */
  display: block;
  margin: 0 auto;
}

/* Positive Ratio styles */
.positive-ratio-cell {
  vertical-align: middle; /* 테이블 셀 기본 세로 정렬 */
  text-align: center; /* 텍스트 중앙 정렬 */
  padding: 10px 15px; /* 셀 내부 여백 */
}

.positive-ratio-wrapper {
  display: inline-block; /* 블록으로 콘텐츠 정렬 */
  text-align: center;
  width: 100%; /* 너비를 셀에 맞춤 */
}

.positive-ratio-container {
  position: relative;
  width: 80%; /* 바의 너비 조정 */
  height: 10px;
  background-color: #333;
  border-radius: 5px;
  margin: 5px auto; /* 상하 여백 추가 */
  overflow: hidden;
}

.positive-ratio-bar {
  position: absolute;
  height: 100%;
  background-color: #4caf50;
  transition: width 0.3s ease;
  right: 0;
}

.positive-ratio-text {
  text-align: right; /* 텍스트를 오른쪽 정렬 */
  margin-right: 15px;
  font-size: 1.1rem;
  color: #ffffff;
  display: block;
  margin-top: 10px; /* 텍스트와 바 사이 간격 */
}

/* Change classes */
.positive {
  color: #4caf50; /* Green */
}

.negative {
  color: #f44336; /* Red */
}

.neutral {
  color: #ffeb3b; /* Yellow */
}
</style>

