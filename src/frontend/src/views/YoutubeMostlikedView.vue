<template>
  <div class="youtube-container">
    <header class="youtube-header">
      <h1 class="title">Youtube Most Liked Videos</h1>
      <p class="subtitle">Explore the latest trends in Youtube</p>
    </header>
    <main class="youtube-content">
      <table class="stats-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Preview</th>
            <th @click="sortTable('videoName')">
              Video Name
              <span class="sort-icon" :class="getSortIcon('videoName')"></span>
            </th>
            <th @click="sortTable('channelName')">
              Channel Name
              <span class="sort-icon" :class="getSortIcon('channelName')"></span>
            </th>
            <th @click="sortTable('curLike')">
              Like Counts
              <span class="sort-icon" :class="getSortIcon('curLike')"></span>
            </th>
            <th @click="sortTable('changeLike')">
              Increase
              <span class="sort-icon" :class="getSortIcon('changeLike')"></span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(game, index) in sortedGames" :key="game.rank">
            <td>{{ index + 1 }}</td>
            <td>
              <a
                :href="'https://www.youtube.com/shorts/' + game.videoId"
                target="_blank"
                rel="noopener noreferrer"
              >
                <img
                  :src="'https://img.youtube.com/vi/' + game.videoId + '/hqdefault.jpg'"
                  alt="Video Preview"
                  class="preview-image"
                />
              </a>
            </td>
            <td class="video-name">
              <a
                :href="'https://www.youtube.com/shorts/' + game.videoId"
                target="_blank"
                rel="noopener noreferrer"
                v-html="truncateText(game.videoName, 30)"
              ></a>
            </td>
            <td class="channel-name">{{ game.channelName }}</td>
            <td class="cur">
              {{ formatNumber(game.curLike) }}
              <br>
              <span class="change-positive">(+{{ formatNumber(game.changeLike) }})</span>
            </td>
            <td class="ratio">
              <span :class="getRatioClass(game.ratioLike)">
                {{ game.ratioLike }}%
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
  name: "YoutubeMostlikedView",
  data() {
    return {
      games: [], // Original games data
      sortKey: "curLike", // Default column for sorting
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
        const response = await axios.get("http://43.202.154.13:8000/api/youtube/like");
        const rawData = response.data.result;

        this.games = rawData.map((game, index) => ({
          rank: index + 1,
          videoId: game[0],
          videoName: game[1],
          channelName: game[2],
          curLike: game[3],
          changeLike: parseInt(game[5].replace(/[+,]/g, ""), 10),
          ratioLike: parseInt(game[6]) === 0 ? 100 : parseInt(game[6]),
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
    truncateText(text, maxLength) {
      return text.replace(new RegExp(`(.{${maxLength}})`, 'g'), '$1<br>');
    },
    formatNumber(num) {
      if (num === null || num === undefined) {
        return "0"; // Null or undefined 값을 0으로 처리
      }
      return num.toLocaleString();
    },
    ratioAdj(num) {
      if (num == 0) num += 100;
      return num
    },
    getRatioClass(value) {
      const ratio = value
      if (ratio > 100) {
        return "ratio-perfect";
      } else if (ratio > 90) {
        return "ratio-excellent";
      } else if (ratio > 70) {
        return "ratio-good";
      } else if (ratio > 50) {
        return "ratio-average";
      } else if (ratio > 30) {
        return "ratio-below-average";
      } else {
        return "ratio-poor";
      }
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
  width: 180px;
  height: auto;
  border-radius: 5px;
  display: block;
  margin: 0 auto;
}

.video-name {
  font-size: 1.1rem;
  white-space: pre-wrap; /* 줄바꿈 문자(\n) 적용 */
  word-wrap: break-word; /* 긴 단어 줄바꿈 */
  overflow-wrap: break-word; /* CSS3에서 줄바꿈 지원 */
  text-align: left; /* 텍스트 왼쪽 정렬 */
}

.ratio-perfect {
  color: #2196f3;
  font-weight: bold;
}

.change-positive {
  color: #4caf50; /* 초록색 */
  font-weight: bold; /* 강조 */
}

.ratio-excellent {
  color: #4caf50; /* 초록색: 매우 높은 비율 */
  font-weight: bold;
}

.ratio-good {
  color: #8bc34a; /* 연한 초록색: 높은 비율 */
  font-weight: bold;
}

.ratio-average {
  color: #ffc107; /* 노란색: 평균 비율 */
  font-weight: normal;
}

.ratio-below-average {
  color: #ff9800; /* 주황색: 낮은 비율 */
  font-weight: normal;
}

.ratio-poor {
  color: #f44336; /* 빨간색: 매우 낮은 비율 */
  font-weight: bold;
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
</style>