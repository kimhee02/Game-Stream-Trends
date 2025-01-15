import { createRouter, createWebHistory } from 'vue-router';
import HomeView from '@/views/HomeView.vue';
import SteamTopratedView from '@/views/SteamTopratedView.vue';
import SteamMostplayedView from '@/views/SteamMostplayedView.vue';
import YoutubeMostlikedView from '@/views/YoutubeMostlikedView.vue';
import YoutubeMostviewedView from '@/views/YoutubeMostviewedView.vue';
import TwitchGamesummaryView from '@/views/TwitchGamesummaryView.vue';
import NotFoundPage from "@/views/404Page.vue";

const routes = [
  {
    path: "/",
    component: () => import("@/components/Layout.vue"),
    children: [
      {
        path: '/home',
        name: 'Home',
        component: HomeView,
      },
      {
        path: '/steam-toprated',
        name: 'SteamToprated',
        component: SteamTopratedView,
      },
      {
        path: '/steam-mostplayed',
        name: 'SteamMostplayed',
        component: SteamMostplayedView,
      },
      {
        path: '/youtube-mostviewed',
        name: 'YoutubeMostviewed',
        component: YoutubeMostviewedView,
      },
      {
        path: '/youtube-mostliked',
        name: 'YoutubeMostliked',
        component: YoutubeMostlikedView,
      },
      {
        path: '/twitch-gamesummary',
        name: 'TwitchGamesummary',
        component: TwitchGamesummaryView,
      },
    ]
  },
  {
    path: "/:pathMatch(.*)*", // 모든 매칭되지 않는 경로
    name: "not-found",
    component: NotFoundPage,
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;
