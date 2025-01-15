import requests
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv  # .env 파일 로드

# .env 파일 로드
load_dotenv()  

# API 키와 엔드포인트 설정
API_KEY = os.getenv("YTB_API_KEY")  # .env에서 API 키 가져오기
URL = "https://www.googleapis.com/youtube/v3/videos"

if not API_KEY:
    raise ValueError("API 키가 .env 파일에 없거나 잘못 설정되었습니다.")

# 요청 파라미터 기본 설정
params = {
    "part": "snippet,statistics",  # 메타데이터와 통계 포함
    "chart": "mostPopular",        # 인기 동영상 가져오기
    "videoCategoryId": "20",       # 게임 카테고리 ID
    "regionCode": "KR",            # 한국 데이터
    "maxResults": 10,              # 가져올 최대 동영상 수
    "key": API_KEY                 # API 키
}

# 로그 설정
log_dir = os.path.join("logs", "youtube")  # logs/youtube 디렉토리 설정
os.makedirs(log_dir, exist_ok=True)  # 디렉토리 생성
current_time = datetime.now().strftime("%Y%m%d_%H%M")
log_file = os.path.join(log_dir, f"youtube_fetch_{current_time}.log")

logging.basicConfig(
    level=logging.INFO,  # 로그 레벨
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),  # 로그 파일 저장
        logging.StreamHandler()  # 콘솔 출력
    ]
)

# 데이터를 가져오는 함수
def fetch_all_pages():
    next_page_token = None  # 초기값 설정
    all_data = []  # 모든 결과를 저장할 리스트

    # 현재 시간 가져오기
    current_time = datetime.now().strftime("%Y%m%d_%H%M")
    
    # 저장 경로 설정 (현재 시간 반영)
    save_dir = os.path.join("data", "raw", "youtube", "video")
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"youtube_dataapi_{current_time}.json")

    logging.info(f"데이터 저장 경로: {save_path}")

    while True:
        try:
            # nextPageToken 추가
            if next_page_token:
                params["pageToken"] = next_page_token

            # API 호출
            response = requests.get(URL, params=params)
            if response.status_code == 200:
                data = response.json()

                # 현재 페이지 데이터 추가
                all_data.extend(data.get("items", []))
                logging.info(f"현재까지 {len(all_data)}개의 데이터를 가져왔습니다.")

                # 다음 페이지 토큰 가져오기
                next_page_token = data.get("nextPageToken", None)

                # 다음 페이지가 없으면 종료
                if not next_page_token:
                    logging.info("모든 페이지 데이터를 가져왔습니다.")
                    break
            else:
                logging.error(f"API 호출 실패: {response.status_code}, 내용: {response.text}")
                break
        except Exception as e:
            logging.exception(f"에러 발생: {e}")
            break

    # 모든 데이터 저장
    with open(save_path, "w", encoding="utf-8") as file:
        json.dump(all_data, file, ensure_ascii=False, indent=2)
    logging.info(f"총 {len(all_data)}개의 데이터가 {save_path}에 저장되었습니다.")

# 함수 실행
if __name__ == "__main__":
    fetch_all_pages()
