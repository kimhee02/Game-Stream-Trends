import aiohttp
import asyncio
import json
import os
from datetime import datetime
import logging
import re
from dotenv import load_dotenv  # .env 파일 로드

# .env 파일 로드
load_dotenv()  


# RAWG API 키
API_KEY = os.getenv("RAWG_API_KEY1")
BASE_URL = "https://api.rawg.io/api/games"

# 로그 설정
logs_dir = os.path.join("logs", "rawg")  # logs/rawg 디렉토리 설정
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, f"games_per50_{datetime.now().strftime('%Y%m%d_%H%M')}.log")

# 로그 설정 - 터미널과 파일에 동시 출력
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(stream_handler)

# 마지막 저장된 페이지를 찾는 함수
def get_last_saved_page(output_dir):
    try:
        files = os.listdir(output_dir)
        json_files = [f for f in files if f.endswith(".json")]
        if not json_files:
            return 1

        page_numbers = []
        for file in json_files:
            match = re.search(r"games_per50_(\d+)_(\d+)\.json", file)
            if match:
                page_numbers.append(int(match.group(2)))
        return max(page_numbers) + 1
    except Exception as e:
        logger.error(f"마지막 저장된 페이지를 찾는 중 오류 발생: {e}")
        return 1

# 비동기 API 요청 함수
async def fetch_page(session, page, page_size, retries=10, wait_time=300):
    """
    502 발생 시 고정된 대기 시간(5분) 후 재시도
    - retries: 최대 재시도 횟수
    - wait_time: 재시도 간격(초)
    """
    params = {
        "key": API_KEY,
        "page_size": page_size,
        "page": page
    }
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(BASE_URL, params=params) as response:
                if response.status == 200:
                    games_data = await response.json()
                    return games_data.get("results", []), games_data.get("next")
                elif response.status == 502:
                    logger.warning(f"502 Bad Gateway 발생 - {page} 페이지 요청 재시도 중...")
                    logger.info(f"{wait_time // 60}분 대기 후 다시 시도합니다.")
                    await asyncio.sleep(wait_time)  # 5분 대기
                elif response.status in [401, 429]:
                    logger.error(f"API 요청 실패: {response.status} - {await response.text()}")
                    return [], None
                else:
                    logger.error(f"오류 발생: {response.status} - {await response.text()}")
                    return [], None
        except Exception as e:
            logger.error(f"페이지 {page} 요청 중 오류: {e}")

        attempt += 1
        logger.info(f"재시도 {attempt}/{retries} - {page} 페이지 요청...")

    logger.error(f"{page} 페이지 요청이 502 오류로 인해 {retries}번 재시도 후 실패했습니다.")
    return [], None

# 50페이지 단위로 데이터를 저장하는 함수
async def fetch_and_save_pages(start_page, page_size, output_dir, max_calls=20000):
    all_results = []
    page = start_page
    call_count = 0

    async with aiohttp.ClientSession() as session:
        while call_count < max_calls:
            results, next_page = await fetch_page(session, page, page_size)
            call_count += 1

            if results:
                all_results.extend(results)
                logger.info(f"{page} 페이지에서 {len(results)}개의 데이터를 가져왔습니다.")

                if page % 50 == 0:
                    file_name = f"games_per50_{page-49}_{page}.json"
                    file_path = os.path.join(output_dir, file_name)

                    with open(file_path, "w", encoding="utf-8") as file:
                        json.dump(all_results, file, indent=4, ensure_ascii=False)

                    logger.info(f"{page-49}~{page} 페이지 데이터 저장 완료: {file_path}")
                    all_results = []

            if not next_page:
                logger.info("더 이상 데이터가 없습니다. 페이지네이션을 종료합니다.")
                break

            page += 1

        if all_results:
            file_name = f"games_per50_{page-((page-1)%50)}_{page}.json"
            file_path = os.path.join(output_dir, file_name)

            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(all_results, file, indent=4, ensure_ascii=False)

            logger.info(f"마지막 {page-((page-1)%50)}~{page} 페이지 데이터 저장 완료: {file_path}")

    logger.info(f"총 {call_count}번의 API 호출이 완료되었습니다.")

# 전체 데이터 가져오기 함수
async def fetch_all_data(page_size):
    # JSON 파일 저장 경로를 data/raw/rawg로 설정
    output_dir = os.path.join("data", "raw", "rawg")
    os.makedirs(output_dir, exist_ok=True)

    start_page = get_last_saved_page(output_dir)
    logger.info(f"시작 페이지: {start_page}")
    await fetch_and_save_pages(start_page, page_size, output_dir)

# 실행
if __name__ == "__main__":
    page_size = 40
    asyncio.run(fetch_all_data(page_size))
    logger.info("모든 데이터 가져오기 및 저장 완료.")
