#!/bin/bash

# 이동
cd gst-back

# 최신 코드 업데이트
git pull origin feature/web-back

# 백엔드 디렉토리로 이동
cd /home/ubuntu/gst-back/src/backend

# 가상 환경 활성화
source fastapi/bin/activate

# 패키지 설치
pip install -r requirements.txt

# uvicorn 프로세스 확인 및 종료
pkill -f "uvicorn main:app --host 0.0.0.0 --port 8000"

# uvicorn 실행 (백그라운드)
echo "Uvicorn 백그라운드에서 재시작 중..."
nohup uvicorn main:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
echo "Uvicorn 백그라운드 실행 완료."
