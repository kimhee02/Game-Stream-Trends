import boto3
import requests
import json
import os
from datetime import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import pytz

KST = pytz.timezone('Asia/Seoul')
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
hook = AwsBaseHook(aws_conn_id="aws_gureum")
session = hook.get_session()
s3 = session.client('s3')

def fetch_youtube_data():
    API_KEY = Variable.get('YTB_API_KEY')
    S3_PREFIX = "data/raw/youtube"



    URL = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "chart": "mostPopular",
        "videoCategoryId": "20",
        "regionCode": "KR",
        "maxResults": 10,
        "key": API_KEY,
    }

    next_page_token = None
    all_data = []

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token

        response = requests.get(URL, params=params)
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data.get("items", []))
            next_page_token = data.get("nextPageToken", None)
            if not next_page_token:
                break
        else:
            raise Exception(f"API 호출 실패: {response.status_code}, {response.text}")

    current_time = datetime.now(KST)
    date_path = current_time.strftime("%Y-%m-%d")
    hour_path = current_time.strftime("%H")
    timestamp = current_time.strftime('%Y-%m-%d_%H-%M-%S')
    s3_key = f"{S3_PREFIX}/videos/{date_path}/{hour_path}/youtube_videos_{timestamp}.json"

    s3 = boto3.client('s3')

    try:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(all_data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        print(f"데이터가 S3에 저장되었습니다: s3://{S3_BUCKET_NAME}/{s3_key}")
    except Exception as e:
        raise Exception(f"S3 업로드 실패: {e}")
