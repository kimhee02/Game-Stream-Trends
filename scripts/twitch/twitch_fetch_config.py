# https://api.twitch.tv/helix/streams  # 시청자 많은 순으로 현재 생방송 목록 표시 (최대 100개)
# https://api.twitch.tv/helix/games/top  # 시청자 많은 카테고리 게임 표시


##### Twitch 요청 제한을 보는 방법 (헤더에 포함돼 있음)
# ratelimit_limit = response.headers.get('Ratelimit-Limit')
# ratelimit_remaining = response.headers.get('Ratelimit-Remaining')
# ratelimit_reset = response.headers.get('Ratelimit-Reset')
# print(f"Ratelimit-Limit: {ratelimit_limit}")
# print(f"Ratelimit-Remaining: {ratelimit_remaining}")
# print(f"Ratelimit-Reset: {ratelimit_reset}")

import json
import io
import logging
import pytz
import boto3
from datetime import datetime
from airflow.models import Variable
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

KST = pytz.timezone('Asia/Seoul')

class Config:
    TWC_CLIENT_ID = Variable.get("TWC_CLIENT_ID")
    TWC_ACCESS_TOKEN = Variable.get("TWC_ACCESS_TOKEN")
    S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    # AWS_CONNECTION_ID = "aws_gureum"

    @staticmethod
    def get_s3_client():
        return boto3.client('s3')
        # hook = AwsBaseHook(aws_conn_id=Config.AWS_CONNECTION_ID)
        # session = hook.get_session()
        # return session.client('s3')
    
    @staticmethod
    def upload_to_s3(data, key):
        try:
            json_data = json.dumps(data, indent=4)
            buffer = io.BytesIO(json_data.encode('utf-8'))
            s3_client = Config.get_s3_client()
            s3_client.upload_fileobj(buffer, Config.S3_BUCKET_NAME, key)
            logging.info(f"Successfully uploaded data to S3 with key: {key}")
        except Exception as e:
            raise RuntimeError(f"Failed to upload to S3 (key: {key}): {e}")

    class S3LogHandler(logging.Handler):
        def __init__(self, bucket_name, date_str, data_type, buffer_size=10):
            super().__init__()
            self.bucket_name = bucket_name
            self.date_str = date_str
            self.data_type = data_type
            self.buffer_size = buffer_size
            self.log_buffer = io.StringIO()
            self.buffer = []
            self.s3_client = Config.get_s3_client()
        
        def emit(self, record):
            msg = self.format(record)
            self.log_buffer.write(msg + "\n")
            self.buffer.append(record)
            if len(self.buffer) >= self.buffer_size:
                self.flush()
                
        def flush(self):
            if len(self.buffer) == 0:
                return
            self.log_buffer.seek(0)

            try:
                timestamp = datetime.now(KST).strftime('%Y-%m-%d_%H-%M-%S')
                s3_key = f"logs/twitch/{self.data_type}/{self.date_str}/fetch_{self.data_type}_{timestamp}.log"
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=self.log_buffer.getvalue().encode('utf-8')
                )
                logging.info(f"Logs uploaded to S3: {s3_key}")
            except Exception as e:
                logging.error(f"Failed to upload logs to S3: {e}")

            self.log_buffer = io.StringIO()
            self.buffer = []

    @staticmethod
    def setup_s3_logging(bucket_name, data_type, buffer_size=1000, log_level=logging.INFO):
        date_str = datetime.now(KST).strftime('%Y-%m-%d')
        log_handler = Config.S3LogHandler(bucket_name, date_str, data_type, buffer_size=buffer_size)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)
        
        logger = logging.getLogger()
        logger.setLevel(log_level)
        logger.addHandler(log_handler)