import boto3
import os

ssm_client = boto3.client('ssm')

def lambda_handler(event, context):
    # 이벤트 디버깅 로그
    print("Received event:", event)
    
    # 동기화 명령어
    command = "bash /home/airflow/sync_dags.sh"
    
    # 동기화할 EC2 인스턴스 ID 리스트
    instance_ids = [
        "i-0c013bb25f254a1a7",  # Airflow Scheduler
        "i-0202170b37c3d6ec4",  # Airflow Worker
        "i-07637c85280d97511"   # Airflow Webserver
    ]
    
    # SSM 명령 실행
    responses = []
    for instance_id in instance_ids:
        try:
            # SSM 명령 실행
            response = ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={'commands': [command]},
            )
            # 명령 ID 저장
            command_id = response['Command']['CommandId']
            responses.append({
                "InstanceId": instance_id,
                "CommandId": command_id,
            })
            print(f"Command sent to {instance_id}: {command_id}")
        except Exception as e:
            print(f"Failed to send command to {instance_id}: {e}")
            responses.append({
                "InstanceId": instance_id,
                "Error": str(e)
            })
    
    return {
        "statusCode": 200,
        "body": responses
    }
