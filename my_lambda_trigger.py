from datetime import datetime
import json
from pprint import pprint
from typing import List
import boto3
import math

ecs_client = boto3.client("ecs")
batch_client = boto3.client("batch")
s3_client = boto3.client("s3")

input_bucket_name = "noaa-gfs-bdp-pds"


def filter_s3_files(bucket: str, date: str) -> List[str]:
    """Filter S3 files based on the specified path format."""
    prefix = f"gfs.{date}/06/atmos/gfs.t06z.pgrb2.0p25.f"
    filtered_files = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for item in page["Contents"]:
                    if not (x := item["Key"]).endswith(".idx"):
                        i_ = int(x[-3:])
                        if i_ % 3 == 0:
                            filtered_files.append(item["Key"])

    except Exception as e:
        print(f"Error retrieving files from bucket {bucket}: {e}")

    return filtered_files


def lambda_handler(event, context):
    date = datetime.now().strftime("%Y%m%d")

    files_to_process = filter_s3_files(input_bucket_name, date)

    print(f"Found {len(files_to_process)} files to process")
    # files_to_process = files_to_process[:5]
    total_files = len(files_to_process)

    # task_count = (len(files_to_process) // 10) + 1
    task_count = 10
    files_per_task = math.ceil(total_files / task_count)

    all_slices = {}
    for i in range(task_count):
        start_index = i * files_per_task
        end_index = min(start_index + files_per_task, total_files)
        file_batch = files_to_process[start_index:end_index]
        all_slices[i] = file_batch

    s3_uri = f"s3://awstests-lavkge/lambda_slices/tasks_{date}.json"
    try:
        response = s3_client.put_object(
            Bucket="awstests-lavkge",
            Key=f"lambda_slices/tasks_{date}.json",
            Body=json.dumps(all_slices),
            ContentType="application/json",  # Optionnel, mais recommandé pour les fichiers JSON
        )

        print("Données envoyées avec succès à S3:", s3_uri)
    except Exception as e:
        print(f"Erreur lors de l'envoi des données à S3 : {str(e)}")
        raise

    container_overrides = {
        "environment": [
            {"name": "INPUT_FILE", "value": s3_uri},
        ]
    }

    # Lancer le job array
    try:
        response = batch_client.submit_job(
            jobName="MyPythonJob",
            jobQueue="arn:aws:batch:us-east-1:604815197344:job-queue/jobQ",
            jobDefinition="arn:aws:batch:us-east-1:604815197344:job-definition/JobDef:5",
            arrayProperties={"size": task_count},
            containerOverrides=container_overrides,
            shareIdentifier="sharegroupjob",
            schedulingPriorityOverride=1,
        )
        print("job started", response)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Job array lancé avec succès", "jobId": response["jobId"]}
            ),
        }

    except Exception as e:
        print(e)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": "Erreur lors du lancement du job array", "error": str(e)}
            ),
        }


# lambda_handler(1, 1)
