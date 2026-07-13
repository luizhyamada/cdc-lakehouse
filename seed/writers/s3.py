import os
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class S3Writer:
    def __init__(self):
        self.bucket = os.getenv("s3_bucket")
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key")
        )
        self.batch_size = 50
        self.buffers = {}


    def write(self, event):
        entity = event["entity"]

        if entity not in self.buffers:
            self.buffers[entity] = []

        self.buffers[entity].append(event)

        if len(self.buffers[entity]) >= self.batch_size:
            self.flush(entity)

    def flush(self, entity):
        events = self.buffers[entity]

        if not events:
            return

        timestamp = datetime.utcnow()

        key = (
            f"staging/"
            f"{entity}/"
            f"ingestion_date={timestamp.strftime('%Y-%m-%d')}/"
            f"{int(timestamp.timestamp() * 1000)}.jsonl"
        )

        body = "\n".join(
            json.dumps(event)
            for event in events
        )

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body
        )

        print(
            f"Uploaded {len(events)} events "
            f"to s3://{self.bucket}/{key}"
        )

        self.buffers[entity] = []