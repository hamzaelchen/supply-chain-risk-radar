"""Archivage des fichiers bruts dans MinIO (API S3)."""

from datetime import datetime

import boto3
from botocore.config import Config

from ingestion.config import Settings


class RawStore:
    def __init__(self, settings: Settings):
        self._bucket = settings.bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

    def put(self, key: str, body: bytes, *, source_url: str, fetched_at: datetime, md5: str) -> str:
        """Écrit l'objet (écrasement idempotent : même clé, mêmes octets) et rend son URI.

        L'URL source et l'horodatage voyagent avec l'objet en métadonnées S3 :
        le zip reste traçable même sans le manifeste.
        """
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType="application/zip",
            Metadata={
                "source-url": source_url,
                "fetched-at": fetched_at.isoformat(),
                "md5": md5,
            },
        )
        return f"s3://{self._bucket}/{key}"
