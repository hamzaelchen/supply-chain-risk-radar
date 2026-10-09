"""Paramètres de connexion vus depuis l'hôte, mêmes défauts que docker-compose.yml."""

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    rest_uri: str
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        return cls(
            kafka_bootstrap=f"localhost:{env.get('KAFKA_PORT', '19092')}",
            rest_uri=f"http://localhost:{env.get('ICEBERG_REST_PORT', '18181')}",
            s3_endpoint=f"http://localhost:{env.get('MINIO_API_PORT', '19000')}",
            s3_access_key=env.get("MINIO_ROOT_USER", "minioadmin"),
            s3_secret_key=env.get("MINIO_ROOT_PASSWORD", "minioadmin"),
            bucket=env.get("LAKEHOUSE_BUCKET", "lakehouse"),
        )
