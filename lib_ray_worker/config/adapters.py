"""
The config module contains logic for loading, parsing, and formatting faust configuration.
"""
from __future__ import annotations

from typing import Optional

from pydantic import PostgresDsn, model_validator

from lib_ray_worker.config._base import Base


class Adapters(Base):
    """Settings derived from command line, environment variables, .env file or defaults"""

    # Postgresql config
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    DATABASE_NAME: str
    DATABASE_URI: Optional[str] = None

    @model_validator(mode="after")
    def assemble_db_connection(self):
        if self.DATABASE_URI is not None:
            return self
        self.DATABASE_URI = str(
            PostgresDsn.build(  # pylint: disable=no-member
                scheme="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=str(self.POSTGRES_HOST),
                port=int(self.POSTGRES_PORT),
                path=f"{self.DATABASE_NAME or ''}",
            )
        )
        return self

    # Kafka configs
    KAFKA_BOOTSTRAP_SERVER: str = "UNSET"

    KAFKA_TOPIC_CASTIRON_ETL_SOURCE_FILE: str = "castiron_etl_source_file"
    CONSUMER_GRP_ETL_SOURCE_FILE: str = "etl-source-file-grp"

    KAFKA_ENABLE_AUTO_COMMIT: bool = True
    KAFKA_MAX_POLL_RECORDS: int = 50
    KAFKA_MAX_POLL_INTERVAL_MS: int = 600000
    KAFKA_MAX_PARTITION_FETCH_BYTES: int = 20971520  # 20MB, need to handle video
    KAFKA_PIZZA_TRACKER_TOPIC: str = "pizza-tracker"

    # Minio configs
    MINIO_ETL_BUCKET: str = "etl"
    MINIO_HOST: str = "UNSET"
    MINIO_PORT: int = "UNSET"
    MINIO_ROOT_USER: str = "UNSET"
    MINIO_ROOT_PASSWORD: str = "UNSET"
    MINIO_SECURE: bool = False
    MINIO_NOTIFICATION_ARN_ETL_SOURCE_FILE: str = "arn:minio:sqs::source:kafka"

    LOG_LEVEL: str = "info"

    # Ray configs
    # [WS] Cast Iron will create (num_s3_workflow_workers * 2) + 2 actors + num_text_streaming_workers text
    # streaming worker, plus schedule remote tasks up to the CPU limit
    NUM_S3_WORKFLOW_WORKERS: int = 5

    MAX_RESTARTS: int = 2
    MAX_RETRIES: int = 2

    LOCAL_MODE: str = "Y"
    RAY_HEAD_ADDRESS: str = "UNSET"


adapters = Adapters()
