"""
The config module contains logic for loading, parsing, and formatting faust configuration.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings derived from command line, environment variables, .env file or defaults"""

    # Click House file status tracking
    clickhouse_host: str = "UNSET"
    clickhouse_port: int = -1

    # Kafka configs
    kafka_bootstrap_server: str = "UNSET"

    kafka_topic_castiron_etl_source_file: str = "castiron_etl_source_file"
    kafka_topic_castiron_text_payload: str = "castiron_text_payload"
    kafka_topic_castiron_video_payload: str = "castiron_video_payload"

    consumer_grp_etl_source_file: str = "etl-source-file-grp"
    consumer_grp_streaming_text_payload: str = "streaming-text-playload-grp"
    consumer_grp_streaming_video_payload: str = "streaming-video-playload-grp"

    kafka_enable_auto_commit: bool = True
    kafka_max_poll_records: int = 50
    kafka_max_poll_interval_ms: int = 600000
    kafka_max_partition_fetch_bytes: int = 20971520  # 20MB, need to handle video
    kafka_pizza_tracker_topic: str = "pizza-tracker"

    # Minio configs
    minio_etl_bucket: str = "etl"
    minio_host: str = "UNSET"
    minio_port: int = "UNSET"
    minio_root_user: str = "UNSET"
    minio_root_password: str = "UNSET"
    minio_secure: bool = False
    minio_notification_arn_etl_source_file: str = "arn:minio:sqs::source:kafka"

    # Misc configs
    user_system_default_classification: str = "UNCLASSIFIED"
    log_level: str = "info"

    # Ray configs
    # [WS] Cast Iron will create (num_s3_workflow_workers * 2) + 2 actors + num_text_streaming_workers text
    # streaming worker, plus schedule remote tasks up to the CPU limit
    num_s3_workflow_workers: int = 5

    max_restarts: int = 2
    max_retries: int = 2

    LOCAL_MODE: str = "Y"
    RAY_HEAD_ADDRESS: str = "UNSET"


settings = Settings()
