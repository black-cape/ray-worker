"""
The config module contains logic for loading, parsing, and formatting faust configuration.
"""
from typing import Dict, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Settings derived from command line, environment variables, .env file or defaults """

    # Consumer group, also used as FAUST APP ID
    # we want this to operate in pub/sub mode so all cast iron worker instance gets a same copy of
    #  config, hence this should be overwritten to be different per worker
    consumer_grp_etl_config = 'etl-config-grp'
    consumer_grp_etl_source_file = 'etl-source-file-grp'

    database_host: str = 'postgres'
    database_password: str = '12345678'  # Default for local debugging
    database_port: int = 5432
    database_user: str = 'castiron'
    database_db: str = 'castiron'

    kafka_broker: str = 'localhost:9092'
    kafka_topic_castiron_etl_config = 'castiron_etl_config'
    kafka_topic_castiron_etl_source_file = 'castiron_etl_source_file'
    kafka_enable_auto_commit: bool = True
    kafka_max_poll_records: int = 50
    kafka_max_poll_interval_ms: int = 600000

    kafka_pizza_tracker_topic: str = 'pizza-tracker'

    minio_etl_bucket: str = 'etl'
    minio_host: str = 'localhost:9000'
    minio_access_key: str = 'castiron'
    minio_secret_key: str = 'castiron'
    minio_secure: bool = False

    minio_notification_arn_etl_config: str = 'arn:minio:sqs::config:kafka'
    minio_notification_arn_etl_source_file: str = 'arn:minio:sqs::source:kafka'
    log_level: str = 'info'

    # Tika service settings
    connection_params: Optional[Dict]
    client_cert: Optional[str]
    client_key: Optional[str]
    enable_tika: bool = False
    tika_host: str = 'localhost:9998'

    num_workers: int = 5
    max_restarts: int = 2
    max_retries: int = 2

    LOCAL_MODE: str = 'Y'
    RAY_HEAD_ADDRESS: str = ''

settings = Settings()
