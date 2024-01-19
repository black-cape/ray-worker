"""Contains the Minio implementation of the object store backend interface"""
import json
from datetime import datetime
from typing import Any, Optional, List
from lib_ray_worker.utils.constants import STATUS_QUEUED
from lib_ray_worker.utils import get_logger
from lib_ray_worker.models.file_status_record import FileStatusRecord

LOGGER = get_logger(__name__)


class PostGresWrapper:
    def __init__(self):
        pass

    async def get(self, id: str) -> Optional[FileStatusRecord]:
        LOGGER.info("get called")
        return None

    async def insert(self, filedata: FileStatusRecord):
        LOGGER.info("upsert called")

    async def update(
        self, id: str, status: Optional[str] = None, file_name: Optional[str] = None
    ):
        file_status_record: Optional[FileStatusRecord] = await self.get(id=id)
        LOGGER.info("upsert called")

    async def delete(self, id: str) -> None:
        LOGGER.info("delete called")
        pass

    async def query(self, status: Optional[str] = None) -> List[FileStatusRecord]:
        LOGGER.info("query called")
        return []

    def parse_notification(self, evt_data: Any) -> FileStatusRecord:
        """Parse a Minio notification to create a DB row"""
        bucket_name, file_name = evt_data["Key"].split("/", 1)
        metadata = evt_data["Records"][0]["s3"]["object"].get("userMetadata", {})

        return FileStatusRecord(
            id=metadata.get("X-Amz-Meta-Id", None),
            bucket_name=bucket_name,
            file_name=file_name,
            status=STATUS_QUEUED,  # everything starts out queued
            original_filename=metadata.get("X-Amz-Meta-Originalfilename", None),
            event_name=evt_data["EventName"],
            size=evt_data["Records"][0]["s3"]["object"]["size"],
            content_type=evt_data["Records"][0]["s3"]["object"]["contentType"],
            created_dt=datetime.now(),
            updated_dt=datetime.now(),
            metadata=json.dumps(metadata),
            user_dn=metadata.get("X-Amz-Meta-Owner_dn", None),
        )
