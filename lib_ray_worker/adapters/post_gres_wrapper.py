"""Contains the Minio implementation of the object store backend interface"""
import json
from datetime import datetime
from typing import Any, Optional, List

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from lib_ray_worker.models.file_status_record import FileStatusRecord
from lib_ray_worker.utils import get_logger
from lib_ray_worker.utils.constants import STATUS_QUEUED

LOGGER = get_logger(__name__)


class PostGresWrapper:
    def __init__(self):
        pass

    async def get(self, session: AsyncSession, id: str) -> Optional[FileStatusRecord]:
        LOGGER.info("get called %s", id)
        statement = select(FileStatusRecord).where(FileStatusRecord.id == id)
        try:
            result = await session.exec(statement)
            return result.one()
        except Exception as e:
            LOGGER.warning("no user found due to %s", e)
            return None

    async def insert(self, session: AsyncSession, file_status_record: FileStatusRecord):
        LOGGER.info("insert called %s", file_status_record)
        session.add(file_status_record)
        await session.commit()
        await session.refresh(file_status_record)
        return file_status_record

    async def update(self, session: AsyncSession, file_status_record: FileStatusRecord):
        LOGGER.info("upsert called")
        session.add(file_status_record)
        await session.commit()
        await session.refresh(file_status_record)
        return file_status_record

    async def update_by_id(
        self,
        id: str,
        session: AsyncSession,
        status: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        LOGGER.info("upate by ID called")
        file_status_record: Optional[FileStatusRecord] = await self.get(
            id=id, session=session
        )
        if file_status_record:
            file_status_record.status = status
            file_status_record.file_name = file_name
            session.add(file_status_record)
            await session.commit()
            await session.refresh(file_status_record)
            return file_status_record

    async def purge(
        self, session: AsyncSession, file_status_record: FileStatusRecord
    ) -> None:
        LOGGER.info("purge record called")
        session.delete(file_status_record)

    async def query(
        self, session: AsyncSession, status: Optional[str] = None
    ) -> List[FileStatusRecord]:
        LOGGER.info("query called")
        statement = select(FileStatusRecord)
        if status:
            statement = statement.where(FileStatusRecord.status == status)

        statement = statement.order_by(FileStatusRecord.created_dt.desc())

        result = session.exec(statement)
        return result.all()


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
