"""Contains the Minio implementation of the object store backend interface"""
import json
from datetime import datetime
from typing import Any, Optional, List

from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import sessionmaker
from lib_ray_worker.config import adapters
from lib_ray_worker.models.file_status_record import FileStatusRecord
from lib_ray_worker.utils import get_logger
from lib_ray_worker.utils.constants import STATUS_QUEUED

LOGGER = get_logger(__name__)


class PostGresWrapper:
    def __init__(self):
        engine = create_async_engine(
            adapters.DATABASE_URI, echo=True, pool_pre_ping=True
        )
        # followed pattern from https://docs.sqlalchemy.org/en/20/_modules/examples/asyncio/async_orm.html
        self.async_session = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

    async def _get_with_session(self, id: str, session: AsyncSession):
        try:
            result = await session.exec(
                select(FileStatusRecord).where(FileStatusRecord.id == id)
            )
            result.one()
        except Exception as e:
            LOGGER.warning("no user found due to %s", e)
            return None

    async def get(self, id: str) -> Optional[FileStatusRecord]:
        LOGGER.info("get called %s", id)
        async with self.async_session() as session:
            async with session.begin():
                return await self._get_with_session(id=id, session=session)

    async def insert(self, file_status_record: FileStatusRecord):
        LOGGER.info("insert called %s", file_status_record)
        async with self.async_session() as session:
            async with session.begin():
                await session.commit()
                await session.refresh(file_status_record)
                return file_status_record

    async def update(self, file_status_record: FileStatusRecord):
        LOGGER.info("upsert called")
        async with self.async_session() as session:
            async with session.begin():
                session.add(file_status_record)
                await session.commit()
                await session.refresh(file_status_record)
                return file_status_record

    async def update_by_id(
        self,
        id: str,
        status: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        LOGGER.info("upate by ID called")
        async with self.async_session() as session:
            async with session.begin():
                if file_status_record := await self._get_with_session(
                    id=id, session=session
                ):
                    file_status_record.status = status
                    file_status_record.file_name = file_name
                    session.add(file_status_record)
                    await session.commit()
                    await session.refresh(file_status_record)
                    return file_status_record

    async def delete_by_id(self, id: str) -> None:
        LOGGER.info("purge record called")
        async with self.async_session() as session:
            async with session.begin():
                if file_status_record := await self._get_with_session(
                    id=id, session=session
                ):
                    await session.delete(file_status_record)

    async def query(self, status: Optional[str] = None) -> List[FileStatusRecord]:
        LOGGER.info("query called")
        statement = select(FileStatusRecord)
        if status:
            statement = statement.where(FileStatusRecord.status == status)

        statement = statement.order_by(FileStatusRecord.created_dt.desc())
        async with self.async_session() as session:
            async with session.begin():
                result = await session.exec(statement=statement)
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
