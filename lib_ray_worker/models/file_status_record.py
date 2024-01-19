from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel
from uuid import UUID, uuid4


class FileStatusRecordBase(SQLModel):
    bucket_name: str
    file_name: str
    original_filename: str
    status: Optional[str] = None
    event_name: Optional[str] = None
    size: Optional[int] = None
    content_type: Optional[str] = None
    minio_metadata: Optional[str] = None
    user_dn: Optional[str] = None


class FileStatusRecord(FileStatusRecordBase, table=True):  # type: ignore
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    created_dt: datetime = Field(
        default=datetime.utcnow(),
        nullable=False,
        schema_extra={"examples": ["2023-08-09T20:26:04.028766"]},
    )
    updated_dt: Optional[datetime] = Field(
        default=datetime.utcnow(),
        nullable=False,
        schema_extra={"examples": ["2023-08-09T20:26:04.028766"]},
    )
