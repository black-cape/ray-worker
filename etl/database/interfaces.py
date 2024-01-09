import abc
from datetime import datetime
from typing import Any, List, Optional
from etl.config import settings
from pydantic import BaseModel, Field

# status that the files go through
STATUS_QUEUED = "Queued"
STATUS_PROCESSING = "Processing"
STATUS_SUCCESS = "Complete"
STATUS_FAILED = "Failed"
STATUS_CANCELED = "Canceled"


class FileObject(BaseModel):
    # keep this in sync with Clickhouse schema used to track file status
    # since we use built in pydantic functions to build query
    id: str
    bucket_name: str
    file_name: str
    status: Optional[str] = None
    original_filename: str
    mission_id: Optional[str] = None
    event_name: Optional[str] = None
    source_ip: Optional[str] = None
    size: Optional[int] = None
    etag: Optional[str] = None
    content_type: Optional[str] = None
    created_dt: datetime
    updated_dt: Optional[datetime] = None
    metadata: Optional[str] = None
    user_dn: Optional[str] = None
    classification: str = settings.user_system_default_classification
    owner_producer: Optional[str] = None
    sci_controls: List[str] = Field(default_factory=list, logical_and=True)
    sar_identifier: List[str] = Field(default_factory=list, logical_and=True)
    atomic_energy_control: Optional[str] = None
    dissemination_controls: List[str] = Field(default_factory=list)
    fgi_source_open: Optional[str] = None
    fgi_source_protected: Optional[str] = None
    releasable_to: List[str] = Field(default_factory=list)
    non_ic_markings: List[str] = Field(default_factory=list)


class DatabaseStore(abc.ABC):
    """Interface for message producer backend"""

    async def insert_file(self, filedata: FileObject) -> None:
        raise NotImplementedError

    async def update_status_by_fileName(self, filename: str, new_status: str) -> None:
        # Update a file record status by filename
        raise NotImplementedError

    async def update_status_and_fileName(
        self, rowid: str, new_status: str, new_filename: str
    ) -> None:
        # Update a file record status and file name
        raise NotImplementedError

    async def delete_file(self, rowid: str) -> None:
        """Delete a record
        :param rowid: The data base id
        """
        raise NotImplementedError

    async def query(self, status: Optional[str] = None) -> List[FileObject]:
        """Retrieve records based on query params"""
        raise NotImplementedError

    def parse_notification(self, evt_data: Any) -> dict:
        """Parse the event into a DB row/dict
        :param evt_data: The event data from S3/Minio
        """
        raise NotImplementedError
