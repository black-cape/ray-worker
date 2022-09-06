"""Contains the Minio implementation of the object store backend interface"""
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any, Dict, Iterable, Optional, Protocol

from minio import Minio
from minio.commonconfig import REPLACE, CopySource
from minio.notificationconfig import (NotificationConfig, QueueConfig)

from etl.config import settings
from etl.object_store.interfaces import EventType, ObjectEvent, ObjectStore
from etl.object_store.object_id import ObjectId
from etl.util import get_logger

KEEP_FILENAME = '.keep'
LOGGER = get_logger(__name__)


class MinioObjectResponse(Protocol):
    """A duck type interface describing the minio object response"""
    data: bytes

    def close(self):
        """Closes this response object"""


#notification only for ETL TOML config file, as well as for everything
#Note ideally we would have liked to be able to filter non TOML config file via a syntax such as
# Filter exclude suffix .toml.   But this is not supported by S3 https://github.com/minio/minio/issues/8217
#hence the notification with etl_file_upload_notification is going to get everything, and we have to rely on
#application level filtering
notification_configs = NotificationConfig(queue_config_list=[
    QueueConfig(
        settings.minio_notification_arn_etl_source_file,
        ['s3:ObjectCreated:*', 's3:ObjectRemoved:*'],
        config_id='etl_file_upload_notification',
    ),
], )


class MinioObjectStore(ObjectStore):
    """Implements the ObjectStore interface using Minio as the backend service"""

    def __init__(self):
        self._minio_client = Minio(
            f'{settings.minio_host}:{settings.minio_port}',
            access_key=settings.minio_root_user,
            secret_key=settings.minio_root_password,
            secure=settings.minio_secure)

        # Create bucket notification
        if not self._minio_client.bucket_exists(settings.minio_etl_bucket):
            self._minio_client.make_bucket(settings.minio_etl_bucket)

        self._minio_client.set_bucket_notification(settings.minio_etl_bucket,
                                                   notification_configs)

    def download_object(self, src: ObjectId, dest_file: str) -> None:
        self._minio_client.fget_object(src.namespace, src.path, dest_file)

    def upload_object(self, dest: ObjectId, src_file: str,
                      metadata: Optional[Dict]) -> None:
        self._minio_client.fput_object(bucket_name=dest.namespace,
                                       object_name=dest.path,
                                       file_path=src_file,
                                       metadata=metadata)

    def read_object(self, obj: ObjectId) -> bytes:
        response: Optional[MinioObjectResponse] = None
        try:
            response = self._minio_client.get_object(obj.namespace, obj.path)
            if response is None:
                raise KeyError()
            return response.data
        finally:
            if response:
                response.close()

    def write_object(self, obj: ObjectId, data: bytes) -> None:
        self._minio_client.put_object(obj.namespace, obj.path, BytesIO(data),
                                      len(data))

    def move_object(self,
                    src: ObjectId,
                    dest: ObjectId,
                    metadata: Optional[Dict] = None) -> None:
        if metadata:
            self._minio_client.copy_object(
                dest.namespace,
                dest.path,
                CopySource(src.namespace, src.path),
                metadata=metadata,
                metadata_directive=REPLACE,
            )
        else:
            self._minio_client.copy_object(dest.namespace, dest.path,
                                           CopySource(src.namespace, src.path))
        self._minio_client.remove_object(src.namespace, src.path)

    def delete_object(self, obj: ObjectId) -> None:
        self._minio_client.remove_object(obj.namespace, obj.path)

    def ensure_directory_exists(self, directory: ObjectId) -> None:
        if not next(
                self._minio_client.list_objects(directory.namespace,
                                                directory.path), None):
            keep_file_path = str(
                PurePosixPath(directory.path).joinpath(KEEP_FILENAME))
            self.write_object(ObjectId(directory.namespace, keep_file_path),
                              b'')

    def parse_notification(self, evt_data: Any) -> ObjectEvent:
        bucket_name, file_name = evt_data['Key'].split('/', 1)
        object_id = ObjectId(bucket_name, file_name)
        event_type = EventType.Delete if evt_data['EventName'].startswith(
            's3:ObjectRemoved') else EventType.Put
        metadata = evt_data['Records'][0]['s3']['object'].get(
            'userMetadata', None)
        original_filename = None
        event_status = None
        if metadata:
            original_filename = metadata.get('X-Amz-Meta-Originalfilename',
                                             None)
            event_status = metadata.get('X-Amz-Meta-Status', None)

        return ObjectEvent(object_id, event_type, original_filename,
                           event_status)

    def list_objects(self,
                     namespace: str,
                     path: Optional[str] = None,
                     recursive=False) -> Iterable[ObjectId]:
        for minio_object in self._minio_client.list_objects(
                namespace, path, recursive):
            yield ObjectId(minio_object.bucket_name, minio_object.object_name)

    def retrieve_object_metadata(self, src: ObjectId) -> dict:
        return self._minio_client.stat_object(src.namespace, src.path).metadata
