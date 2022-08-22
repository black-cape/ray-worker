"""Contains the Minio implementation of the object store backend interface"""
import json
from datetime import datetime
from typing import Any, Optional, List

from aioch import Client

from etl.config import settings
from etl.database.interfaces import DatabaseStore, FileObject
from etl.util import get_logger

LOGGER = get_logger(__name__)


class ClickHouseDatabase(DatabaseStore):
    def __init__(self):
        self.client = Client(host=settings.clickhouse_host, port=settings.clickhouse_port, database='rubicon')

    async def insert_file(self, filedata: FileObject):
        """ Track a new file from Minio"""
        try:
            sql = f'INSERT INTO cast_iron_file_status ({",".join(FileObject.__fields__.keys())}) VALUES'

            print('YOYO 2')
            print(sql)
            print(filedata.dict())

            await self.client.execute(sql, [filedata.dict()])

        except Exception as exc:
            LOGGER.error(f'unexpected error occured inserting a record in file tracking table {exc}')

    async def update_status_by_fileName(self, filename: str, new_status: str):
        """ Update the file status/state """
        try:
            dt_seconds_since_epoch = round(datetime.utcnow().timestamp())
            sql = f"ALTER TABLE cast_iron_file_status UPDATE status='{new_status}', " \
                  f" updated_dt = {dt_seconds_since_epoch} WHERE file_name = '{filename}'  "
            await self.client.execute(sql)
        except Exception as exc:
            LOGGER.error(f'unexpected error occured updating a record in file tracking table {exc}')

    async def update_status_and_fileName(self, rowid: str, new_status: str, new_filename: str):
        """ Update the file status/state and the file name """
        try:
            dt_seconds_since_epoch = round(datetime.utcnow().timestamp())
            sql = f"ALTER TABLE cast_iron_file_status UPDATE status='{new_status}', " \
                  f" file_name='{new_filename}', updated_dt = {dt_seconds_since_epoch} WHERE id = '{rowid}'  "
            await self.client.execute(sql)
        except Exception as exc:
            LOGGER.error(f'unexpected error occured updating a record in file tracking table {exc}')

    async def delete_file(self, rowid: str) -> None:
        try:
            sql = f"ALTER TABLE cast_iron_file_status DELETE WHERE id = '{rowid}' "
            await self.client.execute(sql)
        except Exception as exc:
            LOGGER.error(f'unexpected error occured deleting a record {exc}')

    async def query(self, status: Optional[str] = None) -> List[FileObject]:
        sql = f"SELECT * FROM cast_iron_file_status WHERE 1 = 1 "
        if status:
            sql = f"{sql} AND status = '{status}' "

        try:
            rep = await self.client.execute(sql, with_column_types=True)

            col_names = [t[0] for t in rep[1]]
            file_objs: List[FileObject] = []

            # rep[0] is list of tuples
            for db_record_tup in rep[0]:
                file_obj_dict = {}
                for index, val in enumerate(db_record_tup):
                    file_obj_dict[col_names[index]] = val
                file_objs.append(FileObject(**file_obj_dict))

            return file_objs
        except Exception as exc:
            LOGGER.error(f'unexpected error occured querying file tracking table {exc}')

    def parse_notification(self, evt_data: Any) -> FileObject:
        """ Parse a Minio notification to create a DB row """
        bucket_name, file_name = evt_data['Key'].split('/', 1)
        metadata = evt_data['Records'][0]['s3']['object'].get('userMetadata', {})

        classification_meta_obj_minio = json.loads(metadata['X-Amz-Meta-Classification']) \
            if metadata.get('X-Amz-Meta-Classification', None) else {}

        ob_evt = FileObject(
            id=metadata.get('X-Amz-Meta-Id', None),
            bucket_name=bucket_name,
            file_name=file_name,
            status='Queued',
            original_filename=metadata.get('X-Amz-Meta-Originalfilename', None) if metadata else None,
            event_name=evt_data['EventName'],
            source_ip=evt_data['Records'][0]['requestParameters']['sourceIPAddress'],
            size=evt_data['Records'][0]['s3']['object']['size'],
            etag=evt_data['Records'][0]['s3']['object']['eTag'],
            content_type=evt_data['Records'][0]['s3']['object']['contentType'],
            created_dt=datetime.now(),
            updated_dt=datetime.now(),
            metadata=json.dumps(metadata),
            user_dn=metadata.get('X-Amz-Owner_dn', None),
            classification=classification_meta_obj_minio.get('classification', settings.user_system_default_classification),
            owner_producer=classification_meta_obj_minio.get('owner_producer', None),
            sci_controls=classification_meta_obj_minio.get('sci_controls', []),
            sar_identifier=classification_meta_obj_minio.get('sar_identifier', []),
            atomic_energy_control=classification_meta_obj_minio.get('atomic_energy_control', None),
            dissemination_controls=classification_meta_obj_minio.get('dissemination_controls', []),
            fgi_source_open=classification_meta_obj_minio.get('fgi_source_open', None),
            fgi_source_protected=classification_meta_obj_minio.get('fgi_source_protected', None),
            releasable_to=classification_meta_obj_minio.get('releasable_to', []),
            non_ic_markings=classification_meta_obj_minio.get('non_ic_markings', [])
        )

        return ob_evt
