"""Contains the Minio implementation of the object store backend interface"""
import json
from threading import Lock
from datetime import datetime
from sqlite3 import DatabaseError
from typing import Any, Dict, Optional
from asyncpg import InvalidPasswordError

from asyncpg_utils.databases import PoolDatabase
from asyncpg_utils.managers import TableManager

from etl.config import settings
from etl.database.interfaces import DatabaseStore, FileObject
from etl.util import get_logger

LOGGER = get_logger(__name__)


class PGDatabase(DatabaseStore):
    """Implements the DatabaseStore interface using Minio as the backend service"""
    def __init__(self):
        self._database = PoolDatabase(f'postgres://{settings.database_user}:{settings.database_password}@{settings.database_host}/{settings.database_db}')
        if self._database:
            self._table_manager = TableManager(self._database, 'files', pk_field='id', hooks=None)

    async def create_table(self) -> bool:
        """ Check for and create database table """
        LOGGER.info('Creating DB table...')
        try:
            if self._database:
                await self._database.init_pool()
                conn = await self._database.get_connection()
                lock = Lock()
                with lock:
                    await conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS files (
                            id uuid PRIMARY KEY,
                            bucket_name text,
                            file_name text,
                            status text,
                            processing_status text,
                            original_filename text,
                            event_name text,
                            source_ip text,
                            size int,
                            etag text,
                            content_type text,
                            create_datetime timestamp with time zone,
                            update_datetime timestamp with time zone,
                            classification jsonb,
                            metadata jsonb
                        );
                        """
                    )
                await conn.close()
                return True
        except (DatabaseError, InvalidPasswordError) as db_error:
            LOGGER.info('Database not active.  Exception: %s', db_error)
        return False

    async def insert_file(self, filedata: FileObject):
        """ Track a new file from Minio"""
        LOGGER.info("Inserting file into DB...")
        await self._database.insert('files', dict(filedata))

    async def move_file(self, rowid: str, new_name: str):
        """ Track the moving of a file in Minio"""
        rec_data = {}
        rec_data['path'] = new_name
        rec_data['update_datetime'] = f'{datetime.now().isoformat()}Z'
        await self._table_manager.update(rowid, rec_data)

    async def delete_file(self, rowid: str):
        """ Track the deleting of a file in Minio"""
        await self._table_manager.delete(rowid)

    async def list_files(self, metadata: Optional[Dict]):
        """ List all tracked files by provided filter """
        return await self._table_manager.list(filters=metadata)

    async def retrieve_file_metadata(self, rowid: str):
        """ Retrieve a row based on ID """
        return await self._table_manager.detail(rowid)

    async def update_status(self, rowid: str, new_status: str, new_filename: str):
        """ Update the file status/state """
        rec_data = {}
        rec_data['status'] = new_status
        rec_data['file_name'] = new_filename
        rec_data['update_datetime'] = datetime.now()
        await self._table_manager.update(rowid, rec_data)

    def parse_notification(self, evt_data: Any):
        """ Parse a Minio notification to create a DB row """
        LOGGER.info(evt_data)
        bucket_name, file_name = evt_data['Key'].split('/', 1)
        metadata = evt_data['Records'][0]['s3']['object'].get('userMetadata', None)
        db_evt = {
            'id': metadata.get('X-Amz-Meta-Id', None),
            'bucket_name': bucket_name,
            'file_name': file_name,
            'status': 'Queued',
            'processing_status': None,
            'original_filename': metadata.get('X-Amz-Meta-Originalfilename', None),
            'event_name': evt_data['EventName'],
            'source_ip': evt_data['Records'][0]['requestParameters']['sourceIPAddress'],
            'size': evt_data['Records'][0]['s3']['object']['size'],
            'etag': evt_data['Records'][0]['s3']['object']['eTag'],
            'content_type': evt_data['Records'][0]['s3']['object']['contentType'],
            'create_datetime': datetime.now(),
            'classification': metadata.get('X-Amz-Meta-Classification', None),
            'metadata': json.dumps(metadata)
        }
        return db_evt
