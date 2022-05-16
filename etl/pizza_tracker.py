"""
Provides handling of pipe-based ETL shell status notification
"""
import os
from io import BufferedReader, FileIO, TextIOWrapper

from etl.messaging.interfaces import MessageProducer
from etl.util import get_logger

logger = get_logger(__name__)


class PizzaTracker:
    """
    Represents one status notification processor
    """
    def __init__(self, message_producer: MessageProducer, working_dir: str, job_id: str):
        self.working_dir = working_dir
        self.message_producer = message_producer
        self.job_id = job_id
        self.pipe_file_name = os.path.join(working_dir, 'pizza_tracker_file')
        self.f_pipe = None

    def process(self):
        """
        Reads any available status commands, not blocking
        """
        while True:
            try:
                line = self.f_pipe.readline()
                logger.debug('pizza tracker read line: %s', line)
                if not line or line.strip() == 'END':
                    # [WS] break out of loop when there's nothing to do or the end is reached
                    # Expects an outer loop calling pizza_tracker.process() while processor is alive
                    break
                cmd, args = line.strip().split(' ', 1)
                handler = self.__getattribute__(f'_handle_{cmd.lower()}')
                handler(args)
            except (AttributeError, ValueError):
                pass

    def _handle_task(self, data: str):
        self.message_producer.job_evt_task(self.job_id, data)

    def _handle_committed(self, data: str):
        try:
            committed = int(data)
            self.message_producer.job_evt_committed(self.job_id, committed)
        except ValueError:
            pass

    def _handle_progress(self, data: str):
        progress = None

        # check for float scalar
        try:
            progress = float(data)
        except ValueError:
            pass

        # check for fraction
        if progress is None:
            fraction = data.split('/')
            if len(fraction) == 2:
                try:
                    num = float(fraction[0].strip())
                    denum = float(fraction[1].strip())
                    progress = num / denum
                except ValueError:
                    pass

        if progress is not None and 0.0 <= progress <= 1.0:
            logger.debug('pizza tracker is logging progress to kafka. job_id: %s, progress: %s',
            self.job_id, progress)
            self.message_producer.job_evt_progress(self.job_id, progress)

    def __enter__(self):
        os.mkfifo(self.pipe_file_name)
        self.f_pipe = TextIOWrapper(
            BufferedReader(FileIO(os.open(self.pipe_file_name,
                                          os.O_RDONLY | os.O_NONBLOCK), mode='r'))
        )
        return self

    def __exit__(self, type_, value, error):
        self.f_pipe.close()
