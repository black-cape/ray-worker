import logging
import ray

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

from etl.config import settings
from etl.messaging.kafka_consumer import ConsumerWorkerManager
from etl.util import get_logger

logging.config.fileConfig(settings.logging_conf_file)

LOGGER = get_logger(__name__)

app = FastAPI(title="Cast Iron Worker Using Ray - Manager")
cwm = ConsumerWorkerManager()


@app.on_event("startup")
def on_startup():
    cwm.start_all_workers()


@app.on_event("shutdown")
def on_shutdown():
    cwm.stop_all_workers()


@app.get('/manager/health')
def health():
    return {'message': 'Running'}


@app.get('/manager/status')
def status():
    return {'message': ray.cluster_resources()}


@app.get('/manager/start-consumers')
def start_consumers():
    cwm.start_all_workers()
    return "Successfully started all workers!"


@app.get('/manager/stop-consumers')
def stop_consumers():
    cwm.stop_all_workers()
    return "Successfully Stopped all workers!"


@app.exception_handler(Exception)
def generic_exception_handler(request: Request, exc: Exception):
    LOGGER.error(exc)
    return JSONResponse(
        status_code=500,
        content={"message": f"Manager error: {exc}"},
    )

