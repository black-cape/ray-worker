import ray
from contextlib import asynccontextmanager
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from etl.messaging.kafka_consumer import ConsumerWorkerManager
from etl.util import get_logger
from fastapi_offline import FastAPIOffline

LOGGER = get_logger(__name__)


cwm = ConsumerWorkerManager()


@asynccontextmanager
async def lifespan(application: FastAPI):
    await bootstrap(application)
    yield
    await shutdown()


async def bootstrap(application: FastAPI):
    cwm.start_all_workers()


async def shutdown():
    cwm.stop_all_workers()


app = FastAPIOffline(
    title="Cast Iron Worker Using Ray - Manager",
    root_path="/castiron",
    lifespan=lifespan,
)

# Setup CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/manager/health")
def health():
    return {"message": "Running"}


@app.get("/manager/status")
def status():
    return {"message": ray.cluster_resources()}


@app.get("/manager/start-consumers")
def start_consumers():
    cwm.start_all_workers()
    return "Successfully started all workers!"


@app.get("/manager/stop-consumers")
def stop_consumers():
    cwm.stop_all_workers()
    return "Successfully Stopped all workers!"


@app.get("/manager/cancel-record/{filename}")
async def cancel_record(filename: str):
    await cwm.cancel_processing_task(filename)
    return f"Successfully canceled task for filename: {filename}"


@app.exception_handler(Exception)
def generic_exception_handler(request: Request, exc: Exception):
    LOGGER.error(exc)
    return JSONResponse(
        status_code=500,
        content={"message": f"Manager error: {exc}"},
    )
