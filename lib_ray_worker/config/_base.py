from pydantic_settings import BaseSettings


class Base(BaseSettings):
    class Config:
        case_sensitive = True
        env_prefix = "RAY_WORKER_"
