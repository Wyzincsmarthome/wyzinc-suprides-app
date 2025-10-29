# settings.py
# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

@dataclass(frozen=True)
class Settings:
    # Execução
    ENV: str = os.getenv("ENV", "dev")
    SIMULATE: bool = _bool_env("SPAPI_SIMULATE", True)

    # Amazon SP-API / LWA
    SELLER_ID: str = os.getenv("SELLER_ID", "")
    MARKETPLACE_ID: str = os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS")
    SPAPI_ENDPOINT: str = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")

    LWA_CLIENT_ID: str = os.getenv("LWA_CLIENT_ID", "")
    LWA_CLIENT_SECRET: str = os.getenv("LWA_CLIENT_SECRET", "")
    LWA_REFRESH_TOKEN: str = os.getenv("LWA_REFRESH_TOKEN", "") or os.getenv("REFRESH_TOKEN", "")

    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION: str = os.getenv("AWS_REGION", "eu-west-1")

    # HTTP
    HTTP_TIMEOUT_S: int = int(os.getenv("HTTP_TIMEOUT_S", "60"))

settings = Settings()
