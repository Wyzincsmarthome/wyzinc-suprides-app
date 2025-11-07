# storage.py
# -*- coding: utf-8 -*-
import os
import io
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

log = logging.getLogger("storage")
if not log.handlers:
    h = logging.StreamHandler()
    log.addHandler(h)
log.setLevel(logging.INFO)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

class LocalStorage:
    def __init__(self, base_dir: Optional[str] = None) -> None:
        self.base_dir = base_dir or DATA_DIR
        os.makedirs(self.base_dir, exist_ok=True)
    def _path(self, name: str) -> str:
        return os.path.join(self.base_dir, name)
    def write_json(self, name: str, data: Dict[str, Any]) -> None:
        path = self._path(name); os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    def read_json(self, name: str) -> Dict[str, Any]:
        path = self._path(name)
        if not os.path.exists(path): return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
    def write_csv(self, name: str, rows_or_df) -> None:
        path = self._path(name); os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        if isinstance(rows_or_df, pd.DataFrame):
            rows_or_df.to_csv(tmp, index=False)
        else:
            pd.DataFrame(rows_or_df).to_csv(tmp, index=False)
        os.replace(tmp, path)
    def read_csv(self, name: str) -> List[Dict[str, Any]]:
        path = self._path(name)
        if not os.path.exists(path): return []
        try:
            df = pd.read_csv(path, dtype=str).fillna("")
            return df.to_dict(orient="records")
        except Exception:
            return []

class S3Storage:
    """ Requer STORAGE_PROVIDER='s3' e vars: S3_BUCKET, S3_REGION, (opcional) S3_PREFIX. """

    ARN_PREFIX = "arn:aws:s3:::"
    
    def __init__(self) -> None:
        import boto3

        raw_bucket = (os.environ.get("S3_BUCKET") or "").strip()
        bucket, inferred_prefix = self._extract_bucket_and_prefix(raw_bucket)

        self.region = (os.environ.get("S3_REGION") or "").strip()
        explicit_prefix = (os.environ.get("S3_PREFIX") or "").strip()

        if not bucket or not self.region:
            raise RuntimeError("S3_STORAGE: S3_BUCKET e S3_REGION são obrigatórios.")

        self.bucket = bucket
        self.prefix = self._normalize_prefix(explicit_prefix or inferred_prefix)

        if raw_bucket != bucket:
            log.info(
                "S3Storage bucket normalizado de '%s' para '%s' (prefix='%s')",
                raw_bucket,
                self.bucket,
                self.prefix,
            )

        self.s3 = boto3.client("s3", region_name=self.region)

    @classmethod
    def _extract_bucket_and_prefix(cls, raw_bucket: str) -> Tuple[str, str]:
        value = (raw_bucket or "").strip()
        if not value:
            return "", ""
        if value.startswith(cls.ARN_PREFIX):
            remainder = value[len(cls.ARN_PREFIX) :].lstrip("/")
            if not remainder:
                return "", ""
            if "/" in remainder:
                bucket_name, inferred_prefix = remainder.split("/", 1)
            else:
                bucket_name, inferred_prefix = remainder, ""
            return bucket_name.strip(), cls._normalize_prefix(inferred_prefix)
        return value, ""

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        return prefix.strip().strip("/") if prefix else ""
    def _key(self, name: str) -> str:
        return f"{self.prefix.strip().rstrip('/')}/{name.lstrip('/')}" if self.prefix else name.lstrip("/")
    def write_json(self, name: str, data: Dict[str, Any]) -> None:
        key = self._key(name)
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
    def read_json(self, name: str) -> Dict[str, Any]:
        key = self._key(name)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            txt = obj["Body"].read().decode("utf-8")
            return json.loads(txt) or {}
        except Exception:
            return {}
    def write_csv(self, name: str, rows_or_df) -> None:
        key = self._key(name)
        if isinstance(rows_or_df, pd.DataFrame):
            buf = io.StringIO(); rows_or_df.to_csv(buf, index=False)
        else:
            buf = io.StringIO(); pd.DataFrame(rows_or_df).to_csv(buf, index=False)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue().encode("utf-8"), ContentType="text/csv")
    def read_csv(self, name: str) -> List[Dict[str, Any]]:
        key = self._key(name)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = obj["Body"].read().decode("utf-8")
            df = pd.read_csv(io.StringIO(data), dtype=str).fillna("")
            return df.to_dict(orient="records")
        except Exception:
            return []

def get_storage():
    provider = (os.environ.get("STORAGE_PROVIDER") or "local").strip().lower()
    if provider == "s3":
        try:
            return S3Storage()
        except Exception as e:
            log.error("Falha a iniciar S3Storage (%s). A usar LocalStorage.", e)
            return LocalStorage()
    return LocalStorage()
