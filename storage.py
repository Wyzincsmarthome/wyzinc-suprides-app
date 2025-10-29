# storage.py
import csv, io, json, os
from typing import List, Dict, Any, Optional

ENV = os.getenv("ENV", "local").lower()
PROVIDER = os.getenv("STORAGE_PROVIDER", "local").lower()

# --- Local backend ---
class LocalStorage:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path(self, rel: str) -> str:
        path = os.path.join(self.base_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def write_csv(self, rel_path: str, rows: List[Dict[str, Any]]):
        path = self._path(rel_path)
        if not rows:
            # cria header mínimo se vazio
            with open(path, "w", newline="", encoding="utf-8") as f:
                f.write("")
            return
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, path)

    def read_csv(self, rel_path: str) -> List[Dict[str, Any]]:
        path = self._path(rel_path)
        if not os.path.exists(path):
            return []
        with open(path, "r", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            return [dict(row) for row in r]

    def write_json(self, rel_path: str, data: Any):
        path = self._path(rel_path)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def read_json(self, rel_path: str) -> Optional[Any]:
        path = self._path(rel_path)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

# --- S3 backend ---
class S3Storage:
    def __init__(self):
        import boto3
        self.bucket = os.getenv("S3_BUCKET")
        self.prefix = os.getenv("S3_PREFIX", "").strip().strip("/")
        self.s3 = boto3.client("s3", region_name=os.getenv("S3_REGION"))

    def _key(self, rel: str) -> str:
        rel = rel.lstrip("/").replace("\\", "/")
        return f"{self.prefix}/{rel}" if self.prefix else rel

    def write_csv(self, rel_path: str, rows: List[Dict[str, Any]]):
        if not rows:
            body = ""
        else:
            output = io.StringIO()
            w = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
            body = output.getvalue()
        self.s3.put_object(Bucket=self.bucket, Key=self._key(rel_path), Body=body.encode("utf-8"))

    def read_csv(self, rel_path: str) -> List[Dict[str, Any]]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self._key(rel_path))
        except Exception:
            return []
        text = obj["Body"].read().decode("utf-8")
        if not text.strip():
            return []
        r = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in r]

    def write_json(self, rel_path: str, data: Any):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=self._key(rel_path), Body=body, ContentType="application/json; charset=utf-8")

    def read_json(self, rel_path: str) -> Optional[Any]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self._key(rel_path))
        except Exception:
            return None
        return json.loads(obj["Body"].read().decode("utf-8"))

# --- Factory ---
def get_storage():
    if ENV == "prod" and PROVIDER == "s3":
        return S3Storage()
    return LocalStorage()
