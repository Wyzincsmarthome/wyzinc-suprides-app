# run_suprides_sync.py
# -*- coding: utf-8 -*-
import os
import time
from typing import Any

import requests

BASE = os.getenv("LOCAL_SYNC_BASE", "http://127.0.0.1:5000")
LIMIT = int(os.getenv("SUPRIDES_SYNC_LIMIT", "500"))


def call(method: str, path: str) -> Any:
    url = f"{BASE}{path}"
    response = requests.request(method=method.upper(), url=url, timeout=300)
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    t0 = time.time()
    print("-> Gerar CSV para feed Amazon")
    response = call("POST", f"/suprides/sync/feed?limit={LIMIT}")
    print(response)
    if isinstance(response, dict) and response.get("download_url"):
        print("Feed disponível em:", response["download_url"])
    print("Elapsed:", round(time.time() - t0, 1), "s")
