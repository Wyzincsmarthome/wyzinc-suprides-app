# run_suprides_sync.py
# -*- coding: utf-8 -*-
import requests, os, time
 
BASE = os.getenv("LOCAL_SYNC_BASE", "http://127.0.0.1:5000")
LIMIT = int(os.getenv("SUPRIDES_SYNC_LIMIT", "500"))
 
def call(path: str):
def call(method: str, path: str):
    url = f"{BASE}{path}"
    r = requests.post(url, timeout=120)
    r = requests.request(method=method.upper(), url=url, timeout=300)
    r.raise_for_status()
    return r.json()
 
if __name__ == "__main__":
    t0 = time.time()
    print("-> Sync OFFERS")
    print(call("/suprides/sync/offers?limit=500"))
    print("-> Sync PRICES")
    print(call("/suprides/sync/prices?limit=500"))
    print("-> Gerar CSV para feed Amazon")
    response = call("POST", f"/suprides/sync/feed?limit={LIMIT}")
    print(response)
    if response.get("download_url"):
        print("Feed disponível em:", response["download_url"])
    print("Elapsed:", round(time.time() - t0, 1), "s")
