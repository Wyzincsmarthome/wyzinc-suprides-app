# diag_asin_es.py
# -*- coding: utf-8 -*-
import os, json, sys, requests
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

load_dotenv()

SP = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")
MKT_ES = "A1RKKUPIHCS9HS"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID","").strip()
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY","").strip()
AWS_REGION = os.getenv("AWS_REGION","eu-west-1").strip()
LWA_CLIENT_ID = os.getenv("LWA_CLIENT_ID","").strip()
LWA_CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET","").strip()
LWA_REFRESH_TOKEN = (os.getenv("LWA_REFRESH_TOKEN","") or os.getenv("REFRESH_TOKEN","")).strip()

def lwa_token():
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type":"refresh_token",
        "refresh_token": LWA_REFRESH_TOKEN,
        "client_id": LWA_CLIENT_ID,
        "client_secret": LWA_CLIENT_SECRET,
    }, timeout=60)
    print("[LWA]", r.status_code)
    r.raise_for_status()
    return r.json()["access_token"]

def signed(method, path, params=None, body=b"", token=""):
    url = f"{SP}{path}"
    headers = {
        "x-amz-access-token": token,
        "user-agent": "TiagoAmazonSync/diag/1.0",
        "accept": "application/json",
    }
    req = AWSRequest(method=method, url=url, params=params or {}, data=body, headers=headers)
    SigV4Auth(Credentials(AWS_ACCESS_KEY, AWS_SECRET_KEY), "execute-api", AWS_REGION).add_auth(req)
    prepped = req.prepare()
    return requests.request(method, prepped.url, headers=dict(prepped.headers),
                            params=params, data=body, timeout=60)

def main():
    asin = (sys.argv[1] if len(sys.argv)>1 else input("ASIN: ")).strip().upper()
    token = lwa_token()

    # 1) Catalog Items -> confirma existência no ES
    r1 = signed("GET", f"/catalog/2022-04-01/items/{asin}", params={"marketplaceIds": MKT_ES}, token=token)
    print(f"[Catalog ES] {r1.status_code}")
    if r1.status_code != 200:
        print(r1.text[:600])
        sys.exit(1)
    payload = r1.json()
    titles = []
    try:
        attrs = payload.get("attributes") or {}
        # título pode estar em attributes.item_name[0].value
        if isinstance(attrs.get("item_name"), list) and attrs["item_name"]:
            titles.append(attrs["item_name"][0].get("value",""))
    except Exception:
        pass
    print("[Catalog ES] OK. Title:", (titles[0] if titles else "(sem título no payload)"))

    # 2) Offers -> preços para o mesmo marketplace
    r2 = signed("GET", f"/products/pricing/v0/items/{asin}/offers",
                params={"MarketplaceId": MKT_ES, "ItemCondition":"New"}, token=token)
    print(f"[Offers ES] {r2.status_code}")
    if r2.status_code != 200:
        print(r2.text[:800])
        sys.exit(2)
    j = r2.json()
    offers = (j.get("payload") or {}).get("Offers") or []
    print(f"[Offers ES] n_ofertas={len(offers)}")
    for o in offers[:5]:
        sid = o.get("SellerId")
        amt = None
        if isinstance(o.get("LandedPrice"), dict):
            amt = (o["LandedPrice"].get("Amount") or {}).get("Amount")
        print("  -", sid, amt)

if __name__ == "__main__":
    main()
