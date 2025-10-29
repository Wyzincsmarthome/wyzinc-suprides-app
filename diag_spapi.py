# diag_spapi.py
# -*- coding: utf-8 -*-
import os, json, sys, traceback, requests
from dotenv import load_dotenv
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

load_dotenv()

def _bool(s, default=False):
    if s is None: return default
    return str(s).strip().lower() in ("1","true","yes","on")

SPAPI_ENDPOINT   = os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com").strip()
MARKETPLACE_ID   = os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS").strip()
AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID","").strip()
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY","").strip()
AWS_REGION       = os.getenv("AWS_REGION", "eu-west-1").strip()
LWA_CLIENT_ID    = os.getenv("LWA_CLIENT_ID","").strip()
LWA_CLIENT_SECRET= os.getenv("LWA_CLIENT_SECRET","").strip()
LWA_REFRESH_TOKEN= os.getenv("LWA_REFRESH_TOKEN","").strip() or os.getenv("REFRESH_TOKEN","").strip()
SELLER_ID        = os.getenv("SELLER_ID","").strip()
SIMULATE         = _bool(os.getenv("SPAPI_SIMULATE","true"), True)
TEST_ASIN        = os.getenv("TEST_ASIN","").strip()

# ---------- helpers robustos ----------
def amount_to_float(x):
    """
    Converte formatos variados em float:
    - 12.34
    - "12.34"
    - {"Amount": 12.34, "CurrencyCode": "EUR"}
    - {"Amount": {"Amount": 12.34, "CurrencyCode": "EUR"}}
    - {"value": 12.34}
    - None  -> None
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace(",", "."))
        except Exception:
            return None
    if isinstance(x, dict):
        # Tenta cascata comum
        if "Amount" in x:
            return amount_to_float(x.get("Amount"))
        if "value" in x:
            return amount_to_float(x.get("value"))
        # Alguns payloads têm sub-estrutura { "Amount": {"Amount": 12.34}}
        # amount_to_float acima já trata a recursividade
    return None

def lwa_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": LWA_REFRESH_TOKEN,
        "client_id": LWA_CLIENT_ID,
        "client_secret": LWA_CLIENT_SECRET,
    }
    r = requests.post(url, data=data, timeout=60)
    print("[LWA] status:", r.status_code)
    r.raise_for_status()
    j = r.json()
    return j["access_token"]

def signed_request(method, path, params=None, body=b"", access_token=None):
    url = f"{SPAPI_ENDPOINT}{path}"
    headers = {
        "x-amz-access-token": access_token or "",
        "user-agent": "TiagoAmazonSync/diag/1.1",
        "accept": "application/json",
    }
    req = AWSRequest(method=method, url=url, params=params or {}, data=body, headers=headers)
    creds = Credentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    SigV4Auth(creds, "execute-api", AWS_REGION).add_auth(req)
    prepped = req.prepare()
    return requests.request(method, prepped.url, headers=dict(prepped.headers), params=params, data=body, timeout=60)

def test_offers(asin, token):
    path = f"/products/pricing/v0/items/{asin}/offers"
    params = {"MarketplaceId": MARKETPLACE_ID, "ItemCondition": "New"}
    r = signed_request("GET", path, params=params, access_token=token)
    print(f"[Offers] {asin} status:", r.status_code)
    print("[Offers] headers:", {k.lower(): v for k, v in r.headers.items() if k.lower().startswith("x-amzn")})
    if r.status_code != 200:
        print("[Offers] body:", r.text[:800]); sys.exit(2)
    j = r.json()

    offers = (j.get("payload") or {}).get("Offers") or []
    print(f"[Offers] n_ofertas={len(offers)}")

    rows = []
    for o in offers:
        sid = o.get("SellerId")
        # Landed direto
        landed = amount_to_float(o.get("LandedPrice"))
        # Fallback se Landed estiver vazio
        if landed is None:
            listing = amount_to_float(o.get("ListingPrice"))
            shipping = amount_to_float(o.get("Shipping"))
            if listing is not None:
                landed = listing + (shipping or 0.0)

        # Currency (se existir)
        currency = None
        lp = o.get("LandedPrice")
        if isinstance(lp, dict):
            cc = (lp.get("Amount") or {}).get("CurrencyCode")
            currency = cc or lp.get("CurrencyCode")

        rows.append({
            "SellerId": sid,
            "landed": landed,
            "currency": currency or "EUR",
        })

    # Ordena por landed quando existir
    rows.sort(key=lambda r: (1e12 if r["landed"] is None else r["landed"]))

    # Exclui a tua oferta, se quiseres (ativa por defeito neste diag)
    my_sid = SELLER_ID.strip()
    competitors = [r for r in rows if not my_sid or r["SellerId"] != my_sid]
    min_comp = next((r for r in competitors if r["landed"] is not None), None)

    # Print compacto
    for r0 in rows[:10]:
        print(f"  - SellerId: {r0['SellerId'] or '(?)'}  Landed(calc): {r0['landed']}  {r0['currency']}")

    print("\nResumo:")
    print("  - Min concorrente:", (min_comp["landed"] if min_comp else None), (min_comp["currency"] if min_comp else ""))

def main():
    print("== Diag SP-API ==")
    print("SIMULATE:", SIMULATE)
    if SIMULATE:
        print("SPAPI_SIMULATE=true -> desliga para chamadas reais.")
        return
    for var in ("LWA_CLIENT_ID","LWA_CLIENT_SECRET","LWA_REFRESH_TOKEN","AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","MARKETPLACE_ID"):
        if not os.getenv(var, "").strip():
            print(f"FALTA variável: {var}")
            sys.exit(1)
    token = lwa_token()
    print("[LWA] token OK (recebido).")
    asin = TEST_ASIN or (input("ASIN para testar GetItemOffers: ").strip()).upper()
    if not asin:
        print("Sem ASIN. Define TEST_ASIN no .env ou escreve um ASIN.")
        sys.exit(0)
    test_offers(asin, token)
    print("OK ✅")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(99)
