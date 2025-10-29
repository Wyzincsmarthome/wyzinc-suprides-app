# spapi_probe.py
# -*- coding: utf-8 -*-
import json, sys
from amazon_client import AmazonClient

ASIN = (sys.argv[1] if len(sys.argv) > 1 else "B0BY6YGTKC").strip().upper()
MKT = (sys.argv[2] if len(sys.argv) > 2 else "A1RKKUPIHCS9HS").strip()

def pretty(label, resp):
    print(f"\n=== {label} ===")
    if hasattr(resp, "status_code"):
        print("HTTP:", resp.status_code)
    try:
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
    except Exception:
        try:
            print(json.dumps(resp, ensure_ascii=False, indent=2))
        except Exception:
            print(resp)

def main():
    ac = AmazonClient(simulate=False)

    # 1) Sellers: marketplaces do seller (sanity check de credenciais/host/região)
    r1 = ac.request(
        method="GET",
        path="/sellers/v1/marketplaceParticipations",
        params={}
    )
    pretty("Sellers v1 marketplaceParticipations", r1)

    # 2) Product Pricing v0: lowest offers por ASIN
    #    GET /products/pricing/v0/items/{ASIN}/offers?MarketplaceId=...&ItemCondition=New
    r2 = ac.request(
        method="GET",
        path=f"/products/pricing/v0/items/{ASIN}/offers",
        params={"MarketplaceId": MKT, "ItemCondition": "New"}
    )
    pretty("Pricing v0 getItemOffers", r2)

    # 3) Product Pricing v2022-05-01: Competitive Summary (batch POST)
    #    POST /batches/products/pricing/2022-05-01/items/competitiveSummary
    body = {
        "requests": [
            {
                "uri": "/products/pricing/2022-05-01/items/competitiveSummary",
                "method": "POST",
                "marketplaceId": MKT,
                "body": {"asins": [ASIN]}
            }
        ]
    }
    r3 = ac.request(
        method="POST",
        path="/batches/products/pricing/2022-05-01/items/competitiveSummary",
        params={},
        json=body
    )
    pretty("Pricing v2022-05-01 getCompetitiveSummary (batch)", r3)

if __name__ == "__main__":
    main()
