# spapi_pricing_debug.py
# -*- coding: utf-8 -*-
import json
import os
import sys
from amazon_client import AmazonClient

MKT = os.environ.get("MARKETPLACE_ID", "A1RKKUPIHCS9HS")  # ES

def show(title, obj):
    print("\n=== " + title + " ===")
    try:
        print(json.dumps(obj, indent=2, ensure_ascii=False))
    except Exception:
        print(obj)

def pick_min_landed_from_summary(payload):
    try:
        lowest = (payload or {}).get("Summary", {}).get("LowestPrices") or []
        best = None
        for row in lowest:
            cond = (row.get("condition") or "").lower()
            lp = row.get("LandedPrice") or {}
            amt = lp.get("Amount")
            if amt is None:
                listing = (row.get("ListingPrice") or {}).get("Amount")
                ship    = (row.get("Shipping") or {}).get("Amount")
                amt = (float(listing or 0) + float(ship or 0)) if listing is not None else None
            try:
                val = float(amt)
            except Exception:
                continue
            key = (0 if cond == "new" else 1, val)
            if best is None or key < best[0]:
                best = (key, val)
        return best[1] if best else None
    except Exception:
        return None

def pick_buybox_from_summary(payload):
    try:
        bb = (payload or {}).get("Summary", {}).get("BuyBoxPrices") or []
        best = None
        for row in bb:
            amt = (row.get("LandedPrice") or {}).get("Amount")
            if amt is None:
                listing = (row.get("ListingPrice") or {}).get("Amount")
                ship    = (row.get("Shipping") or {}).get("Amount")
                amt = (float(listing or 0) + float(ship or 0)) if listing is not None else None
            try:
                val = float(amt)
            except Exception:
                continue
            if best is None or val < best:
                best = val
        return best
    except Exception:
        return None

def pick_min_from_offers(payload):
    try:
        offers = (payload or {}).get("Offers") or []
        best = None
        for row in offers:
            amt = (row.get("LandedPrice") or {}).get("Amount")
            if amt is None:
                listing = (row.get("ListingPrice") or {}).get("Amount")
                ship    = (row.get("Shipping") or {}).get("Amount")
                amt = (float(listing or 0) + float(ship or 0)) if listing is not None else None
            try:
                val = float(amt)
            except Exception:
                continue
            if best is None or val < best:
                best = val
        return best
    except Exception:
        return None

def pick_competitor(payload):
    for fn in (pick_min_landed_from_summary, pick_buybox_from_summary, pick_min_from_offers):
        v = fn(payload)
        if v is not None:
            return v
    return None

def probe_one(asin, cond):
    ac = AmazonClient(simulate=False)
    payload = ac.pricing_get_item_offers(asin=asin, marketplace_id=MKT, condition=cond)
    show(f"Pricing v0 getItemOffers {asin} [{cond}]", payload)
    comp = pick_competitor(payload)
    print(f"→ menor landed ({cond}):", comp)
    return comp

def main():
    if len(sys.argv) < 2:
        print("Uso: python spapi_pricing_debug.py <ASIN> [MARKETPLACE_ID]")
        sys.exit(2)
    asin = sys.argv[1].strip().upper()
    if len(sys.argv) >= 3:
        os.environ["MARKETPLACE_ID"] = sys.argv[2].strip()
    # tenta por ordem New / Used / Refurbished
    best = None
    for cond in ("New", "Used", "Refurbished"):
        comp = probe_one(asin, cond)
        if comp is not None:
            best = comp if best is None else min(best, comp)
    print("\n=== RESUMO ===")
    print(f"ASIN {asin} @ {os.environ.get('MARKETPLACE_ID','A1RKKUPIHCS9HS')} → menor landed (qualquer condição):", best)

if __name__ == "__main__":
    main()
