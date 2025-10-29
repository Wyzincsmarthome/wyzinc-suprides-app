# amazon_insights.py
# -*- coding: utf-8 -*-
import os, json, logging, pandas as pd
from typing import Dict
from amazon_client import AmazonClient
from pricing_engine import calc_final_price

log = logging.getLogger(__name__)

def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")

def _f(x):
    try: return float(x)
    except: return 0.0

def build_overview(cfg: Dict, seller_id: str | None, simulate: bool | None) -> pd.DataFrame:
    """
    Consolida:
      - data/produtos_processados.csv (SKU, cost, stock, floor/selling)
      - data/produtos_classificados.csv (ASIN, status)
      - data/my_inventory.csv (listagens ativas)
      - ofertas por ASIN (min concorrente excluindo o teu SellerId)
    Calcula:
      - recommended_price = max(floor, min_competitor - 0.01)
    """
    os.makedirs("data", exist_ok=True)

    dfp = _read_csv("data/produtos_processados.csv")
    dfc = _read_csv("data/produtos_classificados.csv").rename(columns={"seller_sku":"sku"})
    dfi = _read_csv("data/my_inventory.csv").rename(columns={"seller_sku":"sku","price":"my_listing_price","quantity":"my_qty"})

    # base segura
    for c in ["sku","brand","title","ean","cost","stock","floor_price","selling_price"]:
        if c not in dfp.columns: dfp[c] = ""
    for c in ["sku","asin","status"]:
        if c not in dfc.columns: dfc[c] = ""
    for c in ["sku","asin","my_listing_price","my_qty","status"]:
        if c not in dfi.columns: dfi[c] = ""

    base = dfp.merge(dfc[["sku","asin","status"]], how="left", on="sku")
    base = base.merge(dfi[["sku","asin","my_listing_price","my_qty","status"]]
                      .rename(columns={"status":"listing_status","asin":"asin_inv"}), how="left", on="sku")
    base["asin"] = base.apply(lambda r: r["asin"] if str(r.get("asin","")).strip() else (r.get("asin_inv") or ""), axis=1)
    base.drop(columns=["asin_inv"], inplace=True)

    client = AmazonClient(simulate=simulate)
    asins = sorted({a for a in base["asin"].astype(str) if a})
    # mapa ASIN -> min concorrente (exclui meu SellerId)
    min_comp_map = {}
    for a in asins:
        offers = client.get_listing_offers(a) or []
        prices_other = []
        for o in offers:
            sid = str(o.get("SellerId") or "")
            landed = o.get("LandedPrice")
            if landed is None:
                continue
            if seller_id and sid == seller_id:
                continue
            try:
                prices_other.append(float(landed))
            except:
                continue
        min_comp_map[a] = min(prices_other) if prices_other else None

    base["min_competitor"] = base["asin"].map(lambda a: min_comp_map.get(a))
    # recommended = concorrente - 0,01 se >= floor; senão floor
    def _reco(row):
        cost = _f(row.get("cost"))
        comp = row.get("min_competitor")
        comp_val = float(comp) if comp not in (None,"") else None
        res = calc_final_price(cost=cost, competitor_price=comp_val, cfg=cfg)
        return res["floor_price"], res["final_price"]
    base[["floor_calc","recommended_price"]] = base.apply(lambda r: pd.Series(_reco(r)), axis=1)

    cols = [
        "sku","brand","title","ean",
        "asin","status","listing_status",
        "stock","my_qty",
        "cost","floor_price","floor_calc","selling_price","my_listing_price",
        "min_competitor","recommended_price",
    ]
    for c in cols:
        if c not in base.columns: base[c] = ""
    out = base[cols].copy()

    out.to_csv("data/amazon_overview.csv", index=False)
    with open("data/amazon_overview.json", "w", encoding="utf-8") as f:
        json.dump(out.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    return out
