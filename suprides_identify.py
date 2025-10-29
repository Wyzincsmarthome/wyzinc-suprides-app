# suprides_identify.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
import logging
import unicodedata
import re
from typing import Dict, Any, List, Tuple

import pandas as pd

from suprides_client import SupridesClient
from supplier_suprides import normalize  # devolve: sku, ean, brand, name, price_cost, qty_available
from amazon_client import AmazonClient
from pricing_engine import calc_final_price

log = logging.getLogger("suprides_identify")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

DEFAULT_MARKETPLACE_ID = os.environ.get("DEFAULT_MARKETPLACE_ID", "").strip() or "A1RKKUPIHCS9HS"
MARKETPLACE_ID = os.environ.get("MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)
SELLER_ID = os.environ.get("SELLER_ID", "").strip()

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
CLASSIFIED_CSV = os.path.join(DATA_DIR, "suprides_classified.csv")


def _s(x) -> str:
    try:
        return "" if x is None else str(x)
    except Exception:
        return ""

def _pfloat(x) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", ".")
        return float(s) if s else None
    except Exception:
        return None

# -----------------------
# BRAND BLOCKLIST (aceleração)
# -----------------------
BRAND_BLOCKLIST_DEFAULT = [
    "Acer","AMD","Apple","Asus","Axis","Brother","Cooler Master","Crucial","DDIGITAL","DJI","Dell",
    "ELAGO","Equip","Ewent","Extreme","Gigaset","HP","Hoto","Inmove","Jabra","Kobo","Livall","Maxtor",
    "Microsoft","PNY","QDOS","Rapoo","Roidmi","SKYPOS","SUNMI","Samsung","Satechi","ServiÃ§os","Serviços",
    "Seiko","Socomec","Spirit of Gamer","TCL","Team Group","Tech-Protect","Tooq","Toshiba","Trust",
    "Ubiquiti","Vekoby","Vivo","Wozinsky","Yealink"
]

def _canon_brand(s: str) -> str:
    if s is None:
        return ""
    # normaliza acentos/encoding e casefold
    return unicodedata.normalize("NFKC", str(s)).casefold().strip()

_env_block = os.getenv("SUPRIDES_BRAND_BLOCKLIST", "")
if _env_block.strip():
    _BRAND_BLOCKLIST = [x.strip() for x in _env_block.split(",") if x.strip()]
else:
    _BRAND_BLOCKLIST = BRAND_BLOCKLIST_DEFAULT
_BRAND_BLOCK_KEYS = {_canon_brand(x) for x in _BRAND_BLOCKLIST}

# -----------------------
# STOCK MAPPING (garantido na saída)
# -----------------------
_STOCK_LABELS_ZERO = {"0", "oos", "outofstock", "semstock", "sem_estoque", "semestoque", "no", "false"}

def _map_stock_value(v) -> int:
    """
    Garante o mapping:
      0 -> 0
      <2 -> 1
      <10 -> 5
      >10 -> 10
    Se for numérico (e.g., 3), respeita o valor (>=0).
    """
    if v is None:
        return 0
    s = str(v).strip()
    if s == "":
        return 0
    ss = s.lower().replace(" ", "")
    if ss in _STOCK_LABELS_ZERO:
        return 0
    if ss in {"<2", "≤2", "le2"}:
        return 1
    if ss in {"<10", "≤10", "le10"}:
        return 5
    if ss in {">10", "≥10", "ge10", "10+", ">9"}:
        return 10
    # tenta numérico direto
    try:
        return max(0, int(float(s.replace(",", "."))))
    except Exception:
        pass
    # tenta extrair número embutido
    m = re.search(r"(-?\d+(?:[.,]\d+)?)", s)
    if m:
        try:
            return max(0, int(float(m.group(1).replace(",", "."))))
        except Exception:
            return 0
    return 0


def _choose_best_catalog_item(items: List[Dict[str, Any]], supplier_brand: str) -> Tuple[str | None, str]:
    """
    Recebe a resposta do Catalog (search por EAN) e devolve (asin, status_base)
    status_base ∈ {"catalog_match","catalog_ambiguous","not_found"}
    """
    if not items:
        return None, "not_found"

    # filtra marketplace
    cand: List[Tuple[str, Dict[str, Any]]] = []
    for it in items:
        asin = it.get("asin") or it.get("ASIN") or it.get("Asin")
        summaries = it.get("summaries") or []
        take = None
        for s in summaries:
            if s.get("marketplaceId") == MARKETPLACE_ID:
                take = s
                break
        if asin and take:
            cand.append((asin, take))

    if not cand:
        return None, "not_found"

    if len(cand) == 1:
        return cand[0][0], "catalog_match"

    # várias opções: tenta favorecer marca igual (case-insensitive)
    sb = (supplier_brand or "").strip().lower()
    ranked = []
    for asin, s in cand:
        b = (s.get("brand") or "").strip().lower()
        score = 0
        if sb and b and (sb in b or b in sb):
            score += 10
        title = (_s(s.get("itemName"))).lower()
        if sb and sb in title:
            score += 3
        ranked.append((score, asin))
    ranked.sort(reverse=True)

    if ranked and ranked[0][0] >= 10:
        return ranked[0][1], "catalog_match"
    return None, "catalog_ambiguous"


def classify_suprides_products(simulate: bool = False) -> pd.DataFrame:
    """
    1) Lê catálogo do fornecedor (Suprides) e normaliza: sku, ean, brand, name, price_cost, qty_available
    2) Para cada linha:
        - se ean vazio -> status=missing_ean
        - caso contrário, procura no Catalog por EAN; decide asin + status base
        - se asin encontrado, chama getItemOffers(New) e:
            * se teu SELLER_ID aparece -> status=listed
            * competitor_price = menor landed que NÃO seja teu seller
        - calcula floor/final via pricing_engine
    3) Grava CSV com colunas completas para a UI.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    sup = SupridesClient()
    ac = AmazonClient(simulate=False)

    rows: List[Dict[str, Any]] = []

    limit = getattr(sup, "limit", 250) or 250
    for raw in sup.iter_products(limit=limit):
        base = normalize(raw)
        sku = base.get("sku", "")
        ean = base.get("ean", "")
        brand = base.get("brand", "")
        title = base.get("name", "")
        cost = _pfloat(base.get("price_cost"))
        # força mapping de stock aqui, independentemente do upstream
        stock = _map_stock_value(base.get("qty_available"))

        # ---- filtro de marcas a ignorar (acelera o processo) ----
        if _canon_brand(brand) in _BRAND_BLOCK_KEYS:
            # ignora este item
            continue

        if not ean:
            rows.append({
                "sku": sku, "ean": ean, "brand": brand, "title": title,
                "asin": "", "status": "missing_ean", "score": "0.00", "listed": "no",
                "provenance": "supplier", "candidates": "[]",
                "stock": str(stock), "cost": "" if cost is None else f"{cost:.2f}",
                "competitor_price": "", "floor_price": "", "selling_price": ""
            })
            continue

        # 2) procura no catálogo por EAN
        try:
            cat = ac.catalog_search_by_ean(ean=ean, marketplace_id=MARKETPLACE_ID) or {}
            items = cat.get("items") or []
        except Exception:
            items = []

        asin, base_status = _choose_best_catalog_item(items, brand)

        status = base_status
        listed = "no"
        competitor_price = None

        if asin:
            # 2b) verificar ofertas “New” e detetar se EU estou listado
            try:
                raw_off = ac.pricing_get_item_offers(asin=asin, marketplace_id=MARKETPLACE_ID, item_condition="New") or {}
                payload = raw_off.get("payload", raw_off)
                my_seen = False
                best_other = math.inf
                for off in (payload.get("Offers") or []):
                    sid = (_s(off.get("SellerId"))).strip()
                    lp = off.get("ListingPrice") or {}
                    sp = off.get("Shipping") or {}
                    try:
                        landed = float(lp.get("Amount", 0)) + float(sp.get("Amount", 0))
                    except Exception:
                        continue
                    if SELLER_ID and sid == SELLER_ID:
                        my_seen = True
                    else:
                        if landed < best_other:
                            best_other = landed
                if my_seen:
                    listed = "yes"
                    status = "listed"
                if best_other < math.inf:
                    competitor_price = best_other
            except Exception:
                pass

        # 3) calcula floor/final
        floor_fmt = ""
        final_fmt = ""
        if cost is not None:
            px = calc_final_price(cost=cost, competitor_price=competitor_price)
            if px.get("floor_price") is not None:
                floor_fmt = f"{px['floor_price']:.2f}"
            if px.get("final_price") is not None:
                final_fmt = f"{px['final_price']:.2f}"

        rows.append({
            "sku": sku, "ean": ean, "brand": brand, "title": title,
            "asin": asin or "",
            "status": status,
            "score": "1.00" if status in ("listed", "catalog_match") else "0.50" if status == "catalog_ambiguous" else "0.00",
            "listed": listed,
            "provenance": "inventory-asin" if listed == "yes" else "supplier",
            "candidates": "[]",
            "stock": str(stock),  # <-- garantido 0/1/5/10
            "cost": "" if cost is None else f"{cost:.2f}",
            "competitor_price": "" if competitor_price is None else f"{competitor_price:.2f}",
            "floor_price": floor_fmt,
            "selling_price": final_fmt,
        })

    df = pd.DataFrame(rows)

    # ordenação previsível na UI (resiliente mesmo sem algumas colunas)
    if "status" not in df.columns:
        df["status"] = ""
    if "brand" not in df.columns:
        df["brand"] = ""
    if "title" not in df.columns:
        df["title"] = ""

    order = {"listed": 0, "catalog_match": 1, "catalog_ambiguous": 2, "missing_ean": 3, "not_found": 4}
    df["__ord"] = df["status"].map(lambda s: order.get(str(s), 9))
    df.sort_values(["__ord", "brand", "title"], inplace=True)
    df.drop(columns=["__ord"], inplace=True)

    tmp = CLASSIFIED_CSV + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, CLASSIFIED_CSV)
    return df
