# product_identify.py
# -*- coding: utf-8 -*-
"""
Classificação de produtos (rápida e correta):
- Lê data/produtos_processados.csv (fornecedor, já normalizado)
- Lê data/my_inventory.csv (Amazon)
- Marca 'listed' se:
    a) seller_sku do inventário == sku do processado, OU
    b) ASIN do inventário coincide com o ASIN resolvido (mesmo que o SKU seja diferente)
- Resolve ASIN apenas por EAN (estrito), sem gerar candidatos (mais rápido).
  Candidatos por keywords são gerados em /enrich_ambiguous quando necessário.
- Grava data/produtos_classificados.csv
"""

from __future__ import annotations
import os, json, logging
from typing import Dict, Any, List
import pandas as pd

from amazon_client import AmazonClient
# NOTA: não importamos asin_resolver aqui para evitar buscas por keywords na fase de classificar

log = logging.getLogger(__name__)

DATA_DIR = "data"
PROCESSED_CSV   = os.path.join(DATA_DIR, "produtos_processados.csv")
CLASSIFIED_CSV  = os.path.join(DATA_DIR, "produtos_classificados.csv")
INVENTORY_CSV   = os.path.join(DATA_DIR, "my_inventory.csv")
EAN_CACHE_JSON  = os.path.join(DATA_DIR, "cache_ean_to_asin.json")

def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")

def _norm_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def _digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _load_ean_cache() -> Dict[str, Dict[str, str]]:
    try:
        if os.path.exists(EAN_CACHE_JSON):
            with open(EAN_CACHE_JSON, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

def _save_ean_cache(cache: Dict[str, Dict[str, str]]) -> None:
    try:
        with open(EAN_CACHE_JSON, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _resolve_asin_by_ean(client: AmazonClient, ean: str) -> Dict[str, Any]:
    """
    Devolve {"asin": str|"" , "status": "catalog_match"|"catalog_ambiguous", "score": float}
    Regras:
      - Se a Amazon devolve items para identifiers=EAN → catalog_match (mesmo que não exponha o EAN no payload).
      - Se não devolver nada → catalog_ambiguous (sem candidatos nesta fase).
    """
    ean = _digits(ean)
    if not ean:
        return {"asin": "", "status": "catalog_ambiguous", "score": 0.0}

    items = client.catalog_search_by_ean(ean) or []
    if not items:
        return {"asin": "", "status": "catalog_ambiguous", "score": 0.0}

    # Preferir item com EAN exposto; senão, qualquer item (Amazon confirmou a query)
    exact = [it for it in items if ean in (it.get("eans") or [])]
    best = None
    if exact:
        best = exact[0]
        return {"asin": best.get("asin") or "", "status": "catalog_match", "score": 0.99}
    else:
        best = items[0]
        return {"asin": best.get("asin") or "", "status": "catalog_match", "score": 0.95}

def classify_products(simulate: bool = False, seller_id: str | None = None) -> pd.DataFrame:
    """
    Gera data/produtos_classificados.csv com colunas:
      sku, ean, brand, title, asin, status, score, listed, candidates, provenance,
      cost, stock, selling_price, floor_price, competitor_price
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    dfp = _read_csv(PROCESSED_CSV)
    if dfp.empty:
        raise RuntimeError("Sem produtos processados. Faz upload do CSV primeiro.")

    # Campos úteis para a UI
    dfp = _norm_cols(dfp, [
        "sku", "ean", "brand", "title",
        "cost", "stock", "selling_price", "floor_price", "competitor_price"
    ])

    # Inventário Amazon (podem variar cabeçalhos entre contas/reportes)
    dfi_raw = _read_csv(INVENTORY_CSV)
    inv_by_sku: Dict[str, Dict[str, str]] = {}
    inv_by_asin: Dict[str, Dict[str, str]] = {}
    if not dfi_raw.empty:
        dfi = dfi_raw.rename(columns={
            "seller_sku": "sku",
            "SellerSKU": "sku",
            "asin1": "asin",
            "ASIN": "asin",
            "asin": "asin",
        }).copy()
        dfi = _norm_cols(dfi, ["sku", "asin", "status", "price", "quantity"])
        dfi["sku"]  = dfi["sku"].astype(str).str.strip()
        dfi["asin"] = dfi["asin"].astype(str).str.strip().str.upper()
        for _, row in dfi.iterrows():
            sku_i  = row.get("sku") or ""
            asin_i = (row.get("asin") or "").upper()
            if sku_i:
                inv_by_sku[sku_i] = row
            if asin_i:
                inv_by_asin[asin_i] = row

    client = AmazonClient(simulate=simulate)

    # cache EAN->ASIN para acelerar run-to-run
    cache = _load_ean_cache()

    rows_out: List[Dict[str, Any]] = []
    for _, r in dfp.iterrows():
        sku = str(r.get("sku") or "").strip()
        ean = _digits(r.get("ean") or "")
        brand = str(r.get("brand") or "")
        title = str(r.get("title") or "")

        base = {
            "sku": sku,
            "ean": str(r.get("ean") or "").strip(),
            "brand": brand,
            "title": title,
            "cost": str(r.get("cost") or ""),
            "stock": str(r.get("stock") or ""),
            "selling_price": str(r.get("selling_price") or ""),
            "floor_price": str(r.get("floor_price") or ""),
            "competitor_price": str(r.get("competitor_price") or ""),
        }

        # 1) Se o SKU já existe no inventário → listed (usa ASIN do inventário)
        inv_row = inv_by_sku.get(sku)
        if inv_row:
            asin_inv = (inv_row.get("asin") or "").upper()
            rows_out.append({
                **base,
                "asin": asin_inv,
                "status": "listed",
                "score": "1.00",
                "listed": "yes",
                "candidates": "[]",
                "provenance": "inventory-sku"
            })
            continue

        # 2) Não existe por SKU — tenta resolver por EAN (cache → SP-API)
        asin = ""
        status = "catalog_ambiguous"
        score: float | str = 0.0

        if ean:
            cached = cache.get(ean)
            if cached:
                asin = (cached.get("asin") or "").upper()
                status = cached.get("status") or status
                score  = cached.get("score") if cached.get("score") is not None else score
            else:
                res = _resolve_asin_by_ean(client, ean)
                asin = (res.get("asin") or "").upper()
                status = res.get("status") or status
                score  = res.get("score") if res.get("score") is not None else score
                cache[ean] = {"asin": asin, "status": status, "score": score}

        # 3) Se resolvemos ASIN e esse ASIN existe no inventário → listed (mesmo com SKU diferente)
        if asin and asin in inv_by_asin:
            rows_out.append({
                **base,
                "asin": asin,
                "status": "listed",
                "score": "1.00",
                "listed": "yes",
                "candidates": "[]",
                "provenance": "inventory-asin"
            })
            continue

        # 4) Caso contrário, mantém o resultado da resolução por EAN (sem candidatos nesta fase)
        rows_out.append({
            **base,
            "asin": asin,
            "status": status,
            "score": score,
            "listed": "no",
            "candidates": "[]",   # candidatos só em /enrich_ambiguous
            "provenance": ("ean" if status == "catalog_match" else ""),
        })

    # persistir cache e CSV final
    _save_ean_cache(cache)
    dfc = pd.DataFrame(rows_out)
    dfc.to_csv(CLASSIFIED_CSV, index=False, encoding="utf-8")
    return dfc

if __name__ == "__main__":
    df = classify_products(simulate=False)
    print(f"Classificação concluída: {len(df)} linhas -> {CLASSIFIED_CSV}")
