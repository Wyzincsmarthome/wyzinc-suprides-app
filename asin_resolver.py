# asin_resolver.py
# -*- coding: utf-8 -*-
"""
Regras de resolução de ASIN:
- EAN estrito: catalog_match se a Amazon devolver items para identifiers=EAN
  (matched_by_query=True) OU se o EAN existir na lista extraída do item.
- Sem EAN válido: gera candidatos por keywords (marca + título) com scoring.
"""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Dict, List
from amazon_client import AmazonClient

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _digits(s: str) -> str:
    return "".join(ch for ch in str(s or "") if ch.isdigit())

def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio() if a and b else 0.0

def _token_overlap(a: str, b: str) -> float:
    ta = set([t for t in re.split(r"[^\w]+", a) if t])
    tb = set([t for t in re.split(r"[^\w]+", b) if t])
    if not ta or not tb: return 0.0
    inter = len(ta & tb)
    return inter / max(len(ta), len(tb))

def _build_keywords(brand: str, title: str) -> str:
    brand = (brand or "").strip()
    title = (title or "").strip()
    if brand and brand.lower() not in title.lower():
        return f"{brand} {title}"
    return title or brand

def suggest_candidates(row: Dict, client: AmazonClient, max_candidates: int = 5) -> List[Dict]:
    """Procura por keywords e devolve [{asin,title,brand,score} ...] ordenado por score."""
    brand = _norm(row.get("brand") or "")
    title = _norm(row.get("title") or "")
    kw = _build_keywords(brand, title)
    results = client.catalog_search_by_keywords(kw, limit=max_candidates * 3) or []

    scored: List[Dict] = []
    for it in results:
        it_title = _norm(it.get("title") or "")
        it_brand = _norm(it.get("brand") or "")
        sim = _similarity(title, it_title)
        tok = _token_overlap(title, it_title)
        brand_bonus = 0.2 if (brand and it_brand and (brand == it_brand or brand in it_brand or it_brand in brand)) else 0.0
        score = 0.6 * sim + 0.3 * tok + brand_bonus
        scored.append({
            "asin": it.get("asin"),
            "title": it.get("title") or "",
            "brand": it.get("brand") or "",
            "score": round(float(score), 3)
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    # única e top-N
    seen = set()
    out: List[Dict] = []
    for c in scored:
        if c["asin"] and c["asin"] not in seen:
            out.append(c)
            seen.add(c["asin"])
        if len(out) >= max_candidates:
            break
    return out

def resolve_asin(row: Dict, client: AmazonClient) -> Dict:
    """
    Entrada esperada: {sku, ean, brand, title}
    Saída: {status, asin, score, candidates}
    """
    brand = _norm(row.get("brand") or "")
    title = _norm(row.get("title") or "")
    ean   = _digits(row.get("ean") or "")

    # 1) EAN estrito
    if ean:
        items = client.catalog_search_by_ean(ean) or []
        if items:
            query_confirmed = any(bool(it.get("matched_by_query")) for it in items)
            exact = [it for it in items if ean in (it.get("eans") or [])]
            if exact:
                # preferir coincidência de marca quando possível
                best = None
                for it in exact:
                    ib = _norm(it.get("brand") or "")
                    if brand and ib and (brand == ib or brand in ib or ib in brand):
                        best = it; break
                if best is None: best = exact[0]
                return {"status":"catalog_match","asin":best.get("asin"),"score":0.99,"candidates":[]}
            if query_confirmed:
                best = None
                for it in items:
                    ib = _norm(it.get("brand") or "")
                    if brand and ib and (brand == ib or brand in ib or ib in brand):
                        best = it; break
                if best is None: best = items[0]
                return {"status":"catalog_match","asin":best.get("asin"),"score":0.95,"candidates":[]}
        # tinha EAN mas não bateu → ambiguous com candidatos
        cand = suggest_candidates(row, client, max_candidates=5)
        return {"status":"catalog_ambiguous","asin":None,"score":0.0,"candidates":cand}

    # 2) Sem EAN → ambiguous com candidatos
    cand = suggest_candidates(row, client, max_candidates=5)
    return {"status":"catalog_ambiguous","asin":None,"score":0.0,"candidates":cand}
