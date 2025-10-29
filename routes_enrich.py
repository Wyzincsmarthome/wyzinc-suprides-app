# routes_enrich.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from typing import List, Dict

import pandas as pd
from flask import Blueprint, request, redirect, url_for, jsonify

from amazon_client import AmazonClient
from asin_resolver import suggest_candidates

bp_enrich = Blueprint("bp_enrich", __name__)

DATA_DIR = "data"
CLASSIFIED_CSV = os.path.join(DATA_DIR, "produtos_classificados.csv")
PROCESSED_CSV  = os.path.join(DATA_DIR, "produtos_processados.csv")

def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

@bp_enrich.route("/enrich_ambiguous")
def enrich_ambiguous():
    """
    GET /enrich_ambiguous?limit=5&simulate=0&json=0
    - Preenche 'candidates' para linhas com status 'catalog_ambiguous'
    - Guarda em data/produtos_classificados.csv
    - Por default faz redirect para /review_classified; se json=1, devolve um resumo JSON
    """
    limit = int(request.args.get("limit", "5") or 5)
    simulate = (str(request.args.get("simulate", "0")).lower() in ("1", "true", "yes", "on"))
    want_json = (str(request.args.get("json", "0")).lower() in ("1", "true", "yes", "on"))

    if not os.path.exists(CLASSIFIED_CSV):
        msg = "Ficheiro 'produtos_classificados.csv' não existe. Corre /classify primeiro."
        if want_json:
            return jsonify({"success": False, "error": msg}), 400
        # fallback: leva para a página de classificação com erro
        return redirect(url_for("review_classified"))

    dfc = pd.read_csv(CLASSIFIED_CSV, dtype=str).fillna("")
    dfc = _ensure_cols(dfc, ["sku","status","asin","brand","title","ean","candidates"])

    dfp = None
    if os.path.exists(PROCESSED_CSV):
        dfp = pd.read_csv(PROCESSED_CSV, dtype=str).fillna("")

    client = AmazonClient(simulate=simulate)

    updated = 0
    total_targets = 0
    items_summary: List[Dict] = []

    for idx, row in dfc.iterrows():
        if str(row.get("status") or "") != "catalog_ambiguous":
            continue
        total_targets += 1

        sku   = str(row.get("sku") or "").strip()
        brand = str(row.get("brand") or "")
        title = str(row.get("title") or "")
        ean   = str(row.get("ean") or "")

        # se faltarem campos, tenta puxar dos processados
        if dfp is not None and (not brand or not title or not ean):
            m = dfp[dfp["sku"].astype(str) == sku]
            if not m.empty:
                if not brand: brand = str(m.iloc[0].get("brand") or "")
                if not title: title = str(m.iloc[0].get("title") or "")
                if not ean:   ean   = str(m.iloc[0].get("ean") or "")

        cand = suggest_candidates({"sku":sku,"brand":brand,"title":title,"ean":ean}, client, max_candidates=limit)
        dfc.at[idx, "candidates"] = json.dumps(cand, ensure_ascii=False)
        updated += 1

        if want_json:
            items_summary.append({
                "sku": sku,
                "added": len(cand),
            })

    dfc.to_csv(CLASSIFIED_CSV, index=False, encoding="utf-8")

    if want_json:
        return jsonify({
            "success": True,
            "updated_rows": updated,
            "total_ambiguous": total_targets,
            "items": items_summary
        })

    # redireciona de volta à lista
    return redirect(url_for("review_classified"))
