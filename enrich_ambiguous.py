# enrich_ambiguous.py
# -*- coding: utf-8 -*-
"""
Preenche candidatos (ASINs sugeridos) para SKUs com status 'catalog_ambiguous'
no ficheiro data/produtos_classificados.csv, usando o asin_resolver.suggest_candidates.
Uso:
    python enrich_ambiguous.py --limit 5
"""
from __future__ import annotations
import os, json, argparse
import pandas as pd
from amazon_client import AmazonClient
from asin_resolver import suggest_candidates

DATA_DIR = "data"
CLASSIFIED_CSV = os.path.join(DATA_DIR, "produtos_classificados.csv")
PROCESSED_CSV  = os.path.join(DATA_DIR, "produtos_processados.csv")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5, help="máximo de candidatos por SKU")
    ap.add_argument("--simulate", action="store_true", help="força modo simulado")
    args = ap.parse_args()

    if not os.path.exists(CLASSIFIED_CSV):
        raise SystemExit("Ficheiro de classificados não existe. Corre /classify primeiro.")

    dfc = pd.read_csv(CLASSIFIED_CSV, dtype=str).fillna("")
    if dfc.empty:
        raise SystemExit("Classificados vazio.")

    dfp = pd.read_csv(PROCESSED_CSV, dtype=str).fillna("") if os.path.exists(PROCESSED_CSV) else None

    client = AmazonClient(simulate=args.simulate)

    # garantir colunas
    for c in ["sku","status","asin","brand","title","ean","candidates"]:
        if c not in dfc.columns: dfc[c] = ""

    count = 0
    for idx, row in dfc.iterrows():
        if str(row.get("status")) != "catalog_ambiguous":
            continue
        sku = str(row.get("sku") or "").strip()
        brand = str(row.get("brand") or "")
        title = str(row.get("title") or "")
        ean = str(row.get("ean") or "")

        # se o classificados estiver sem brand/title/ean, tentamos buscar dos processados
        if dfp is not None and (not brand or not title or not ean):
            m = dfp[dfp["sku"].astype(str) == sku]
            if not m.empty:
                if not brand: brand = str(m.iloc[0].get("brand") or "")
                if not title: title = str(m.iloc[0].get("title") or "")
                if not ean:   ean   = str(m.iloc[0].get("ean") or "")

        cand = suggest_candidates({"sku":sku,"brand":brand,"title":title,"ean":ean}, client, max_candidates=args.limit)
        dfc.at[idx, "candidates"] = json.dumps(cand, ensure_ascii=False)
        count += 1

    dfc.to_csv(CLASSIFIED_CSV, index=False, encoding="utf-8")
    print(f"Atualizado: {count} linhas 'catalog_ambiguous' com candidatos.")

if __name__ == "__main__":
    main()
