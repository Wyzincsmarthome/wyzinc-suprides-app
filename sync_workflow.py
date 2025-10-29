# sync_workflow.py
# -*- coding: utf-8 -*-
"""
Sincronização de ofertas (preço + stock) para Amazon via JSON_LISTINGS_FEED.

- Lê CSVs processados/classificados (data/produtos_processados.csv, data/produtos_classificados.csv)
- Recebe uma lista de linhas selecionadas (cada uma com pelo menos 'sku')
- Constrói mensagens com:
    * preço (our_price), quantity e lead time (se disponível)
    * identificadores de matching:
        - ASIN: attributes.merchant_suggested_asin
        - EAN/UPC/GTIN opcional: attributes.externally_assigned_product_identifier
- Submete feed JSON_LISTINGS_FEED e guarda o relatório.

Nota: não usa feeds legados. Só envia LISTING_OFFER_ONLY.
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from amazon_client import AmazonClient

log = logging.getLogger(__name__)


# ----------------------------
# Helpers de IO
# ----------------------------

def _read_csv(path: str, dtype: Optional[dict] = None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Falta {path}. Gera/atualiza este CSV antes de sincronizar.")
    return pd.read_csv(path, dtype=dtype).fillna("")


def _load_processed() -> pd.DataFrame:
    # Contém colunas como: sku, price, selling_price, stock, ean/barcode...
    return _read_csv("data/produtos_processados.csv")


def _load_classified() -> pd.DataFrame:
    # Contém colunas como: sku, asin, existence (listed/catalog_match/...), listed (yes/no)
    return _read_csv("data/produtos_classificados.csv", dtype=str)


def _norm_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    try:
        txt = str(s).strip()
        if not txt:
            return None
        return float(txt.replace(",", "."))
    except Exception:
        return None


def _norm_int(s: Any) -> int:
    try:
        txt = str(s).strip()
        if not txt:
            return 0
        v = int(float(txt.replace(",", ".")))
        return max(0, v)
    except Exception:
        return 0


def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        t = str(v).strip() if v is not None else ""
        if t:
            return t
    return ""


# ----------------------------
# Core
# ----------------------------

def plan_and_sync(selected_rows: List[Dict[str, Any]], simulate: bool = True) -> Dict[str, Any]:
    """
    Planeia e executa a sincronização de ofertas (preço+stock) via JSON_LISTINGS_FEED.

    Args:
        selected_rows: lista de dicts com pelo menos {"sku": "..."}.
                       Se trouxerem "asin" e/ou "ean", são usados com prioridade.
        simulate: se True, não envia feed (apenas conta e guarda o plano).

    Returns:
        dict com resumo, feeds submetidos e caminho do relatório (quando aplicável).
    """
    df_proc = _load_processed()
    df_cls = _load_classified()

    if not selected_rows:
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "offers_updated": 0,
            "feeds": {},
            "note": "Sem SKUs selecionadas."
        }

    sel = pd.DataFrame(selected_rows)
    sel["sku"] = sel["sku"].astype(str)

    # Merge com processados (preço/stock/ean) e classificados (asin/existence/listed)
    merged = sel.merge(df_proc, on="sku", how="left", suffixes=("", "_p"))
    merged = merged.merge(
        df_cls.rename(columns={"asin": "asin_cls", "existence": "existence_cls", "listed": "listed_cls"}),
        on="sku", how="left"
    )

    # Escolha final de ASIN e indicadores de estado
    merged["asin"] = merged.apply(
        lambda r: _first_nonempty(r.get("asin"), r.get("asin_cls")), axis=1
    )
    merged["existence"] = merged.apply(
        lambda r: _first_nonempty(r.get("existence"), r.get("existence_cls")), axis=1
    )
    merged["listed"] = merged.apply(
        lambda r: _first_nonempty(r.get("listed"), r.get("listed_cls")), axis=1
    )

    # Fonte de EAN/GTIN: tentar várias colunas comuns
    possible_gtin_cols = ["ean", "EAN", "barcode", "gtin", "upc", "BARCODE"]
    for col in possible_gtin_cols:
        if col not in merged.columns:
            merged[col] = ""

    def _pick_gtin(row) -> str:
        return _first_nonempty(*(row.get(c, "") for c in possible_gtin_cols))

    merged["gtin"] = merged.apply(_pick_gtin, axis=1)

    # Determinar conjunto a atualizar (preço/stock) — já devem estar listadas ou com match em catálogo
    to_update = merged[merged["existence"].isin(["listed", "catalog_match"])].copy()

    # Preparar linhas para o builder (SKU, preço, stock, ASIN, EAN)
    rows: List[Dict[str, Any]] = []
    for _, row in to_update.iterrows():
        sku = str(row.get("sku", "")).strip()
        asin = str(row.get("asin", "")).strip().upper()
        ean = str(row.get("gtin", "")).strip()

        # Quantidade
        qty = _norm_int(row.get("stock", row.get("quantity")))
        # Preço preferencial
        price = _norm_float(_first_nonempty(row.get("selling_price"), row.get("price")))

        item: Dict[str, Any] = {
            "sku": sku,
            "asin": asin,      # identificador principal para matching
            "ean": ean,        # opcional (externally_assigned_product_identifier)
            "quantity": qty,
            "price": price,
            "currency": "EUR",
        }
        rows.append(item)

    # Construir mensagens JSON_LISTINGS_FEED
    client = AmazonClient(simulate=simulate)
    messages = client.build_offer_update_messages(rows=rows, default_currency="EUR", product_type="PRODUCT")

    summary: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
        "offers_updated": len(rows),
        "feeds": {},
        "simulate": bool(simulate),
    }

    if simulate or not messages:
        # Guardar plano de sincronização (dry-run)
        os.makedirs("data", exist_ok=True)
        with open(f"data/sync_dryrun_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
            json.dump({"rows": rows, "messages": messages}, f, ensure_ascii=False, indent=2)
        return summary

    # Enviar feed JSON_LISTINGS_FEED
    try:
        info = client.submit_json_listings_feed(messages, use_gzip=False)
        st = client.wait_feed_done(info["feedId"])
        report_file = client.save_processing_report(info["feedId"], "JSON_LISTINGS_FEED")
        summary["feeds"]["offer"] = {
            "feedId": info.get("feedId"),
            "status": st.get("status"),
            "report_file": report_file,
            "last_status_json": st.get("last_status_json"),
        }
    except Exception as exc:
        log.exception("Falha ao enviar JSON_LISTINGS_FEED")
        summary["feeds"]["offer"] = {"error": str(exc)}

    return summary


if __name__ == "__main__":
    # Execução direta para teste rápido (dry-run)
    logging.basicConfig(level=logging.INFO)
    demo_rows = [{"sku": "DEMO-SKU-1"}, {"sku": "DEMO-SKU-2"}]
    result = plan_and_sync(demo_rows, simulate=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
