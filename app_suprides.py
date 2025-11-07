 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/app_suprides.py b/app_suprides.py
index fc8cd6a1b99b411fa377446c70e4d3e339353053..ed8a722ee317e4e61fffe459dca28a7b654cedaf 100644
--- a/app_suprides.py
+++ b/app_suprides.py
@@ -1,118 +1,203 @@
 # app_suprides.py
 # -*- coding: utf-8 -*-
 from __future__ import annotations
 
 import os
 import logging
+from datetime import datetime
 from typing import List, Dict, Any
 
 import pandas as pd
-from flask import Blueprint, request, jsonify, render_template, current_app
+from flask import (
+    Blueprint,
+    request,
+    jsonify,
+    render_template,
+    current_app,
+    send_file,
+    abort,
+    url_for,
+)
 
 from suprides_identify import classify_suprides_products
 from pricing_engine import calc_final_price
 from amazon_client import AmazonClient
+from suprides_sync import collect_for_sync
+from amazon_feed_csv import generate_feed_csv
 
 log = logging.getLogger("app_suprides")
 
 APP_ROOT = os.path.dirname(os.path.abspath(__file__))
 DATA_DIR = os.path.join(APP_ROOT, "data")
 os.makedirs(DATA_DIR, exist_ok=True)
 
 # Caminho único e absoluto para o CSV da Suprides
 CLASSIFIED_CSV = os.path.join(DATA_DIR, "suprides_classified.csv")
 SUPRIDES_CSV = CLASSIFIED_CSV  # alias para manter compatibilidade
+AMAZON_FEED_CSV = os.path.join(DATA_DIR, "amazon_suprides_feed.csv")
 
 DEFAULT_MARKETPLACE_ID = os.environ.get("DEFAULT_MARKETPLACE_ID", "").strip() or "A1RKKUPIHCS9HS"
 MARKETPLACE_ID = os.environ.get("MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)
 
 NEEDED_COLS = [
     "sku", "ean", "brand", "title", "asin", "status", "score", "listed",
     "provenance", "candidates", "stock", "cost", "competitor_price",
     "floor_price", "selling_price"
 ]
 
 bp = Blueprint("suprides", __name__, url_prefix="/suprides")
 
 
 def _ensure_columns(df: pd.DataFrame, needed: List[str] | None = None) -> pd.DataFrame:
     """
     Garante que o DataFrame tem todas as colunas 'needed'.
     Se 'needed' for None, usa NEEDED_COLS por defeito.
     """
     cols = needed or NEEDED_COLS
     for c in cols:
         if c not in df.columns:
             df[c] = ""
     return df
 
 
 def save_suprides_df(df: pd.DataFrame) -> None:
     """
     Normaliza e grava sempre no mesmo CSV absoluto.
     """
     df = _ensure_columns(df)
     df = df.astype(str).replace("nan", "").fillna("")
     tmp = SUPRIDES_CSV + ".tmp"
     df.to_csv(tmp, index=False, encoding="utf-8")
     os.replace(tmp, SUPRIDES_CSV)
 
 
 def load_suprides_df() -> pd.DataFrame:
     """
     Lê sempre do mesmo CSV absoluto.
     """
     if not os.path.exists(SUPRIDES_CSV):
         return pd.DataFrame(columns=NEEDED_COLS)
     df = pd.read_csv(SUPRIDES_CSV, dtype=str, encoding="utf-8")
     df = df.replace("nan", "").fillna("")
     return _ensure_columns(df)
 
 
+def _feed_metadata() -> dict:
+    if not os.path.exists(AMAZON_FEED_CSV):
+        return {"exists": False, "path": AMAZON_FEED_CSV, "rows": 0}
+    try:
+        stat = os.stat(AMAZON_FEED_CSV)
+        with open(AMAZON_FEED_CSV, "r", encoding="utf-8") as handle:
+            rows = max(0, sum(1 for _ in handle) - 1)
+        updated_at = datetime.utcfromtimestamp(int(stat.st_mtime)).isoformat() + "Z"
+    except Exception:
+        rows = 0
+        updated_at = ""
+    return {
+        "exists": True,
+        "path": AMAZON_FEED_CSV,
+        "rows": rows,
+        "updated_at": updated_at,
+    }
+
+
 def _pfloat(x):
     try:
         if x is None:
             return None
         s = str(x).strip().replace(",", ".")
         if s == "":
             return None
         return float(s)
     except Exception:
         return None
 
 
 def _summary_by_status(rows: List[dict]) -> dict:
     out = {"total": len(rows)}
     for r in rows:
         st = (r.get("status") or "").strip()
         if not st:
             continue
         out[st] = out.get(st, 0) + 1
     return out
 
 
+def _parse_limit(default: int = 200) -> int:
+    raw = (request.args.get("limit") or "").strip()
+    if not raw:
+        return default
+    try:
+        value = int(raw)
+        if value <= 0:
+            return default
+        return min(value, 5000)
+    except Exception:
+        return default
+
+
+@bp.route("/sync/feed", methods=["POST"])
+def sync_feed_csv():
+    """Executa a recolha da Suprides e grava um CSV compatível com feeds Amazon."""
+    limit = _parse_limit()
+    marketplace_id = (request.args.get("marketplace_id") or MARKETPLACE_ID).strip() or MARKETPLACE_ID
+    try:
+        offers, prices, total = collect_for_sync(max_items=limit)
+    except Exception as exc:
+        log.exception("Falha na recolha Suprides para feed CSV: %s", exc)
+        return jsonify({"success": False, "error": str(exc)}), 500
+
+    result = generate_feed_csv(offers, prices, AMAZON_FEED_CSV, marketplace_id)
+    download_url = url_for("suprides.download_feed_csv", _external=True)
+    return jsonify(
+        {
+            "success": True,
+            "offers": len(offers),
+            "prices": len(prices),
+            "total_processed": total,
+            "csv": result["path"],
+            "rows": result["rows"],
+            "columns": result["columns"],
+            "marketplace_id": marketplace_id,
+            "download_url": download_url,
+        }
+    )
+
+
+@bp.route("/feed.csv", methods=["GET"])
+def download_feed_csv():
+    if not os.path.exists(AMAZON_FEED_CSV):
+        abort(404, description="Ainda não existe CSV gerado. Executa /suprides/sync/feed primeiro.")
+    return send_file(
+        AMAZON_FEED_CSV,
+        mimetype="text/csv",
+        as_attachment=True,
+        download_name=os.path.basename(AMAZON_FEED_CSV),
+    )
+
+
 @bp.route("/suprides/classify", methods=["GET"])
 def suprides_classify_route():
     """
     Dispara a classificação Suprides e grava o CSV para posterior leitura no UI.
     """
     df = classify_suprides_products(simulate=False)  # garante simulate=False
 
     if df is None or df.empty:
         # grava CSV vazio com headers (para a UI saber as colunas)
         save_suprides_df(pd.DataFrame(columns=NEEDED_COLS))
         return jsonify({"success": True, "rows": 0, "csv": SUPRIDES_CSV})
 
     # normaliza headers (caso a function devolva nomes alternativos)
     rename_map = {
         "name": "title",
         "qty_available": "stock",
         "price_cost": "cost",
         "final_price": "selling_price",
     }
     df = df.rename(columns=rename_map)
 
     # garante colunas esperadas e strings e grava
     save_suprides_df(df)
     return jsonify({"success": True, "rows": int(df.shape[0]), "csv": SUPRIDES_CSV})
 
 
EOF
)
