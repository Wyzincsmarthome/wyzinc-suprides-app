diff --git a/amazon_feed_csv.py b/amazon_feed_csv.py
new file mode 100644
index 0000000000000000000000000000000000000000..b66db7bb3fd7d11c7518a71d90845c91f870222a
--- /dev/null
+++ b/amazon_feed_csv.py
@@ -0,0 +1,145 @@
+"""Utility helpers to build a CSV compatible with Amazon feed expectations."""
+from __future__ import annotations
+
+import csv
+import os
+from pathlib import Path
+from typing import Dict, Iterable, List, Sequence
+
+DEFAULT_COLUMNS: Sequence[str] = (
+    "sku",
+    "quantity",
+    "price",
+    "currency",
+    "min_price",
+    "max_price",
+    "fulfillment_latency",
+    "operation_type",
+    "marketplace_id",
+)
+
+
+def _as_str(value) -> str:
+    if value is None:
+        return ""
+    return str(value)
+
+
+def _format_quantity(value) -> str:
+    if value in (None, ""):
+        return ""
+    try:
+        return str(int(float(str(value).replace(",", "."))))
+    except Exception:
+        return ""
+
+
+def _format_price(value) -> str:
+    if value in (None, ""):
+        return ""
+    try:
+        return f"{float(str(value).replace(',', '.')):.2f}"
+    except Exception:
+        return ""
+
+
+def _format_latency(value) -> str:
+    if value in (None, ""):
+        return ""
+    try:
+        return str(int(float(str(value).replace(",", "."))))
+    except Exception:
+        return ""
+
+
+def _normalise_sku(raw) -> str:
+    if not raw:
+        return ""
+    return str(raw).strip()
+
+
+def build_feed_rows(
+    offers: Iterable[Dict[str, object]],
+    prices: Iterable[Dict[str, object]],
+    marketplace_id: str,
+) -> List[Dict[str, str]]:
+    """Merge offer and price data into stringified rows ready for CSV export."""
+    price_by_sku: Dict[str, Dict[str, object]] = {}
+    for price in prices:
+        sku = _normalise_sku(price.get("sku"))
+        if not sku:
+            continue
+        price_by_sku[sku] = price
+
+    rows: List[Dict[str, str]] = []
+    seen: set[str] = set()
+
+    for offer in offers:
+        sku = _normalise_sku(offer.get("sku") or offer.get("seller_sku"))
+        if not sku:
+            continue
+        seen.add(sku)
+        price_info = price_by_sku.get(sku, {})
+        row = {
+            "sku": sku,
+            "quantity": _format_quantity(offer.get("quantity")),
+            "price": _format_price(price_info.get("price") or offer.get("price")),
+            "currency": _as_str(price_info.get("currency") or offer.get("currency") or "EUR"),
+            "min_price": _format_price(price_info.get("min_price") or offer.get("min_price")),
+            "max_price": _format_price(price_info.get("max_price") or offer.get("max_price")),
+            "fulfillment_latency": _format_latency(offer.get("fulfillment_latency")),
+            "operation_type": _as_str(offer.get("operation_type") or "Update"),
+            "marketplace_id": _as_str(marketplace_id),
+        }
+        rows.append(row)
+
+    for sku, price_info in price_by_sku.items():
+        if sku in seen:
+            continue
+        rows.append(
+            {
+                "sku": sku,
+                "quantity": "",
+                "price": _format_price(price_info.get("price")),
+                "currency": _as_str(price_info.get("currency") or "EUR"),
+                "min_price": _format_price(price_info.get("min_price")),
+                "max_price": _format_price(price_info.get("max_price")),
+                "fulfillment_latency": "",
+                "operation_type": "Update",
+                "marketplace_id": _as_str(marketplace_id),
+            }
+        )
+
+    rows.sort(key=lambda r: r["sku"])
+    return rows
+
+
+def write_feed_csv(rows: Iterable[Dict[str, str]], path: os.PathLike[str] | str, columns: Sequence[str] = DEFAULT_COLUMNS) -> Path:
+    """Write rows to a CSV using an atomic replace strategy."""
+    target = Path(path)
+    target.parent.mkdir(parents=True, exist_ok=True)
+    tmp_path = target.with_suffix(target.suffix + ".tmp")
+    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
+        writer = csv.DictWriter(handle, fieldnames=list(columns))
+        writer.writeheader()
+        for row in rows:
+            writer.writerow({column: row.get(column, "") for column in columns})
+    tmp_path.replace(target)
+    return target
+
+
+def generate_feed_csv(
+    offers: Iterable[Dict[str, object]],
+    prices: Iterable[Dict[str, object]],
+    path: os.PathLike[str] | str,
+    marketplace_id: str,
+    columns: Sequence[str] = DEFAULT_COLUMNS,
+) -> Dict[str, object]:
+    """High level helper to build and persist the CSV feed."""
+    rows = build_feed_rows(offers, prices, marketplace_id)
+    output_path = write_feed_csv(rows, path, columns)
+    return {
+        "path": str(output_path),
+        "rows": len(rows),
+        "columns": list(columns),
+    }
