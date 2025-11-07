 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/tests/test_amazon_feed_csv.py b/tests/test_amazon_feed_csv.py
new file mode 100644
index 0000000000000000000000000000000000000000..d5c8dc8f420b8f0b328d7b43d129c988dda1f9d4
--- /dev/null
+++ b/tests/test_amazon_feed_csv.py
@@ -0,0 +1,64 @@
+import csv
+from pathlib import Path
+
+from amazon_feed_csv import build_feed_rows, generate_feed_csv, DEFAULT_COLUMNS
+
+
+def test_build_feed_rows_merges_offers_and_prices():
+    offers = [
+        {"sku": "SUP001", "quantity": 5},
+        {"sku": "SUP002", "quantity": 0, "fulfillment_latency": 2},
+    ]
+    prices = [
+        {"sku": "SUP001", "price": 12.345, "currency": "EUR", "min_price": 10, "max_price": 20},
+        {"sku": "SUP003", "price": "7,80", "currency": "EUR"},
+    ]
+
+    rows = build_feed_rows(offers, prices, "A1TEST")
+
+    assert {row["sku"] for row in rows} == {"SUP001", "SUP002", "SUP003"}
+    row_1 = next(r for r in rows if r["sku"] == "SUP001")
+    assert row_1["quantity"] == "5"
+    assert row_1["price"] == "12.35"
+    assert row_1["min_price"] == "10.00"
+    assert row_1["max_price"] == "20.00"
+    assert row_1["currency"] == "EUR"
+    assert row_1["marketplace_id"] == "A1TEST"
+
+    row_2 = next(r for r in rows if r["sku"] == "SUP002")
+    assert row_2["quantity"] == "0"
+    assert row_2["price"] == ""
+    assert row_2["fulfillment_latency"] == "2"
+
+    row_3 = next(r for r in rows if r["sku"] == "SUP003")
+    assert row_3["quantity"] == ""
+    assert row_3["price"] == "7.80"
+
+
+def test_generate_feed_csv_writes_file(tmp_path):
+    offers = [{"sku": "SUP100", "quantity": 3, "price": 15.5}]
+    prices = [{"sku": "SUP100", "price": 15.5, "currency": "EUR"}]
+
+    output_path = tmp_path / "amazon_feed.csv"
+    result = generate_feed_csv(offers, prices, output_path, "A1RKKUPIHCS9HS")
+
+    assert result["rows"] == 1
+    assert result["columns"] == list(DEFAULT_COLUMNS)
+    assert Path(result["path"]).exists()
+
+    with output_path.open("r", encoding="utf-8") as handle:
+        reader = csv.DictReader(handle)
+        rows = list(reader)
+    assert rows == [
+        {
+            "sku": "SUP100",
+            "quantity": "3",
+            "price": "15.50",
+            "currency": "EUR",
+            "min_price": "",
+            "max_price": "",
+            "fulfillment_latency": "",
+            "operation_type": "Update",
+            "marketplace_id": "A1RKKUPIHCS9HS",
+        }
+    ]
 
EOF
)
