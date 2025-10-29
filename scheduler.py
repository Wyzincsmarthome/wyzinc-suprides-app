# scheduler.py
import os, logging
from apscheduler.schedulers.background import BackgroundScheduler
from suprides_client import SupridesClient
from supplier_suprides import normalize
from pricing_engine import calc_final_price  # o teu módulo atual
from spapi_wrapper import publish_offer_feed, patch_quantity  # teus helpers

MARKETPLACE_ID = os.getenv("SPAPI_MARKETPLACE_ID_ES", "A1RKKUPIHCS9HS")

def tick():
    client = SupridesClient()
    batch = []
    for raw in client.iter_products(limit=100):
        n = normalize(raw)
        if not n.get("allowed_brand"):  # só Baseus, conforme pedido
            continue
        # qty pelas regras
        qty = n["qty_suggested"]
        # preço com o teu motor
        if n["price_cost"] is None:
            continue
        price = calc_final_price(cost=n["price_cost"], brand=n["brand"])
        batch.append({"sku": n["sku_supplier"], "qty": qty, "price": price, "ean": n.get("ean"), "manual": n["needs_manual_match"]})

    # 1) PATCH rápido para qty onde já houver SKU listado
    for item in batch:
        try:
            patch_quantity(spapi_client, item["sku"], MARKETPLACE_ID, item["qty"])
        except Exception as e:
            logging.warning("patch qty falhou %s: %s", item["sku"], e)

    # 2) Feed de preço em lote
    offers_for_feed = [b for b in batch if b["price"] is not None]
    if offers_for_feed:
        publish_offer_feed(spapi_client, offers_for_feed, MARKETPLACE_ID)

def start_scheduler():
    sched = BackgroundScheduler(timezone="Europe/Lisbon")
    sched.add_job(tick, "interval", hours=12, id="suprides_sync", coalesce=True, max_instances=1)
    sched.start()
