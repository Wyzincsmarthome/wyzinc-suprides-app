"""Utilities to collect and prepare Suprides catalogue data for Amazon feeds."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from pricing_engine import calc_final_price
from supplier_suprides import normalize
from suprides_client import SupridesClient

log = logging.getLogger(__name__)


def collect_for_sync(
    max_items: int = 200,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """Return offers, prices and total products obtained from Suprides.

    Args:
        max_items: Maximum number of offers to include in the output list. We
            continue iterating through the remote catalogue to keep an accurate
            ``total_collected`` counter even after reaching the limit.
    """

    offers: List[Dict[str, Any]] = []
    prices: List[Dict[str, Any]] = []
    total_collected = 0

    client = SupridesClient()

    for raw_item in client.iter_products(limit=100):
        item = normalize(raw_item)
        total_collected += 1

        sku = item.get("sku")
        if not sku:
            continue

        if len(offers) >= max_items:
            # Keep counting items but avoid adding new offers once the limit is
            # reached so callers can cap feed size.
            continue

        qty = item.get("qty_available") or 0
        offer: Dict[str, Any] = {"sku": sku, "quantity": qty}

        price_cost = item.get("price_cost")
        if price_cost is not None:
            final_price = None
            try:
                price_info = calc_final_price(cost=price_cost, competitor_price=None)
                final_price = price_info.get("final_price")
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception(
                    "calc_final_price falhou para sku=%s cost=%s: %s", sku, price_cost, exc
                )
            if final_price is not None:
                offer["price"] = final_price
                prices.append({"sku": sku, "price": final_price, "currency": "EUR"})

        offers.append(offer)

    return offers, prices, total_collected
