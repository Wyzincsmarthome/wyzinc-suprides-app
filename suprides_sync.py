"""
suprides_sync.py
~~~~~~~~~~~~~~~~~

Este módulo encapsula a lógica de recolha de produtos da API da Suprides,
aplica normalização e calcula as ofertas (quantidades) e preços finais
para serem enviados para a Amazon. Expõe a função `collect_for_sync` que
retorna três listas: offers, prices e total de itens recolhidos.

Cada dicionário em `offers` deve ter as chaves:
  - 'sku' (identificador do produto para a Amazon)
  - 'quantity' (stock a publicar)
  - opcionalmente 'price' se pretender enviar o preço no mesmo feed

Cada dicionário em `prices` deve ter as chaves:
  - 'sku'
  - 'price' (preço final de venda)
  - 'currency' (por defeito EUR)

Uso:
    offers, prices, total = collect_for_sync(max_items=200)
"""

from typing import List, Tuple, Dict, Any
from suprides_client import SupridesClient
from supplier_suprides import normalize
from pricing_engine import calc_final_price

def collect_for_sync(max_items: int = 200) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """
    Collect up to ``max_items`` products from Suprides, normalize them and
    build structures suitable for Amazon inventory and pricing feeds.

    Each returned offer includes the SKU and quantity available. When a cost
    is provided for an item, the final selling price is computed using
    ``calc_final_price`` and included in both the offer and a separate price
    update structure.

    Args:
        max_items: maximum number of items to collect. Defaults to 200.

    Returns:
        A tuple ``(offers, prices, total_collected)`` where:

          - ``offers``: list of dictionaries with keys ``sku`` and ``quantity``.
            The ``price`` key will be present when a cost is available.
          - ``prices``: list of dictionaries with keys ``sku``, ``price`` and
            ``currency``. A price record is added only when a cost is available.
          - ``total_collected``: the total number of items processed (including
            those without SKU).

    Notes:
        The original implementation relied on keys such as ``allowed_brand``,
        ``sku_supplier`` and ``qty_suggested`` on the normalized item. Those keys
        are not provided by ``supplier_suprides.normalize``, which returns
        ``sku``, ``ean``, ``brand``, ``name``, ``price_cost`` and ``qty_available``.
        This implementation instead processes all items and uses ``sku`` and
        ``qty_available`` directly.
    """
    offers: List[Dict[str, Any]] = []
    prices: List[Dict[str, Any]] = []
    total_collected = 0

    client = SupridesClient()
    # Use page size of 100 by default to minimise API calls; stop when we hit max_items
    for raw in client.iter_products(limit=100):
        # Normalize supplier data
        item = normalize(raw)
        total_collected += 1

        sku = item.get("sku")
        if not sku:
            # skip items without a SKU
            continue

        qty = item.get("qty_available", 0) or 0
        offer: Dict[str, Any] = {"sku": sku, "quantity": qty}

        price_cost = item.get("price_cost")
        if price_cost is not None:
            # Compute final selling price (no competitor price)
            try:
                price_info = calc_final_price(cost=price_cost, competitor_price=None)
                final_price = price_info.get("final_price")
            except Exception:
                final_price = None
            if final_price is not None:
                offer["price"] = final_price
                prices.append({"sku": sku, "price": final_price, "currency": "EUR"})

        offers.append(offer)

        if len(offers) >= max_items:
            break

    return offers, prices, total_collected