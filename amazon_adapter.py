# -*- coding: utf-8 -*-
"""
amazon_adapter.py
Sincronização Amazon SP-API via:
- Feeds 2021-06-30 (mantidos):
    * Stock: JSON_LISTINGS_FEED (XML)
    * Preços: JSON_LISTINGS_FEED (XML)
- Listings Items 2021-08-01 (NOVO):
    * patch_listings_item(...) para PATCH direto de atributos top-level
"""

from __future__ import annotations
import os
from typing import List, Dict, Any, Optional
from xml.sax.saxutils import escape

# Tipos de feed Amazon
FEED_QTY = "JSON_LISTINGS_FEED"
FEED_PRICE = "JSON_LISTINGS_FEED"


# ----------------------------- Helpers comuns -----------------------------

def _pick_sku(obj: Dict[str, Any]) -> str:
    for k in ("sku", "seller_sku", "sku_supplier"):
        v = obj.get(k)
        if v:
            return str(v).strip()
    return ""


def _pick_qty(obj: Dict[str, Any]) -> int:
    for k in ("qty", "quantity", "stock", "qty_suggested"):
        if k in obj and obj[k] is not None:
            try:
                return max(0, int(float(obj[k])))
            except Exception:
                pass
    return 0


def _pick_price(obj: Dict[str, Any]) -> Optional[float]:
    # prioridade a campos “já finais”
    for k in ("price_final", "standard_price", "price", "price_calculated"):
        v = obj.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            continue
    return None


def _to_decimal_str(val: float) -> str:
    # Amazon quer ponto como separador decimal
    return f"{val:.2f}"


# ----------------------------- STOCK (Inventory via FEED) -----------------------------

def _build_inventory_xml(offers: List[Dict[str, Any]]) -> str:
    """
    Constrói o envelope XML para JSON_LISTINGS_FEED.
    """
    msgs = []
    msg_id = 1

    for o in offers:
        sku = _pick_sku(o)
        if not sku:
            continue
        qty = _pick_qty(o)
        lat = o.get("fulfillment_latency")
        latency_xml = f"<FulfillmentLatency>{int(lat)}</FulfillmentLatency>" if lat not in (None, "") else ""

        msgs.append(f"""
  <Message>
    <MessageID>{msg_id}</MessageID>
    <OperationType>Update</OperationType>
    <Inventory>
      <SKU>{escape(sku)}</SKU>
      <Quantity>{qty}</Quantity>
      {latency_xml}
    </Inventory>
  </Message>""")
        msg_id += 1

    if not msgs:
        # envelope mínimo para não mandar XML vazio
        msgs.append("""
  <Message>
    <MessageID>1</MessageID>
    <OperationType>Update</OperationType>
    <Inventory>
      <SKU>__PLACEHOLDER__</SKU>
      <Quantity>0</Quantity>
    </Inventory>
  </Message>""")

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>DEFAULT</MerchantIdentifier>
  </Header>
  <MessageType>Inventory</MessageType>
  {''.join(msgs)}
</AmazonEnvelope>"""
    return envelope


def sync_qty(ac, marketplace_id: str, offers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Envia feed de INVENTÁRIO (quantidades) em XML.
    - ac: AmazonClient
    - marketplace_id: ex. "A1RKKUPIHCS9HS"
    - offers: ver convenções no topo
    """
    submitted = len(offers) if offers else 0
    if not offers:
        return {"success": True, "submitted": 0, "feed_type": FEED_QTY}

    body_xml = _build_inventory_xml(offers)

    if os.getenv("DRY_RUN") == "1":
        return {
            "success": True,
            "dry_run": True,
            "feed_type": FEED_QTY,
            "would_submit": submitted
        }

    feed_id = ac.submit_document(
        FEED_QTY,
        body_xml.encode("utf-8"),
        "text/xml",
        marketplace_ids=[marketplace_id]
    )
    return {
        "success": True,
        "submitted": submitted,
        "feed_type": FEED_QTY,
        "feed_id": feed_id
    }


# ----------------------------- PREÇOS (Pricing via FEED) -----------------------------

def _build_pricing_xml(prices: List[Dict[str, Any]], currency: str = "EUR") -> str:
    """
    Constrói o envelope XML para JSON_LISTINGS_FEED.
    Suporta StandardPrice e (opcional) Minimum/MaximumSellerAllowedPrice.
    """
    msgs = []
    msg_id = 1

    for p in prices:
        sku = _pick_sku(p)
        if not sku:
            continue

        std_price = _pick_price(p)
        if std_price is None:
            # sem preço, ignora
            continue

        min_p = p.get("min_price")
        max_p = p.get("max_price")

        min_xml = f'<MinimumSellerAllowedPrice currency="{escape(currency)}">{_to_decimal_str(float(min_p))}</MinimumSellerAllowedPrice>' if min_p not in (None, "") else ""
        max_xml = f'<MaximumSellerAllowedPrice currency="{escape(currency)}">{_to_decimal_str(float(max_p))}</MaximumSellerAllowedPrice>' if max_p not in (None, "") else ""

        msgs.append(f"""
  <Message>
    <MessageID>{msg_id}</MessageID>
    <Price>
      <SKU>{escape(sku)}</SKU>
      <StandardPrice currency="{escape(currency)}">{_to_decimal_str(float(std_price))}</StandardPrice>
      {min_xml}
      {max_xml}
    </Price>
  </Message>""")
        msg_id += 1

    if not msgs:
        msgs.append(f"""
  <Message>
    <MessageID>1</MessageID>
    <Price>
      <SKU>__PLACEHOLDER__</SKU>
      <StandardPrice currency="{escape(currency)}">0.00</StandardPrice>
    </Price>
  </Message>""")

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amzn-envelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>DEFAULT</MerchantIdentifier>
  </Header>
  <MessageType>Price</MessageType>
  {''.join(msgs)}
</AmazonEnvelope>"""
    return envelope


def sync_prices(ac, marketplace_id: str, prices: List[Dict[str, Any]], currency: str = "EUR") -> Dict[str, Any]:
    """
    Envia feed de PREÇOS em XML.
    - ac: AmazonClient
    - marketplace_id: ex. "A1RKKUPIHCS9HS"
    - prices: ver convenções no topo
    - currency: por omissão "EUR"
    """
    submitted = len(prices) if prices else 0
    if not prices:
        return {"success": True, "submitted": 0, "feed_type": FEED_PRICE}

    body_xml = _build_pricing_xml(prices, currency=currency)

    if os.getenv("DRY_RUN") == "1":
        return {
            "success": True,
            "dry_run": True,
            "feed_type": FEED_PRICE,
            "would_submit": submitted
        }

    feed_id = ac.submit_document(
        FEED_PRICE,
        body_xml.encode("utf-8"),
        "text/xml",
        marketplace_ids=[marketplace_id]
    )
    return {
        "success": True,
        "submitted": submitted,
        "feed_type": FEED_PRICE,
        "feed_id": feed_id
    }


# ----------------------------- NOVO: Listings Items PATCH -----------------------------

def _spapi_request(ac, method: str, path: str, params: Optional[Dict[str, Any]] = None, json: Optional[Dict[str, Any]] = None) -> Any:
    """
    Tenta invocar a SP-API usando o AmazonClient fornecido.
    É resiliente a diferentes implementações do teu AmazonClient.
    Retorna o dict JSON (se possível) ou o objeto de resposta bruto.
    """
    # 1) wrapper com método 'request'
    if hasattr(ac, "request") and callable(getattr(ac, "request")):
        resp = ac.request(method=method, path=path, params=params or {}, json=json or {})
        try:
            return resp.json() if hasattr(resp, "json") else resp
        except Exception:
            return resp

    # 2) wrapper com método 'signed_request'
    if hasattr(ac, "signed_request") and callable(getattr(ac, "signed_request")):
        resp = ac.signed_request(method=method, path=path, params=params or {}, json=json or {})
        try:
            return resp.json() if hasattr(resp, "json") else resp
        except Exception:
            return resp

    # 3) SDK interno (ex.: ac.client.listings_items.patch_listings_item)
    client = getattr(ac, "client", None)
    if client is not None:
        # tentar caminho específico Listings Items
        li = getattr(client, "listings_items", None)
        if li and hasattr(li, "patch_listings_item"):
            # tentar mapear params de acordo com SDK
            kwargs = {}
            # naive mapping:
            # sellerId e sku no path
            # marketplaceIds em lista
            # mode via query
            # body com productType e patches
            if "/listings/2021-08-01/items/" in path:
                # extrair sellerId e sku do path simples
                try:
                    parts = path.strip("/").split("/")
                    # ... items/{sellerId}/{sku}
                    seller_id = parts[-2]
                    sku = parts[-1]
                    marketplace_ids = []
                    mode = None
                    if params:
                        if "marketplaceIds" in params:
                            m = params["marketplaceIds"]
                            marketplace_ids = m if isinstance(m, list) else [m]
                        mode = params.get("mode")
                    body = json or {}
                    resp = li.patch_listings_item(
                        sellerId=seller_id,
                        sku=sku,
                        marketplaceIds=marketplace_ids,
                        body=body,
                        mode=mode
                    )
                    try:
                        return resp.payload if hasattr(resp, "payload") else resp
                    except Exception:
                        return resp
                except Exception:
                    pass

    raise RuntimeError("AmazonClient não expõe um método compatível para chamadas SP-API (request/signed_request/SDK). Adapta amazon_client.py para suportar chamadas genéricas.")

def get_listings_item(ac, seller_id: str, sku: str, marketplace_id: str, included: str = "attributes,issues,fulfillmentAvailability") -> dict:
    """
    GET Listings Items v2021-08-01 para inspecionar atributos (stock/preço/erros).
    """
    path = f"/listings/2021-08-01/items/{seller_id}/{sku}"
    params = {"marketplaceIds": marketplace_id, "includedData": included}
    resp = _spapi_request(ac, method="GET", path=path, params=params, json=None)
    return resp if isinstance(resp, dict) else {"raw": str(resp)}

def put_listings_item(ac, seller_id: str, sku: str, marketplace_id: str, product_type: str, attributes: dict, requirements: str = "LISTING_OFFER_ONLY", issue_locale: str = "es_ES") -> dict:
    """
    PUT Listings Items para criar/atualizar uma oferta (LISTING_OFFER_ONLY).
    Usa para 'create offer' quando ainda não tens SKU no catálogo.
    """
    path = f"/listings/2021-08-01/items/{seller_id}/{sku}"
    params = {"marketplaceIds": marketplace_id, "issueLocale": issue_locale}
    body = {
        "productType": product_type or "PRODUCT",
        "requirements": requirements,
        "attributes": attributes,
    }
    resp = _spapi_request(ac, method="PUT", path=path, params=params, json=body)
    return resp if isinstance(resp, dict) else {"raw": str(resp)}

def _build_offer_attributes(price: float, qty: int, asin: str | None = None, audience: str = "ALL", currency: str = "EUR", shipping_group: str | None = None) -> dict:
    """
    Constrói bloco 'attributes' só de oferta, pronto para PUT (LISTING_OFFER_ONLY) ou PATCH (value de replace).
    """
    attrs: dict = {
        "condition_type": [{"value": "new_new"}],
        "purchasable_offer": [{
            "currency": currency,
            "audience": audience,
            "our_price": [{
                "schedule": [{
                    # Se ES implicar com decimais, troca por 4877 no teu chamador
                    "value_with_tax": float(price)
                }]
            }]
        }],
        "fulfillment_availability": [{
            "fulfillment_channel_code": "DEFAULT",
            "quantity": int(max(0, qty))
        }]
    }
    if asin:
        attrs["merchant_suggested_asin"] = [{"value": asin}]
    if shipping_group:
        attrs["merchant_shipping_group"] = [{"value": shipping_group}]
    return attrs

def upsert_offer(
    ac,
    seller_id: str,
    sku: str,
    marketplace_id: str,
    price: float,
    qty: int,
    asin: str | None = None,
    audience: str = "ALL",
    currency: str = "EUR",
    product_type_fallback: str = "PRODUCT",
    prefer_minor_units: bool = False,
) -> dict:
    """
    Se SKU existir -> PATCH (replace dos atributos top-level).
    Se não existir -> PUT com requirements=LISTING_OFFER_ONLY para criar a oferta referindo o ASIN.
    """
    # 1) tenta GET
    try:
        _ = get_listings_item(ac, seller_id, sku, marketplace_id)
        exists = True
    except Exception as e:
        # 404 => não existe; qualquer outro 4xx/5xx propaga
        msg = str(e)
        exists = " 404" in msg or "status_code': 404" in msg.lower()

    # 2) prepara atributos
    v = float(price)
    if prefer_minor_units:
        # troca para minor units (4877) se o teu marketplace/seller preferir assim
        v = int(round(v * 100))

    attrs = _build_offer_attributes(v, qty, asin=asin, audience=audience, currency=currency, shipping_group=getattr(ac, "default_shipping_group", None))

    # 3) PATCH se existe
    if exists:
        patches = []
        patches.append({
            "op": "replace",
            "path": "/attributes/fulfillment_availability",
            "value": attrs["fulfillment_availability"]
        })
        patches.append({
            "op": "replace",
            "path": "/attributes/purchasable_offer",
            "value": attrs["purchasable_offer"]
        })
        body = {
            "productType": product_type_fallback,
            "patches": patches
        }
        path = f"/listings/2021-08-01/items/{seller_id}/{sku}"
        params = {"marketplaceIds": marketplace_id}
        resp = _spapi_request(ac, method="PATCH", path=path, params=params, json=body)
        return {"mode": "PATCH", "result": resp}

    # 4) PUT se não existe: tentar descobrir productType pelo ASIN (se tiveres helper no client)
    product_type = None
    try:
        if hasattr(ac, "get_product_type_for_asin") and asin:
            product_type = ac.get_product_type_for_asin(asin) or None
    except Exception:
        product_type = None
    if not product_type:
        product_type = product_type_fallback

    resp = put_listings_item(
        ac=ac,
        seller_id=seller_id,
        sku=sku,
        marketplace_id=marketplace_id,
        product_type=product_type,
        attributes=attrs,
        requirements="LISTING_OFFER_ONLY",
        issue_locale="es_ES"
    )
    return {"mode": "PUT", "result": resp}

def patch_listings_item(
    ac,
    seller_id: str,
    sku: str,
    marketplace_id: str,
    product_type: str,
    patches: List[Dict[str, Any]],
    mode: Optional[str] = None
) -> Dict[str, Any]:
    """
    PATCH Listings Items v2021-08-01
    - Atualiza atributos top-level (ex.: /attributes/fulfillment_availability, /attributes/purchasable_offer)
    - product_type: usa "PRODUCT" quando não souberes o tipo específico
    - mode: "VALIDATION_PREVIEW" para validar sem persistir, senão None/LIVE
    """
    path = f"/listings/2021-08-01/items/{seller_id}/{sku}"
    params: Dict[str, Any] = {"marketplaceIds": marketplace_id}
    if mode and mode.upper() == "VALIDATION_PREVIEW":
        params["mode"] = "VALIDATION_PREVIEW"

    body = {
        "productType": product_type,
        "patches": patches
    }

    resp = _spapi_request(ac, method="PATCH", path=path, params=params, json=body)
    # normalizar retorno em dict
    if isinstance(resp, dict):
        return resp
    try:
        # alguns wrappers devolvem objeto com .json()
        data = resp.json()  # type: ignore[attr-defined]
        return data if isinstance(data, dict) else {"raw": str(data)}
    except Exception:
        return {"raw": str(resp)}
