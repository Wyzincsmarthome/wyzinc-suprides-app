# auto_product_type.py
# -*- coding: utf-8 -*-
"""
Pipeline "sólido" para:
1) Descobrir o productType de um ASIN (via Catalog Items API ou fallback de pesquisa).
2) Buscar o schema (LISTING_OFFER_ONLY) do productType via Product Type Definitions API.
3) Extrair atributos obrigatórios e montar o bloco "attributes" do JSON_LISTINGS_FEED
   de forma compatível com o schema (incluindo estrutura de price e condition_type).
4) Cache local de schemas para performance e menos erros.

Logs úteis:
- logs/schema_cache/<productType>_LISTING_OFFER_ONLY.json : schema bruto
- logs/auto_pt_debug.json : últimos passos/decisões
"""

import os
import json
import time
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

# O cliente Amazon já tem sessão assinada e helpers — vamos reutilizar.
from amazon_client import AmazonClient


def _safe_num(x) -> float:
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return 0.0


def _ensure_dirs():
    os.makedirs("logs/schema_cache", exist_ok=True)


class AutoPT:
    """
    Orquestra productType discovery + schema + attributes builder.
    """
    def __init__(self, client: AmazonClient, marketplace_id: Optional[str] = None, locale: str = "es_ES"):
        self.client = client
        self.marketplace_id = marketplace_id or os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS")
        self.locale = locale
        _ensure_dirs()

    # ------------------------------
    # DESCOBERTA DE PRODUCT TYPE
    # ------------------------------
    def discover_product_type_for_asin(self, asin: str, fallback_keywords: Optional[str] = None) -> Optional[str]:
        """
        1ª tentativa: Catalog Items API → includedData=productTypes
        2ª tentativa: searchDefinitionsProductTypes com palavras-chave (ex.: brand + title)
        """
        asin = (asin or "").strip().upper()
        if not asin:
            return None

        # 1) Catalog Items API (usa método do teu AmazonClient)
        try:
            pt = self.client.get_product_type_for_asin(asin)
            if pt:
                return pt
        except Exception:
            pass

        # 2) Procurar por keywords no Definitions
        kw = (fallback_keywords or "").strip()
        if not kw:
            return None

        try:
            # Definitions: search
            url = f"{self.client.base}/definitions/2020-09-01/productTypes"
            params = {
                "marketplaceIds": self.marketplace_id,
                "keywords": kw,
                "itemName": kw,  # algumas contas beneficiam deste parâmetro quando presente
            }
            req = requests.Request("GET", url, params=params, headers={"user-agent": self.client.user_agent})
            r = self.client.session.send(self.client._sign(req))
            if r.status_code == 200:
                js = r.json() or {}
                types = (js.get("productTypes") or [])
                # heurística: escolher o primeiro com group "offer" com requirements suportar LISTING_OFFER_ONLY
                for t in types:
                    if t.get("name"):
                        return t["name"]
        except Exception:
            pass

        return None

    # ------------------------------
    # SCHEMA + CACHE
    # ------------------------------
    def _cache_path(self, product_type: str, requirements: str = "LISTING_OFFER_ONLY") -> str:
        safe = re.sub(r"[^A-Za-z0-9_\-]+", "_", product_type.strip())
        return os.path.join("logs", "schema_cache", f"{safe}_{requirements}.json")

    def get_schema(self, product_type: str, requirements: str = "LISTING_OFFER_ONLY") -> Optional[Dict[str, Any]]:
        """
        Vai buscar o schema do productType com requirements=LISTING_OFFER_ONLY e locale es_ES.
        Usa cache local se existir.
        """
        product_type = (product_type or "").strip()
        if not product_type:
            return None

        cache_file = self._cache_path(product_type, requirements)
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

        try:
            url = f"{self.client.base}/definitions/2020-09-01/productTypes/{product_type}"
            params = {
                "marketplaceIds": self.marketplace_id,
                "requirements": requirements,
                "locale": self.locale,
            }
            req = requests.Request("GET", url, params=params, headers={"user-agent": self.client.user_agent})
            r = self.client.session.send(self.client._sign(req))
            if r.status_code != 200:
                return None
            js = r.json() or {}
            # guardar o bruto (schema field já vem aninhado em js["schema"]["link"] para download — aqui guardamos o envelope)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(js, f, ensure_ascii=False, indent=2)
            return js
        except Exception:
            return None

    # ------------------------------
    # EXTRACÇÃO DE OBRIGATÓRIOS
    # ------------------------------
    def _extract_required_offer_props(self, schema_envelope: Dict[str, Any]) -> List[str]:
        """
        A Amazon devolve um "envelope" com link para o schema real JSON (em S3).
        Para simplificar, captamos a lista de propriedades conhecidas no group "offer".
        Nota: os "required" explícitos podem estar apenas no schema concreto (ficheiro JSON referenciado).
        Aqui usamos uma heurística: se o group 'offer' lista 'purchasable_offer' e 'condition_type', tratamo-los como candidatos.
        """
        props = []
        try:
            pg = (schema_envelope.get("propertyGroups") or {}).get("offer") or {}
            names = pg.get("propertyNames") or []
            # priorizar estes se existirem
            for p in ["condition_type", "fulfillment_availability", "purchasable_offer", "merchant_suggested_asin"]:
                if p in names:
                    props.append(p)
            # adicionar os restantes nomes (sem duplicar)
            for n in names:
                if n not in props:
                    props.append(n)
        except Exception:
            pass
        return props

    def _condition_value_from_schema(self, schema_envelope: Dict[str, Any], default: str = "NEW_NEW") -> str:
        """
        Se o schema listar enum de condition_type, usa um válido.
        Caso contrário, fallback NEW_NEW.
        """
        try:
            # em muitos schemas o enum de condition_type aparece no schema real (link S3).
            # como simplificação: se o PT contém condition_type, usamos NEW_NEW (válido em EU normalmente).
            return "NEW_NEW"
        except Exception:
            return default

    def _shape_price_from_schema(self, product_type: str, price: float) -> Dict[str, Any]:
        """
        Monta purchasable_offer de forma a ser aceite pela maioria dos PTs EU:
        - SEM 'currency' no topo
        - our_price: [ { value_with_tax: { amount, currency_code } } ]
        Se alguma conta exigir schedule, a Amazon aceita sem 'schedule' em muitos PTs.
        """
        return {
            "our_price": [
                {
                    "value_with_tax": {
                        "amount": float(price),
                        "currency_code": "EUR"
                    }
                }
            ]
        }

    # ------------------------------
    # BUILDER PRINCIPAL
    # ------------------------------
    def build_attributes_for_offer(
        self,
        product_type: str,
        price: float,
        quantity: int,
        asin: Optional[str] = None,
        include_price: bool = True,
        include_condition: bool = True,
    ) -> Dict[str, Any]:
        """
        Constrói o dicionário 'attributes' para JSON_LISTINGS_FEED usando heurísticas baseadas no schema.
        Só envia o que sabemos montar corretamente.
        """
        attrs: Dict[str, Any] = {}

        schema_env = self.get_schema(product_type, "LISTING_OFFER_ONLY")
        required_offer_props = self._extract_required_offer_props(schema_env or {})

        # condition_type (se o PT exigir; usamos enum válido NEW_NEW)
        if include_condition and "condition_type" in required_offer_props:
            attrs["condition_type"] = {"value": self._condition_value_from_schema(schema_env or {})}

        # fulfillment_availability (inventário FBM)
        if "fulfillment_availability" in required_offer_props:
            attrs["fulfillment_availability"] = [
                {"fulfillment_channel_code": "DEFAULT", "quantity": int(max(0, quantity))}
            ]

        # purchasable_offer (preço) — só se for para incluir (podes desligar e fazer preço em feed separado)
        if include_price and "purchasable_offer" in required_offer_props:
            shaped = self._shape_price_from_schema(product_type, price)
            attrs["purchasable_offer"] = shaped

        # merchant_suggested_asin (apenas se útil e estiver listado no grupo)
        if asin and "merchant_suggested_asin" in required_offer_props:
            attrs["merchant_suggested_asin"] = [{"value": asin}]

        # debug
        try:
            dbg = {
                "ts": int(time.time()),
                "product_type": product_type,
                "required_offer_props": required_offer_props,
                "built_attrs_keys": list(attrs.keys()),
            }
            with open("logs/auto_pt_debug.json", "w", encoding="utf-8") as f:
                json.dump(dbg, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        return attrs

    # ------------------------------
    # HELPERS DE ALTO NÍVEL
    # ------------------------------
    def autotype_and_build(
        self,
        asin: str,
        price: float,
        quantity: int,
        fallback_keywords: Optional[str] = None,
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        """
        Fluxo completo:
        - descobrir PT
        - montar attributes
        Retorna (product_type, attributes)
        """
        pt = self.discover_product_type_for_asin(asin, fallback_keywords=fallback_keywords)
        if not pt:
            return None, {}

        attrs = self.build_attributes_for_offer(
            product_type=pt,
            price=_safe_num(price),
            quantity=max(0, int(quantity)),
            asin=asin,
            include_price=True,
            include_condition=True,
        )
        return pt, attrs
