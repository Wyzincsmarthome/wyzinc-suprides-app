# supplier_suprides.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Dict, Any

_number = re.compile(r"[-+]?\d+(\.\d+)?")

def _to_str(x) -> str:
    if x is None:
        return ""
    try:
        return str(x).strip()
    except Exception:
        return ""

def _to_float(x) -> float | None:
    """
    Converte valores genéricos para float quando faz sentido.
    NÃO deve ser usada para interpretar labels de stock do tipo "<10".
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        # tenta extrair número de textos tipo "€ 12,34" ou "12 uds."
        m = _number.search(s.replace(",", "."))
        return float(m.group(0)) if m else None

def _to_int(v: Any) -> int:
    """
    Converte valores de stock para quantidade inteira com o mapping EXPLÍCITO:
      0   -> 0
      <2  -> 1
      <10 -> 5
      >10 -> 10
    Se vier número (ex.: "3" / 3 / "3,0"), respeita o número (>=0).
    """
    if v is None:
        return 0

    s = str(v).strip()
    if s == "":
        return 0

    ss = s.lower().replace(" ", "")

    # labels de ausência de stock
    if ss in {"0", "oos", "outofstock", "semstock", "sem_estoque", "semestoque", "no", "false"}:
        return 0

    # MAPEAMENTO EXPLÍCITO (ponto crítico do bug)
    if ss in {"<2", "≤2", "le2"}:
        return 1
    if ss in {"<10", "≤10", "le10"}:
        return 5
    if ss in {">10", "≥10", "ge10", "10+", ">9"}:
        return 10

    # caso numérico direto (e.g., "3", "3.0", "3,0")
    try:
        return max(0, int(float(s.replace(",", "."))))
    except Exception:
        pass

    # extrai primeiro número se vier num texto misto, mas SEM re-interpretar "<10" como 10
    # (já tratámos os casos categóricos acima, por isso aqui pode ser seguro)
    m = re.search(r"(-?\d+(?:[.,]\d+)?)", s)
    if m:
        try:
            return max(0, int(float(m.group(1).replace(",", "."))))
        except Exception:
            return 0

    return 0

def normalize(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza qualquer linha vinda da API/CSV da Suprides para:
      sku, ean, brand, name, price_cost, qty_available
    """
    # aceita várias chaves possíveis
    sku = (
        _to_str(raw.get("sku_supplier"))
        or _to_str(raw.get("sku"))
        or _to_str(raw.get("code"))
        or _to_str(raw.get("id"))
    )

    ean = (
        _to_str(raw.get("ean"))
        or _to_str(raw.get("barcode"))
        or _to_str(raw.get("EAN"))
    )

    brand = (
        _to_str(raw.get("brand"))
        or _to_str(raw.get("manufacturer"))
        or _to_str(raw.get("marca"))
    )

    name = (
        _to_str(raw.get("name"))
        or _to_str(raw.get("title"))
        or _to_str(raw.get("description"))
    )

    # custo (float genérico; aqui faz sentido aceitar "12,34", "€ 12.34", etc.)
    price_cost = (
        _to_float(raw.get("price_cost"))
        or _to_float(raw.get("cost"))
        or _to_float(raw.get("price"))
        or _to_float(raw.get("price_net"))
    )

    # stock/qty com o mapping EXPLÍCITO (chaves alternativas mais comuns)
    qty_available = (
        _to_int(raw.get("qty_available"))
        or _to_int(raw.get("stock"))
        or _to_int(raw.get("qty"))
        or 0
    )

    # Expor sku_supplier como alias de sku para compatibilidade
    return {
        "sku": sku,
        "sku_supplier": sku,
        "ean": ean,
        "brand": brand,
        "name": name,
        "price_cost": price_cost,
        "qty_available": qty_available,  # <- 0/1/5/10 conforme mapping
    }
