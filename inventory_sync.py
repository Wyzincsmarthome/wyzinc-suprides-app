# inventory_sync.py
# -*- coding: utf-8 -*-
"""
Carrega/atualiza o inventário local usado para marcar 'listed'.

Versão simples (fiável):
- Se existir data/my_inventory.csv, lê e normaliza cabeçalhos.
- Se não existir, cria um CSV vazio com as colunas certas, para permitir o fluxo /classify.
- (Integração SP-API para gerar este ficheiro pode ser adicionada depois via Reports API.)
"""

import os
import pandas as pd
from datetime import datetime

DATA_DIR = "data"
INV_PATH = os.path.join(DATA_DIR, "my_inventory.csv")

def _ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza cabeçalhos típicos de reports da Amazon
    rename = {
        "seller_sku": "seller_sku",
        "SellerSKU": "seller_sku",
        "sku": "seller_sku",
        "ASIN": "asin",
        "asin1": "asin",
        "asin": "asin",
        "price": "price",
        "Price": "price",
        "quantity": "quantity",
        "Quantity": "quantity",
        "status": "status",
        "Status": "status",
    }
    # aplica renomeação só para colunas existentes
    cols = {c: rename.get(c, c) for c in df.columns}
    df = df.rename(columns=cols).copy()

    # garante colunas base
    for c in ["seller_sku", "asin", "price", "quantity", "status"]:
        if c not in df.columns:
            df[c] = ""

    # limpeza
    df["seller_sku"] = df["seller_sku"].astype(str).str.strip()
    df["asin"] = df["asin"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    # números (não falhar se estiver vazio)
    def _safe_float(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return ""
    def _safe_int(x):
        try:
            return int(float(x))
        except Exception:
            return ""
    df["price"] = df["price"].apply(_safe_float)
    df["quantity"] = df["quantity"].apply(_safe_int)

    return df[["seller_sku", "asin", "price", "quantity", "status"]]

def refresh_inventory(simulate: bool = False) -> pd.DataFrame:
    """
    Atualiza/garante o ficheiro local de inventário:
    - Se existir, lê e normaliza.
    - Se não existir, cria vazio com headers padrão.
    Retorna o DataFrame final e deixa gravado em data/my_inventory.csv
    """
    _ensure_dirs()
    if os.path.exists(INV_PATH):
        df = pd.read_csv(INV_PATH, dtype=str).fillna("")
        df = _normalize(df)
    else:
        # cria um CSV vazio com as colunas certas
        df = pd.DataFrame(columns=["seller_sku", "asin", "price", "quantity", "status"])
        df.to_csv(INV_PATH, index=False)

    # salva normalizado (idempotente)
    df.to_csv(INV_PATH, index=False)
    return df
