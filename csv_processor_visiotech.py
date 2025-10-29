# csv_processor_visiotech.py
# -*- coding: utf-8 -*-
import os, json, re, pandas as pd
from typing import Dict, Tuple, List, Any
from pricing_engine import calc_final_price

CFG_FILE = "config.json"

def load_cfg() -> Dict:
    with open(CFG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def _to_float(x: Any) -> float:
    try:
        return float(str(x).strip().replace(",", "."))
    except:
        return 0.0

def _to_int(x: Any) -> int:
    try:
        return int(float(str(x).strip().replace(",", ".")))
    except:
        return 0

def _read_csv_any(path_csv: str) -> Tuple[pd.DataFrame, str]:
    attempts = [
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "latin1"},
    ]
    errors = []
    for opts in attempts:
        try:
            df = pd.read_csv(path_csv, dtype=str, **opts).fillna("")
            return df, f"OK {opts}"
        except Exception as e:
            errors.append(f"{opts} -> {e}")
    raise RuntimeError("Falha a ler CSV do fornecedor:\n" + "\n".join(errors))

MAP_CANDIDATES = {
    "sku":      ["sku","cod","codigo","code","reference","ref","ref_proveedor","supplier_ref"],
    "brand":    ["brand","marca","manufacturer","fabricante"],
    "ean":      ["ean","gtin","barcode","codigo_barras","cod_barras","upc"],
    "title":    ["title","titulo","name","nombre","descricao","descrição","description","descripcion"],
    "category": ["category","categoria","familia","familía","family","familia_producto","category_parent"],
    "cost":     ["precio_neto_compra","precio_compra","precio_neto","net_cost","purchase_price",
                 "cost","custo","price_cost","preco_custo","precio_coste","precio","price","pvd","pneto","pcoste"],
    "stock":    ["stock","qty","quantity","cantidad","qtd","existencias","disponible","availability"],
}

def _choose_first(df: pd.DataFrame, choices: List[str]) -> str:
    for c in choices:
        if c in df.columns and not df[c].eq("").all():
            return c
    return ""

def _slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", s.strip(), flags=re.UNICODE)
    s = re.sub(r"-+", "-", s)
    return s.strip("-").upper()

def _map_columns(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    out = {}
    for k, cands in MAP_CANDIDATES.items():
        out[k] = next((c for c in cands if c in df_raw.columns and not df_raw[c].eq("").all()), "")
    df = pd.DataFrame({k: (df_raw[v] if v else "") for k, v in out.items()})
    # Fallback de SKU
    if "sku" in df and df["sku"].eq("").all():
        df["sku"] = (
            df.get("ean", pd.Series([""]*len(df))).astype(str).str.strip()
              .where(lambda s: s != "", None)
              .fillna(df.get("title", pd.Series([""]*len(df))).astype(str).map(_slug))
        )
    return df, f"map:{json.dumps(out, ensure_ascii=False)}"

# ---------- NOVO: normalização de stock high/medium/low/none ----------
_STOCK_MAP = {
    "high": 10, "alta": 10, "alto": 10, "elevado": 10,
    "medium": 5, "media": 5, "medio": 5,
    "low": 1, "baja": 1, "baixa": 1, "baixo": 1, "pouco": 1,
    "none": 0, "sin": 0, "sem": 0, "no": 0, "ninguno": 0, "ninguna": 0,
    "outofstock": 0, "out_of_stock": 0, "agotado": 0,
    # também tratamos strings como "high stock", "stock: medium", etc.
}

def _normalize_stock_value(x: Any) -> int:
    """
    Converte o campo de stock do fornecedor em inteiro.
    - 'high' -> 10, 'medium' -> 5, 'low' -> 1, 'none' -> 0
    - tenta apanhar sinónimos comuns PT/ES
    - se for número, usa o inteiro
    - caso contrário, default 0
    """
    s = str(x).strip().lower()
    if s == "":
        return 0

    # match exatos
    if s in _STOCK_MAP:
        return _STOCK_MAP[s]

    # contém palavras-chave, p.ex. "high stock", "stock: medium"
    for key, val in _STOCK_MAP.items():
        if key in s:
            return val

    # número?
    try:
        return max(0, int(float(s.replace(",", "."))))
    except:
        return 0

# ----------------------------------------------------------------------

def process_csv(path_csv: str, cfg: Dict) -> pd.DataFrame:
    os.makedirs("data", exist_ok=True)
    df_raw, how = _read_csv_any(path_csv)
    df_mapped, chosen = _map_columns(df_raw)

    # Normalizações base
    if "cost" in df_mapped.columns:
        df_mapped["cost"] = df_mapped["cost"].map(_to_float)
    else:
        df_mapped["cost"] = 0.0

    # >>> AQUI aplicamos a nova regra de stock <<<
    if "stock" in df_mapped.columns:
        df_mapped["stock"] = df_mapped["stock"].map(_normalize_stock_value).astype(int)
    else:
        df_mapped["stock"] = 0

    # Guarda SEM filtro (para debug/visualização)
    raw_out = "data/produtos_mapeados_sem_filtro.csv"
    df_mapped.to_csv(raw_out, index=False)

    # Aplica filtro de marcas, se existir
    df = df_mapped.copy()
    allowed_cfg = cfg.get("allowed_brands") or []
    if allowed_cfg:
        allowed = {str(b).strip().casefold() for b in allowed_cfg}
        df = df[df["brand"].astype(str).str.strip().str.casefold().isin(allowed)].copy()

    # Preços preview/floor/selling
    previews, floors = [], []
    for _, r in df.iterrows():
        out = calc_final_price(cost=float(r.get("cost",0.0)), competitor_price=None, cfg=cfg)
        previews.append(out["final_price"])
        floors.append(out["floor_price"])
    df["preview_price"] = previews
    df["floor_price"]   = floors
    df["selling_price"] = df["preview_price"]
    df["status"]        = "ativo"

    # Guardar COM filtro (processados)
    out_path = "data/produtos_processados.csv"
    df.to_csv(out_path, index=False)

    # Escrever relatório de leitura
    try:
        with open("data/_last_csv_read_info.txt","w",encoding="utf-8") as f:
            f.write(how + "\n\n")
            f.write("Colunas RAW:\n")
            f.write(", ".join(df_raw.columns) + "\n\n")
            f.write("Amostra mapeada SEM filtro (5 linhas):\n")
            cols_dbg = [c for c in ["sku","brand","title","ean","cost","stock"] if c in df_mapped.columns]
            f.write(df_mapped[cols_dbg].head(5).to_csv(index=False))
            f.write("\n\nEstatísticas SEM filtro:\n")
            f.write(f"Linhas totais: {len(df_mapped)}\n")
            f.write(f"Brands top:\n{df_mapped['brand'].value_counts().head(10).to_string()}\n\n")
            f.write("Estatísticas COM filtro:\n")
            f.write(f"Linhas após filtro de marcas: {len(df)}\n")
            if len(df) == 0 and (allowed_cfg):
                f.write(f"ATENÇÃO: allowed_brands no config.json = {allowed_cfg}\n")
    except Exception:
        pass

    return df
