# -*- coding: utf-8 -*-
import os
import json
import logging
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, redirect, url_for

# -------------------------- Setup .env e logging --------------------------
load_dotenv()  # TEM MESMO DE VIR ANTES DE CRIAR CLIENTES/BLUEPRINTS
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# -------------------------- Criar a aplicação ----------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET", "change_me")

# -------------------------- Imports locais (rotas, etc.) -----------------
from amazon_client import AmazonClient
# do NOT importar aqui de novo classify_suprides_products (fica mais abaixo no try/except)
from auto_product_type import AutoPT
from csv_processor_visiotech import process_csv, load_cfg
from product_identify import classify_products
from amazon_insights import build_overview
from routes_enrich import bp_enrich
from inventory_sync import refresh_inventory
from app_suprides import bp as suprides_bp  # blueprint da Suprides
from pricing_engine import calc_final_price

# Importação da função de classificação da Suprides (protegida)
try:
    from suprides_identify import classify_suprides_products  # normaliza, calcula preços e estados
except Exception:
    classify_suprides_products = None

# -------------------------- Registar blueprints UMA vez -------------------
app.register_blueprint(bp_enrich)                       # já existia no teu projeto
app.register_blueprint(suprides_bp, url_prefix="/suprides")   # blueprint da Suprides com prefixo

# -------------------------- Pastas e ficheiros ----------------------------
os.makedirs("data", exist_ok=True)
os.makedirs("uploads", exist_ok=True)
os.makedirs("logs", exist_ok=True)

SETTINGS_FILE = "data/settings.json"
SELECTED_SKUS_FILE = "data/selected_skus.json"

# -------------------------- Healthcheck ----------------------------
@app.route("/healthz")
def healthz():
    return "ok", 200

# ----------------------- Helpers ---------------------------    
@app.post("/reprice_selected")
def reprice_selected():
    """
    Recebe itens selecionados (JSON: {items:[{sku, cost, brand, ...}]})
    e devolve o selling_price calculado. Sem efeitos colaterais.
    """
    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    out = []
    for it in items:
        cost = it.get("cost") or it.get("cost_eur") or 0
        try:
            price = calc_final_price(cost=float(cost))
        except Exception:
            price = None
        out.append({
            "sku": it.get("sku"),
            "ean": it.get("ean"),
            "brand": it.get("brand"),
            "cost": cost,
            "selling_price": price
        })
    return jsonify({"count": len(out), "items": out})

def _get_simulate_flag() -> bool:
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                s = json.load(f)
                return bool(s.get("simulate", True))
    except Exception:
        pass
    return str(os.getenv("SPAPI_SIMULATE", "true")).strip().lower() in ("1", "true", "yes", "on")


def _set_simulate_flag(val: bool) -> None:
    data = {}
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    data["simulate"] = bool(val)
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_df(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _save_selected_skus(skus: list) -> None:
    clean = sorted(set([str(s).strip() for s in skus if (s or "").strip()]))
    with open(SELECTED_SKUS_FILE, "w", encoding="utf-8") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)


def _read_selected_skus() -> list:
    if not os.path.exists(SELECTED_SKUS_FILE):
        return []
    try:
        with open(SELECTED_SKUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or []
    except Exception:
        return []


def _status_summary() -> dict:
    dfc = _load_df("data/produtos_classificados.csv")
    if dfc.empty or "status" not in dfc.columns:
        return {"total": 0, "catalog_match": 0, "catalog_ambiguous": 0, "listed": 0, "not_found": 0}
    counts = dfc["status"].value_counts().to_dict()
    return {
        "total": int(len(dfc)),
        "catalog_match": int(counts.get("catalog_match", 0)),
        "catalog_ambiguous": int(counts.get("catalog_ambiguous", 0)),
        "listed": int(counts.get("listed", 0)),
        "not_found": int(counts.get("not_found", 0)),
    }


def _suprides_status_summary() -> dict:
    """
    Similar ao resumo de estados para Visiotech, mas aplicado ao CSV da Suprides.
    Retorna contagem de estados encontrados em data/suprides_classified.csv.
    """
    path = "data/suprides_classified.csv"
    if not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return {}
    if "status" not in df.columns:
        return {}
    counts = df["status"].value_counts().to_dict()
    summary = {k: int(counts.get(k, 0)) for k in sorted(counts.keys())}
    summary["total"] = int(len(df))
    return summary


def _load_suprides_df() -> pd.DataFrame:
    """
    Carrega o CSV de classificados da Suprides. Se não existir, devolve DataFrame vazio.
    """
    path = "data/suprides_classified.csv"
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()


def _fallback_table(rows: list, cols: list, title: str) -> str:
    import html
    th = "".join([f"<th>{html.escape(c)}</th>" for c in cols])
    trs = []
    for r in rows:
        tds = "".join([f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols])
        trs.append(f"<tr>{tds}</tr>")
    return f"<h2>{html.escape(title)}</h2><table border='1' cellpadding='6' cellspacing='0'><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"


# ------------------------ UI -------------------------------
@app.route("/", endpoint="root_index")
def index():
    cfg = load_cfg()
    stats = {
        "simulate": _get_simulate_flag(),
        "marketplace": os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS"),
        "mapeados_sem_filtro": os.path.exists("data/produtos_mapeados_sem_filtro.csv"),
        "processados": os.path.exists("data/produtos_processados.csv"),
        "classificados": os.path.exists("data/produtos_classificados.csv"),
        "status_summary": _status_summary(),
        "selected_count": len(_read_selected_skus()),
        "overview": os.path.exists("data/amazon_overview.csv"),
        # indica se já existe classificação de Suprides e o resumo de estados
        "suprides_classificados": os.path.exists("data/suprides_classified.csv"),
        "suprides_status_summary": _suprides_status_summary(),
    }
    try:
        return render_template("index.html", stats=stats, cfg=cfg)
    except Exception:
        links = [
            ("/review_data", "Rever produtos (processados)"),
            ("/review_data?raw=1", "Rever produtos (mapeados sem filtro)"),
            ("/review_classified", "Rever classificados"),
            ("/amazon_overview", "Amazon Overview"),
            ("/actions/update_selected_patch_top", "PATCH preço/stock — selecionados"),
            ("/actions/update_selected_put", "PUT oferta ASIN-only — selecionados"),
            ("/debug/mapping_selected", "DEBUG: mapping seleção"),
            ("/actions/select_by_skus?skus=SKU1,SKU2", "Selecionar SKUs via URL"),
            ("/actions/clear_selection", "Limpar seleção"),
            ("/suprides/review_classified", "Suprides — classificados"),
        ]
        a = "".join([f"<li><a href='{u}'>{t}</a></li>" for u,t in links])
        return f"<h1>App — UI básica</h1><ul>{a}</ul><pre>{json.dumps(stats, ensure_ascii=False, indent=2)}</pre>"

# ------------------ Suprides Classification ------------------
@app.route("/suprides/classify")
def suprides_classify_route():
    """
    Executa a classificação de todos os produtos da Suprides.
    Gera o ficheiro data/suprides_classified.csv e devolve um resumo em JSON.
    """
    if classify_suprides_products is None:
        return jsonify({"success": False, "error": "Função classify_suprides_products não disponível."}), 500
    try:
        df = classify_suprides_products()
        # Guarda CSV
        path = "data/suprides_classified.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        # Devolve counts
        summary = _suprides_status_summary()
        return jsonify({"success": True, "rows": len(df), "summary": summary})
    except Exception as e:
        log.exception("Erro na classificação Suprides")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/suprides/review_classified")
def suprides_review_classified():
    """
    Mostra os produtos da Suprides classificados (catalog_match/catalog_ambiguous/listed/not_found/missing_ean).
    Inclui filtros de estado e marca e permite pesquisar.
    """
    df = _load_suprides_df()
    # Calcula resumo de estados
    summary = _suprides_status_summary()
    # Extrair lista única de estados e marcas
    statuses = []
    brands = []
    if not df.empty:
        statuses = sorted(set(df["status"].tolist()))
        # Inclui 'listed' se existir coluna listed
        if "listed" in df.columns:
            # Quando listed=True, considera status=listed independentemente da coluna status
            if any(df["listed"].astype(str).str.lower() == "true") and "listed" not in statuses:
                statuses.insert(0, "listed")
        brands = sorted(set([b for b in df.get("brand", []).tolist() if b]))
    try:
        rows = df.to_dict(orient="records")
        return render_template(
            "suprides_classified.html",
            rows=rows,
            summary=summary,
            statuses=statuses,
            brands=brands,
            selected=_read_selected_skus(),
        )
    except Exception:
        # fallback simples
        if df.empty:
            return "<h3>Sem dados em data/suprides_classified.csv</h3>"
        cols = list(df.columns)
        rows = df.to_dict(orient="records")
        extra = f"<pre>{json.dumps(summary, ensure_ascii=False, indent=2)}</pre>"
        return _fallback_table(rows, cols, "Fallback: Suprides classificados") + extra


@app.route("/review_data")
def review_data():
    raw = request.args.get("raw") in ("1", "true", "yes", "on")
    path = "data/produtos_mapeados_sem_filtro.csv" if raw else "data/produtos_processados.csv"
    label = "MAPEADOS (SEM FILTRO)" if raw else "PROCESSADOS (COM FILTRO)"
    df = _load_df(path)
    try:
        rows = df.to_dict(orient="records")
        return render_template("review.html", label=label, rows=rows, error=None, selected=_read_selected_skus())
    except Exception:
        if df.empty:
            return f"<h3>Sem dados em {path}</h3>"
        cols = list(df.columns)
        rows = df.to_dict(orient="records")
        return _fallback_table(rows, cols, f"Fallback: {label}")


@app.route("/review_classified")
def review_classified():
    dfc = _load_df("data/produtos_classificados.csv")
    summary = _status_summary()
    try:
        return render_template("review_classified.html", rows=dfc.to_dict(orient='records'), summary=summary)
    except Exception:
        if dfc.empty:
            return "<h3>Sem dados em data/produtos_classificados.csv</h3>"
        cols = list(dfc.columns)
        rows = dfc.to_dict(orient="records")
        extra = f"<pre>{json.dumps(summary, ensure_ascii=False, indent=2)}</pre>"
        return _fallback_table(rows, cols, "Fallback: Classificados") + extra


@app.route("/amazon_listings")
def amazon_listings():
    dfi = _load_df("data/my_inventory.csv")
    try:
        return render_template("review.html", label="LISTAGENS ATIVAS (AMAZON)", rows=dfi.to_dict(orient="records"), error=None, selected=_read_selected_skus())
    except Exception:
        if dfi.empty:
            return "<h3>Sem dados em data/my_inventory.csv</h3>"
        cols = list(dfi.columns)
        return _fallback_table(dfi.to_dict(orient="records"), cols, "Fallback: Listagens ativas")


# --------------------- Ações de dados ----------------------
@app.route("/upload_csv", methods=["POST"])
def upload_csv():
    f = request.files.get("csv_file")
    if not f or f.filename == "":
        return jsonify({"success": False, "error": "Nenhum ficheiro selecionado"}), 400
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join("uploads", f"visiotech_{ts}.csv")
    f.save(path)
    try:
        cfg = load_cfg()
        df = process_csv(path, cfg)
        return jsonify({"success": True, "message": "CSV processado com sucesso.", "rows": int(len(df))})
    except Exception as e:
        log.exception("Erro processamento CSV")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/toggle_simulate", methods=["POST"])
def toggle_simulate():
    val = bool(request.form.get("simulate") in ("1", "true", "on", "yes"))
    _set_simulate_flag(val)
    return redirect(url_for("root_index"))


# ---------------- CLASSIFY (robusto, por EAN) ---------------
def _classify_core(client: AmazonClient, ean: str):
    ean = (ean or "").strip()
    if not ean:
        return jsonify(success=False, error="Parâmetro 'ean' em falta"), 400

    res = client.catalog_search_by_ean(ean, included="identifiers,summaries,productTypes")

    # Garantir que 'res' é um dict
    if not isinstance(res, dict):
        return jsonify(success=False, error="Resposta inesperada do Catalog (tipo inválido).", raw=res), 502

    # Extrair items com fallback (alguns SDKs devolvem dentro de 'payload')
    items = []
    if isinstance(res.get("items"), list):
        items = res["items"]
    elif isinstance(res.get("payload"), dict) and isinstance(res["payload"].get("items"), list):
        items = res["payload"]["items"]
    else:
        return jsonify(success=True, classification="no_match", asin_candidates=[], raw=res), 200

    # Verifica se o item contém exatamente o EAN indicado
    def item_has_ean(it: dict) -> bool:
        try:
            for grp in (it.get("identifiers") or []):
                for ident in (grp.get("identifiers") or []):
                    id_val = str(ident.get("identifier") or "").strip()
                    id_type = str(ident.get("identifierType") or "").strip().upper()
                    if id_val == ean and id_type in ("EAN", "GTIN"):
                        return True
        except Exception:
            pass
        return False

    matched = [it for it in items if isinstance(it, dict) and item_has_ean(it)]
    asins = list({str((it.get("asin") or "")).upper() for it in matched if it.get("asin")}) if matched else []
    if not matched or not asins:
        return jsonify(success=True, classification="no_match", asin_candidates=[], raw=res), 200
    if len(asins) == 1:
        return jsonify(success=True, classification="catalog_match", asin_candidates=asins, raw=res), 200
    else:
        return jsonify(success=True, classification="catalog_ambiguous", asin_candidates=asins, raw=res), 200


@app.route("/classify")
def classify():
    ean = request.args.get("ean") or ""
    client = AmazonClient(simulate=False)
    return _classify_core(client, ean)


@app.route("/catalog/classify_by_ean")
def catalog_classify_by_ean():
    ean = request.args.get("ean") or ""
    client = AmazonClient(simulate=False)
    return _classify_core(client, ean)


@app.route("/refresh_inventory")
def refresh_inventory_route():
    try:
        simulate = _get_simulate_flag()
        df = refresh_inventory(simulate=simulate)
        n = len(df)
        return jsonify({"success": True, "rows": n, "file": "data/my_inventory.csv"})
    except Exception as e:
        log.exception("Erro inventário")
        return jsonify({"success": False, "error": str(e)}), 500


# ----------------- Seleção + Repricing ---------------------
@app.route("/select_products", methods=["POST"])
def select_products():
    skus = request.form.getlist("selected")
    _save_selected_skus(skus)
    return redirect(url_for("review_data"))


@app.route("/selected")
def selected():
    return jsonify({"selected": _read_selected_skus(), "count": len(_read_selected_skus())})


# Helpers de debug/controlo de seleção
@app.get("/actions/select_by_skus")
def actions_select_by_skus():
    """
    Define a seleção pela querystring: /actions/select_by_skus?skus=SKU1,SKU2,SKU3
    """
    skus = (request.args.get("skus") or "").strip()
    arr = [s.strip() for s in skus.split(",") if s.strip()]
    _save_selected_skus(arr)
    return jsonify({"success": True, "selected": arr, "count": len(arr)})


@app.get("/actions/clear_selection")
def actions_clear_selection():
    _save_selected_skus([])
    return jsonify({"success": True, "selected": [], "count": 0})


@app.get("/debug/mapping_selected")
def debug_mapping_selected():
    sel = set(_read_selected_skus())
    dfp = _load_df("data/produtos_processados.csv")
    dfc = _load_df("data/produtos_classificados.csv")
    if "seller_sku" in dfc.columns and "sku" not in dfc.columns:
        dfc = dfc.rename(columns={"seller_sku": "sku"})
    df = dfp.merge(dfc[["sku", "asin", "status"]], on="sku", how="left")
    if sel:
        df = df[df["sku"].astype(str).isin(sel)].copy()
    cols = [c for c in ("sku","asin","status","selling_price","stock") if c in df.columns]
    if df.empty:
        return "<h3>Sem dados/seleção.</h3>"
    return _fallback_table(df.to_dict(orient="records"), cols, "DEBUG: mapping seleção (sku → asin/price/stock)")


# --------------- Consolidação/Insights Amazon --------------
@app.route("/fetch_amazon_data")
def fetch_amazon_data():
    simulate = _get_simulate_flag()
    cfg = load_cfg()
    seller_id = os.getenv("SELLER_ID", "").strip() or None
    df = build_overview(cfg=cfg, seller_id=seller_id, simulate=simulate)
    return jsonify({"success": True, "rows": int(len(df)), "files": ["data/amazon_overview.csv", "data/amazon_overview.json"]})


@app.route("/amazon_overview")
def amazon_overview():
    path = "data/amazon_overview.csv"
    if not os.path.exists(path):
        return redirect(url_for("fetch_amazon_data"))
    df = _load_df(path)
    try:
        return render_template("amazon_overview.html", rows=df.to_dict(orient="records"), cols=list(df.columns))
    except Exception:
        return _fallback_table(df.to_dict(orient="records"), list(df.columns), "Fallback: Amazon Overview")


# -------- Listings: Diag preview (ASIN-only) ---------------
@app.get("/diag_listings_payload_preview")
def diag_listings_payload_preview():
    dfp = _load_df("data/produtos_processados.csv")
    sel = set(_read_selected_skus())
    if sel:
        dfp = dfp[dfp["sku"].astype(str).isin(sel)].copy()
    if dfp.empty:
        return jsonify({"success": False, "error": "Seleção vazia ou sem processados."}), 400

    dfc = _load_df("data/produtos_classificados.csv")
    if "seller_sku" in dfc.columns and "sku" not in dfc.columns:
        dfc = dfc.rename(columns={"seller_sku": "sku"})
    for c in ["sku", "asin", "status"]:
        if c not in dfc.columns:
            dfc[c] = ""
    dfc = dfc[dfc["status"].astype(str).isin(["catalog_match", "listed"])]
    df = dfp.merge(dfc[["sku", "asin", "status"]], on="sku", how="inner")
    if df.empty:
        return jsonify({"success": False, "error": "Sem itens catalog_match/listed na seleção."}), 400

    r = df.iloc[0]
    sku = str(r["sku"]).strip()
    asin = str(r["asin"]).strip().upper()

    def _to_price(x):
        s = str(x).replace(",", ".").strip()
        try:
            return round(float(s), 2)
        except Exception:
            return 0.0

    def _to_qty(x):
        s = str(x).replace(",", ".").strip()
        try:
            return int(float(s))
        except Exception:
            return 0

    price = _to_price(r.get("selling_price", "0"))
    qty = _to_qty(r.get("stock", "0"))
    marketplace_id = os.getenv("MARKETPLACE_ID", "A1RKKUPIHCS9HS")
    seller_id = os.getenv("SELLER_ID", "").strip()

    attributes = {
        "merchant_suggested_asin": [{"value": asin}],
        "condition_type": [{"value": "new_new"}],
        "fulfillment_availability": [{
            "fulfillment_channel_code": "DEFAULT",
            "quantity": qty
        }],
        "purchasable_offer": [{
            "audience": "ALL",
            "currency": "EUR",
            "marketplace_id": marketplace_id,
            "our_price": [{
                "schedule": [{"value_with_tax": price}]
            }]
        }]
    }

    payload = {
        "header": {"sellerId": seller_id, "version": "2.0"},
        "messages": [{
            "messageId": 1,
            "sku": sku,
            "operationType": "UPDATE",
            "productType": "PRODUCT",
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": attributes
        }]}
    return jsonify({"success": True, "payload": payload})


# -------- Listings: PUT (offer-only, ASIN-only) -------------
@app.post("/listings_put_offer")
def listings_put_offer():
    data = request.get_json(force=True)
    sku = str(data.get("sku", "")).strip()
    asin = str(data.get("asin", "")).strip().upper()
    price = float(data.get("price_eur", 0))
    qty = int(float(data.get("quantity", 0)))
    ltd = int(float(data.get("lead_time_days", 1)))
    condition_value = (data.get("condition_type") or "new_new").strip().lower()
    if not (sku and asin):
        return jsonify({"success": False, "error": "sku e asin são obrigatórios"}), 400

    client = AmazonClient(simulate=False)
    seller_id = os.getenv("SELLER_ID", "").strip()
    marketplace_id = client.marketplace_id

    payload = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [{"value": asin}],
            "condition_type": [{"value": condition_value}],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": qty,
                "lead_time_to_ship_max_days": ltd
            }],
            "purchasable_offer": [{
                "audience": "ALL",
                "currency": "EUR",
                "marketplace_id": marketplace_id,
                "our_price": [{
                    "schedule": [{"value_with_tax": round(float(price), 2)}]
                }]
            }]
        }
    }

    url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
    params = {"marketplaceIds": marketplace_id}
    req = requests.Request("PUT", url, params=params, json=payload, headers={
        "content-type": "application/json",
        "user-agent": client.user_agent
    })
    resp = client.session.send(client._sign(req))
    try:
        body = resp.json()
    except Exception:
        body = {"text": resp.text[:2000]}
    return jsonify({"success": resp.ok, "status": resp.status_code,
                    "request": {"url": url, "params": params, "payload": payload},
                    "response": body})


# -------- Listings: GET (estado) ----------------------------
@app.get("/listings_get_item")
def listings_get_item():
    sku = (request.args.get("sku") or "").strip()
    included = (request.args.get("includedData") or "").strip()
    issue_locale = (request.args.get("issueLocale") or "es_ES").strip()
    if not sku:
        return jsonify({"success": False, "error": "sku é obrigatório"}), 400

    client = AmazonClient(simulate=False)
    seller_id = os.getenv("SELLER_ID", "").strip()

    params = {"marketplaceIds": client.marketplace_id, "issueLocale": issue_locale}
    if included:
        params["includedData"] = included

    url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
    req = requests.Request("GET", url, params=params, headers={"user-agent": client.user_agent})
    r = client.session.send(client._sign(req))
    try:
        body = r.json()
    except Exception:
        body = {"text": r.text[:2000]}
    return jsonify({"success": r.ok, "status": r.status_code, "request": {"url": url, "params": params}, "body": body})


# -------- Listings: PATCH (single SKU, top-level only) ------
@app.post("/listings_patch_price_stock_top")
def listings_patch_price_stock_top():
    data = request.get_json(force=True)
    sku = str(data.get("sku", "")).strip()
    price = float(data.get("price_eur", 0))
    qty = int(float(data.get("quantity", 0)))
    if not sku:
        return jsonify({"success": False, "error": "sku é obrigatório"}), 400

    client = AmazonClient(simulate=False)
    seller_id = os.getenv("SELLER_ID", "").strip()
    marketplace_id = client.marketplace_id

    # 1) GET attributes para decidir add/replace
    get_url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
    get_params = {"marketplaceIds": marketplace_id, "includedData": "attributes", "issueLocale": "es_ES"}
    get_req = requests.Request("GET", get_url, params=get_params, headers={"user-agent": client.user_agent})
    get_resp = client.session.send(client._sign(get_req))
    try:
        get_body = get_resp.json()
    except Exception:
        get_body = {"text": get_resp.text[:2000]}
    attrs = (get_body or {}).get("attributes") or {}
    has_fa = bool(attrs.get("fulfillment_availability"))
    has_po = bool(attrs.get("purchasable_offer"))
    op_fa = "replace" if has_fa else "add"
    op_po = "replace" if has_po else "add"

    patches = [
        {
            "op": op_fa,
            "path": "/attributes/fulfillment_availability",
            "value": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": int(max(0, qty))
            }]
        },
        {
            "op": op_po,
            "path": "/attributes/purchasable_offer",
            "value": [{
                "audience": "ALL",
                "currency": "EUR",
                "marketplace_id": marketplace_id,
                "our_price": [{
                    "schedule": [{"value_with_tax": round(float(price), 2)}]
                }]
            }]
        }
    ]
    payload = {"productType": "PRODUCT", "patches": patches}

    # 2) PATCH
    patch_req = requests.Request(
        "PATCH", get_url,
        params={"marketplaceIds": marketplace_id},
        json=payload,
        headers={"content-type": "application/json", "user-agent": client.user_agent}
    )
    patch_resp = client.session.send(client._sign(patch_req))
    try:
        patch_body = patch_resp.json()
    except Exception:
        patch_body = {"text": patch_resp.text[:2000]}

    return jsonify({
        "success": patch_resp.ok,
        "status": patch_resp.status_code,
        "request": {
            "get": {"url": get_url, "params": get_params},
            "patch": {"url": get_url, "params": {"marketplaceIds": marketplace_id}, "payload": payload}
        },
        "response": {
            "get": {"status": get_resp.status_code, "body": get_body},
            "patch": {"status": patch_resp.status_code, "body": patch_body}
        }
    })


# ---- UI Action: PATCH top-level para SKUs selecionadas -----
@app.get("/actions/update_selected_patch_top")
def actions_update_selected_patch_top():
    import html

    sel = set(_read_selected_skus())
    if not sel:
        return "<h3>Sem SKUs selecionados na app.</h3>"

    dfp = _load_df("data/produtos_processados.csv")
    for col in ("sku", "selling_price", "stock"):
        if col not in dfp.columns:
            return f"<h3>Falta a coluna '{col}' em data/produtos_processados.csv.</h3>"

    dfp = dfp[dfp["sku"].astype(str).isin(sel)].copy()
    if dfp.empty:
        return "<h3>A seleção não corresponde a linhas em data/produtos_processados.csv.</h3>"

    def _to_price(x):
        s = str(x).replace(",", ".").strip()
        try:
            return round(float(s), 2)
        except Exception:
            return 0.0

    def _to_qty(x):
        s = str(x).replace(",", ".").strip()
        try:
            return int(float(s))
        except Exception:
            return 0

    rows_html = []
    for _, r in dfp.iterrows():
        sku = str(r["sku"]).strip()
        price = _to_price(r["selling_price"])
        qty = _to_qty(r["stock"])

        with app.test_request_context(json={"sku": sku, "price_eur": price, "quantity": qty}):
            resp = listings_patch_price_stock_top()
        try:
            data = resp.get_json() if hasattr(resp, "get_json") else resp[0].get_json()
        except Exception:
            data = {"success": False, "error": "Resposta inesperada do patch."}

        ok = bool(data.get("success"))
        status = data.get("status")
        issues = (data.get("response", {}) or {}).get("patch", {}).get("body", {}).get("issues", [])
        submission = (data.get("response", {}) or {}).get("patch", {}).get("body", {}).get("submissionId", "")

        rows_html.append(
            f"<tr>"
            f"<td>{html.escape(sku)}</td>"
            f"<td style='text-align:right'>{price:.2f}</td>"
            f"<td style='text-align:right'>{qty}</td>"
            f"<td>{status}</td>"
            f"<td>{'OK' if ok else 'FAIL'}</td>"
            f"<td>{html.escape(str(submission))}</td>"
            f"<td><pre style='white-space:pre-wrap'>{html.escape(json.dumps(issues, ensure_ascii=False))}</pre></td>"
            f"</tr>"
        )

    table = (
        "<h2>Atualização de preço/stock (PATCH top-level) — Selecionados</h2>"
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<thead><tr>"
        "<th>SKU</th><th>Preço</th><th>Stock</th><th>HTTP</th><th>Estado</th><th>Submission</th><th>Issues</th>"
        "</tr></thead>"
        "<tbody>" + "".join(rows_html) + "</tbody></table>"
    )
    return table


# ---- UI Action: PUT oferta ASIN-only — Selecionados --------
@app.get("/actions/update_selected_put")
def actions_update_selected_put():
    import html

    sel = set(_read_selected_skus())
    if not sel:
        return "<h3>Sem SKUs selecionados na app.</h3>"

    dfp = _load_df("data/produtos_processados.csv")
    dfc = _load_df("data/produtos_classificados.csv")
    if "seller_sku" in dfc.columns and "sku" not in dfc.columns:
        dfc = dfc.rename(columns={"seller_sku": "sku"})
    for col in ("sku", "selling_price", "stock"):
        if col not in dfp.columns:
            return f"<h3>Falta a coluna '{col}' em data/produtos_processados.csv.</h3>"
    for col in ("sku", "asin", "status"):
        if col not in dfc.columns:
            return "<h3>Faltam colunas ('sku','asin','status') em data/produtos_classificados.csv.</h3>"

    dfc = dfc[dfc["status"].astype(str).isin(["catalog_match", "listed"])]
    df = dfp.merge(dfc[["sku", "asin"]], on="sku", how="inner")
    df = df[df["sku"].astype(str).isin(sel)].copy()
    if df.empty:
        return "<h3>Seleção sem ASIN catalog_match/listed.</h3>"

    def _to_price(x):
        s = str(x).replace(",", ".").strip()
        try:
            return round(float(s), 2)
        except Exception:
            return 0.0

    def _to_qty(x):
        s = str(x).replace(",", ".").strip()
        try:
            return int(float(s))
        except Exception:
            return 0

    client = AmazonClient(simulate=False)
    seller_id = os.getenv("SELLER_ID", "").strip()
    marketplace_id = client.marketplace_id

    rows_html = []
    for _, r in df.iterrows():
        sku = str(r["sku"]).strip()
        asin = str(r["asin"]).strip().upper()
        price = _to_price(r["selling_price"])
        qty = _to_qty(r["stock"])

        payload = {
            "productType": "PRODUCT",
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": {
                "merchant_suggested_asin": [{"value": asin}],
                "condition_type": [{"value": "new_new"}],
                "fulfillment_availability": [{
                    "fulfillment_channel_code": "DEFAULT",
                    "quantity": int(max(0, qty))
                }],
                "purchasable_offer": [{
                    "audience": "ALL",
                    "currency": "EUR",
                    "marketplace_id": marketplace_id,
                    "our_price": [{
                        "schedule": [{"value_with_tax": price}]
                    }]
                }]
            }
        }

        url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
        params = {"marketplaceIds": marketplace_id}
        req = requests.Request("PUT", url, params=params, json=payload, headers={
            "content-type": "application/json",
            "user-agent": client.user_agent
        })
        resp = client.session.send(client._sign(req))
        try:
            body = resp.json()
        except Exception:
            body = {"text": resp.text[:2000]}

        issues = (body or {}).get("issues", [])
        submission = (body or {}).get("submissionId", "")
        rows_html.append(
            "<tr>"
            f"<td>{html.escape(sku)}</td>"
            f"<td>{html.escape(asin)}</td>"
            f"<td style='text-align:right'>{price:.2f}</td>"
            f"<td style='text-align:right'>{qty}</td>"
            f"<td>{resp.status_code}</td>"
            f"<td>{'OK' if resp.ok else 'FAIL'}</td>"
            f"<td>{html.escape(str(submission))}</td>"
            f"<td><pre style='white-space:pre-wrap'>{html.escape(json.dumps(issues, ensure_ascii=False))}</pre></td>"
            "</tr>"
        )

    table = (
        "<h2>PUT (offer-only, ASIN-only) — Selecionados</h2>"
        "<table border='1' cellpadding='6' cellspacing='0'>"
        "<thead><tr>"
        "<th>SKU</th><th>ASIN</th><th>Preço</th><th>Stock</th><th>HTTP</th><th>Estado</th><th>Submission</th><th>Issues</th>"
        "</tr></thead><tbody>"
        + "".join(rows_html) +
        "</tbody></table>"
    )
    return table


# ------ Fluxo automático (restrições -> PUT -> GET) --------
@app.post("/offer_only/auto_by_asin")
def offer_only_auto_by_asin():
    data = request.get_json(force=True)
    sku = (data.get("sku") or "").strip()
    asin = (data.get("asin") or "").strip().upper()
    price = float(data.get("price_eur") or 0.0)
    qty = int(float(data.get("quantity") or 0))
    ltd = int(float(data.get("lead_time_days") or 1))
    cond = (data.get("condition_type") or "new_new").strip().lower()

    if not (sku and asin):
        return jsonify({"success": False, "error": "sku e asin são obrigatórios"}), 400

    client = AmazonClient(simulate=False)
    seller_id = os.getenv("SELLER_ID", "").strip()
    marketplace_id = client.marketplace_id

    # 1) Restrictions
    rest_url = f"{client.base}/listings/2021-08-01/restrictions"
    rest_params = {
        "asin": asin,
        "sellerId": seller_id,
        "marketplaceIds": marketplace_id,
        "conditionType": cond
    }
    rest_req = requests.Request("GET", rest_url, params=rest_params, headers={"user-agent": client.user_agent})
    rest_resp = client.session.send(client._sign(rest_req))
    try:
        rest_body = rest_resp.json()
    except Exception:
        rest_body = {"text": rest_resp.text[:2000]}

    # 2) PUT
    put_payload = {
        "productType": "PRODUCT",
        "requirements": "LISTING_OFFER_ONLY",
        "attributes": {
            "merchant_suggested_asin": [{"value": asin}],
            "condition_type": [{"value": cond}],
            "fulfillment_availability": [{
                "fulfillment_channel_code": "DEFAULT",
                "quantity": qty,
                "lead_time_to_ship_max_days": ltd
            }],
            "purchasable_offer": [{
                "audience": "ALL",
                "currency": "EUR",
                "marketplace_id": marketplace_id,
                "our_price": [{
                    "schedule": [{"value_with_tax": round(float(price), 2)}]
                }]
            }]
        }
    }
    put_url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
    put_params = {"marketplaceIds": marketplace_id}
    put_req = requests.Request("PUT", put_url, params=put_params, json=put_payload, headers={
        "content-type": "application/json",
        "user-agent": client.user_agent
    })
    put_resp = client.session.send(client._sign(put_req))
    try:
        put_body = put_resp.json()
    except Exception:
        put_body = {"text": put_resp.text[:2000]}

    # 3) GET
    get_url = f"{client.base}/listings/2021-08-01/items/{seller_id}/{sku}"
    get_params = {
        "marketplaceIds": marketplace_id,
        "includedData": "summaries,issues,offers,fulfillmentAvailability,attributes",
        "issueLocale": "es_ES"
    }
    get_req = requests.Request("GET", get_url, params=get_params, headers={"user-agent": client.user_agent})
    get_resp = client.session.send(client._sign(get_req))
    try:
        get_body = get_resp.json()
    except Exception:
        get_body = {"text": get_resp.text[:2000]}

    return jsonify({
        "success": True,
        "request": {
            "restrictions": {"url": rest_url, "params": rest_params},
            "put": {"url": put_url, "params": put_params, "payload": put_payload},
            "get": {"url": get_url, "params": get_params},
        },
        "response": {
            "restrictions": {"status": rest_resp.status_code, "body": rest_body},
            "put": {"status": put_resp.status_code, "body": put_body},
            "get": {"status": get_resp.status_code, "body": get_body},
        }
    })


# --------------------------- Main --------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    # Local only: sem reloader (evita duplicar rotas). No Render, usas waitress-serve.
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
