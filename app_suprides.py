# app_suprides.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import logging
from typing import List, Dict, Any

import pandas as pd
from flask import Blueprint, request, jsonify, render_template, current_app

from suprides_identify import classify_suprides_products
from pricing_engine import calc_final_price
from amazon_client import AmazonClient

log = logging.getLogger("app_suprides")

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Caminho único e absoluto para o CSV da Suprides
CLASSIFIED_CSV = os.path.join(DATA_DIR, "suprides_classified.csv")
SUPRIDES_CSV = CLASSIFIED_CSV  # alias para manter compatibilidade

DEFAULT_MARKETPLACE_ID = os.environ.get("DEFAULT_MARKETPLACE_ID", "").strip() or "A1RKKUPIHCS9HS"
MARKETPLACE_ID = os.environ.get("MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)

NEEDED_COLS = [
    "sku", "ean", "brand", "title", "asin", "status", "score", "listed",
    "provenance", "candidates", "stock", "cost", "competitor_price",
    "floor_price", "selling_price"
]

bp = Blueprint("suprides", __name__, url_prefix="/suprides")


def _ensure_columns(df: pd.DataFrame, needed: List[str] | None = None) -> pd.DataFrame:
    """
    Garante que o DataFrame tem todas as colunas 'needed'.
    Se 'needed' for None, usa NEEDED_COLS por defeito.
    """
    cols = needed or NEEDED_COLS
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df


def save_suprides_df(df: pd.DataFrame) -> None:
    """
    Normaliza e grava sempre no mesmo CSV absoluto.
    """
    df = _ensure_columns(df)
    df = df.astype(str).replace("nan", "").fillna("")
    tmp = SUPRIDES_CSV + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8")
    os.replace(tmp, SUPRIDES_CSV)


def load_suprides_df() -> pd.DataFrame:
    """
    Lê sempre do mesmo CSV absoluto.
    """
    if not os.path.exists(SUPRIDES_CSV):
        return pd.DataFrame(columns=NEEDED_COLS)
    df = pd.read_csv(SUPRIDES_CSV, dtype=str, encoding="utf-8")
    df = df.replace("nan", "").fillna("")
    return _ensure_columns(df)


def _pfloat(x):
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", ".")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _summary_by_status(rows: List[dict]) -> dict:
    out = {"total": len(rows)}
    for r in rows:
        st = (r.get("status") or "").strip()
        if not st:
            continue
        out[st] = out.get(st, 0) + 1
    return out


@bp.route("/suprides/classify", methods=["GET"])
def suprides_classify_route():
    """
    Dispara a classificação Suprides e grava o CSV para posterior leitura no UI.
    """
    df = classify_suprides_products(simulate=False)  # garante simulate=False

    if df is None or df.empty:
        # grava CSV vazio com headers (para a UI saber as colunas)
        save_suprides_df(pd.DataFrame(columns=NEEDED_COLS))
        return jsonify({"success": True, "rows": 0, "csv": SUPRIDES_CSV})

    # normaliza headers (caso a function devolva nomes alternativos)
    rename_map = {
        "name": "title",
        "qty_available": "stock",
        "price_cost": "cost",
        "final_price": "selling_price",
    }
    df = df.rename(columns=rename_map)

    # garante colunas esperadas e strings e grava
    save_suprides_df(df)
    return jsonify({"success": True, "rows": int(df.shape[0]), "csv": SUPRIDES_CSV})


@bp.route("/review_classified", methods=["GET"])
def suprides_review_classified():
    """
    Lista de classificados com paginação server-side e filtros/pesquisa server-side.
    - Filtros: brand, status (aceitam um valor; se precisares múltiplos, converto depois)
    - Pesquisa: q (sku, title, asin, ean, brand) sobre TODO o dataset
    - Paginação: page (1..N), page_size (default 100, máx 1000) ou 'all' para trazer tudo
    """
    import math, re

    if not os.path.exists(CLASSIFIED_CSV):
        brands, statuses = [], ["listed", "catalog_match", "catalog_ambiguous", "not_found", "missing_ean"]
        meta = {"total": 0, "page": 1, "pages": 1, "page_size": 100,
                "brands_sel": "", "statuses_sel": "", "q": ""}
        return render_template("suprides_classified.html",
                               rows=[], brands=brands, statuses=statuses,
                               meta=meta, summary={"total": 0})

    # --- parâmetros GET ---
    brand_param  = (request.args.get("brand")  or "").strip()
    status_param = (request.args.get("status") or "").strip()
    q            = (request.args.get("q")      or "").strip()

    # múltiplos (CSV) — mantemos compatibilidade caso venham CSV
    brands_sel   = [b.strip() for b in brand_param.split(",") if b.strip()] if brand_param else []
    statuses_sel = [s.strip() for s in status_param.split(",") if s.strip()] if status_param else []
    brands_sel_l = {b.lower() for b in brands_sel}
    statuses_sel_l = {s.lower() for s in statuses_sel}

    raw_ps = (request.args.get("page_size") or "100").strip().lower()
    show_all = (raw_ps == "all")
    if show_all:
        page_size = None
    else:
        try:
            page_size = int(raw_ps or 100)
        except Exception:
            page_size = 100
        page_size = max(25, min(page_size, 1000))

    try:
        page = int(request.args.get("page") or 1)
    except Exception:
        page = 1
    page = max(1, page)

    # --- leitura eficiente: só colunas necessárias ---
    usecols = ["sku","ean","brand","title","asin","status","score","listed",
               "provenance","candidates","stock","cost","competitor_price",
               "floor_price","selling_price"]
    df = pd.read_csv(
        CLASSIFIED_CSV,
        dtype=str,
        encoding="utf-8",
        usecols=lambda c: c in usecols,
        engine="c",
        memory_map=True,
    ).fillna("")

    # listas para UI
    brands = sorted({str(b).strip() for b in df["brand"].tolist() if str(b).strip()})
    sts = [str(s).strip() for s in df["status"].tolist() if str(s).strip()]
    statuses = sorted(set(sts)) or ["listed","catalog_match","catalog_ambiguous","not_found","missing_ean"]

    # --- filtros/pesquisa (dataset inteiro) ---
    mask = pd.Series(True, index=df.index)

    if brands_sel_l:
        mask &= df["brand"].fillna("").str.strip().str.lower().isin(brands_sel_l)

    if statuses_sel_l:
        mask &= df["status"].fillna("").str.strip().str.lower().isin(statuses_sel_l)

    if q:
        qpat = re.escape(q)
        m_sku   = df["sku"].str.contains(qpat, case=False, regex=True, na=False)
        m_title = df["title"].str.contains(qpat, case=False, regex=True, na=False)
        m_asin  = df["asin"].str.contains(qpat, case=False, regex=True, na=False)
        m_ean   = df["ean"].str.contains(qpat, case=False, regex=True, na=False)
        m_brand = df["brand"].str.contains(qpat, case=False, regex=True, na=False)
        mask &= (m_sku | m_title | m_asin | m_ean | m_brand)

    dff = df.loc[mask]

    # --- paginação (ou "all") ---
    total = int(dff.shape[0])
    if show_all:
        page = 1
        pages = 1
        page_df = dff.copy()
        eff_page_size = "all"
    else:
        pages = max(1, math.ceil(total / page_size))
        if page > pages:
            page = pages
        start = (page - 1) * page_size
        end   = start + page_size
        page_df = dff.iloc[start:end].copy()
        eff_page_size = page_size

    # --- preparar linhas ---
    rows = [{
        "sku": r.get("sku",""),
        "ean": r.get("ean",""),
        "brand": r.get("brand",""),
        "title": r.get("title",""),
        "asin": r.get("asin",""),
        "status": r.get("status",""),
        "score": r.get("score",""),
        "listed": r.get("listed",""),
        "provenance": r.get("provenance",""),
        "candidates": r.get("candidates","[]"),
        "stock": r.get("stock",""),
        "cost": r.get("cost",""),
        "competitor_price": r.get("competitor_price",""),
        "floor_price": r.get("floor_price",""),
        "selling_price": r.get("selling_price",""),
    } for _, r in page_df.iterrows()]

    # --- meta e summary ---
    meta = {
        "total": total, "page": page, "pages": pages, "page_size": eff_page_size,
        "brands_sel": ",".join(brands_sel), "statuses_sel": ",".join(statuses_sel), "q": q
    }
    summary = {"total": total}
    for s in ["catalog_ambiguous","catalog_match","listed","missing_ean","not_found"]:
        summary[s] = int((dff["status"].str.strip().str.lower() == s).sum())

    return render_template("suprides_classified.html",
                           rows=rows,
                           brands=brands,
                           statuses=statuses,
                           meta=meta,
                           summary=summary)


@bp.get("/review_data")
def suprides_review_data():
    """
    API JSON para o UI pedir blocos (offset/limit) filtrados.
    """
    brand = (request.args.get("brand") or "").strip()
    status = (request.args.get("status") or "").strip()
    q = (request.args.get("q") or "").strip().lower()
    try:
        limit = int(request.args.get("limit") or 400)
    except Exception:
        limit = 400
    limit = max(100, min(limit, 1000))
    try:
        offset = int(request.args.get("offset") or 0)
    except Exception:
        offset = 0
    offset = max(0, offset)

    usecols = [
        "sku","ean","brand","title","asin","status","score","listed",
        "provenance","candidates","stock","cost","competitor_price",
        "floor_price","selling_price"
    ]

    if not os.path.exists(CLASSIFIED_CSV):
        return jsonify({"total": 0, "offset": offset, "limit": limit, "rows": []})

    df = pd.read_csv(
        CLASSIFIED_CSV, dtype=str, encoding="utf-8",
        usecols=lambda c: c in usecols, engine="c", memory_map=True
    ).fillna("")

    if brand:
        df = df[df["brand"].str.strip() == brand]
    if status:
        df = df[df["status"].str.strip() == status]
    if q:
        txt = df.apply(lambda r: f"{r.get('sku','')} {r.get('title','')} {r.get('asin','')} {r.get('ean','')} {r.get('brand','')}".lower(), axis=1)
        df = df[txt.str.contains(q, na=False)]

    total = int(df.shape[0])
    if offset >= total:
        return jsonify({"total": total, "offset": offset, "limit": limit, "rows": []})

    dfp = df.iloc[offset: offset + limit].copy()
    rows = [{
        "sku": r.get("sku",""),
        "ean": r.get("ean",""),
        "brand": r.get("brand",""),
        "title": r.get("title",""),
        "asin": r.get("asin",""),
        "status": r.get("status",""),
        "score": r.get("score",""),
        "listed": r.get("listed",""),
        "provenance": r.get("provenance",""),
        "candidates": r.get("candidates","[]"),
        "stock": r.get("stock",""),
        "cost": r.get("cost",""),
        "competitor_price": r.get("competitor_price",""),
        "floor_price": r.get("floor_price",""),
        "selling_price": r.get("selling_price",""),
    } for _, r in dfp.iterrows()]

    return jsonify({"total": total, "offset": offset, "limit": limit, "rows": rows})


@bp.route("/enrich_competitive", methods=["POST"])
def suprides_enrich_competitive():
    """
    Atualiza preços competitivos e volta a gravar o CSV.
    """
    try:
        payload = request.get_json(silent=True) or {}
        asins = payload.get("asins") or []
        asins = [a.strip().upper() for a in asins if isinstance(a, str) and a.strip()]
        current_app.logger.info("enrich_competitive payload asins=%s", asins)

        if not os.path.exists(CLASSIFIED_CSV):
            return jsonify({"items": [], "message": "Sem classificação disponível", "received": len(asins), "updated": 0}), 200

        df = pd.read_csv(CLASSIFIED_CSV, dtype=str, encoding="utf-8").fillna("")
        df = _ensure_columns(df, ["asin", "competitor_price", "selling_price", "floor_price", "cost"])

        marketplace_id = os.environ.get("MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)
        ac = AmazonClient(simulate=False)
        results = []
        updated_count = 0

        for asin in asins:
            try:
                comp_val, fonte = ac.pricing_pick_competitor_any(
                    asin=asin,
                    marketplace_id=marketplace_id,
                    exclude_self=True,
                    fallback_when_excluding=False
                )
            except Exception as e:
                comp_val, fonte = None, f"erro:{e}"

            competitor_price = f"{comp_val:.2f}" if comp_val is not None else ""
            if not competitor_price and not (fonte or "").startswith("erro"):
                fonte = fonte or "no_competition"

            mask = df["asin"].str.upper() == asin
            if mask.any():
                idx = int(df.index[mask][0])

                if competitor_price:
                    df.at[idx, "competitor_price"] = competitor_price

                sp_before = df.at[idx, "selling_price"]
                cost_s = df.at[idx, "cost"] or ""
                try:
                    cost_val = float(cost_s) if cost_s else None
                except Exception:
                    cost_val = None

                if cost_val is not None:
                    try:
                        px = calc_final_price(cost=cost_val, competitor_price=comp_val)
                        if px.get("floor_price") is not None:
                            df.at[idx, "floor_price"] = f"{px['floor_price']:.2f}"
                        if px.get("final_price") is not None:
                            df.at[idx, "selling_price"] = f"{px['final_price']:.2f}"
                    except Exception:
                        pass

                sp_after = df.at[idx, "selling_price"]
                updated = (competitor_price != "") or (sp_after != sp_before)
                if updated:
                    updated_count += 1

                results.append({
                    "asin": asin,
                    "competitor_price": competitor_price,
                    "selling_price": sp_after,
                    "updated": updated,
                    "reason": fonte if (competitor_price or (sp_after != sp_before)) else (fonte or "no_offers"),
                })
            else:
                results.append({
                    "asin": asin,
                    "competitor_price": competitor_price,
                    "selling_price": "",
                    "updated": False,
                    "reason": fonte or "ASIN não está na tabela",
                })

        try:
            tmp = CLASSIFIED_CSV + ".tmp"
            df.to_csv(tmp, index=False, encoding="utf-8")
            os.replace(tmp, CLASSIFIED_CSV)
        except Exception as e:
            current_app.logger.error("Falha a gravar CSV %s: %s", CLASSIFIED_CSV, e)

        out = {"items": results, "received": len(asins), "updated": updated_count, "message": "OK"}
        current_app.logger.info("enrich_competitive result: %s", out)
        return jsonify(out), 200

    except Exception as e:
        current_app.logger.exception("Erro no enrich_competitive")
        return jsonify({"error": str(e)}), 500


# ========= NOVO: Push para Amazon (selecionados) =========
@bp.route("/push_selected", methods=["POST"])
def suprides_push_selected():
    """
    Recebe SKUs e/ou ASINs, consolida a lista a partir do CSV e envia:
      - PRODUCT (se SKU ainda não listada, mas tiver ASIN)
      - INVENTORY (quantidade)
      - PRICING (preço)
    """
    try:
        payload = request.get_json(silent=True) or {}
        # Aceita tanto 'skus' como 'asins' por compatibilidade
        skus_in = payload.get("skus") or []
        asins_in = payload.get("asins") or []
        skus_in = [str(s).strip() for s in skus_in if str(s).strip()]
        asins_in = [str(a).strip().upper() for a in asins_in if str(a).strip()]

        if not skus_in and not asins_in:
            return jsonify({"ok": False, "message": "Sem SKUs/ASINs no payload."}), 400

        if not os.path.exists(CLASSIFIED_CSV):
            return jsonify({"ok": False, "message": "CSV não encontrado. Classifica primeiro."}), 400

        df = pd.read_csv(CLASSIFIED_CSV, dtype=str, encoding="utf-8").fillna("")
        df = _ensure_columns(df)

        # Se vieram só ASINs, mapeia para SKUs
        if asins_in and not skus_in:
            mask = df["asin"].str.upper().isin(asins_in)
            skus_in = [str(x).strip() for x in df.loc[mask, "sku"].tolist() if str(x).strip()]

        # Filtra CSV pelos SKUs
        dff = df[df["sku"].isin(skus_in)].copy()
        if dff.empty:
            return jsonify({"ok": False, "message": "SKUs não encontrados no CSV."}), 404

        entries = []
        for _, r in dff.iterrows():
            sku = str(r.get("sku", "")).strip()
            asin = str(r.get("asin", "")).strip()
            listed = str(r.get("listed", "")).strip().lower() == "yes"

            # quantidade
            qty_raw = str(r.get("stock", "")).strip()
            qty = 0
            try:
                qty = max(0, int(float(qty_raw.replace(",", ".")))) if qty_raw else 0
            except Exception:
                qty = 0

            # preço
            price_raw = str(r.get("selling_price", "")).strip()
            price = None
            try:
                price = float(price_raw.replace(",", ".")) if price_raw else None
            except Exception:
                price = None

            entries.append({
                "sku": sku,
                "asin": asin,
                "listed": listed,
                "quantity": qty,
                "price": price
            })

        # Separa os que precisam de PRODUCT (criação) dos já listados
        to_create = [e for e in entries if (not e["listed"]) and e["asin"]]
        ac = AmazonClient(simulate=False)
        feed_results = []
        mpid = os.environ.get("MARKETPLACE_ID", DEFAULT_MARKETPLACE_ID)

        # 1) PRODUCT (se necessário)
        if to_create:
            xml_prod = ac.build_product_feed(to_create, marketplace_id=mpid)
            fid = ac.submit_feed_xml("POST_PRODUCT_DATA", xml_prod)
            feed_results.append({"type": "PRODUCT", "feedId": fid})
            ac.wait_feed_done(fid)

        # 2) INVENTORY (todos)
        if entries:
            xml_inv = ac.build_inventory_feed(entries, marketplace_id=mpid)
            fid = ac.submit_feed_xml("POST_INVENTORY_AVAILABILITY_DATA", xml_inv)
            feed_results.append({"type": "INVENTORY", "feedId": fid})
            ac.wait_feed_done(fid)

        # 3) PRICING (quem tem preço)
        priced = [e for e in entries if e["price"] is not None]
        if priced:
            xml_pr = ac.build_price_feed(priced, marketplace_id=mpid, currency="EUR")
            fid = ac.submit_feed_xml("POST_PRODUCT_PRICING_DATA", xml_pr)
            feed_results.append({"type": "PRICING", "feedId": fid})
            ac.wait_feed_done(fid)

        return jsonify({"ok": True, "submitted": feed_results, "count": len(entries)}), 200

    except Exception as e:
        current_app.logger.exception("Falha em push_selected")
        return jsonify({"ok": False, "message": str(e)}), 500


# ---------- DEBUG ----------
@bp.route("/debug/offers", methods=["POST"])
def debug_offers():
    try:
        payload = request.get_json(silent=True) or {}
        asin = (payload.get("asin") or "").strip().upper()
        marketplace_id = payload.get("marketplace_id") or MARKETPLACE_ID
        if not asin:
            return jsonify({"error": "asin em falta"}), 400

        ac = AmazonClient(simulate=False)
        raw = ac.pricing_get_item_offers(asin=asin, marketplace_id=marketplace_id, item_condition="New") or {}
        p = raw.get("payload", raw)

        rows = []
        for off in (p.get("Offers") or []):
            sid = (off.get("SellerId") or "").strip()
            lp = off.get("ListingPrice") or {}
            sp = off.get("Shipping") or {}
            try:
                landed = float(lp.get("Amount", 0)) + float(sp.get("Amount", 0))
            except Exception:
                continue
            rows.append({
                "seller_id": sid,
                "listing_price": lp.get("Amount"),
                "shipping": sp.get("Amount"),
                "landed": landed
            })
        return jsonify({"asin": asin, "marketplace_id": marketplace_id, "offers": rows}), 200
    except Exception as e:
        current_app.logger.exception("debug_offers falhou")
        return jsonify({"error": str(e)}), 500


@bp.get("/debug/row")
def suprides_debug_row():
    asin = str(request.args.get("asin", "")).strip().upper()
    if not asin:
        return jsonify({"error": "asin em falta"}), 400
    if not os.path.exists(CLASSIFIED_CSV):
        return jsonify({"error": f"CSV não encontrado: {CLASSIFIED_CSV}"}), 400
    df = pd.read_csv(CLASSIFIED_CSV, dtype=str, encoding="utf-8").fillna("")
    df = _ensure_columns(df, ["asin"])
    mask = df["asin"].str.upper() == asin
    if not mask.any():
        return jsonify({"error": f"ASIN {asin} não encontrado no CSV"}), 404
    row = df[mask].iloc[0].to_dict()
    return jsonify({"row": row})
