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
 from auto_product_type import AutoPT
 from csv_processor_visiotech import process_csv, load_cfg
 from product_identify import classify_products
 from amazon_insights import build_overview
 from routes_enrich import bp_enrich
 from inventory_sync import refresh_inventory
-from app_suprides import bp as suprides_bp  # blueprint da Suprides
+from app_suprides import bp as suprides_bp, _feed_metadata  # blueprint da Suprides
 from pricing_engine import calc_final_price
 from storage import get_storage
 
 # Importação da função de classificação da Suprides.
 try:
     from suprides_identify import classify_suprides_products
 except Exception:
     classify_suprides_products = None
 
 # -------------------------- Registar blueprints UMA vez -------------------
 app.register_blueprint(bp_enrich)     # já existia no teu projeto
 app.register_blueprint(suprides_bp)   # blueprint da Suprides
 
 # -------------------------- Pastas e ficheiros ----------------------------
 os.makedirs("data", exist_ok=True)
 os.makedirs("uploads", exist_ok=True)
 os.makedirs("logs", exist_ok=True)
 
 SETTINGS_FILE = "data/settings.json"
 SELECTED_SKUS_FILE = "data/selected_skus.json"
 
 # ---------- Health ----------
 @app.route("/healthz", methods=["GET"])
 def healthz():
     return jsonify({"ok": True, "ts": time.time()})
@@ -177,50 +177,51 @@ def _fallback_table(rows: list, cols: list, title: str) -> str:
     import html
     th = "".join([f"<th>{html.escape(c)}</th>" for c in cols])
     trs = []
     for r in rows:
         tds = "".join([f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols])
         trs.append(f"<tr>{tds}</tr>")
     return f"<h2>{html.escape(title)}</h2><table border='1' cellpadding='6' cellspacing='0'><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"
 
 
 # ------------------------ UI -------------------------------
 @app.route("/")
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
+        "suprides_feed": _feed_metadata(),
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
             ("/suprides/classify_async", "Classificar Suprides (ASSÍNCRONO)"),
             ("/jobs/suprides/status", "Estado do job Suprides"),
         ]
         a = "".join([f"<li><a href='{u}'>{t}</a></li>" for u,t in links])
         return f"<h1>App — UI básica</h1><ul>{a}</ul><pre>{json.dumps(stats, ensure_ascii=False, indent=2)}</pre>"
 
 
 # ------------------ Suprides Classification (SINCRONO) ------------------
 @app.route("/suprides/classify")
 def suprides_classify_route():
     """
