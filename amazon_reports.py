# amazon_reports.py
# -*- coding: utf-8 -*-
"""
SP-API Reports:
- Cria relatório GET_MERCHANT_LISTINGS_ALL_DATA
- Faz polling até estar pronto
- Faz download e descomprime (gzip)
- Devolve conteúdo TSV (str)
"""
from __future__ import annotations

import io
import os
import time
import json
import gzip
import logging
from typing import Optional, Dict

import requests
from dotenv import load_dotenv

from amazon_client import AmazonClient, MARKTPLACE_FALLBACK

load_dotenv()
log = logging.getLogger(__name__)

HTTP_TIMEOUT_S = int(os.getenv("HTTP_TIMEOUT_S", "60"))
REPORT_POLL_SECONDS = float(os.getenv("REPORT_POLL_SECONDS", "10"))
REPORT_POLL_MAX = int(os.getenv("REPORT_POLL_MAX", "60"))

# Tipos usuais
REPORT_TYPE_ALL_LISTINGS = "GET_MERCHANT_LISTINGS_ALL_DATA"

class AmazonReports:
    def __init__(self, client: AmazonClient):
        self.client = client

    # Usa o mecanismo de request assinado do amazon_client
    def _signed(self, method: str, path: str, params=None, body: bytes = b"", headers: Optional[Dict] = None):
        return self.client._signed(method, path, params=params, body=body, headers=headers)

    def create_report(self, report_type: str, marketplace_id: Optional[str] = None) -> str:
        """
        Cria um relatório e devolve reportId
        """
        mkt = marketplace_id or MARKTPLACE_FALLBACK()
        path = "/reports/2021-06-30/reports"
        body = {
            "reportType": report_type,
            "marketplaceIds": [mkt]
        }
        r = self._signed("POST", path, headers={"content-type":"application/json"}, body=json.dumps(body).encode("utf-8"))
        r.raise_for_status()
        j = r.json() or {}
        report_id = j.get("reportId") or (j.get("payload") or {}).get("reportId")
        if not report_id:
            raise RuntimeError(f"Falha a criar relatório: {j}")
        return report_id

    def get_report(self, report_id: str) -> Dict:
        """
        Lê estado de um relatório
        """
        path = f"/reports/2021-06-30/reports/{report_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json() or {}

    def wait_report_done(self, report_id: str) -> Dict:
        """
        Polling até o relatório estar DONE, devolve payload do get_report()
        """
        for i in range(REPORT_POLL_MAX):
            info = self.get_report(report_id)
            status = (info.get("processingStatus") or (info.get("payload") or {}).get("processingStatus") or "").upper()
            if status in ("DONE", "DONE_NO_DATA", "FATAL"):
                return info
            time.sleep(REPORT_POLL_SECONDS)
        raise TimeoutError(f"Relatório {report_id} não ficou pronto a tempo.")

    def get_report_document(self, report_document_id: str) -> Dict:
        """
        Pede o documento (contém URL de download e compressão)
        """
        path = f"/reports/2021-06-30/documents/{report_document_id}"
        r = self._signed("GET", path)
        r.raise_for_status()
        return r.json() or {}

    def download_document_bytes(self, url: str, compression_algorithm: Optional[str]) -> bytes:
        rr = requests.get(url, timeout=HTTP_TIMEOUT_S)
        rr.raise_for_status()
        raw = rr.content
        if (compression_algorithm or "").upper() == "GZIP":
            return gzip.decompress(raw)
        return raw

    def get_all_listings_tsv(self, marketplace_id: Optional[str] = None) -> str:
        """
        Gera e descarrega o GET_MERCHANT_LISTINGS_ALL_DATA (TSV como string)
        """
        rid = self.create_report(REPORT_TYPE_ALL_LISTINGS, marketplace_id=marketplace_id)
        info = self.wait_report_done(rid)

        doc_id = info.get("reportDocumentId") or (info.get("payload") or {}).get("reportDocumentId")
        if not doc_id:
            raise RuntimeError(f"Sem reportDocumentId: {info}")

        doc = self.get_report_document(doc_id)
        url = doc.get("url")
        compression = doc.get("compressionAlgorithm")
        if not url:
            raise RuntimeError(f"Documento inválido: {doc}")

        b = self.download_document_bytes(url, compression)
        # Garantir que temos texto
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b.decode("latin-1", errors="replace")
