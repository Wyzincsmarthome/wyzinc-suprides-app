# amazon_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
import uuid
import gzip
import io
import logging
from typing import Any, Dict, List, Optional

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


def _mk_logger(name: str = "amazon_client") -> logging.Logger:
    log = logging.getLogger(name)
    if not log.handlers:
        h = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s %(levelname)s:%(name)s:%(message)s")
        h.setFormatter(fmt)
        log.addHandler(h)
    log.setLevel(logging.INFO)
    return log


class AmazonClient:
    def __init__(self, simulate: bool = False) -> None:
        self.simulate = simulate
        self.endpoint = os.environ.get("SPAPI_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com").rstrip("/")
        self.region = os.environ.get("SPAPI_REGION", "eu-west-1")
        self.marketplace_id = os.environ.get("MARKETPLACE_ID", "A1RKKUPIHCS9HS")
        self.seller_id = os.environ.get("SELLER_ID", "")
        self.app_client_id = os.environ.get("LWA_CLIENT_ID", "")
        self.app_client_secret = os.environ.get("LWA_CLIENT_SECRET", "")
        self.refresh_token = os.environ.get("LWA_REFRESH_TOKEN", "")
        self.role_arn = os.environ.get("ROLE_ARN", "")
        self.aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        self.aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        self.timeout = int(os.environ.get("SPAPI_TIMEOUT", "30"))

        self.session = self._mk_session()
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

        self._logger = logging.getLogger("amazon_client")
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
            self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def _mk_session(self) -> requests.Session:
        s = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "PATCH"])
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=8, pool_maxsize=16)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": "wyzinc-suprides-app/1.0"})
        return s

    # -------------------- LWA --------------------
    def _ensure_access_token(self) -> str:
        if self.simulate:
            self._access_token = "SIMULATED"
            self._token_expires_at = time.time() + 3600
            return self._access_token

        now = time.time()
        if self._access_token and now < (self._token_expires_at - 60):
            return self._access_token

        client_id = os.environ.get("LWA_CLIENT_ID")
        client_secret = os.environ.get("LWA_CLIENT_SECRET")
        refresh_token = os.environ.get("LWA_REFRESH_TOKEN")
        if not client_id or not client_secret or not refresh_token:
            raise RuntimeError("LWA creds em falta (LWA_CLIENT_ID/SECRET/REFRESH_TOKEN).")

        resp = requests.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            raise requests.HTTPError(f"LWA {resp.status_code}: {getattr(resp,'text','')}", response=resp)
        data = resp.json()
        self._access_token = data.get("access_token")
        self._token_expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._access_token

    # -------------------- SigV4 --------------------
    def _sign_prepared(self, p: requests.PreparedRequest) -> requests.PreparedRequest:
        if not (self.aws_key and self.aws_secret):
            self._logger.warning("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY em falta — chamadas reais podem falhar.")

        creds = Credentials(self.aws_key, self.aws_secret)
        aws_req = AWSRequest(
            method=p.method,
            url=p.url,
            data=p.body or b"",
            headers=dict(p.headers),
        )
        SigV4Auth(creds, "execute-api", self.region).add_auth(aws_req)

        for k, v in aws_req.headers.items():
            p.headers[k] = v
        return p

    # -------------------- request wrapper --------------------
    def request(self, method: str, path: str, params: dict | None = None, json: dict | None = None):
        method = method.upper()
        url = f"{self.endpoint}{path}"

        def _send_once():
            self._ensure_access_token()
            headers = {
                "x-amz-access-token": self._access_token or "",
                "content-type": "application/json",
                "accept": "application/json",
            }
            req = requests.Request(method=method, url=url, params=params or {}, json=json, headers=headers)
            prep = self.session.prepare_request(req)
            prep = self._sign_prepared(prep)
            resp = self.session.send(prep, timeout=self.timeout)

            if resp.status_code >= 400:
                try:
                    out = resp.json()
                except Exception:
                    out = {"status_code": resp.status_code, "text": getattr(resp, "text", "")}
                raise requests.HTTPError(f"SP-API {resp.status_code}: {out}", response=resp)

            try:
                return resp.json()
            except Exception:
                return {"status_code": resp.status_code, "text": getattr(resp, "text", "")}

        try:
            return _send_once()
        except requests.HTTPError as e:
            msg = str(e)
            if ("access token" in msg.lower()) or ("unauthorized" in msg.lower()) or ("403" in msg):
                self._access_token = None
                return _send_once()
            raise

    # -------------------- helpers de parsing --------------------

    def _safe_str(self, x):
        try:
            return "" if x is None else str(x)
        except Exception:
            return ""

    def _gzip(self, data: bytes) -> bytes:
        """
        Comprime em GZIP quando createFeedDocument foi pedido com compressionAlgorithm=GZIP.
        Se falhar por algum motivo, devolve o original para não quebrar o fluxo.
        """
        import gzip
        try:
            return gzip.compress(data)
        except Exception:
            return data
    

    @staticmethod
    def _resp_to_json(resp: Any) -> Dict[str, Any]:
        if isinstance(resp, dict):
            return resp
        if isinstance(resp, Response):
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text, "status": resp.status_code}
        return {}

    # =================== PRICING v0 ===================

    def pricing_get_item_offers(self, asin: str, marketplace_id: str, item_condition: str | None = None):
        asin = (asin or "").strip().upper()
        qs = {"MarketplaceId": marketplace_id}
        if item_condition:
            qs["ItemCondition"] = item_condition  # 'New', 'Used', etc.
        path = f"/products/pricing/v0/items/{asin}/offers"
        return self.request("GET", path, params=qs, json=None)

    def pricing_get_competitive_v0(self, asins: List[str], marketplace_id: str) -> Dict[str, Any]:
        asins = [str(a).strip().upper() for a in asins if str(a).strip()]
        params = {"MarketplaceId": marketplace_id, "Asins": ",".join(asins)}
        return self.request(method="GET", path="/products/pricing/v0/competitivePrice", params=params, json=None)

    # -------------------- CATALOG 2022-04-01 --------------------

    def catalog_search_by_ean(self, ean: str, marketplace_id: str) -> Dict[str, Any]:
        ean = (ean or "").strip()
        if not ean:
            return {"error": "EAN em falta"}
        params = {
            "identifiers": ean,
            "identifiersType": "EAN",
            "marketplaceIds": marketplace_id,
            "include": "identifiers,attributes,summaries"
        }
        return self.request("GET", "/catalog/2022-04-01/items", params=params, json=None)

    # =================== FEEDS: Builders XML ===================

    def _envelope_header(self, message_type: str, message_count: int, merchant_id: Optional[str] = None) -> str:
        merchant_id = merchant_id or os.environ.get("MERCHANT_ID") or os.environ.get("SELLER_ID") or ""
        return f"""<?xml version="1.0" encoding="utf-8"?>
<AmazonEnvelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="amznenvelope.xsd">
  <Header>
    <DocumentVersion>1.01</DocumentVersion>
    <MerchantIdentifier>{merchant_id}</MerchantIdentifier>
  </Header>
  <MessageType>{message_type}</MessageType>
  <PurgeAndReplace>false</PurgeAndReplace>
"""

    def build_product_feed(self, entries: List[Dict[str, Any]], marketplace_id: str) -> str:
        parts = [self._envelope_header("Product", len(entries))]
        msg_id = 1
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        for e in entries:
            sku = e["sku"]
            asin = e["asin"]
            parts.append(f"""
  <Message>
    <MessageID>{msg_id}</MessageID>
    <OperationType>Update</OperationType>
    <Product>
      <SKU>{sku}</SKU>
      <StandardProductID>
        <Type>ASIN</Type>
        <Value>{asin}</Value>
      </StandardProductID>
      <Condition>
        <ConditionType>New</ConditionType>
      </Condition>
      <LaunchDate>{now}</LaunchDate>
    </Product>
  </Message>""")
            msg_id += 1
        parts.append("\n</AmazonEnvelope>")
        return "".join(parts)

    def build_inventory_feed(self, entries: List[Dict[str, Any]], marketplace_id: str) -> str:
        parts = [self._envelope_header("Inventory", len(entries))]
        msg_id = 1
        for e in entries:
            sku = e["sku"]
            qty = max(0, int(e.get("quantity", 0)))
            parts.append(f"""
  <Message>
    <MessageID>{msg_id}</MessageID>
    <OperationType>Update</OperationType>
    <Inventory>
      <SKU>{sku}</SKU>
      <Quantity>{qty}</Quantity>
      <FulfillmentLatency>1</FulfillmentLatency>
    </Inventory>
  </Message>""")
            msg_id += 1
        parts.append("\n</AmazonEnvelope>")
        return "".join(parts)

    def build_price_feed(self, entries: List[Dict[str, Any]], marketplace_id: str, currency: str = "EUR") -> str:
        parts = [self._envelope_header("Price", len(entries))]
        msg_id = 1
        for e in entries:
            sku = e["sku"]
            price = float(e.get("price"))
            parts.append(f"""
  <Message>
    <MessageID>{msg_id}</MessageID>
    <Price>
      <SKU>{sku}</SKU>
      <StandardPrice currency="{currency}">{price:.2f}</StandardPrice>
    </Price>
  </Message>""")
            msg_id += 1
        parts.append("\n</AmazonEnvelope>")
        return "".join(parts)

    # =================== FEEDS: Submissão SP-API ===================

    def submit_feed_xml(self, feed_type: str, xml_body: str) -> str:
        """
        1) POST /feeds/2021-06-30/documents  -> devolve feedDocumentId, url e, por vezes, headers obrigatórios.
        2) PUT para o URL pré-assinado do S3 -> usar EXACTAMENTE os headers devolvidos (ou, na falta deles, Content-Type igual ao pedido).
        3) POST /feeds/2021-06-30/feeds      -> devolve feedId.
        """
        # 1) cria o documento de feed com GZIP (corpo será comprimido)
        doc = self.request(
            "POST",
            "/feeds/2021-06-30/documents",
            json={"contentType": "text/xml; charset=UTF-8", "compressionAlgorithm": "GZIP"},
            params=None
        )
        doc_id = doc["feedDocumentId"]
        upload_url = doc["url"]
        # alguns ambientes devolvem 'headers' assinados que DEVEM ser replicados no PUT
        doc_headers = doc.get("headers") or {}
        # em alguns responses vem também contentType — usa-o se presente
        content_type = doc.get("contentType") or "text/xml; charset=UTF-8"

        # 2) GZIP do corpo (sem inventar headers extra)
        gz_body = self._gzip(xml_body.encode("utf-8"))

        # constrói headers do PUT: usa 1:1 os devolvidos; senão, apenas Content-Type igual ao pedido
        put_headers = {}
        if doc_headers:
            for k, v in doc_headers.items():
                put_headers[k] = v
        else:
            put_headers["Content-Type"] = content_type

        # IMPORTANTE: não acrescentar 'Content-Encoding' se não vier nos headers assinados
        up_resp = requests.put(upload_url, data=gz_body, headers=put_headers, timeout=90, allow_redirects=True)
        if up_resp.status_code >= 400:
            snippet = up_resp.text[:400] if hasattr(up_resp, "text") else f"bytes={len(up_resp.content)}"
            raise RuntimeError(f"Falha upload feed S3: {up_resp.status_code} {snippet}")

        # 3) cria o feed a apontar para o documento carregado
        payload = {
            "feedType": feed_type,
            "marketplaceIds": [self.marketplace_id],
            "inputFeedDocumentId": doc_id
        }
        res = self.request("POST", "/feeds/2021-06-30/feeds", json=payload, params=None)
        return res.get("feedId") or res.get("FeedId") or ""


    def _download_feed_result(self, doc_id: str) -> dict:
        """
        getFeedDocument -> descarrega resultado respeitando headers devolvidos.
        Descomprime se vier com compressionAlgorithm=GZIP.
        """
        meta = self.request("GET", f"/feeds/2021-06-30/documents/{doc_id}", params=None, json=None)
        url = meta.get("url")
        if not url:
            return {"error": "no url"}
        dl_headers = meta.get("headers") or {}
        r = requests.get(url, headers=dl_headers, timeout=90, allow_redirects=True)

        raw = r.content or b""
        compression = (meta.get("compressionAlgorithm") or "").upper()
        if compression == "GZIP":
            import gzip
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
        try:
            return {"text": raw.decode("utf-8", "ignore")}
        except Exception:
            return {"bytes": len(raw)}

    def wait_feed_done(self, feed_id: str, timeout_sec: int = 600, poll_sec: int = 10) -> Dict[str, Any]:
        t0 = time.time()
        status = None
        result = None
        while True:
            st = self.request("GET", f"/feeds/2021-06-30/feeds/{feed_id}", params=None, json=None)
            status = st.get("processingStatus")
            if status in ("DONE", "CANCELLED", "FATAL", "IN_FATAL_STATE", "ERROR"):
                if status == "DONE" and st.get("resultFeedDocumentId"):
                    result = self._download_feed_result(st["resultFeedDocumentId"])
                break
            if (time.time() - t0) > timeout_sec:
                break
            time.sleep(poll_sec)
        return {"status": status, "report": result}

    
