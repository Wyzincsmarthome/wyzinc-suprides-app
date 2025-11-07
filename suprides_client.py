 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/suprides_client.py b/suprides_client.py
index d055feeae74656d52c7f132f29189f10f4a83e3d..856a9c5a3b967d9edecc30a54542f8825aa1c08c 100644
--- a/suprides_client.py
+++ b/suprides_client.py
@@ -1,33 +1,42 @@
 import os
 import time
 import logging
 from typing import Dict, Iterable, Optional
 
 import requests
 
 
+class SupridesAPIError(RuntimeError):
+    """Erro ao comunicar com a API da Suprides."""
+
+    def __init__(self, message: str, *, response: Optional[requests.Response] = None, status_code: Optional[int] = None) -> None:
+        super().__init__(message)
+        self.response = response
+        self.status_code = status_code
+
+
 log = logging.getLogger("suprides_client")
 
 
 def _env(name: str, default: Optional[str] = None) -> Optional[str]:
     v = os.environ.get(name)
     return v if v not in (None, "", "None") else default
 
 
 class SupridesClient:
     """
     Cliente resiliente para a API da Suprides, com:
       - Header Authorization: Bearer <SUPRIDES_BEARER> sempre que definido
       - auth_mode "query": adiciona user/password na query
       - timeouts folgados e retries leves com backoff
     Variáveis de ambiente:
       SUPRIDES_BASE_URL (ex: https://www.suprides.pt)
       SUPRIDES_PRODUCTS_PATH (ex: /rest/V1/integration/products-list)
       SUPRIDES_BEARER
       SUPRIDES_AUTH_MODE = "query" | "header"
       SUPRIDES_USER, SUPRIDES_PASSWORD
       SUPRIDES_TIMEOUT (segundos, default 60)
       SUPRIDES_LIMIT (default 250)
       SUPRIDES_DEBUG (1 para mais logs)
     """
 
@@ -59,151 +68,171 @@ class SupridesClient:
             h["Authorization"] = f"Bearer {self.bearer}"
         return h
 
     def _auth_query(self) -> Dict[str, str]:
         # Quando a Suprides quer auth por query
         q: Dict[str, str] = {}
         if self.auth_mode == "query":
             if self.user:
                 q["user"] = self.user
             if self.password:
                 q["password"] = self.password
         return q
 
     def _request(self, method: str, path: str, params: Optional[Dict] = None) -> requests.Response:
         url = f"{self.base_url}{path}"
         params = dict(params or {})
         params.update(self._auth_query())  # injeta user/password se auth=query
 
         headers = {
             "Accept": "application/json",
             "User-Agent": "wyzinc-suprides-client/1.0",
         }
         headers.update(self._auth_headers())  # injeta Bearer, se houver
 
         # Retries leves com backoff: 0.5s, 1s, 2s
-        last_exc = None
+        last_exc: Optional[Exception] = None
+        last_response: Optional[requests.Response] = None
         for attempt in range(3):
             try:
                 if self.debug:
                     log.debug(
                         "REQ %s %s params=%s headers=%s",
                         method, url, params,
                         {k: (v if k != "Authorization" else "Bearer ***") for k, v in headers.items()},
                     )
                 resp = requests.request(method, url, params=params, headers=headers, timeout=self.timeout)
                 if self.debug:
                     log.debug("RESP %s %s -> %s %s", method, url, resp.status_code, resp.reason)
                 if resp.status_code == 401:
                     # Mesmo com Bearer + query auth, deu 401: problema de permissões do lado da Suprides
                     try:
                         body = resp.text
                     except Exception:
                         body = "<no body>"
                     log.warning("HTTP 401 em %s params=%s body=%s", url, params, body[:500])
                 if resp.status_code in (429, 500, 502, 503, 504):
+                    last_response = resp
                     time.sleep(0.5 * (2 ** attempt))
                     continue
                 return resp
             except Exception as e:
                 last_exc = e
                 log.warning("Falha a chamar %s %s: %s", method, url, e)
                 time.sleep(0.5 * (2 ** attempt))
 
         if last_exc:
-            raise last_exc
-        raise RuntimeError("Falha desconhecida ao chamar Suprides.")
+            raise SupridesAPIError(f"Erro ao chamar {method} {url}: {last_exc}") from last_exc
+        if last_response is not None:
+            raise SupridesAPIError(
+                f"HTTP {last_response.status_code} em {method} {url} após retries",
+                response=last_response,
+                status_code=last_response.status_code,
+            )
+        raise SupridesAPIError(f"Falha desconhecida ao chamar {method} {url}")
 
     # ---------- EXTRAÇÃO ROBUSTA DA LISTA ----------
     def _extract_items(self, data):
         """
         Extrai a lista de produtos de várias estruturas:
         - lista direta
         - dict com 'items', 'products', 'rows', 'result', 'content'
         - dict com 'data' (que pode ser lista ou dict aninhado)
         """
         if isinstance(data, list):
             return data
 
         if isinstance(data, dict):
             for k in ("items", "products", "rows", "result", "content"):
                 v = data.get(k)
                 if isinstance(v, list):
                     return v
 
             v = data.get("data")
             if isinstance(v, list):
                 return v
             if isinstance(v, dict):
                 return self._extract_items(v)
 
         return []
 
     # ---------- ITERADOR DE PRODUTOS ----------
     def iter_products(self, limit: Optional[int] = None) -> Iterable[dict]:
         """
         Lê catálogo paginado. Devolve cada item como dict.
         Usa 'limit' (env SUPRIDES_LIMIT ou 250).
         Paginação por page=1..N. Se falhar, tenta offset.
         """
         limit = int(limit or self.limit_default)
         path = self.products_path
 
         # 1) Por página
         page = 1
         total_items = 0
+        page_failed = False
         while True:
-            resp = self._request("GET", path, params={"limit": limit, "page": page})
+            try:
+                resp = self._request("GET", path, params={"limit": limit, "page": page})
+            except SupridesAPIError as exc:
+                log.warning("Falha na paginação page-based (page=%s): %s", page, exc)
+                page_failed = True
+                break
             if resp.status_code == 401:
                 raise PermissionError("401 Unauthorized: confirma permissões Toogas_Integration::integration_api do teu consumer.")
             if resp.status_code != 200:
-                log.warning("Falha a ler page=%s via limit/page. Vou tentar offset/limit a seguir.", page)
+                log.warning("Falha a ler page=%s via limit/page (status=%s). Vou tentar offset/limit a seguir.", page, resp.status_code)
+                page_failed = True
                 break
 
             try:
                 data = resp.json()
             except Exception:
                 data = None
 
             if not data:
                 break
 
             items = self._extract_items(data)
             if not items:
                 break
 
             for it in items:
                 total_items += 1
                 yield it
             page += 1
 
-        if total_items > 0:
+        if total_items > 0 and not page_failed:
             return  # já devolvemos coisas, não precisamos de offset
 
         # 2) Offset/limit como fallback
-        offset = 0
+        offset = total_items if page_failed and total_items > 0 else 0
         while True:
-            resp = self._request("GET", path, params={"limit": limit, "offset": offset})
+            try:
+                resp = self._request("GET", path, params={"limit": limit, "offset": offset})
+            except SupridesAPIError as exc:
+                log.error("Não foi possível obter produtos da Suprides via offset=%s: %s", offset, exc)
+                return
             if resp.status_code == 401:
                 raise PermissionError("401 Unauthorized: confirma permissões Toogas_Integration::integration_api do teu consumer.")
             if resp.status_code != 200:
                 log.error("Não foi possível obter produtos da Suprides: %s %s", resp.status_code, resp.reason)
                 try:
                     log.error("Body: %s", resp.text[:500])
                 except Exception:
                     pass
                 return
 
             try:
                 data = resp.json()
             except Exception:
                 data = None
             if not data:
                 return
 
             items = self._extract_items(data)
             if not items:
                 return
 
             for it in items:
+                total_items += 1
                 yield it
             offset += limit
 
EOF
)
