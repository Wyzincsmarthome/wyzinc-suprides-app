# -*- coding: utf-8 -*-
import os, re, requests
from dotenv import load_dotenv; load_dotenv()

ZERO = r"[\u200b\u200c\u200d\u2060\ufeff]"
def clean(s):
    return re.sub(ZERO, "", (s or "").strip())

base = clean(os.getenv("SUPRIDES_BASE_URL","https://www.suprides.pt")).rstrip("/")
path = clean(os.getenv("SUPRIDES_PRODUCTS_PATH","/rest/V1/integration/products-list"))
user = clean(os.getenv("SUPRIDES_USER"))
pwd  = clean(os.getenv("SUPRIDES_PASSWORD"))
bear = clean(os.getenv("SUPRIDES_BEARER"))

print("ENV:", {"base":base, "path":path, "user_len":len(user or ""), "pass_len":len(pwd or ""), "has_bearer":bool(bear)})

params = {"user": user, "password": pwd, "page": 1, "limit": 1}
headers = {"Accept": "application/json"}
if bear:
    headers["Authorization"] = f"Bearer {bear}"

url = f"{base}{path}"

req = requests.Request("GET", url, params=params, headers=headers)
prep = req.prepare()

# Mostrar o pedido realmente enviado, com user/pass mascarados
from urllib.parse import urlsplit, parse_qsl, urlunsplit, urlencode
parts = urlsplit(prep.url)
qs = []
for k, v in parse_qsl(parts.query, keep_blank_values=True):
    if k.lower() in ("user","password"):
        qs.append((k, "***"))
    else:
        qs.append((k, v))
masked_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))
auth = prep.headers.get("Authorization","")
print("REQ_URL:", masked_url)
print("REQ_HDR_AUTH:", (auth[:12] + f"...(len={len(auth)})") if auth else "<none>")

s = requests.Session()
r = s.send(prep, timeout=30)
print("STATUS:", r.status_code)
print("CT:", r.headers.get("Content-Type"))
print("BODY:", r.text[:400])
