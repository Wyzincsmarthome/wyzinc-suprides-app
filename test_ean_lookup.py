# test_ean_lookup.py
# -*- coding: utf-8 -*-
import os, sys
from amazon_client import AmazonClient

def main():
    if os.getenv("SPAPI_SIMULATE","").lower() in ("1","true","yes","on"):
        print("ERRO: SPAPI_SIMULATE está ativo. Define SPAPI_SIMULATE=false no .env")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Uso: python test_ean_lookup.py <EAN>")
        sys.exit(1)

    ean = "".join(ch for ch in sys.argv[1] if ch.isdigit())
    if not ean:
        print("EAN inválido. Dá-me um EAN numérico.")
        sys.exit(1)

    client = AmazonClient(simulate=False)
    items = client.catalog_search_by_ean(ean)
    print(f"EAN consultado: {ean}")
    if not items:
        print("Nenhum item encontrado para este EAN no marketplace atual.")
        sys.exit(2)

    for it in items:
        asin = it.get("asin")
        eans = it.get("eans") or []
        title = (it.get("title") or "")[:120]
        brand = it.get("brand") or ""
        print("-"*70)
        print(f"ASIN:  {asin}")
        print(f"EANs:  {eans}")
        print(f"Brand: {brand}")
        print(f"Title: {title}")

    print("-"*70)
    # Novo critério: é match exato se (a) o EAN aparece nos EANs extraídos OU (b) a Amazon devolveu itens para identifiers=EAN
    ean_in_any = any(ean in (it.get("eans") or []) for it in items)
    query_confirmed = any(bool(it.get("matched_by_query")) for it in items)
    print("MATCH EAN EXATO:", "SIM" if (ean_in_any or query_confirmed) else "NÃO")

if __name__ == "__main__":
    main()
