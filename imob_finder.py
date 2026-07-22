"""
Finder de imobiliárias via Google Places API (New) — Text Search.

Descobre nome/site/telefone de imobiliárias pequenas por cidade pra
alimentar o diretório `imobiliarias` (o coletor depois decide se dá pra
raspar). Usa a API só pra achar o site — não republica o diretório do
Google (endereço/place_id não são gravados como produto, só nome/site/tel).

Uso:
    python3 imob_finder.py "Paraty" RJ                 # mostra candidatos (dry-run)
    python3 imob_finder.py "Paraty" RJ --gravar         # grava os novos no diretório
    python3 imob_finder.py "Paraty" RJ --cap 60 --gravar
"""
import os
import sys
import time
from urllib.parse import urlparse

import requests

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

FIELD_MASK = ("places.displayName,places.websiteUri,places.nationalPhoneNumber,"
              "places.formattedAddress,places.id,nextPageToken")

# Portais/agregadores grandes — não são "imobiliária pequena", ficam fora do escopo do AVM local.
PORTAIS_EXCLUIR = {
    "zapimoveis.com.br", "vivareal.com.br", "olx.com.br", "imovelweb.com.br",
    "chavesnamao.com.br", "facebook.com", "instagram.com", "wa.me", "api.whatsapp.com",
    "linktr.ee", "quintoandar.com.br", "grupozap.com", "imovelweb.com",
}


def _dominio(url):
    try:
        d = urlparse(url).netloc.lower()
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return None


def buscar_cidade(cidade, uf, cap=40):
    """Text Search (New): pagina até `cap` resultados pra 'imobiliária em {cidade}, {uf}'."""
    if not GOOGLE_PLACES_API_KEY:
        raise RuntimeError("GOOGLE_PLACES_API_KEY não configurada no ambiente")
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    body = {"textQuery": f"imobiliária em {cidade}, {uf}", "languageCode": "pt-BR"}
    achados, page_token = [], None
    while len(achados) < cap:
        if page_token:
            body["pageToken"] = page_token
            time.sleep(2)  # token só fica válido após um pequeno delay
        r = requests.post(url, headers=headers, json=body, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"Places API {r.status_code}: {r.text[:300]}")
        data = r.json()
        achados.extend(data.get("places", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return achados[:cap]


def coletar_candidatos(cidade, uf, cap=40):
    candidatos = []
    for p in buscar_cidade(cidade, uf, cap):
        site = p.get("websiteUri")
        dom = _dominio(site) if site else None
        if not dom or dom in PORTAIS_EXCLUIR:
            continue
        candidatos.append({
            "nome": (p.get("displayName") or {}).get("text"),
            "dominio": dom,
            "site": site,
            "telefone": p.get("nationalPhoneNumber"),
            "endereco": p.get("formattedAddress"),
            "cidade": cidade, "uf": uf,
        })
    return candidatos


def gravar(conn, candidatos):
    cur = conn.cursor()
    novos = 0
    for c in candidatos:
        cur.execute("""INSERT INTO imobiliarias (nome, site, telefone, cidade, uf, dominio, endereco, coletavel)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE)
                       ON CONFLICT (dominio) DO NOTHING""",
                    (c["nome"], c["site"], c["telefone"], c["cidade"], c["uf"], c["dominio"], c["endereco"]))
        novos += cur.rowcount
    conn.commit()
    return novos


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    cidade, uf = sys.argv[1], sys.argv[2]
    cap = int(next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--cap"), 40))

    candidatos = coletar_candidatos(cidade, uf, cap)
    for c in candidatos:
        print(f"  {c['nome']:<35} {c['dominio']:<32} {c['telefone'] or ''}")
    print(f"\n{len(candidatos)} candidatos ({cidade}/{uf}, portais grandes já filtrados)")

    if "--gravar" in sys.argv:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        novos = gravar(conn, candidatos)
        print(f"{novos} novas imobiliárias gravadas no diretório (duplicadas por domínio foram ignoradas)")
        conn.close()
    else:
        print("(dry-run — rode com --gravar pra salvar no diretório)")
