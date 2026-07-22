"""
Coletor de mercado imobiliário — diretório + preços das imobiliárias.

Estratégia (rápido + estável, sem burlar nada):
- Descobre anúncios na página pública da imobiliária (SSR).
- Extrai campos do JSON-LD schema.org RealEstateListing (preço/endereço) —
  confiável e sem LLM; regex pra área/tipo/quartos do texto livre.
- Concorrência entre MUITAS imobiliárias; gentil em cada uma (delay).
- Respeita bloqueios: se um site responde !=200 ou sem dado, pula.

Grava em: imobiliarias (diretório) + imoveis_mercado (anúncios).
"""
import os
import re
import json
import time
import html
from urllib.parse import urljoin, urlparse

import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
PAUSA_SITE = float(os.environ.get("IMOB_PAUSA", "1.0"))  # gentil por site

TIPOS = ["apartamento", "cobertura", "casa", "terreno", "área", "area", "sítio", "sitio",
         "chácara", "chacara", "loja", "sala", "galpão", "galpao", "fazenda", "kitnet", "flat"]


def _get(url, timeout=20):
    r = requests.get(url, headers=UA, timeout=timeout)
    if r.status_code != 200:
        return None
    return r.text


def _jsonld(page):
    out = []
    for b in re.findall(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', page, re.S | re.I):
        try:
            d = json.loads(b)
        except Exception:
            continue
        out.extend(d if isinstance(d, list) else [d])
    return out


OLLAMA = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:latest")


def _ollama_extract(texto):
    """Fallback via LLM local: extrai área/tipo/quartos do texto livre do anúncio."""
    prompt = (
        "Você extrai dados de um anúncio imobiliário brasileiro. Responda SOMENTE um JSON "
        '{"area_m2": <número ou null>, "tipo": "<casa|apartamento|terreno|cobertura|sítio|chácara|loja|sala|galpão|fazenda|kitnet|flat ou null>", "quartos": <número ou null>}. '
        "area_m2 = área do imóvel em metros quadrados (só o número). Anúncio:\n" + texto[:1000]
    )
    try:
        r = requests.post(f"{OLLAMA}/api/generate",
                          json={"model": OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False},
                          timeout=45)
        return json.loads(r.json().get("response", "{}"))
    except Exception:
        return {}


def _limpa(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def _num(s):
    s = re.sub(r"[^\d]", "", str(s or ""))
    return int(s) if s else None


def parse_anuncio(page, url):
    """Extrai um anúncio do JSON-LD RealEstateListing (+ regex no texto livre)."""
    listing = next((d for d in _jsonld(page)
                    if isinstance(d, dict) and d.get("@type") in ("RealEstateListing", "Product", "Offer")), None)
    if not listing:
        return None
    nome = _limpa(listing.get("name"))
    desc = _limpa(listing.get("description"))
    blob = f"{nome} {desc}"
    offers = listing.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    preco = offers.get("price")
    preco = float(preco) if preco not in (None, "", 0, "0") else None
    addr = listing.get("address") or {}
    bairro = None
    m = re.search(r"Bairro:\s*([^,)<]+)", _limpa(addr.get("streetAddress")))
    if m:
        bairro = m.group(1).strip()
    cidade = _limpa(addr.get("addressLocality")) or None
    uf = (_limpa(addr.get("addressRegion")) or "")[:2] or None
    # área (m²), tipo, quartos do texto livre
    ma = re.search(r"([\d.]+)\s*m[²2]", blob, re.I)
    area = None
    if ma:
        area = float(re.sub(r"[^\d]", "", ma.group(1)) or 0) or None
    tipo = next((t.replace("area", "área").replace("sitio", "sítio").replace("chacara", "chácara").replace("galpao", "galpão")
                 for t in TIPOS if re.search(rf"\b{t}\b", blob, re.I)), None)
    mq = re.search(r"(\d+)\s*(quarto|dormit|su[ií]te)", blob, re.I)
    quartos = int(mq.group(1)) if mq else None
    finalidade = "aluguel" if re.search(r"alug|loca[çc]", blob, re.I) else "venda"
    # fallback LLM local quando a regex não pegou a área (campo-chave pro preço/m²)
    if area is None or tipo is None or quartos is None:
        ex = _ollama_extract(blob)
        if area is None and ex.get("area_m2"):
            try: area = float(str(ex["area_m2"]).replace(",", ".")) or None
            except Exception: pass
        tipo = tipo or (ex.get("tipo") or None)
        if quartos is None and ex.get("quartos"):
            try: quartos = int(ex["quartos"])
            except Exception: pass
    preco_m2 = round(preco / area, 2) if (preco and area) else None
    return {
        "url": (listing.get("url") or url).split("?")[0],
        "preco": preco, "area": area, "preco_m2": preco_m2, "quartos": quartos,
        "bairro": bairro, "cidade": cidade, "uf": uf, "tipo": tipo, "finalidade": finalidade,
    }


_PROP_PAT = re.compile(r"detalhe|imovel|imoveis|/ref|codigo|/id[-_/]|comprar|/venda/|/aluguel/", re.I)


def descobrir_urls(dom, home, cap=45):
    """Acha páginas de anúncio de forma robusta (independe de plataforma):
    1) sitemap.xml (índice → sub-sitemaps), filtrando URLs de imóvel;
    2) fallback: links da home. Depois o parse exige JSON-LD, o que filtra o resto."""
    base = f"https://{dom}"
    urls = set()
    for sm in ("/sitemap.xml", "/sitemap_index.xml", "/sitemap-imoveis.xml", "/wp-sitemap.xml"):
        x = _get(base + sm)
        if not x:
            continue
        locs = re.findall(r"<loc>\s*([^<\s]+)", x)
        for s in [l for l in locs if l.lower().endswith(".xml")][:8]:
            xx = _get(s)
            if xx:
                locs += re.findall(r"<loc>\s*([^<\s]+)", xx)
        for l in locs:
            if _PROP_PAT.search(l):
                urls.add(l.split("?")[0])
        if urls:
            break
    if len(urls) < 3:  # fallback: links da home
        urls |= {u.split("?")[0] for u in re.findall(r'href="(https?://[^"]+)"', home) if _PROP_PAT.search(u)}
    return list(urls)[:cap]


def coleta_imobiliaria(conn, dom, creci=None, nome=None, cidade=None, uf=None, max_anuncios=45):
    """Coleta uma imobiliária: registra no diretório + extrai anúncios."""
    base = f"https://{dom}"
    home = _get(base)
    if not home:
        print(f"[{dom}] home !=200 — pulando")
        return 0
    if not creci:
        m = re.search(r"creci[:\s/]*([A-Z]{0,2}[-\s]?\d[\d.]{2,7}[-/]?[A-Z0-9]{0,3})", home, re.I)
        creci = m.group(1).strip(" -/.").replace(" ", "") if m else None
    tel = (re.search(r"(\(?\d{2}\)?\s?\d{4,5}-?\d{4})", home) or [None, None])[1]
    cur = conn.cursor()
    cur.execute("""INSERT INTO imobiliarias (creci, nome, site, telefone, cidade, uf, dominio, coletavel)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,TRUE)
                   ON CONFLICT (creci) DO UPDATE SET site=EXCLUDED.site, dominio=EXCLUDED.dominio, atualizado_em=NOW()
                   RETURNING id""",
                (creci, nome or dom, base, tel, cidade, uf, dom))
    imob_id = cur.fetchone()[0]
    conn.commit()

    # anúncios: descoberta robusta via sitemap (+ fallback home)
    urls = descobrir_urls(dom, home, cap=max_anuncios)
    gravados = 0
    for u in urls:
        page = _get(u)
        if page:
            a = parse_anuncio(page, u)
            if a and (a["preco"] or a["area"]):
                cur.execute("""INSERT INTO imoveis_mercado
                    (imobiliaria_id, fonte, origem, finalidade, tipo, preco, area, preco_m2, quartos, bairro, cidade, uf, url)
                    VALUES (%s,%s,'anuncio',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (url) DO UPDATE SET preco=EXCLUDED.preco, coletado_em=NOW()""",
                    (imob_id, dom, a["finalidade"], a["tipo"], a["preco"], a["area"], a["preco_m2"],
                     a["quartos"], a["bairro"], a["cidade"] or cidade, a["uf"] or uf, a["url"]))
                gravados += 1
        time.sleep(PAUSA_SITE)
    conn.commit()
    print(f"[{dom}] imob_id={imob_id} creci={creci} — {gravados}/{len(urls)} anúncios gravados")
    return gravados


if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    # seed de prova — Angra dos Reis
    seed = [
        ("angramarimoveis.com.br", None, "Angramar", "Angra dos Reis", "RJ"),
    ]
    total = 0
    for dom, creci, nm, cid, uf in seed:
        total += coleta_imobiliaria(conn, dom, creci, nm, cid, uf, max_anuncios=20)
    print(f"TOTAL anúncios: {total}")
    conn.close()
