#!/usr/bin/env python3
"""Leilões extrajudiciais de bancos privados: Bradesco, Santander e Itaú.

Imóvel retomado por alienação fiduciária não passa pela Justiça (não aparece no
DJEN): o banco vende por leiloeiros. Fontes, conferidas em 26/09/2026:

  - bradesco : Vitrine Bradesco (vitrinebradesco.com.br), vitrine OFICIAL do
               banco com todos os leiloeiros dele. API pública em JSON
               (api.vitrinebradesco.com.br/v1/auctions?type=realstate); robots
               sem restrição. ~347 imóveis.
  - santander, itau : sem vitrine oficial aberta (o Itaú bloqueia qualquer
               acesso automatizado — Akamai nega até o robots.txt — e não se
               contorna). Vêm do Portal Zuk, o maior leiloeiro deles: robots
               permite as páginas de listagem, termos não proíbem coleta.
               Só páginas públicas GET (/leilao-de-imoveis/v/{banco}/u/{tipo}/{uf}
               e ?order=); NÃO usa o "carregar mais" (POST interno com token) nem
               as rotas /ajax-lote* que o robots bloqueia. Cada página mostra até
               30 lotes: recorta por UF, depois por tipo, depois pelas 3 ordens
               — cobertura quase total; o que ainda passar de 90 num recorte fica
               de fora (dito no log). Pausa de 3 s entre páginas.

Grava em leiloes_outros (fonte = bradesco | santander | itau). valor = lance
inicial (é o que o site mostra; a avaliação não vem na listagem), preco = idem,
desconto_pct quando o leiloeiro informa. O que some da vitrine numa coleta
completa vira ativo = FALSE. Sem fotos (direito dos bancos/leiloeiros).

Uso:  python3 leiloes_privados_etl.py --importar [--fonte bradesco|zuk] [--dry-run]
"""
import argparse
import html as htmlmod
import json
import os
import re
import sys
import time
import unicodedata
import urllib.request

import psycopg2
import psycopg2.extras

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
UFS = "ac al am ap ba ce df es go ma mg ms mt pa pb pe pi pr rj rn ro rr rs sc se sp to".split()
TIPOS_ZUK = ["residenciais", "comerciais", "terrenos", "rurais"]
ORDENS_ZUK = ["data_leilao", "menor_valor", "uf_cidade"]
BANCOS_ZUK = {"santander": "banco-santander", "itau": "banco-itau"}

DDL = """
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS preco NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS desconto_pct NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS area_m2 NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS bairro TEXT;
"""


def norm(s):
    s = unicodedata.normalize("NFD", str(s or ""))
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in s if unicodedata.category(c) != "Mn").lower()).strip()


def num(s):
    t = re.sub(r"[^0-9,.-]", "", str(s or ""))
    if not t:
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def get(url, tentativas=3):
    for n in range(tentativas):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return 404, ""
            if n == tentativas - 1:
                raise
        except Exception:
            if n == tentativas - 1:
                raise
        time.sleep(8 * (n + 1))


def tipo_de(texto):
    t = norm(texto)
    for rot, chaves in (("apartamento", ("apartamento", "flat", "cobertura")), ("casa", ("casa", "sobrado")),
                        ("rural", ("rural", "fazenda", "sitio", "chacara")), ("terreno", ("terreno", "lote", "gleba")),
                        ("comercial", ("comercial", "loja", "sala", "galpao", "predio", "escritorio", "industrial")),
                        ("garagem", ("garagem", "vaga"))):
        if any(c in t for c in chaves):
            return rot
    return None


def area_de(texto):
    for rx in (r"(?:privativa|constr\w*|útil|util)[^0-9]{0,20}([\d.,]+)\s*m", r"([\d.,]+)\s*m²?\s*(?:de\s+)?(?:área\s+)?privativa",
               r"terreno[^0-9]{0,15}([\d.,]+)\s*m"):
        m = re.search(rx, texto or "", re.I)
        if m and (v := num(m.group(1))) and 5 < v < 1e7:
            return v
    return None


# ── Bradesco (vitrine oficial) ───────────────────────────────────────────────

def coletar_bradesco():
    out, pagina, total = [], 1, None
    while True:
        _, txt = get(f"https://api.vitrinebradesco.com.br/v1/auctions?page={pagina}&type=realstate")
        d = json.loads(txt)
        total = d.get("total_auctions")
        for a in d.get("data") or []:
            datas = [x for x in (a.get("date_auction_1"), a.get("date_auction_2"), a.get("final_date_auction"), a.get("auction_date")) if x]
            precos = [x for x in (a.get("min_auction_value_2"), a.get("min_auction_value_1"), a.get("final_auction_value"), a.get("price")) if x]
            modo = norm(a.get("realstate_auction_type"))
            out.append({
                "fonte": "bradesco", "id_externo": a["guid"], "uf": (a.get("state") or "").upper()[:2] or None,
                "cidade": a.get("city"), "bairro": a.get("neighborhood"),
                "tipo": tipo_de(f"{a.get('category') or ''} {a.get('name') or ''}"),
                "modalidade": "venda_direta" if "venda" in modo or "direta" in modo else "leilao",
                "valor": min(precos) if precos else None, "preco": min(precos) if precos else None, "desconto_pct": None,
                "area_m2": area_de(a.get("description")),
                "data_sessao": max(datas)[:19].replace("T", " ") if datas else None,
                "url": f"https://vitrinebradesco.com.br/auctions/{a.get('slug') or a['guid']}",
                "origem": ((a.get("auctioneer") or {}).get("name") or "")[:60],
            })
        print(f"  bradesco página {pagina}/{d.get('total_pages')}: acumulado {len(out)}/{total}", flush=True)
        if pagina >= int(d.get("total_pages") or 0):
            break
        pagina += 1
        time.sleep(2)
    return list({o["id_externo"]: o for o in out}.values()), total


# ── Santander e Itaú (Portal Zuk, páginas públicas) ──────────────────────────

CARD = re.compile(r'<a\s+href="(https://www\.portalzuk\.com\.br/imovel/([a-z]{2})/[^"]+/(\d+-\d+))"[^>]*?title="([^"]*)"', re.S)


def _cards_zuk(s):
    out = {}
    blocos = re.split(r'(?=<div class="card-property-image-wrapper">)', s)[1:]
    for b in blocos:
        m = CARD.search(b)
        if not m:
            continue
        url, uf, idz, titulo = m.groups()
        tipo_txt = re.search(r'card-property-price-lote">([^<]*)<', b)
        local = re.search(r"card-property-address\">.*?<a [^>]*>([^<]+)</a>\s*-?\s*([^<]*)</span>", b, re.S)
        cidade = bairro = None
        if local:
            cidade = local.group(1).split("/")[0].strip()
            bairro = htmlmod.unescape(local.group(2)).strip() or None
        precos = [num(p) for p in re.findall(r'card-property-price-value">\s*R\$\s*([\d.,]+)', b)]
        precos = [p for p in precos if p]
        desc = [num(p) for p in re.findall(r'card-property-price-percent">.*?(\d+)\s*<i data-feather="percent"', b, re.S)]
        datas = re.findall(r'card-property-price-data">\s*(\d{2})/(\d{2})/(\d{4})(?:\s*às\s*(\d{2}):(\d{2}))?', b)
        ds = None
        if datas:
            d, mth, a, h, mi = datas[-1]
            ds = f"{a}-{mth}-{d} {h or '00'}:{mi or '00'}"
        out[idz] = {
            "id_externo": idz, "uf": uf.upper(), "cidade": htmlmod.unescape(cidade) if cidade else None, "bairro": bairro,
            "tipo": tipo_de(f"{tipo_txt.group(1) if tipo_txt else ''} {titulo}"),
            "valor": min(precos) if precos else None, "preco": min(precos) if precos else None,
            "desconto_pct": max(d for d in desc if d) if any(desc) else None,
            "data_sessao": ds, "url": url,
            "modalidade": "venda_direta" if "venda direta" in titulo.lower() else "leilao",
        }
    total = re.search(r"(\d[\d.]*)\s+resultados", re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s)))
    return out, int(total.group(1).replace(".", "")) if total else len(out)


def coletar_zuk(fonte, slug):
    base = f"https://www.portalzuk.com.br/leilao-de-imoveis/v/{slug}"
    _, s = get(base)
    _, total = _cards_zuk(s)
    achados, faltou = {}, 0
    for uf in UFS:
        time.sleep(3)
        st, s = get(f"{base}/u/todos-imoveis/{uf}")
        if st == 404:
            continue
        cards, n = _cards_zuk(s)
        achados.update(cards)
        if n <= 30:
            continue
        for tipo in TIPOS_ZUK:
            time.sleep(3)
            st, s = get(f"{base}/u/{tipo}/{uf}")
            if st == 404:
                continue
            cards, nt = _cards_zuk(s)
            achados.update(cards)
            if nt <= 30:
                continue
            vistos = set(cards)
            for ordem in ORDENS_ZUK[1:]:
                time.sleep(3)
                _, s = get(f"{base}/u/{tipo}/{uf}?order={ordem}")
                c2, _ = _cards_zuk(s)
                achados.update(c2)
                vistos |= set(c2)
            if nt > len(vistos):
                faltou += nt - len(vistos)
                print(f"  ! {fonte} {uf.upper()} {tipo}: {len(vistos)} de {nt} (limite das páginas públicas)", flush=True)
        print(f"  {fonte} {uf.upper()}: {n} (acumulado {len(achados)}/{total})", flush=True)
    out = []
    for c in achados.values():
        out.append({**c, "fonte": fonte, "origem": "Portal Zuk", "area_m2": None})
    return out, total, faltou


# ── gravação ─────────────────────────────────────────────────────────────────

def municipios(conn):
    idx = {}
    with conn.cursor() as cur:
        cur.execute("SELECT municipio_ibge, municipio_nome, uf FROM radar_loteamento")
        for ibge, nome, uf in cur.fetchall():
            idx[(uf, norm(nome))] = (ibge, nome)
    return idx


def gravar(conn, fonte, linhas, mun, completo):
    for l in linhas:
        m = mun.get((l["uf"], norm(l["cidade"]))) or (mun.get(("DF", "brasilia")) if l["uf"] == "DF" else None)
        l["municipio_ibge"], l["cidade"] = (m[0], m[1]) if m else (None, l["cidade"])
    cols = ["fonte", "id_externo", "uf", "cidade", "municipio_ibge", "bairro", "tipo", "modalidade", "valor", "preco",
            "desconto_pct", "area_m2", "data_sessao", "url", "origem"]
    with conn.cursor() as cur:
        cur.execute(DDL)
        psycopg2.extras.execute_values(cur, f"""
            INSERT INTO leiloes_outros ({",".join(cols)}) VALUES %s
            ON CONFLICT (fonte, id_externo) DO UPDATE SET
              uf = EXCLUDED.uf, cidade = EXCLUDED.cidade, municipio_ibge = EXCLUDED.municipio_ibge, bairro = EXCLUDED.bairro,
              tipo = EXCLUDED.tipo, modalidade = EXCLUDED.modalidade, valor = EXCLUDED.valor, preco = EXCLUDED.preco,
              desconto_pct = EXCLUDED.desconto_pct, area_m2 = COALESCE(EXCLUDED.area_m2, leiloes_outros.area_m2),
              data_sessao = EXCLUDED.data_sessao, url = EXCLUDED.url, origem = EXCLUDED.origem, ativo = TRUE, ultimo_visto = now()
        """, [tuple(l.get(c) for c in cols) for l in linhas])
        saidos = 0
        if completo:
            cur.execute("UPDATE leiloes_outros SET ativo = FALSE WHERE fonte = %s AND ativo AND id_externo <> ALL(%s)",
                        (fonte, [l["id_externo"] for l in linhas]))
            saidos = cur.rowcount
        # leilão com data passada não é mais oportunidade
        cur.execute("UPDATE leiloes_outros SET ativo = FALSE WHERE fonte = %s AND ativo AND data_sessao < now() - interval '1 day'", (fonte,))
    conn.commit()
    return saidos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--importar", action="store_true")
    ap.add_argument("--fonte", choices=["bradesco", "zuk"])
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not a.importar:
        ap.print_help()
        return
    conn = None if a.dry_run else psycopg2.connect(os.environ["DATABASE_URL"])
    mun = municipios(conn) if conn else {}
    falhas = 0
    tarefas = []
    if a.fonte in (None, "bradesco"):
        tarefas.append(("bradesco", coletar_bradesco))
    if a.fonte in (None, "zuk"):
        for f, slug in BANCOS_ZUK.items():
            tarefas.append((f, lambda f=f, slug=slug: coletar_zuk(f, slug)))
    for fonte, fn in tarefas:
        print(f"== {fonte}", flush=True)
        try:
            r = fn()
            linhas, total = r[0], r[1]
            faltou = r[2] if len(r) > 2 else 0
            completo = bool(total) and len(linhas) + faltou >= total * 0.9
            por = {}
            for l in linhas:
                por[l["modalidade"]] = por.get(l["modalidade"], 0) + 1
            print(f"  {fonte}: {len(linhas)} de {total} {por}", flush=True)
            if conn and linhas:
                print(f"  gravados; {gravar(conn, fonte, linhas, mun, completo)} saíram da vitrine"
                      + ("" if completo else " (coleta parcial — nada desativado)"), flush=True)
        except Exception as e:
            falhas += 1
            if conn:
                conn.rollback()
            print(f"  ✗ {fonte} falhou: {e}", flush=True)
    print(f"🏁 Leilões de bancos privados concluído ({falhas} fonte(s) com falha).")
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
