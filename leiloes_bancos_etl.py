#!/usr/bin/env python3
"""Leilões e vendas de imóveis fora do PNCP: Banco do Brasil e União (SPU).

Complementa a camada "leilões" do mapa, que até aqui só via os leilões de
órgãos públicos publicados no PNCP (29 no Brasil em 26/09/2026).

Fontes (as duas públicas, sem login nem CAPTCHA):
  - bb  : catálogo do Seu Imóvel BB (seuimovelbb.com.br). A página faz um POST
          em /catalogo que devolve JSON com o HTML dos cards; 50 por página.
          robots.txt sem restrição. Pausa de 2 s entre páginas.
  - spu : API pública do VendasGov / Imóveis da União (imoveis.economia.gov.br),
          salas leilao, concorrencia e venda. JSON com código IBGE.

Fora de propósito: Caixa (lista pública atrás de CAPTCHA anti-robô — não se
contorna; entra por planilha baixada à mão) e editais judiciais (DJEN, etapa 2).

Tabela leiloes_outros (fonte, id_externo) — upsert; o que não aparece numa
coleta bem-sucedida da fonte vira ativo = FALSE (saiu do catálogo).

Uso:  python3 leiloes_bancos_etl.py --importar [--fonte bb|spu] [--dry-run]
"""
import argparse
import html as htmlmod
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
PAUSA = 2.0

DDL = """
CREATE TABLE IF NOT EXISTS leiloes_outros (
  fonte          TEXT NOT NULL,          -- bb | spu
  id_externo     TEXT NOT NULL,
  uf             CHAR(2),
  cidade         TEXT,
  municipio_ibge TEXT,
  tipo           TEXT,                   -- casa, apartamento, terreno...
  modalidade     TEXT NOT NULL,          -- leilao | venda_direta | concorrencia
  valor          NUMERIC,
  data_sessao    TIMESTAMP,
  url            TEXT,
  ativo          BOOLEAN NOT NULL DEFAULT TRUE,
  primeiro_visto TIMESTAMP NOT NULL DEFAULT now(),
  ultimo_visto   TIMESTAMP NOT NULL DEFAULT now(),
  PRIMARY KEY (fonte, id_externo)
);
CREATE INDEX IF NOT EXISTS leiloes_outros_uf ON leiloes_outros (uf) WHERE ativo;
CREATE INDEX IF NOT EXISTS leiloes_outros_mun ON leiloes_outros (municipio_ibge) WHERE ativo;
"""


def _abrir(op, url, data=None, headers=None, tentativas=3):
    for n in range(tentativas):
        try:
            req = urllib.request.Request(url, data=data, headers={**UA, **(headers or {})})
            return op.open(req, timeout=45).read().decode("utf-8", "replace")
        except Exception as e:  # rede instável: tenta de novo com espera maior
            if n == tentativas - 1:
                raise
            print(f"  ! {e} — nova tentativa", flush=True)
            time.sleep(5 * (n + 1))


# ── Banco do Brasil ──────────────────────────────────────────────────────────

def _texto(s):
    return re.sub(r"\s+", " ", htmlmod.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def _cards_bb(lista_html):
    """Um dict por card. A modalidade é o bloco .leilao que NÃO tem d-none."""
    out = []
    for c in re.split(r'(?=<div class="card carta">)', lista_html)[1:]:
        m_id = re.search(r'href="/imovel/id/(\d+)"', c)
        if not m_id:
            continue
        tipo = re.search(r'<div class="tipo">(.*?)</div>', c, re.S)
        valor = re.search(r'<div class="valor">\s*R\$\s*([\d\.]+,\d{2})', c)
        local = re.search(r'<div class="localidade">(.*?)</div>', c, re.S)
        visiveis = [_texto(b) for b in re.findall(r'<div class="leilao pt-\d\s*">(.*?)</div>', c, re.S)]
        mod_txt = next((v for v in visiveis if "ID" in v), "")
        data = next((v for v in visiveis if re.search(r"\d{2}/\d{2}/\d{4}", v)), "")
        cidade, uf = None, None
        if local:
            lt = _texto(local.group(1))
            mm = re.match(r"(.+?)\s*-\s*([A-Z]{2})$", lt)
            if mm:
                cidade, uf = mm.group(1).strip(), mm.group(2)
        mod = "leilao" if mod_txt.lower().startswith("leil") else "venda_direta" if "venda" in mod_txt.lower() else "outro"
        ds = None
        md = re.search(r"(\d{2})/(\d{2})/(\d{4})(?:\D+(\d{2}):(\d{2}))?", data)
        if md:
            ds = f"{md.group(3)}-{md.group(2)}-{md.group(1)} {md.group(4) or '00'}:{md.group(5) or '00'}"
        out.append({
            "fonte": "bb", "id_externo": m_id.group(1), "uf": uf, "cidade": cidade,
            "tipo": _texto(tipo.group(1)) if tipo else None, "modalidade": mod,
            "valor": float(valor.group(1).replace(".", "").replace(",", ".")) if valor else None,
            "data_sessao": ds, "url": f"https://seuimovelbb.com.br/imovel/id/{m_id.group(1)}",
        })
    return out


def coletar_bb():
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    pagina_html = _abrir(op, "https://seuimovelbb.com.br/catalogo")
    tok = re.search(r'id="_cppnp"[^>]*>([^<]+)', pagina_html)
    if not tok:
        raise RuntimeError("BB: token da página (_cppnp) não encontrado — o site mudou?")
    todos, pagina, total = [], 1, None
    while True:
        corpo = urllib.parse.urlencode({
            "pagina": pagina, "categorias": "", "tipoVenda": "", "tipoPagamento": "", "evolua": "",
            "localidade": "||", "minimo": 0, "maximo": 999999999, "texto": "", "ordem": "",
            "contento": "", "cppnp": tok.group(1),
        }).encode()
        j = json.loads(_abrir(op, "https://seuimovelbb.com.br/catalogo", corpo,
                              {"X-Requested-With": "XMLHttpRequest", "Referer": "https://seuimovelbb.com.br/catalogo"}))
        if j.get("erro"):
            raise RuntimeError(f"BB: {j['erro']}")
        total = int(j.get("imoveis") or 0)
        cards = _cards_bb(j.get("lista", ""))
        todos.extend(cards)
        print(f"  bb página {pagina}: {len(cards)} (acumulado {len(todos)}/{total})", flush=True)
        if not cards or len(todos) >= total or pagina > 60:
            break
        pagina += 1
        time.sleep(PAUSA)
    # o mesmo imóvel pode aparecer duas vezes entre páginas se a ordem mudar
    return list({c["id_externo"]: c for c in todos}.values()), total


# ── União (SPU / VendasGov) ──────────────────────────────────────────────────

def coletar_spu():
    op = urllib.request.build_opener()
    out = []
    for sala, mod in (("leilao", "leilao"), ("concorrencia", "concorrencia"), ("venda", "venda_direta")):
        pagina = 0
        while True:
            txt = _abrir(op, f"https://imoveis.economia.gov.br/api/public/imoveis?size=100&page={pagina}&sala={sala}")
            try:
                d = json.loads(txt)
            except ValueError:
                d = {}
            for x in d.get("content") or []:
                if x.get("vendido"):
                    continue
                e = x.get("endereco") or {}
                out.append({
                    "fonte": "spu", "id_externo": str(x.get("idItemEdital") or x["id"]) + ":" + str(x["id"]),
                    "uf": e.get("estado"), "cidade": e.get("cidade"),
                    "municipio_ibge": str(e["idMunicipio"]) if e.get("idMunicipio") else None,
                    "tipo": (x.get("tipoImovel") or {}).get("nome"), "modalidade": mod,
                    "valor": x.get("valor"), "data_sessao": (x.get("dataSessao") or "").replace("T", " ") or None,
                    "url": f"https://imoveis.economia.gov.br/{sala}",
                })
            if pagina + 1 >= int(d.get("totalPages") or 0):
                break
            pagina += 1
            time.sleep(PAUSA)
        print(f"  spu {sala}: {sum(1 for o in out if o['modalidade'] == mod)}", flush=True)
    return out


# ── gravação ─────────────────────────────────────────────────────────────────

def gravar(conn, fonte, linhas):
    cols = ["fonte", "id_externo", "uf", "cidade", "municipio_ibge", "tipo", "modalidade", "valor", "data_sessao", "url"]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, f"""
            INSERT INTO leiloes_outros ({",".join(cols)}) VALUES %s
            ON CONFLICT (fonte, id_externo) DO UPDATE SET
              uf = EXCLUDED.uf, cidade = EXCLUDED.cidade,
              municipio_ibge = COALESCE(EXCLUDED.municipio_ibge, leiloes_outros.municipio_ibge),
              tipo = EXCLUDED.tipo, modalidade = EXCLUDED.modalidade, valor = EXCLUDED.valor,
              data_sessao = EXCLUDED.data_sessao, url = EXCLUDED.url, ativo = TRUE, ultimo_visto = now()
        """, [tuple(l.get(c) for c in cols) for l in linhas])
        # saiu do catálogo desde a última coleta
        cur.execute("UPDATE leiloes_outros SET ativo = FALSE WHERE fonte = %s AND ativo AND id_externo <> ALL(%s)",
                    (fonte, [l["id_externo"] for l in linhas]))
        inativos = cur.rowcount
        # código IBGE pelo nome da cidade + UF (o BB só traz o nome)
        cur.execute("""
            UPDATE leiloes_outros l SET municipio_ibge = r.municipio_ibge
            FROM radar_loteamento r
            WHERE l.municipio_ibge IS NULL AND l.fonte = %s AND r.uf = l.uf
              AND unaccent(lower(r.municipio_nome)) = unaccent(lower(l.cidade))
        """, (fonte,))
    conn.commit()
    return inativos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--importar", action="store_true")
    ap.add_argument("--fonte", choices=["bb", "spu"])
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not a.importar:
        ap.print_help()
        return
    conn = None if a.dry_run else psycopg2.connect(os.environ["DATABASE_URL"])
    if conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    falhas = 0
    for fonte, fn in (("bb", coletar_bb), ("spu", coletar_spu)):
        if a.fonte and a.fonte != fonte:
            continue
        print(f"== {fonte}", flush=True)
        try:
            r = fn()
            linhas = r[0] if isinstance(r, tuple) else r
            if isinstance(r, tuple) and r[1] and len(linhas) < r[1] * 0.9:
                # coleta pela metade não pode desativar o resto do catálogo
                raise RuntimeError(f"{fonte}: só {len(linhas)} de {r[1]} imóveis — não gravo")
            por_mod = {}
            for l in linhas:
                por_mod[l["modalidade"]] = por_mod.get(l["modalidade"], 0) + 1
            print(f"  {fonte}: {len(linhas)} imóveis {por_mod}", flush=True)
            if conn and linhas:
                print(f"  gravados; {gravar(conn, fonte, linhas)} saíram do catálogo", flush=True)
        except Exception as e:
            falhas += 1
            print(f"  ✗ {fonte} falhou: {e}", flush=True)
    print(f"🏁 Leilões BB/União concluído ({falhas} fonte(s) com falha).")
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
