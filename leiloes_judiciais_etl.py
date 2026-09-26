#!/usr/bin/env python3
"""Leilões judiciais de imóveis — editais do DJEN (Diário de Justiça Eletrônico
Nacional, API pública do CNJ: comunicaapi.pje.jus.br).

Por dia saem ~300 publicações com "edital de leilão" (todos os tribunais). Cerca
de metade é de imóvel; o resto é veículo, sucata, equipamento. O texto é livre,
então a extração é por regra, conferida numa amostra de 100 editais:

  - é imóvel?  olha só o trecho da DESCRIÇÃO DO BEM (as condições gerais de todo
               edital falam em "imóvel") — matrícula, terreno, apartamento,
               casa, lote, fazenda… contra placa, chassi, veículo;
  - datas      a primeira data depois de "1º/2º leilão/pregão/praça";
  - avaliação  "avaliado em R$ …" / "avaliação: R$ …" no trecho do bem;
  - cidade     "Cidade/UF" ou "Município de X" no trecho do bem; senão a
               comarca; validada contra a lista de municípios da UF.

LGPD: não guarda o texto, nomes de partes nem advogados — só tribunal, cidade,
datas, valor, número do processo (público) e o link oficial do edital. O site
mostra apenas contagens.

O mesmo edital sai uma vez por destinatário intimado: a chave é
processo + primeira data de leilão (id_externo), em leiloes_outros (fonte='judicial').

Uso:  python3 leiloes_judiciais_etl.py --importar [--dias 3] [--dry-run]
      (primeira carga: --dias 45)
"""
import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request

import psycopg2
import psycopg2.extras

API = "https://comunicaapi.pje.jus.br/api/v1/comunicacao"
UA = {"User-Agent": "Mozilla/5.0 (compatible; allancandido-dados/1.0; +https://allancandido.com)"}
PAUSA = 1.5

TRT_UF = {1: "RJ", 2: "SP", 3: "MG", 4: "RS", 5: "BA", 6: "PE", 7: "CE", 8: "PA", 9: "PR", 10: "DF", 11: "AM", 12: "SC",
          13: "PB", 14: "RO", 15: "SP", 16: "MA", 17: "ES", 18: "GO", 19: "AL", 20: "SE", 21: "RN", 22: "PI", 23: "MT", 24: "MS"}
UFS = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())

MESES = {"janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4, "maio": 5, "junho": 6, "julho": 7,
         "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12}
IMOVEL = re.compile(r"matr[íi]cula\s*(?:n[º°o.]*|sob)|\bim[óo]vel\b|\bterreno\b|apartamento|\bcasa\s+(?:residencial|de|t[ée]rrea|com)"
                    r"|lote\s+(?:de\s+terreno|n[º°o.]|urbano)|gleba|fazenda|ch[áa]cara|s[íi]tio|sala\s+comercial|galp[ãa]o|pr[ée]dio"
                    r"|unidade\s+aut[ôo]noma|fra[çc][ãa]o\s+ideal|vaga\s+de\s+garage", re.I)
VEICULO = re.compile(r"ve[íi]culo|placa\s*[:\w]|chassi|renavam|motocicleta|caminh[ãa]o|autom[óo]vel|marca/modelo", re.I)
MATRICULA = re.compile(r"matr[íi]cula\s*(?:n[º°o.]*|sob)", re.I)
DESCR = re.compile(r"(descri[çc][ãa]o\s+(?:e\s+avalia[çc][ãa]o\s+)?d[oa]s?\s*(?:\(?s?\)?\s*)?bem|\bbem\s*\(?n?s?\)?\s*:|\bbens\s*:"
                   r"|lote\s*0*1\s*:|objeto\s+do\s+leil[ãa]o|designado\s+como\s+im[óo]vel)", re.I)
MARCA_LEILAO = re.compile(r"(1[ºo°]|2[ºo°]|primeir[oa]|segund[oa]|[úu]nic[oa])\s*(leil[ãa]o|preg[ãa]o|pra[çc]a|hasta)", re.I)


def limpa(t):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", t or ""))


def chave(s):
    return re.sub(r"[^a-z ]", "", unicodedata.normalize("NFKD", (s or "").lower()).encode("ascii", "ignore").decode()).strip()


def trecho_bem(t):
    m = DESCR.search(t)
    return t[m.start():m.start() + 1800] if m else ""


def eh_imovel(bem):
    if not bem:
        return False
    im, ve = len(IMOVEL.findall(bem)), len(VEICULO.findall(bem))
    return im >= 1 and (ve == 0 or im > ve * 1.5 or MATRICULA.search(bem) is not None)


def _primeira_data(s):
    cands = []
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})/(20\d\d)\b", s):
        cands.append((m.start(), (int(m.group(3)), int(m.group(2)), int(m.group(1)))))
    for m in re.finditer(r"\b(\d{1,2})\s+de\s+([a-zç]+)\s+de\s+(20\d\d)", s, re.I):
        if m.group(2).lower() in MESES:
            cands.append((m.start(), (int(m.group(3)), MESES[m.group(2).lower()], int(m.group(1)))))
    return min(cands)[1] if cands else None


def _validas(ds, desde):
    out = []
    for a, mth, d in ds:
        try:
            x = dt.date(a, mth, d)
        except ValueError:
            continue
        # data de leilão não é anterior à publicação (as outras são avaliação, penhora…)
        if desde is None or x >= desde:
            out.append(x)
    return sorted(set(out))


def datas_leilao(t, publicado=None):
    desde = dt.date.fromisoformat(publicado) if publicado else None
    ds = [d for m in MARCA_LEILAO.finditer(t) if (d := _primeira_data(t[m.start():m.start() + 160]))]
    out = _validas(ds, desde)
    if out:
        return out
    # sem "1º/2º leilão": qualquer data logo depois de leilão/pregão/praça/hasta/encerramento
    ds = [d for m in re.finditer(r"leil[ãa]o|preg[ãa]o|pra[çc]a|hasta|encerr|lances", t, re.I)
          if (d := _primeira_data(t[m.start():m.start() + 140]))]
    return _validas(ds, desde)


def valor_avaliacao(bem):
    vs = []
    for m in re.finditer(r"(?:avalia[çc][ãa]o|avaliad[oa]s?(?:\s+em)?)[^R]{0,40}R\$\s*([\d\.]+,\d{2})", bem, re.I):
        v = float(m.group(1).replace(".", "").replace(",", "."))
        if 1000 <= v <= 2e9:
            vs.append(v)
    return max(vs) if vs else None


def uf_do_tribunal(sigla):
    s = (sigla or "").upper()
    if s == "TJDFT":
        return "DF"
    if s.startswith("TJ") and s[2:] in UFS:
        return s[2:]
    m = re.match(r"TRT(\d+)$", s)
    if m:
        return TRT_UF.get(int(m.group(1)))
    return None  # TRFs e superiores: a UF sai da cidade


class Municipios:
    """Nome → código IBGE, por UF (a partir de radar_loteamento)."""

    def __init__(self, conn):
        self.por_uf = {}
        with conn.cursor() as cur:
            cur.execute("SELECT municipio_ibge, municipio_nome, uf FROM radar_loteamento")
            for ibge, nome, uf in cur.fetchall():
                self.por_uf.setdefault(uf, {})[chave(nome)] = (ibge, nome)

    def achar(self, nome, uf):
        k = chave(nome)
        if not k:
            return None
        ufs = [uf] if uf else list(self.por_uf)
        for u in ufs:
            r = self.por_uf.get(u, {}).get(k)
            if r:
                return (r[0], r[1], u)
        return None


def cidade(item, t, bem, mun, uf_trib):
    """Cidade do imóvel, na ordem: 'Cidade/UF' no bem, 'Município de X', comarca, órgão."""
    for m in re.finditer(r"([A-ZÀ-Úa-zà-ú][A-Za-zÀ-ú' \-]{2,40}?)\s*[/\-–,]\s*([A-Z]{2})\b", bem):
        uf = m.group(2)
        if uf in UFS:
            nome = re.sub(r"^.*\b(?:de|em|na|no|cidade)\s+", "", m.group(1), flags=re.I) if len(m.group(1).split()) > 4 else m.group(1)
            for tent in (m.group(1), nome, " ".join(m.group(1).split()[-3:]), " ".join(m.group(1).split()[-2:]), m.group(1).split()[-1]):
                r = mun.achar(tent, uf)
                if r:
                    return r
    for rx in (r"munic[íi]pio\s+de\s+([A-ZÀ-Ú][\wÀ-ú' \-]{2,40}?)(?:\s*[-/,.]|\s+estado)",
               r"comarca\s+de\s+([A-ZÀ-Ú][\wÀ-ú' \-]{2,40}?)(?:\s*[-/,.(]|\s+estado|\s{2})"):
        for src in (bem, t[:800]):
            m = re.search(rx, src, re.I)
            if m:
                r = mun.achar(m.group(1), uf_trib)
                if r:
                    return r
    m = re.search(r"\b(?:de|da|do)\s+([A-ZÀ-Ú][\wÀ-ú' \-]+)$", item.get("nomeOrgao") or "")
    if m:
        r = mun.achar(m.group(1), uf_trib)
        if r:
            return r
    return None


def buscar_dia(dia):
    itens, pagina = [], 1
    while True:
        q = urllib.parse.urlencode({"texto": "edital de leilão", "dataDisponibilizacaoInicio": dia,
                                    "dataDisponibilizacaoFim": dia, "itensPorPagina": 100, "pagina": pagina})
        for n in range(4):
            try:
                d = json.load(urllib.request.urlopen(urllib.request.Request(f"{API}?{q}", headers=UA), timeout=120))
                break
            except Exception as e:
                if n == 3:
                    raise
                print(f"  ! {dia} p{pagina}: {e} — nova tentativa", flush=True)
                time.sleep(10 * (n + 1))
        lote = d.get("items") or []
        itens += lote
        if len(itens) >= int(d.get("count") or 0) or not lote or pagina >= 30:
            return itens
        pagina += 1
        time.sleep(PAUSA)


def processar(itens, mun):
    hoje = dt.date.today()
    out = {}
    for it in itens:
        t = limpa(it.get("texto"))
        bem = trecho_bem(t)
        if not eh_imovel(bem):
            continue
        uf_trib = uf_do_tribunal(it.get("siglaTribunal"))
        c = cidade(it, t, bem, mun, uf_trib)
        ds = datas_leilao(t, it.get("data_disponibilizacao"))
        proc = it.get("numero_processo") or it.get("numeroprocessocommascara") or str(it.get("id"))
        idx = f"{proc}:{ds[0].isoformat() if ds else it.get('data_disponibilizacao')}"
        tipo = None
        for rot, rx in (("apartamento", r"apartamento|flat"), ("casa", r"\bcasa\b"), ("terreno", r"terreno|\blote\b"),
                        ("rural", r"fazenda|s[íi]tio|ch[áa]cara|gleba|rural"), ("comercial", r"sala comercial|galp[ãa]o|loja|pr[ée]dio"),
                        ("garagem", r"vaga\s+de\s+garage")):
            if re.search(rx, bem, re.I):
                tipo = rot
                break
        out[idx] = {
            "fonte": "judicial", "id_externo": idx,
            "uf": c[2] if c else uf_trib, "cidade": c[1] if c else None, "municipio_ibge": c[0] if c else None,
            "tipo": tipo, "modalidade": "leilao", "valor": valor_avaliacao(bem),
            "data_sessao": ds[-1].isoformat() if ds else None,
            "url": it.get("link") or f"https://comunica.pje.jus.br/consulta?numeroProcesso={proc}",
            "origem": it.get("siglaTribunal"),
            "publicado": it.get("data_disponibilizacao"),
            "futuro": bool(ds and ds[-1] >= hoje),
        }
    return list(out.values())


DDL = """
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS origem TEXT;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS publicado DATE;
"""


def gravar(conn, linhas):
    cols = ["fonte", "id_externo", "uf", "cidade", "municipio_ibge", "tipo", "modalidade", "valor", "data_sessao", "url", "origem", "publicado"]
    with conn.cursor() as cur:
        cur.execute(DDL)
        if linhas:
            psycopg2.extras.execute_values(cur, f"""
                INSERT INTO leiloes_outros ({",".join(cols)}) VALUES %s
                ON CONFLICT (fonte, id_externo) DO UPDATE SET
                  uf = COALESCE(EXCLUDED.uf, leiloes_outros.uf), cidade = COALESCE(EXCLUDED.cidade, leiloes_outros.cidade),
                  municipio_ibge = COALESCE(EXCLUDED.municipio_ibge, leiloes_outros.municipio_ibge),
                  tipo = COALESCE(EXCLUDED.tipo, leiloes_outros.tipo), valor = COALESCE(EXCLUDED.valor, leiloes_outros.valor),
                  data_sessao = COALESCE(EXCLUDED.data_sessao, leiloes_outros.data_sessao), url = EXCLUDED.url,
                  origem = EXCLUDED.origem, ultimo_visto = now()
            """, [tuple(l.get(c) for c in cols) for l in linhas])
        # aberto = leilão ainda por vir; sem data extraída, vale 30 dias da publicação
        cur.execute("""
            UPDATE leiloes_outros SET ativo = CASE
              WHEN data_sessao IS NOT NULL THEN data_sessao::date >= CURRENT_DATE
              ELSE COALESCE(publicado, ultimo_visto::date) >= CURRENT_DATE - 30 END
            WHERE fonte = 'judicial'
        """)
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--importar", action="store_true")
    ap.add_argument("--dias", type=int, default=3, help="dias de publicação para trás (primeira carga: 45)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if not a.importar:
        ap.print_help()
        return
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    mun = Municipios(conn)
    total_pub, linhas = 0, []
    for n in range(a.dias, -1, -1):
        dia = (dt.date.today() - dt.timedelta(days=n)).isoformat()
        itens = buscar_dia(dia)
        novas = processar(itens, mun)
        total_pub += len(itens)
        linhas += novas
        print(f"  {dia}: {len(itens)} publicações → {len(novas)} leilões de imóvel", flush=True)
        time.sleep(PAUSA)
    linhas = list({l["id_externo"]: l for l in linhas}.values())
    com_cidade = sum(1 for l in linhas if l["municipio_ibge"])
    futuros = sum(1 for l in linhas if l["futuro"])
    print(f"  total: {total_pub} publicações, {len(linhas)} leilões de imóvel únicos, "
          f"{com_cidade} com município, {futuros} com leilão por vir", flush=True)
    if not a.dry_run:
        gravar(conn, linhas)
    print(f"🏁 Leilões judiciais (DJEN) concluído: {len(linhas)} editais de imóvel.")


if __name__ == "__main__":
    main()
