#!/usr/bin/env python3
"""
Índice FipeZAP (Fipe + Zap) — preço médio anunciado do m² residencial, venda e
locação, e a rentabilidade do aluguel, mês a mês, em ~56 cidades + o índice
nacional ("Índice FipeZAP", gravado como cidade "Brasil").

Fonte: planilha pública de séries históricas da Fipe (atualizada todo mês):
  https://downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx
O site cita "Fonte: Índice FipeZAP" em todo número que usa daqui e mostra só
recortes (último mês, 12 meses, série curta) — não republica a planilha.

Complementa o que o coletor próprio (imoveis_mercado) não dá: série longa e
comparável. O coletor cobre cidades que a FipeZap não cobre (Angra, Paraty…);
a FipeZap dá a tendência da capital/cidade-referência do estado.

Lê o .xlsx só com a biblioteca padrão (zipfile + XML), sem openpyxl.
Uso:
  python3 fipezap_etl.py --importar
"""
import argparse
import datetime
import html
import io
import os
import re
import sys
import time
import zipfile

import psycopg2
import requests
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
URL = "https://downloads.fipe.org.br/indices/fipezap/fipezap-serieshistoricas.xlsx"

DDL = """
CREATE TABLE IF NOT EXISTS fipezap (
    cidade            TEXT NOT NULL,     -- nome da aba; "Brasil" = Índice FipeZAP (composto)
    uf                CHAR(2),
    municipio_ibge    TEXT,
    mes               DATE NOT NULL,     -- 1º dia do mês de referência
    venda_m2          NUMERIC,           -- R$/m², residencial, total
    venda_var_mes     NUMERIC,           -- fração (0,01 = 1%)
    venda_var_12m     NUMERIC,
    aluguel_m2        NUMERIC,           -- R$/m² por mês
    aluguel_var_mes   NUMERIC,
    aluguel_var_12m   NUMERIC,
    rentabilidade_mes NUMERIC,           -- rental yield mensal (fração)
    atualizado_em     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (cidade, mes)
);
CREATE INDEX IF NOT EXISTS idx_fipezap_ibge ON fipezap(municipio_ibge, mes);
CREATE INDEX IF NOT EXISTS idx_fipezap_uf ON fipezap(uf, mes);
"""

# colunas da aba de cada cidade (bloco "Imóveis residenciais", coluna "Total")
COL = {
    "venda_idx": "C", "venda_var_mes": "H", "venda_var_12m": "M", "venda_m2": "R",
    "aluguel_var_mes": "AB", "aluguel_var_12m": "AG", "aluguel_m2": "AL",
    "rentabilidade_mes": "AQ",
}
ABAS_FORA = {"Resumo", "Aux"}
NACIONAL = "Índice FipeZAP"


def log(m):
    print(f"[fipezap] {m}", flush=True)


def baixar() -> bytes:
    for t in range(4):
        try:
            r = requests.get(URL, timeout=180, headers={"User-Agent": "Mozilla/5.0"})  # UA com texto extra leva 403 do Cloudflare da Fipe
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            print(f"  ⚠ download (tentativa {t + 1}/4): {e}", file=sys.stderr)
            time.sleep(10 * (t + 1))
    raise SystemExit("❌ planilha da FipeZap indisponível")


class Xlsx:
    """Leitor mínimo de .xlsx: strings compartilhadas + células de cada aba."""

    def __init__(self, dados: bytes):
        self.z = zipfile.ZipFile(io.BytesIO(dados))
        self.ss = []
        if "xl/sharedStrings.xml" in self.z.namelist():
            x = self.z.read("xl/sharedStrings.xml").decode("utf-8")
            for si in re.findall(r"<si>(.*?)</si>", x, re.S):
                self.ss.append(html.unescape("".join(re.findall(r"<t[^>]*>(.*?)</t>", si, re.S))))
        wb = self.z.read("xl/workbook.xml").decode("utf-8")
        rels = self.z.read("xl/_rels/workbook.xml.rels").decode("utf-8")
        alvo = {}
        for tag in re.findall(r"<Relationship [^>]*/>", rels):
            i, t = re.search(r'Id="([^"]+)"', tag), re.search(r'Target="([^"]+)"', tag)
            if i and t:
                alvo[i.group(1)] = "xl/" + t.group(1).lstrip("/").removeprefix("xl/")
        self.abas = {}
        for tag in re.findall(r"<sheet [^>]*/>", wb):
            n, r = re.search(r'name="([^"]+)"', tag), re.search(r'r:id="([^"]+)"', tag)
            if n and r:
                self.abas[html.unescape(n.group(1))] = alvo[r.group(1)]

    def linhas(self, aba):
        x = self.z.read(self.abas[aba]).decode("utf-8")
        for row in re.findall(r"<row [^>]*>(.*?)</row>", x, re.S):
            cel = {}
            for m in re.finditer(r'<c r="([A-Z]+)\d+"([^>]*?)(?:/>|>(.*?)</c>)', row, re.S):
                ref, attrs, inner = m.groups()
                v = re.search(r"<v>(.*?)</v>", inner or "", re.S)
                if not v:
                    continue
                cel[ref] = self.ss[int(v.group(1))] if 't="s"' in attrs else v.group(1)
            yield cel


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None  # "." e "não disponível"


def mes_excel(v):
    try:
        d = datetime.date(1899, 12, 30) + datetime.timedelta(days=int(float(v)))
        return d.replace(day=1)
    except (TypeError, ValueError):
        return None


def importar():
    xl = Xlsx(baixar())
    # UF de cada cidade vem da aba Resumo (colunas B = nome, C = UF)
    uf_de = {}
    for c in xl.linhas("Resumo"):
        nome, uf = c.get("B"), c.get("C")
        if nome and uf and re.fullmatch(r"[A-Z]{2}", uf):
            uf_de[nome] = uf

    linhas = []
    for aba in xl.abas:
        if aba in ABAS_FORA:
            continue
        cidade = "Brasil" if aba == NACIONAL else aba
        uf = None if aba == NACIONAL else uf_de.get(aba)
        if aba != NACIONAL and not uf:
            log(f"  aba sem UF no Resumo, ignorada: {aba}")
            continue
        n = 0
        for c in xl.linhas(aba):
            mes = mes_excel(c.get("B"))
            if not mes or mes.year < 2000:
                continue
            v = {k: num(c.get(col)) for k, col in COL.items()}
            if v["venda_m2"] is None and v["aluguel_m2"] is None:
                continue
            linhas.append((cidade, uf, mes, v["venda_m2"], v["venda_var_mes"], v["venda_var_12m"],
                           v["aluguel_m2"], v["aluguel_var_mes"], v["aluguel_var_12m"], v["rentabilidade_mes"]))
            n += 1
        if n == 0:
            log(f"  aba sem série: {aba}")
    if len(linhas) < 1000:
        raise SystemExit(f"❌ só {len(linhas)} linhas lidas — o layout da planilha mudou? Nada gravado.")

    conn = psycopg2.connect(DATABASE_URL)
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        execute_values(cur, """
            INSERT INTO fipezap (cidade, uf, mes, venda_m2, venda_var_mes, venda_var_12m,
                                 aluguel_m2, aluguel_var_mes, aluguel_var_12m, rentabilidade_mes)
            VALUES %s
            ON CONFLICT (cidade, mes) DO UPDATE SET
              uf = EXCLUDED.uf, venda_m2 = EXCLUDED.venda_m2, venda_var_mes = EXCLUDED.venda_var_mes,
              venda_var_12m = EXCLUDED.venda_var_12m, aluguel_m2 = EXCLUDED.aluguel_m2,
              aluguel_var_mes = EXCLUDED.aluguel_var_mes, aluguel_var_12m = EXCLUDED.aluguel_var_12m,
              rentabilidade_mes = EXCLUDED.rentabilidade_mes, atualizado_em = NOW()""", linhas, page_size=2000)
        # código IBGE pelo nome sem acento + UF (radar_loteamento tem os 5.570 municípios)
        cur.execute("""
            UPDATE fipezap f SET municipio_ibge = r.municipio_ibge
            FROM radar_loteamento r
            WHERE f.municipio_ibge IS NULL AND f.uf = r.uf
              AND lower(unaccent(f.cidade)) = lower(unaccent(r.municipio_nome))""")
        cur.execute("SELECT count(DISTINCT cidade), count(DISTINCT cidade) FILTER (WHERE municipio_ibge IS NULL AND cidade <> 'Brasil'), max(mes) FROM fipezap")
        cidades, sem_ibge, ultimo = cur.fetchone()
    conn.close()
    log(f"🏁 {len(linhas)} linhas · {cidades} séries · último mês {ultimo} · {sem_ibge} cidade(s) sem código IBGE")


def main():
    ap = argparse.ArgumentParser(description="Índice FipeZAP → tabela fipezap")
    ap.add_argument("--importar", action="store_true")
    a = ap.parse_args()
    if not a.importar:
        ap.error("Especifique --importar")
    importar()


if __name__ == "__main__":
    main()
