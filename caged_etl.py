"""CAGED — emprego formal por município e seção de atividade (Novo CAGED, MTE/PDET).

Fonte: microdados mensais em ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/<ano>/<AAAAMM>/
  CAGEDMOV — movimentações declaradas no prazo
  CAGEDFOR — declaradas fora do prazo (corrigem meses anteriores)
  CAGEDEXC — exclusões (desfazem movimentações já publicadas)
Método oficial: MOV + FOR − EXC, agrupado pela competência da movimentação.

Cada arquivo vira linhas em `caged_agregado` (uma por competência × município ×
seção CNAE), identificadas pela `fonte` (ex.: FOR202607). Recarregar um arquivo
substitui só a parte dele — o ETL é idempotente. A visão `caged_municipios`
soma as fontes com o sinal certo.

O código de município do CAGED tem 6 dígitos; o de 7 (mesma chave do Score,
Bolsa Família e licitações) vem da lista oficial do IBGE.

Roda na máquina do Allan (7z de 55 MB → 466 MB de texto por mês, lido em
fluxo, sem gravar o .txt) e grava no Postgres do VPS pelo túnel (5433).

Uso:
  python caged_etl.py                # carrega os meses publicados que faltam
  python caged_etl.py --meses 13     # garante os últimos 13 meses publicados
  python caged_etl.py --recarregar 202607
"""
import argparse
import csv
import gzip
import io
import json
import os
import re
import subprocess
import sys
import unicodedata
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "")
FTP = "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED"
CACHE = os.path.expanduser("~/.cache/caged")
IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
SINAL = {"MOV": 1, "FOR": 1, "EXC": -1}

DDL = """
CREATE TABLE IF NOT EXISTS caged_agregado (
    fonte            VARCHAR(12) NOT NULL,
    competencia      INTEGER     NOT NULL,
    municipio_ibge6  INTEGER     NOT NULL,
    municipio_ibge   INTEGER,
    uf               SMALLINT,
    secao            CHAR(1)     NOT NULL,
    admissoes        INTEGER     NOT NULL,
    desligamentos    INTEGER     NOT NULL,
    soma_salario_adm NUMERIC     NOT NULL DEFAULT 0,
    n_salario_adm    INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (fonte, competencia, municipio_ibge6, secao)
);
CREATE INDEX IF NOT EXISTS caged_agregado_mun ON caged_agregado (municipio_ibge, competencia);
CREATE TABLE IF NOT EXISTS caged_carga (
    fonte        VARCHAR(12) PRIMARY KEY,
    linhas       INTEGER,
    carregado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE OR REPLACE VIEW caged_municipios AS
SELECT competencia, municipio_ibge, municipio_ibge6, uf, secao,
       SUM(CASE WHEN fonte LIKE 'EXC%' THEN -admissoes ELSE admissoes END)::int          AS admissoes,
       SUM(CASE WHEN fonte LIKE 'EXC%' THEN -desligamentos ELSE desligamentos END)::int  AS desligamentos,
       SUM(CASE WHEN fonte LIKE 'EXC%' THEN -(admissoes - desligamentos)
                ELSE admissoes - desligamentos END)::int                                  AS saldo,
       ROUND(SUM(CASE WHEN fonte LIKE 'EXC%' THEN 0 ELSE soma_salario_adm END)
             / NULLIF(SUM(CASE WHEN fonte LIKE 'EXC%' THEN 0 ELSE n_salario_adm END), 0), 2) AS salario_medio_adm
FROM caged_agregado
GROUP BY competencia, municipio_ibge, municipio_ibge6, uf, secao;
-- meses cujo arquivo principal (MOV) já foi carregado: declarações atrasadas de
-- meses mais antigos somam no mês certo, mas não criam meses "fantasmas" na série
CREATE OR REPLACE VIEW caged_meses_completos AS
SELECT substring(fonte from 4)::int AS competencia FROM caged_carga WHERE fonte LIKE 'MOV%';
"""


def log(m):
    print(f"[caged] {m}", flush=True)


def norm(s):
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()


def meses_publicados():
    """Lista AAAAMM publicados no FTP, do mais antigo ao mais novo."""
    anos = re.findall(r"^(\d{4})\s*$", ftp_listar(f"{FTP}/"), re.M)
    meses = []
    for a in sorted(anos):
        meses += re.findall(r"^(\d{6})\s*$", ftp_listar(f"{FTP}/{a}/"), re.M)
    return sorted(meses)


def ftp_listar(url):
    return subprocess.run(["curl", "-s", "-m", "60", "--list-only", url], capture_output=True, text=True, errors="ignore").stdout


def baixar(tipo, mes):
    os.makedirs(CACHE, exist_ok=True)
    destino = os.path.join(CACHE, f"CAGED{tipo}{mes}.7z")
    url = f"{FTP}/{mes[:4]}/{mes}/CAGED{tipo}{mes}.7z"
    r = subprocess.run(["curl", "-s", "-f", "-m", "900", "--retry", "3", "-o", destino, url])
    if r.returncode != 0:
        raise RuntimeError(f"download falhou: {url}")
    return destino


def mapa_ibge():
    with urllib.request.urlopen(IBGE_MUNICIPIOS, timeout=60) as r:
        bruto = r.read()
    if bruto[:2] == b"\x1f\x8b":  # a API do IBGE às vezes responde em gzip
        bruto = gzip.decompress(bruto)
    dados = json.loads(bruto)
    return {int(str(m["id"])[:6]): int(m["id"]) for m in dados}


def agregar(arquivo_7z):
    """Lê o .txt de dentro do 7z em fluxo e soma por competência × município × seção."""
    proc = subprocess.Popen(["7z", "e", "-so", arquivo_7z], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    leitor = csv.reader(io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace", newline=""), delimiter=";")
    cab = [norm(c) for c in next(leitor)]
    i = {n: cab.index(n) for n in ("competenciamov", "uf", "municipio", "secao", "saldomovimentacao", "salario")}
    soma, linhas = {}, 0
    for row in leitor:
        if len(row) < len(cab) - 2:
            continue
        linhas += 1
        try:
            chave = (int(row[i["competenciamov"]]), int(row[i["municipio"]]), row[i["secao"]].strip()[:1] or "?")
            uf = int(row[i["uf"]])
            mov = int(row[i["saldomovimentacao"]])
        except ValueError:
            continue
        a = soma.setdefault(chave, [uf, 0, 0, 0.0, 0])
        if mov > 0:
            a[1] += 1
            try:
                sal = float(row[i["salario"]].replace(",", "."))
            except ValueError:
                sal = 0
            if 300 <= sal <= 100_000:  # descarta salários zerados ou digitados errado
                a[3] += sal
                a[4] += 1
        elif mov < 0:
            a[2] += 1
    proc.wait()
    return soma, linhas


def carregar(conn, tipo, mes, ibge):
    fonte = f"{tipo}{mes}"
    log(f"{fonte}: baixando")
    arq = baixar(tipo, mes)
    soma, linhas = agregar(arq)
    os.remove(arq)
    valores = [(fonte, comp, mun6, ibge.get(mun6), uf, sec, adm, desl, round(sal, 2), nsal)
               for (comp, mun6, sec), (uf, adm, desl, sal, nsal) in soma.items()]
    with conn.cursor() as cur:
        cur.execute("DELETE FROM caged_agregado WHERE fonte = %s", (fonte,))
        execute_values(cur, """INSERT INTO caged_agregado (fonte, competencia, municipio_ibge6, municipio_ibge, uf, secao,
                               admissoes, desligamentos, soma_salario_adm, n_salario_adm) VALUES %s""", valores, page_size=5000)
        cur.execute("""INSERT INTO caged_carga (fonte, linhas) VALUES (%s, %s)
                       ON CONFLICT (fonte) DO UPDATE SET linhas = EXCLUDED.linhas, carregado_em = NOW()""", (fonte, linhas))
    conn.commit()
    sem_ibge = sum(1 for v in valores if v[3] is None)
    log(f"{fonte}: {linhas:,} movimentações → {len(valores):,} linhas agregadas" + (f" ({sem_ibge} sem código IBGE)" if sem_ibge else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--meses", type=int, default=0, help="garante os últimos N meses publicados")
    ap.add_argument("--recarregar", help="AAAAMM a recarregar mesmo já carregado")
    args = ap.parse_args()
    if not DATABASE_URL:
        sys.exit("DATABASE_URL não definido")
    conn = psycopg2.connect(DATABASE_URL, keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3)
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("SELECT fonte FROM caged_carga")
        feitas = {r[0] for r in cur.fetchall()}
    conn.commit()

    publicados = meses_publicados()
    if not publicados:
        sys.exit("não consegui listar o FTP do PDET")
    if args.recarregar:
        alvo = [args.recarregar]
    else:
        janela = publicados[-args.meses:] if args.meses else publicados[-1:]
        # sem --meses: só o que falta entre os já carregados e o último publicado
        alvo = [m for m in janela if f"MOV{m}" not in feitas]
        if not args.meses and feitas:
            ultimo = max(int(f[3:]) for f in feitas if f.startswith("MOV"))
            alvo = [m for m in publicados if int(m) > ultimo]
    if not alvo:
        log(f"nada novo (último publicado: {publicados[-1]})")
        return
    ibge = mapa_ibge()
    log(f"meses a carregar: {', '.join(alvo)}")
    for m in alvo:
        for tipo in ("MOV", "FOR", "EXC"):
            carregar(conn, tipo, m, ibge)
    log("concluído")


if __name__ == "__main__":
    main()
