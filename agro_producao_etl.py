#!/usr/bin/env python3
"""
Produção agropecuária por município — IBGE, pesquisas anuais (API SIDRA v3).
Substitui na prática o Censo Agro 2017 (agro_etl.py), que só terá edição nova
com coleta em 2027:
  PAM  t5457  Produção Agrícola Municipal: valor da produção e área plantada
              das lavouras (total e a cultura que mais rende no município)
  PPM  t3939  Pesquisa da Pecuária Municipal: efetivo dos rebanhos
  PPM  t74    produção de origem animal (leite, ovos, mel…): valor
Nenhuma dessas conta "nº de estabelecimentos" — isso continua vindo do Censo
(agro_municipios).

Uma linha por município × ano em `agro_producao`; a visão `agro_producao_atual`
dá o ano mais recente de cada pesquisa (PAM e PPM saem em datas diferentes).
Puxa por UF, como o agro_etl (o bulk nacional estoura o servidor do IBGE).
Leve: ~2 s e ~4 MB por UF na PAM completa; roda na fila noturna do VPS.

Uso:
  python3 agro_producao_etl.py --importar            # ano mais recente de cada pesquisa
  python3 agro_producao_etl.py --importar --anos 3   # e os 2 anteriores (série para crescimento)
"""
import argparse
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
API = "https://servicodados.ibge.gov.br/api/v3/agregados"
UF_COD = ["11", "12", "13", "14", "15", "16", "17", "21", "22", "23", "24", "25", "26", "27", "28", "29",
          "31", "32", "33", "35", "41", "42", "43", "50", "51", "52", "53"]
TIMEOUT = 120

# rebanhos (t3939, classificação 79) → coluna
REBANHOS = {"2670": "bovinos", "32794": "suinos", "32796": "galinaceos", "2681": "caprinos", "2677": "ovinos"}

DDL = """
CREATE TABLE IF NOT EXISTS agro_producao (
    municipio_ibge            TEXT     NOT NULL,
    ano                       SMALLINT NOT NULL,
    uf                        CHAR(2),
    lavoura_valor_mil         NUMERIC,   -- PAM: valor da produção das lavouras (R$ mil)
    lavoura_area_ha           NUMERIC,   -- PAM: área plantada ou destinada à colheita
    cultura_principal         TEXT,      -- PAM: cultura de maior valor no município
    cultura_principal_valor_mil NUMERIC,
    bovinos                   INTEGER,   -- PPM t3939 (cabeças)
    suinos                    INTEGER,
    galinaceos                INTEGER,
    caprinos                  INTEGER,
    ovinos                    INTEGER,
    origem_animal_valor_mil   NUMERIC,   -- PPM t74: leite, ovos, mel… (R$ mil)
    atualizado_em             TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (municipio_ibge, ano)
);
CREATE OR REPLACE VIEW agro_producao_atual AS
SELECT DISTINCT ON (m.municipio_ibge) m.municipio_ibge, m.uf,
       pam.ano AS pam_ano, pam.lavoura_valor_mil, pam.lavoura_area_ha,
       pam.cultura_principal, pam.cultura_principal_valor_mil,
       ppm.ano AS ppm_ano, ppm.bovinos, ppm.suinos, ppm.galinaceos, ppm.caprinos, ppm.ovinos,
       ppm.origem_animal_valor_mil
FROM (SELECT DISTINCT municipio_ibge, uf FROM agro_producao) m
LEFT JOIN LATERAL (SELECT * FROM agro_producao a WHERE a.municipio_ibge = m.municipio_ibge
                   AND a.lavoura_valor_mil IS NOT NULL ORDER BY ano DESC LIMIT 1) pam ON TRUE
LEFT JOIN LATERAL (SELECT * FROM agro_producao a WHERE a.municipio_ibge = m.municipio_ibge
                   AND (a.bovinos IS NOT NULL OR a.origem_animal_valor_mil IS NOT NULL)
                   ORDER BY ano DESC LIMIT 1) ppm ON TRUE;
"""


def log(m):
    print(f"[agro-producao] {m}", flush=True)


def num(v):
    """Valor do SIDRA: '-' (zero), 'X' (sigiloso), '...'/'..' (sem dado) → None."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def get(url):
    for t in range(4):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            print(f"  ⚠ {url[:110]}… (tentativa {t + 1}/4): {e}", file=sys.stderr)
            time.sleep(5 * (t + 1))
    return None


def anos_disponiveis(tabela, n):
    d = get(f"{API}/{tabela}/periodos") or []
    return [p["id"] for p in d][-n:]


def pam_uf(uf, ano, linhas):
    """Valor por cultura (todas) + área total, e acha a cultura principal."""
    loc = f"N6%5BN3%5B{uf}%5D%5D"
    d = get(f"{API}/5457/periodos/{ano}/variaveis/215?localidades={loc}&classificacao=782%5Ball%5D")
    if d is None:
        return 0
    por_mun = {}
    for res in d[0]["resultados"]:
        cat_id, cat_nome = next(iter(res["classificacoes"][0]["categoria"].items()))
        for s in res["series"]:
            v = num(next(iter(s["serie"].values())))
            reg = por_mun.setdefault(s["localidade"]["id"], {"total": None, "top": (None, 0)})
            if cat_id == "0":
                reg["total"] = v
            elif v and v > reg["top"][1]:
                reg["top"] = (cat_nome.rstrip("*").strip(), v)
    area = get(f"{API}/5457/periodos/{ano}/variaveis/8331?localidades={loc}&classificacao=782%5B0%5D")
    areas = {}
    if area:
        for s in area[0]["resultados"][0]["series"]:
            areas[s["localidade"]["id"]] = num(next(iter(s["serie"].values())))
    for ibge, reg in por_mun.items():
        r = linhas.setdefault((ibge, int(ano)), {})
        r["lavoura_valor_mil"] = reg["total"]
        r["lavoura_area_ha"] = areas.get(ibge)
        r["cultura_principal"], r["cultura_principal_valor_mil"] = reg["top"] if reg["top"][0] else (None, None)
    return len(por_mun)


def ppm_uf(uf, ano, linhas):
    loc = f"N6%5BN3%5B{uf}%5D%5D"
    cats = "%2C".join(REBANHOS)
    d = get(f"{API}/3939/periodos/{ano}/variaveis/105?localidades={loc}&classificacao=79%5B{cats}%5D")
    n = 0
    if d:
        for res in d[0]["resultados"]:
            col = REBANHOS[next(iter(res["classificacoes"][0]["categoria"]))]
            for s in res["series"]:
                v = num(next(iter(s["serie"].values())))
                linhas.setdefault((s["localidade"]["id"], int(ano)), {})[col] = int(v) if v is not None else None
                n += 1
    d = get(f"{API}/74/periodos/{ano}/variaveis/215?localidades={loc}&classificacao=80%5B0%5D")
    if d:
        for s in d[0]["resultados"][0]["series"]:
            linhas.setdefault((s["localidade"]["id"], int(ano)), {})["origem_animal_valor_mil"] = num(next(iter(s["serie"].values())))
    return n


COLS = ["lavoura_valor_mil", "lavoura_area_ha", "cultura_principal", "cultura_principal_valor_mil",
        "bovinos", "suinos", "galinaceos", "caprinos", "ovinos", "origem_animal_valor_mil"]
COD_UF = {"11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO", "21": "MA",
          "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE", "29": "BA",
          "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR", "42": "SC", "43": "RS", "50": "MS",
          "51": "MT", "52": "GO", "53": "DF"}


def gravar(conn, linhas):
    # só sobrescreve as colunas que vieram nesta carga (PAM e PPM chegam em anos diferentes)
    valores = [(ibge, ano, COD_UF.get(ibge[:2]), *[r.get(c) for c in COLS], [c for c in COLS if c in r])
               for (ibge, ano), r in linhas.items()]
    sets = ", ".join(f"{c} = CASE WHEN '{c}' = ANY(v.vieram) THEN v.{c} ELSE a.{c} END" for c in COLS)
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO agro_producao (municipio_ibge, ano, uf, {", ".join(COLS)})
            SELECT municipio_ibge, ano, uf, {", ".join(COLS)} FROM (VALUES %s)
              AS v(municipio_ibge, ano, uf, {", ".join(COLS)}, vieram)
            ON CONFLICT (municipio_ibge, ano) DO NOTHING""",
            [v[:-1] + (v[-1],) for v in valores],
            template="(%s, %s::smallint, %s, %s::numeric, %s::numeric, %s, %s::numeric, %s::int, %s::int, %s::int, %s::int, %s::int, %s::numeric, %s::text[])",
            page_size=2000)
        execute_values(cur, f"""
            UPDATE agro_producao a SET {sets}, atualizado_em = NOW()
            FROM (VALUES %s) AS v(municipio_ibge, ano, uf, {", ".join(COLS)}, vieram)
            WHERE a.municipio_ibge = v.municipio_ibge AND a.ano = v.ano""",
            valores,
            template="(%s, %s::smallint, %s, %s::numeric, %s::numeric, %s, %s::numeric, %s::int, %s::int, %s::int, %s::int, %s::int, %s::numeric, %s::text[])",
            page_size=2000)
    conn.commit()


def importar(n_anos):
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    anos_pam = anos_disponiveis(5457, n_anos)
    anos_ppm = anos_disponiveis(3939, n_anos)
    log(f"PAM {', '.join(anos_pam)} · PPM {', '.join(anos_ppm)}")
    total = 0
    for uf in UF_COD:
        linhas = {}
        mun = 0
        for ano in anos_pam:
            mun = max(mun, pam_uf(uf, ano, linhas))
        for ano in anos_ppm:
            ppm_uf(uf, ano, linhas)
        gravar(conn, linhas)
        total += len(linhas)
        log(f"{COD_UF[uf]}: {mun} municípios, {len(linhas)} linhas município×ano")
        time.sleep(0.4)
    conn.close()
    print(f"🏁 Produção agropecuária concluída: {total} linhas (PAM {anos_pam[-1]}, PPM {anos_ppm[-1]}).")


def main():
    p = argparse.ArgumentParser(description="Produção agropecuária municipal — IBGE PAM + PPM")
    p.add_argument("--importar", action="store_true")
    p.add_argument("--anos", type=int, default=1, help="quantos anos mais recentes de cada pesquisa")
    a = p.parse_args()
    if not a.importar:
        p.error("Especifique --importar")
    importar(a.anos)


if __name__ == "__main__":
    main()
