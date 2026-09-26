"""Recorte do litoral — municípios que o mapa do litoral e a oferta × procura cobrem.

Fonte: IBGE, "Municípios defrontantes com o mar" (2024)
  https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_defrontantes_com_o_mar/2024/
Recorte decidido pelo Allan em 2026-09-25:
  - RJ: litoral inteiro (25 municípios, de São Francisco de Itabapoana a Paraty)
  - BA: litoral sul (regiões intermediárias Ilhéus-Itabuna e Santo Antônio de
    Jesus: Costa do Dendê, Cacau, Descobrimento e Baleias — Valença a Mucuri)
  - CE: litoral inteiro (20 municípios)
Portos importantes marcados à mão (lista PORTOS).

Grava a tabela `regiao_litoral` (idempotente: recria o conteúdo).
Uso: DATABASE_URL=... python regiao_litoral.py
"""
import csv
import io
import os
import subprocess
import sys
import tempfile
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

URL = ("https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/"
       "municipios_defrontantes_com_o_mar/2024/Municipios_Defrontantes_com_o_Mar_2024.ods")
RECORTE = {
    "RJ": ("RJ — litoral", None),
    "BA": ("BA — litoral sul", {"Ilhéus - Itabuna", "Santo Antônio de Jesus"}),
    "CE": ("CE — litoral", None),
}
PORTOS = {
    3305000: "Porto do Açu", 3302403: "Porto de Imbetiba (Macaé)", 3300258: "Porto do Forno",
    3303302: "Porto de Niterói", 3304557: "Porto do Rio de Janeiro", 3302007: "Porto de Itaguaí",
    3300100: "Porto de Angra dos Reis / Terminal da Baía de Ilha Grande (TEBIG)",
    2913606: "Porto de Ilhéus", 2312403: "Porto do Pecém", 2304400: "Porto do Mucuripe (Fortaleza)",
}
DDL = """
CREATE TABLE IF NOT EXISTS regiao_litoral (
    municipio_ibge       INTEGER PRIMARY KEY,
    municipio_nome       TEXT NOT NULL,
    uf                   CHAR(2) NOT NULL,
    recorte              TEXT NOT NULL,
    regiao_intermediaria TEXT,
    regiao_imediata      TEXT,
    area_km2             NUMERIC,
    porto                TEXT,
    atualizado_em        TIMESTAMPTZ DEFAULT NOW()
);
"""


def ler_lista():
    with tempfile.TemporaryDirectory() as d:
        ods = os.path.join(d, "lista.ods")
        urllib.request.urlretrieve(URL, ods)
        subprocess.run(["soffice", "--headless", "--convert-to", "csv", "--outdir", d, ods],
                       check=True, capture_output=True, timeout=120)
        with open(os.path.join(d, "lista.csv"), encoding="utf-8") as f:
            return list(csv.DictReader(f))


def main():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL não definido")
    linhas = []
    for m in ler_lista():
        uf = m["SIGLA_UF"]
        if uf not in RECORTE:
            continue
        nome_recorte, regioes = RECORTE[uf]
        if regioes and m["NM_RGINT"] not in regioes:
            continue
        cod = int(m["CD_MUN"])
        linhas.append((cod, m["NM_MUN"], uf, nome_recorte, m["NM_RGINT"], m["NM_RGI"],
                       float(m["AREA_KM2"] or 0), PORTOS.get(cod)))
    conn = psycopg2.connect(url)
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("DELETE FROM regiao_litoral")
        execute_values(cur, """INSERT INTO regiao_litoral (municipio_ibge, municipio_nome, uf, recorte,
                              regiao_intermediaria, regiao_imediata, area_km2, porto) VALUES %s""", linhas)
    por = {}
    for l in linhas:
        por[l[3]] = por.get(l[3], 0) + 1
    print(f"[regiao_litoral] {len(linhas)} municípios: {por} · portos: {sum(1 for l in linhas if l[7])}")


if __name__ == "__main__":
    main()
