#!/usr/bin/env python3
"""
Radar CNPJ — agregação de calor por município/categoria.

Le a base nacional de CNPJ (28M+ empresas, local, ver project_cnpj_nacional na
memoria) e sobe pro Postgres da VPS só um resumo agregado — contagem de
empresas ativas por município x categoria, e novas aberturas nos últimos
12/90 dias. Não sobe uma linha por empresa (isso é ~6.4M linhas e ~2-3GB,
arriscado no disco apertado da VPS — ver seção "Pendente" no fim deste
arquivo). Isso desbloqueia o mapa de calor macro do Radar CNPJ sem tocar em
nada existente.

Uso:
    python3 radar_cnpj_heatmap.py
"""
import os
import sys
import psycopg2
import psycopg2.extras

LOCAL_URL = "postgres://postgres:postgres_secure_pass_123@localhost:5432/cnpj_nacional"
VPS_URL_PATH = os.path.expanduser("~/.config/pncp-etl/database_url")
DISCO_MINIMO_GB = 2


def checar_disco_vps(cur):
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    print(f"Tamanho atual do pncp_db na VPS: {cur.fetchone()[0]}")


def main():
    local = psycopg2.connect(LOCAL_URL)
    local.set_session(readonly=True)
    lcur = local.cursor()

    print("Agregando por município x categoria (empresas ativas)...")
    lcur.execute("""
        SELECT e.municipio, e.uf, c.categoria,
               count(*) AS ativas,
               count(*) FILTER (WHERE e.data_abertura >= (CURRENT_DATE - INTERVAL '365 days')) AS novas_12m,
               count(*) FILTER (WHERE e.data_abertura >= (CURRENT_DATE - INTERVAL '90 days')) AS novas_90d
        FROM empresas e
        JOIN categorias_cnae c ON c.codigo_cnae = e.cnae_principal
        WHERE e.situacao_cadastral = '02'
        GROUP BY 1, 2, 3
    """)
    por_categoria = lcur.fetchall()
    print(f"  {len(por_categoria)} linhas (município x categoria)")

    print("Agregando totais por município (todas as categorias, baseline de densidade)...")
    lcur.execute("""
        SELECT e.municipio, e.uf, '__total__' AS categoria,
               count(*) AS ativas,
               count(*) FILTER (WHERE e.data_abertura >= (CURRENT_DATE - INTERVAL '365 days')) AS novas_12m,
               count(*) FILTER (WHERE e.data_abertura >= (CURRENT_DATE - INTERVAL '90 days')) AS novas_90d
        FROM empresas e
        WHERE e.situacao_cadastral = '02'
        GROUP BY 1, 2
    """)
    totais = lcur.fetchall()
    print(f"  {len(totais)} linhas (total por município)")

    linhas = por_categoria + totais
    lcur.close()
    local.close()

    vps_url = open(VPS_URL_PATH).read().strip()
    vps = psycopg2.connect(vps_url)
    vcur = vps.cursor()
    checar_disco_vps(vcur)

    print("Criando/atualizando tabela radar_cnpj_heatmap na VPS...")
    vcur.execute("""
        CREATE TABLE IF NOT EXISTS radar_cnpj_heatmap (
            municipio   text NOT NULL,
            uf          varchar(2) NOT NULL,
            categoria   text NOT NULL,
            ativas      integer NOT NULL,
            novas_12m   integer NOT NULL,
            novas_90d   integer NOT NULL,
            atualizado_em timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (municipio, uf, categoria)
        )
    """)
    vcur.execute("TRUNCATE TABLE radar_cnpj_heatmap")
    psycopg2.extras.execute_values(
        vcur,
        "INSERT INTO radar_cnpj_heatmap (municipio, uf, categoria, ativas, novas_12m, novas_90d) VALUES %s",
        linhas,
        page_size=3000,
    )
    vcur.execute("CREATE INDEX IF NOT EXISTS idx_radar_heatmap_uf ON radar_cnpj_heatmap (uf)")
    vcur.execute("CREATE INDEX IF NOT EXISTS idx_radar_heatmap_categoria ON radar_cnpj_heatmap (categoria)")
    vps.commit()

    vcur.execute("SELECT count(*), sum(ativas) FROM radar_cnpj_heatmap")
    n, total_ativas = vcur.fetchone()
    print(f"OK: {n} linhas gravadas em radar_cnpj_heatmap, somando {total_ativas} empresas ativas.")
    vcur.close()
    vps.close()


if __name__ == "__main__":
    main()

# Pendente (não executado por este script, decisão de risco pro Allan):
# subir a base individual filtrada (~6.4M empresas que batem em alguma
# categoria_cnae, situação ativa) pra popular busca-por-nome e marcadores
# individuais no mapa. Estimativa medida em 2026-09-15: ~6.419.292 linhas,
# ~162 bytes/linha de dado puro (sem raw_json) = ~1GB de dado + índices,
# provavelmente 2-3GB reais na tabela+índices. A VPS tem só 8.4GB livres
# (77% em uso) — a mesma operação (ingest nacional sem filtro) já derrubou
# a escrita do Postgres compartilhado uma vez em 2026-09-09 (ver
# project_cnpj_nacional na memória). Não repetir sem ampliar o disco da
# VPS na Hetzner primeiro, ou sem o Allan decidir aceitar o risco.
