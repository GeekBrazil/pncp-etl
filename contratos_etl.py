#!/usr/bin/env python3
"""
contratos_etl — Coleta contratos públicos da API PNCP e salva no Postgres.

Parte flagship do SaaS Investigativo:
Cruza fornecedor de contratos públicas (niFornecedor) × empresa sancionada (CEIS, CNEP, CEPIM)
para identificar órgãos que contrataram empresas impedidas de licitar/contratar.

API PNCP: https://pncp.gov.br/api/consulta/v1/contratos
Parâmetros: dataInicial (YYYYMMDD), dataFinal (YYYYMMDD), pagina (int)

Uso:
  python contratos_etl.py                 # coleta os últimos 30 dias
  python contratos_etl.py --dias 90       # coleta os últimos 90 dias
  python contratos_etl.py --inicio 2026-01-01 --fim 2026-07-23
"""
import argparse
import os
import sys
import re
import time
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
API_BASE = "https://pncp.gov.br/api/consulta/v1/contratos"
PAUSA = float(os.environ.get("CONTRATOS_PAUSA", "0.2"))  # s entre chamadas de página

def log(msg):
    print(f"[contratos_etl] {msg}", flush=True)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def _digits(s):
    return re.sub(r"\D", "", str(s or "")) or None

def _date(s):
    if not s:
        return None
    try:
        if "T" in str(s):
            return str(s).split("T")[0]
        return str(s)
    except Exception:
        return None

def parse_contrato(c):
    num_controle = c.get("numeroControlePNCP") or c.get("numeroControlePncp")
    if not num_controle:
        return None

    fornecedor_ni = _digits(c.get("niFornecedor"))
    if fornecedor_ni and len(fornecedor_ni) != 14:
        # Só mantemos CNPJ válido de 14 dígitos (ignoramos CPF ou inválidos para saneamento)
        fornecedor_ni = None

    orgao = c.get("orgaoEntidade") or {}
    unidade = c.get("unidadeOrgao") or {}

    cnpj_orgao = _digits(orgao.get("cnpj"))
    nome_fornecedor = c.get("nomeRazaoSocialFornecedor") or c.get("usuarioNome")
    objeto = c.get("objetoContrato")

    valor_inicial = c.get("valorInicial")
    valor_global = c.get("valorGlobal")

    data_assinatura = _date(c.get("dataAssinatura"))
    data_inicio = _date(c.get("dataVigenciaInicio"))
    data_fim = _date(c.get("dataVigenciaFim"))

    uf = unidade.get("ufSigla") or orgao.get("uf")
    ibge = unidade.get("codigoIbge")
    municipio = unidade.get("municipioNome")

    url_pncp = f"https://pncp.gov.br/app/contratos/{num_controle}"

    return (
        num_controle,
        fornecedor_ni,
        nome_fornecedor[:500] if nome_fornecedor else None,
        cnpj_orgao,
        orgao.get("razaoSocial", "")[:500] if orgao.get("razaoSocial") else None,
        objeto[:2000] if objeto else None,
        valor_inicial,
        valor_global,
        data_assinatura,
        data_inicio,
        data_fim,
        uf,
        ibge,
        municipio[:200] if municipio else None,
        url_pncp,
    )

UPSERT_CONTRATO = """
INSERT INTO contratos (
    numero_controle_pncp, cnpj_fornecedor, nome_fornecedor, orgao_cnpj, orgao_nome,
    objeto, valor_inicial, valor_global, data_assinatura, data_vigencia_inicio,
    data_vigencia_fim, uf, municipio_ibge, municipio_nome, url_pncp
) VALUES %s
ON CONFLICT (numero_controle_pncp) DO UPDATE SET
    cnpj_fornecedor = EXCLUDED.cnpj_fornecedor,
    nome_fornecedor = EXCLUDED.nome_fornecedor,
    valor_global = EXCLUDED.valor_global,
    data_vigencia_fim = EXCLUDED.data_vigencia_fim,
    importado_em = NOW();
"""

def coletar_dia(dt_str, conn):
    dt_api = dt_str.replace("-", "")
    page = 1
    total_gravado_dia = 0

    log(f"Iniciando coleta para data {dt_str}...")

    while True:
        url = f"{API_BASE}?dataInicial={dt_api}&dataFinal={dt_api}&pagina={page}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                log(f"Erro HTTP {resp.status_code} na url {url}")
                break
            data = resp.json()
        except Exception as e:
            log(f"Exceção ao buscar página {page} da data {dt_str}: {e}")
            break

        items = data.get("data") or data.get("items") or []
        if isinstance(data, list):
            items = data

        if not items:
            break

        registros = []
        for it in items:
            parsed = parse_contrato(it)
            if parsed:
                registros.append(parsed)

        if registros:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_CONTRATO, registros)
            conn.commit()
            total_gravado_dia += len(registros)

        total_paginas = data.get("totalPaginas", page)
        paginas_restantes = data.get("paginasRestantes", 0)

        if page >= total_paginas or paginas_restantes <= 0:
            break

        page += 1
        time.sleep(PAUSA)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO contratos_progress (data_processada, concluido, total_gravado, atualizado_em)
            VALUES (%s, TRUE, %s, NOW())
            ON CONFLICT (data_processada) DO UPDATE SET
                concluido = TRUE, total_gravado = EXCLUDED.total_gravado, atualizado_em = NOW();
        """, (dt_str, total_gravado_dia))
    conn.commit()

    log(f"Data {dt_str} concluída: {total_gravado_dia} contratos gravados.")
    return total_gravado_dia

def main():
    parser = argparse.ArgumentParser(description="Coletor de Contratos do PNCP")
    parser.add_argument("--dias", type=int, default=7, help="Número de dias retroativos a coletar (default: 7)")
    parser.add_argument("--inicio", type=str, help="Data inicial (YYYY-MM-DD)")
    parser.add_argument("--fim", type=str, help="Data final (YYYY-MM-DD)")
    args = parser.parse_args()

    conn = get_conn()

    if args.inicio and args.fim:
        dt_inicio = datetime.strptime(args.inicio, "%Y-%m-%d").date()
        dt_fim = datetime.strptime(args.fim, "%Y-%m-%d").date()
    else:
        dt_fim = datetime.now().date()
        dt_inicio = dt_fim - timedelta(days=args.dias)

    curr = dt_inicio
    total_geral = 0

    log(f"Coletando contratos de {dt_inicio} até {dt_fim}...")

    while curr <= dt_fim:
        dt_str = curr.strftime("%Y-%m-%d")
        tot = coletar_dia(dt_str, conn)
        total_geral += tot
        curr += timedelta(days=1)

    log(f"Processamento finalizado. Total acumulado no período: {total_geral} contratos.")
    conn.close()

if __name__ == "__main__":
    main()
