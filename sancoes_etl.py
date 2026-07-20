#!/usr/bin/env python3
"""
sancoes_etl — carrega CEIS, CNEP e CEPIM (Portal da Transparência) no Postgres.

Empresas/pessoas impedidas de licitar ou contratar com a Administração.
É a base do diferencial investigativo do SaaS: cruzar fornecedor × sanção e
avisar risco na consulta de CNPJ do site.

- Fonte: https://api.portaldatransparencia.gov.br/api-de-dados/{ceis,cnep,cepim}
- Auth: header `chave-api-dados` (env TRANSPARENCIA_API_KEY).
- Limite: 90 req/min (06h–23h59). Usamos ~1.4 req/s com folga.
- Resumível: sancoes_progress guarda a última página por cadastro; um redeploy
  do Coolify não recomeça do zero (re-rodar retoma de onde parou).

Uso:
  python sancoes_etl.py                # todos os cadastros, resumindo
  python sancoes_etl.py --cadastro ceis
  python sancoes_etl.py --reset        # zera o progresso e recarrega
"""
import argparse, os, time, sys, re
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
API_BASE     = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY      = os.environ.get("TRANSPARENCIA_API_KEY", "")
CADASTROS    = ["ceis", "cnep", "cepim"]
PAUSA        = float(os.environ.get("SANCOES_PAUSA", "0.7"))   # s entre chamadas (~85/min)

def log(m): print(f"[sancoes] {m}", flush=True)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def _digits(s):
    return re.sub(r"\D", "", s or "") or None

def _date(s):
    if not s or s == "Sem informação":
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except ValueError:
        return None

def _first(lst, *keys):
    """pega o primeiro valor não-vazio de uma lista de dicts por chave."""
    if not isinstance(lst, list):
        return None
    partes = []
    for item in lst:
        for k in keys:
            v = (item or {}).get(k)
            if v:
                partes.append(str(v))
                break
    return " | ".join(partes)[:2000] or None

def parse_registro(cadastro, r):
    # CEIS/CNEP usam "pessoa"; CEPIM usa "pessoaJuridica"
    pessoa = r.get("pessoa") or r.get("pessoaJuridica") or {}
    sanc   = r.get("sancionado") or {}
    cnpj_fmt = pessoa.get("cnpjFormatado") or sanc.get("codigoFormatado") or ""
    cnpj = _digits(cnpj_fmt)
    cnpj = cnpj if cnpj and len(cnpj) == 14 else None
    # CEIS/CNEP têm tipoSancao; CEPIM é impedimento por convênio irregular
    tipo_sancao = ((r.get("tipoSancao") or {}).get("descricaoPortal")
                   or (r.get("tipoSancao") or {}).get("descricaoResumida")
                   or (r.get("motivo") and f"Impedido (CEPIM): {r['motivo']}"[:200]))
    fonte = ((r.get("fonteSancao") or {}).get("nomeExibicao")
             or (r.get("orgaoSuperior") or {}).get("nome"))
    orgao = ((r.get("orgaoSancionador") or {}).get("nome")
             or (r.get("orgaoSuperior") or {}).get("nome"))
    return (
        int(r["id"]),
        cadastro.upper(),
        cnpj,
        pessoa.get("cpfFormatado") or None,
        pessoa.get("nome") or sanc.get("nome"),
        pessoa.get("tipo"),
        tipo_sancao,
        orgao,
        fonte,
        _date(r.get("dataInicioSancao")),
        _date(r.get("dataFimSancao")),
        _date(r.get("dataPublicacaoSancao")),
        _first(r.get("fundamentacao"), "descricao", "codigo"),
        r.get("linkPublicacao") or None,
        r.get("numeroProcesso") or None,
    )

UPSERT = """
INSERT INTO sancoes (id, cadastro, cnpj, cpf, nome, tipo_pessoa, tipo_sancao,
                     orgao_sancionador, fonte, data_inicio, data_fim,
                     data_publicacao, fundamentacao, link_publicacao, processo)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
  data_fim = EXCLUDED.data_fim,
  tipo_sancao = EXCLUDED.tipo_sancao,
  importado_em = NOW()
"""

def fetch_pagina(cadastro, pagina):
    r = requests.get(f"{API_BASE}/{cadastro}",
                     params={"pagina": pagina},
                     headers={"chave-api-dados": API_KEY},
                     timeout=40)
    if r.status_code == 429:
        log("429 (limite) — pausando 20s")
        time.sleep(20)
        return fetch_pagina(cadastro, pagina)
    r.raise_for_status()
    return r.json()

def carrega_cadastro(conn, cadastro):
    cur = conn.cursor()
    cur.execute("SELECT ultima_pagina, concluido, total_gravado FROM sancoes_progress WHERE cadastro=%s",
                (cadastro.upper(),))
    row = cur.fetchone()
    pagina = (row[0] if row else 0) + 1
    total  = row[2] if row else 0
    if row and row[1]:
        log(f"{cadastro.upper()} já concluído ({total} registros) — pulando")
        return
    if not row:
        cur.execute("INSERT INTO sancoes_progress (cadastro) VALUES (%s) ON CONFLICT DO NOTHING",
                    (cadastro.upper(),))
        conn.commit()

    log(f"{cadastro.upper()}: retomando da página {pagina}")
    while True:
        try:
            registros = fetch_pagina(cadastro, pagina)
        except Exception as e:
            log(f"erro página {pagina}: {e} — parando (retomável)")
            return
        if not registros:
            cur.execute("UPDATE sancoes_progress SET concluido=TRUE, atualizado_em=NOW() WHERE cadastro=%s",
                        (cadastro.upper(),))
            conn.commit()
            log(f"{cadastro.upper()}: concluído com {total} registros")
            return
        linhas = [parse_registro(cadastro, r) for r in registros]
        execute_values(cur, UPSERT, linhas, page_size=500)
        total += len(linhas)
        cur.execute("UPDATE sancoes_progress SET ultima_pagina=%s, total_gravado=%s, atualizado_em=NOW() WHERE cadastro=%s",
                    (pagina, total, cadastro.upper()))
        conn.commit()
        if pagina % 25 == 0:
            log(f"  {cadastro.upper()} pág {pagina} · {total} registros")
        pagina += 1
        time.sleep(PAUSA)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cadastro", choices=CADASTROS, help="só um cadastro")
    ap.add_argument("--reset", action="store_true", help="zera progresso e recarrega tudo")
    args = ap.parse_args()

    if not API_KEY:
        sys.exit("Defina TRANSPARENCIA_API_KEY (chave-api-dados do Portal da Transparência).")

    conn = get_conn()
    if args.reset:
        conn.cursor().execute("TRUNCATE sancoes; DELETE FROM sancoes_progress;")
        conn.commit()
        log("progresso e dados zerados")

    alvo = [args.cadastro] if args.cadastro else CADASTROS
    for c in alvo:
        carrega_cadastro(conn, c)
    conn.close()
    log("fim")

if __name__ == "__main__":
    main()
