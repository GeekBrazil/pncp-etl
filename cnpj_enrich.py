#!/usr/bin/env python3
"""
Enriquecimento de CNPJ via BrasilAPI (consulta pública da Receita Federal).
Uso:
  python3 cnpj_enrich.py --cnpj 60701190000104
  python3 cnpj_enrich.py --pendentes [--limite 20]
"""
import argparse, os, re, sys, time
import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
BRASILAPI_BASE = "https://brasilapi.com.br/api/cnpj/v1"
TIMEOUT_SEC = 20


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def _so_digitos(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def buscar_cnpj(cnpj: str) -> dict | None:
    """Consulta a BrasilAPI com retry/backoff em 429 e erro de rede — mesmo padrão do etl.py."""
    cnpj = _so_digitos(cnpj)
    if len(cnpj) != 14:
        print(f"  ⚠ CNPJ inválido, pulando: {cnpj}", file=sys.stderr)
        return None

    delay = 1.5
    for tentativa in range(6):
        try:
            r = requests.get(f"{BRASILAPI_BASE}/{cnpj}", timeout=TIMEOUT_SEC)
        except requests.RequestException as e:
            print(f"  ⚠ erro de rede (tentativa {tentativa+1}/6, cnpj={cnpj}): {e}", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue

        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            print(f"  — CNPJ não encontrado: {cnpj}", file=sys.stderr)
            return None
        if r.status_code == 429:
            print(f"  ⚠ rate limit 429 — aguardando {delay:.1f}s (tentativa {tentativa+1}/6, cnpj={cnpj})", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 60)
            continue
        print(f"  ⚠ HTTP {r.status_code} cnpj={cnpj}", file=sys.stderr)
        return None

    print(f"  ✗ desistindo após 6 tentativas (cnpj={cnpj})", file=sys.stderr)
    return None


def salvar_empresa(conn, dados: dict) -> None:
    cnpj = dados.get("cnpj")
    if not cnpj:
        return

    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO empresas (
                   cnpj, razao_social, nome_fantasia, situacao_cadastral, data_situacao,
                   cnae_principal, cnae_descricao, uf, municipio, capital_social,
                   data_abertura, raw_json, atualizado_em
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
               ON CONFLICT (cnpj) DO UPDATE SET
                   razao_social = EXCLUDED.razao_social,
                   situacao_cadastral = EXCLUDED.situacao_cadastral,
                   data_situacao = EXCLUDED.data_situacao,
                   capital_social = EXCLUDED.capital_social,
                   raw_json = EXCLUDED.raw_json,
                   atualizado_em = NOW()""",
            (
                cnpj,
                dados.get("razao_social"),
                dados.get("nome_fantasia"),
                dados.get("descricao_situacao_cadastral"),
                dados.get("data_situacao_cadastral"),
                dados.get("cnae_fiscal"),
                dados.get("cnae_fiscal_descricao"),
                dados.get("uf"),
                dados.get("municipio"),
                dados.get("capital_social"),
                dados.get("data_inicio_atividade"),
                psycopg2.extras.Json(dados),
            ),
        )

        for socio in dados.get("qsa") or []:
            cur.execute(
                """INSERT INTO socios (cnpj, nome_socio, qualificacao, data_entrada_sociedade, faixa_etaria)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (cnpj, nome_socio, qualificacao) DO NOTHING""",
                (
                    cnpj,
                    socio.get("nome_socio"),
                    socio.get("qualificacao_socio"),
                    socio.get("data_entrada_sociedade"),
                    socio.get("faixa_etaria"),
                ),
            )
    conn.commit()


def enriquecer_pendentes(conn, limite: int | None = None) -> dict:
    with conn.cursor() as cur:
        sql = """SELECT DISTINCT orgao_cnpj FROM licitacoes
                 WHERE orgao_cnpj IS NOT NULL AND orgao_cnpj != ''
                   AND orgao_cnpj NOT IN (SELECT cnpj FROM empresas)"""
        if limite:
            sql += f" LIMIT {int(limite)}"
        cur.execute(sql)
        pendentes = [row[0] for row in cur.fetchall()]

    print(f"[CNPJ] {len(pendentes)} CNPJ(s) pendente(s) de enriquecimento.")
    ok = erros = 0
    for i, cnpj in enumerate(pendentes, 1):
        print(f"  [{i}/{len(pendentes)}] {cnpj}", end=" ", flush=True)
        dados = buscar_cnpj(cnpj)
        if dados:
            try:
                salvar_empresa(conn, dados)
                ok += 1
                print(f"→ {dados.get('razao_social', '?')}")
            except Exception as e:
                erros += 1
                conn.rollback()
                print(f"→ erro ao salvar: {e}")
        else:
            erros += 1
        time.sleep(0.4)  # delay educado — BrasilAPI é comunitária, sem limite documentado oficialmente

    print(f"\n🏁 Enriquecimento concluído: {ok} ok, {erros} erros de {len(pendentes)} pendentes.")
    return {"ok": ok, "erros": erros, "total": len(pendentes)}


def main():
    parser = argparse.ArgumentParser(description="Enriquecimento de CNPJ via BrasilAPI")
    parser.add_argument("--cnpj", help="Consulta avulsa de um CNPJ")
    parser.add_argument("--pendentes", action="store_true", help="Enriquece todos os CNPJs de licitacoes ainda não cacheados")
    parser.add_argument("--limite", type=int, help="Limite de CNPJs a processar em --pendentes")
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.cnpj:
            dados = buscar_cnpj(args.cnpj)
            if dados:
                salvar_empresa(conn, dados)
                print(f"✅ {dados.get('razao_social')} — {len(dados.get('qsa') or [])} sócio(s) salvos.")
            else:
                print("Não foi possível obter dados para esse CNPJ.")
        elif args.pendentes:
            enriquecer_pendentes(conn, args.limite)
        else:
            parser.error("Especifique --cnpj ou --pendentes")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
