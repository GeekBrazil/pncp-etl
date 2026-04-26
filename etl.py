#!/usr/bin/env python3
"""
ETL PNCP → PostgreSQL
Uso: python3 etl.py [--uf RJ] [--dias 2] [--todas-ufs]
Chamado pelo n8n via HTTP Request (modo server) ou Execute Command.
"""
import argparse, time, json, sys, re
from datetime import date, timedelta
from collections import Counter
import requests
import psycopg2
from psycopg2.extras import execute_values

# ─── Config ──────────────────────────────────────────────────────────────────
DB = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "pncp_db",
    "user":     "allan",
    "password": "Alcachofra",
}
PNCP_BASE   = "https://pncp.gov.br/pncp-consulta/v1"
PAGE_SIZE   = 50   # máx 50 por página
MAX_PAGES   = 5    # máx 5 páginas por modalidade por execução
TIMEOUT_SEC = 30   # servidor é lento

# Todas as modalidades — iterar para capturar tudo
MODALIDADES = {
    1: "Leilão Eletrônico", 2: "Diálogo Competitivo", 3: "Concurso",
    4: "Concorrência Eletrônica", 5: "Concorrência Presencial",
    6: "Pregão Eletrônico", 7: "Pregão Presencial",
    8: "Dispensa", 9: "Inexigibilidade", 10: "Manifestação de Interesse",
    11: "Pré-qualificação", 12: "Credenciamento", 13: "Leilão Presencial",
    14: "Inaplicabilidade", 15: "Chamada Pública",
    16: "Concorrência Eletrônica Internacional", 17: "Concorrência Presencial Internacional",
    18: "Pregão Eletrônico Internacional", 19: "Pregão Presencial Internacional",
}

STOPWORDS = {
    "de","da","do","das","dos","para","com","em","um","uma","os","as","por",
    "na","no","nas","nos","ao","aos","que","se","sua","seu","suas","seus",
    "esta","este","ou","e","a","o","é","são","foi","ser","ter","tem",
    "tipo","item","aquisição","contratação","fornecimento","prestação",
    "execução","manutenção","implantação","instalação","registro","processo",
    "objeto","licitação","pregão","dispensa","mediante","através","conforme",
}

TODAS_UFS = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO",
]

# ─── HTTP ─────────────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({"User-Agent": "pncp-etl/1.0", "Accept": "application/json"})

def fmt_data(d: str) -> str:
    """yyyy-MM-dd → yyyyMMdd (formato que a API aceita)"""
    return d.replace("-", "")

def fetch_page(uf: str, data_ini: str, data_fim: str, modalidade_id: int, pagina: int) -> dict | None:
    params = {
        "dataInicial":                fmt_data(data_ini),
        "dataFinal":                  fmt_data(data_fim),
        "codigoModalidadeContratacao": modalidade_id,
        "pagina":                     pagina,
        "tamanhoPagina":              PAGE_SIZE,
    }
    if uf:
        params["uf"] = uf

    for tentativa in range(3):
        try:
            r = session.get(
                f"{PNCP_BASE}/contratacoes/publicacao",
                params=params,
                timeout=TIMEOUT_SEC,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 204):
                return None
            print(f"  ⚠ HTTP {r.status_code} modalidade={modalidade_id} pag={pagina}", file=sys.stderr)
            return None
        except requests.RequestException as e:
            print(f"  ⚠ tentativa {tentativa+1} falhou (mod={modalidade_id}): {e}", file=sys.stderr)
            time.sleep(3 * tentativa + 1)
    return None

# ─── Normalização ─────────────────────────────────────────────────────────────
def normalizar(l: dict, uf: str) -> dict:
    def s(v, default=""):
        return str(v).strip() if v else default
    def f(v):
        try: return float(v)
        except: return 0.0

    orgao   = l.get("orgaoEntidade") or {}
    unidade = l.get("unidadeOrgao")  or {}

    data_pub = l.get("dataPublicacaoPncp") or l.get("dataInclusao")
    data_enc = l.get("dataEncerramentoProposta")

    return {
        "pncp_id":          s(l.get("numeroControlePNCP")),
        "municipio_ibge":   s(unidade.get("codigoIbge") or unidade.get("codigoMunicipio")) or None,
        "municipio_nome":   s(unidade.get("municipioNome")) or None,
        "uf":               s(unidade.get("ufSigla") or l.get("uf") or uf),
        "orgao_cnpj":       s(orgao.get("cnpj")),
        "orgao_nome":       s(orgao.get("razaoSocial")),
        "objeto":           s(l.get("objetoCompra")),
        "valor_estimado":   f(l.get("valorTotalEstimado")),
        "modalidade_id":    int(l.get("modalidadeId") or 0),
        "modalidade_nome":  s(l.get("modalidadeNome")),
        "data_publicacao":  data_pub[:10] if data_pub else None,
        "data_encerramento":data_enc[:10] if data_enc else None,
        "situacao":         s(l.get("situacaoCompraNome")),
        "url_pncp":         s(l.get("linkSistemaOrigem")),
    }

# ─── Top palavras ─────────────────────────────────────────────────────────────
def calcular_top_palavras(registros: list[dict], uf: str, mes_ref: str) -> list[dict]:
    freq   = Counter()
    valores= {}
    for r in registros:
        texto = re.sub(r"[^\w\s]", " ", (r["objeto"] or "").lower())
        palavras = [p for p in texto.split() if len(p) >= 4 and p not in STOPWORDS]
        for p in palavras:
            freq[p] += 1
            valores[p] = valores.get(p, 0) + (r["valor_estimado"] or 0)

    return [
        {
            "uf": uf,
            "municipio_ibge": None,
            "palavra": p,
            "frequencia": c,
            "valor_total": valores.get(p, 0),
            "mes_ref": mes_ref,
        }
        for p, c in freq.most_common(30)
    ]

# ─── Banco ────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB)

def inserir_licitacoes(conn, registros: list[dict]) -> tuple[int, int]:
    if not registros:
        return 0, 0

    cols = list(registros[0].keys())
    vals = [[r[c] for c in cols] for r in registros]

    sql = f"""
        INSERT INTO licitacoes ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (pncp_id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
        inseridos = cur.rowcount
    conn.commit()
    duplicados = len(registros) - inseridos
    return inseridos, duplicados

def inserir_top_palavras(conn, palavras: list[dict]) -> None:
    if not palavras:
        return
    sql = """
        INSERT INTO top_palavras (uf, municipio_ibge, palavra, frequencia, valor_total, mes_ref)
        VALUES %s
        ON CONFLICT (uf, municipio_ibge, palavra, mes_ref)
        DO UPDATE SET frequencia=EXCLUDED.frequencia, valor_total=EXCLUDED.valor_total
    """
    vals = [(p["uf"], p["municipio_ibge"], p["palavra"], p["frequencia"], p["valor_total"], p["mes_ref"]) for p in palavras]
    with conn.cursor() as cur:
        execute_values(cur, sql, vals)
    conn.commit()

def salvar_log(conn, uf, data_ini, data_fim, paginas, inseridos, duplicados, erros, duracao, status):
    sql = """INSERT INTO etl_log
             (uf, data_inicial, data_final, paginas_lidas, inseridos, duplicados, erros, duracao_seg, status)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    with conn.cursor() as cur:
        cur.execute(sql, (uf, data_ini, data_fim, paginas, inseridos, duplicados, erros, round(duracao,2), status))
    conn.commit()

# ─── ETL principal ────────────────────────────────────────────────────────────
def etl_uf(uf: str, data_ini: str, data_fim: str) -> dict:
    inicio = time.time()
    print(f"\n📥 ETL {uf or 'NACIONAL'}: {data_ini} → {data_fim}")

    conn = get_conn()
    todos_registros = []
    paginas_lidas   = 0
    erros           = 0
    total_inseridos = 0
    total_duplicados= 0

    for mod_id, mod_nome in MODALIDADES.items():
        print(f"  [{mod_id:02d}] {mod_nome[:30]:<30}", end=" ", flush=True)
        mod_inseridos = 0

        for pagina in range(1, MAX_PAGES + 1):
            data = fetch_page(uf, data_ini, data_fim, mod_id, pagina)

            if data is None:
                if pagina == 1:
                    print("— sem dados")
                break

            items     = data.get("data") or []
            total_api = int(data.get("totalRegistros") or 0)
            total_pag = int(data.get("totalPaginas")   or 1)

            if not items:
                if pagina == 1:
                    print("— vazio")
                break

            paginas_lidas += 1
            registros = []
            for l in items:
                try:
                    registros.append(normalizar(l, uf))
                except Exception:
                    erros += 1

            ins, dup = inserir_licitacoes(conn, registros)
            total_inseridos  += ins
            total_duplicados += dup
            mod_inseridos    += ins
            todos_registros  += registros

            if pagina >= total_pag or pagina >= MAX_PAGES:
                break

            time.sleep(1)  # respeitar rate limit

        if mod_inseridos > 0:
            print(f"→ {mod_inseridos} inseridos")

        time.sleep(0.5)

    # Top palavras do mês
    mes_ref = data_fim[:7]
    top = calcular_top_palavras(todos_registros, uf, mes_ref)
    inserir_top_palavras(conn, top)

    duracao = time.time() - inicio
    status  = "ok" if erros == 0 else ("parcial" if total_inseridos > 0 else "erro")
    salvar_log(conn, uf, data_ini, data_fim, paginas_lidas, total_inseridos, total_duplicados, erros, duracao, status)
    conn.close()

    resultado = {
        "uf":          uf or "BR",
        "inseridos":   total_inseridos,
        "duplicados":  total_duplicados,
        "erros":       erros,
        "paginas":     paginas_lidas,
        "duracao_seg": round(duracao, 1),
        "status":      status,
    }
    print(f"\n  ✅ Total: inseridos={total_inseridos} dup={total_duplicados} erros={erros} ({duracao:.1f}s)")
    return resultado

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ETL PNCP → PostgreSQL")
    parser.add_argument("--uf",        default="RJ",  help="UF a processar (ex: RJ)")
    parser.add_argument("--dias",      type=int, default=2, help="Dias para trás (default: 2)")
    parser.add_argument("--todas-ufs", action="store_true", help="Processar todas as UFs")
    parser.add_argument("--data-ini",  help="Data inicial YYYY-MM-DD (override --dias)")
    parser.add_argument("--data-fim",  help="Data final YYYY-MM-DD (default: hoje)")
    args = parser.parse_args()

    data_fim = args.data_fim or str(date.today())
    data_ini = args.data_ini or str(date.today() - timedelta(days=args.dias))

    ufs = TODAS_UFS if args.todas_ufs else [args.uf.upper()]

    resultados = []
    for uf in ufs:
        try:
            r = etl_uf(uf, data_ini, data_fim)
            resultados.append(r)
        except Exception as e:
            print(f"  ❌ ERRO em {uf}: {e}", file=sys.stderr)
            resultados.append({"uf": uf, "status": "erro", "erro": str(e)})
        if len(ufs) > 1:
            time.sleep(1)

    total_ins = sum(r.get("inseridos", 0) for r in resultados)
    total_err = sum(r.get("erros", 0)    for r in resultados)

    print(f"\n🏁 Concluído: {total_ins} inseridos, {total_err} erros, {len(ufs)} UFs")

    # Saída JSON para o n8n parsear
    print("\n__JSON_RESULT__")
    print(json.dumps({
        "total_inseridos": total_ins,
        "total_erros":     total_err,
        "ufs_processadas": len(ufs),
        "detalhes":        resultados,
    }, ensure_ascii=False))

if __name__ == "__main__":
    main()
