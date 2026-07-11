#!/usr/bin/env python3
"""
Radar de Loteamentos — ranqueia municípios pra prospecção de loteamento cruzando:
  • crescimento populacional (IBGE/SIDRA t6579, estimativas anuais)
  • receita per capita (score_municipios, já coletado via SICONFI)
  • investimento público em infraestrutura (licitacoes, filtro por objeto)

Score 0-100 por percentil: 50% crescimento + 25% infra per capita + 25% receita pc.
Uso:
  python3 radar_loteamento_etl.py --importar [--ano-ini 2021] [--ano-fim 2024]
"""
import argparse, os, sys, time
import requests
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
SIDRA = "https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/9324/p/{ano}"
TIMEOUT_SEC = 120

# Palavras de objeto que indicam obra de infraestrutura urbana (sinal de expansão)
INFRA_REGEX = r"pavimenta|saneamento|drenagem|esgoto|abastecimento de água|iluminação pública|infraestrutura urbana|calçamento|asfalt"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def populacao_por_ano(ano: int) -> dict[str, tuple[str, str, int]]:
    """{ibge: (nome, uf, populacao)} — 1 chamada bulk pro SIDRA."""
    for tentativa in range(4):
        try:
            r = requests.get(SIDRA.format(ano=ano), timeout=TIMEOUT_SEC)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            print(f"  ⚠ SIDRA {ano} (tentativa {tentativa+1}/4): {e}", file=sys.stderr)
            time.sleep(5 * (tentativa + 1))
    else:
        raise RuntimeError(f"SIDRA indisponível pro ano {ano}")

    out = {}
    for row in r.json()[1:]:  # linha 0 é cabeçalho
        ibge = row["D1C"]
        nome_uf = row["D1N"]  # "Angra dos Reis - RJ"
        nome, _, uf = nome_uf.rpartition(" - ")
        try:
            pop = int(row["V"])
        except (ValueError, TypeError):
            continue
        out[ibge] = (nome, uf, pop)
    return out


def importar(ano_ini: int, ano_fim: int):
    print(f"[RADAR LOTEAMENTO] população {ano_ini} e {ano_fim} via SIDRA…")
    pop_ini = populacao_por_ano(ano_ini)
    pop_fim = populacao_por_ano(ano_fim)
    print(f"  {len(pop_ini)} municípios em {ano_ini}, {len(pop_fim)} em {ano_fim}")

    conn = get_conn()
    with conn.cursor() as cur:
        n = 0
        for ibge, (nome, uf, p_fim) in pop_fim.items():
            ini = pop_ini.get(ibge)
            if not ini:
                continue
            p_ini = ini[2]
            cresc = ((p_fim - p_ini) / p_ini * 100) if p_ini else None
            cur.execute(
                """INSERT INTO radar_loteamento
                       (municipio_ibge, municipio_nome, uf, pop_inicial, pop_final,
                        ano_inicial, ano_final, crescimento_pct, atualizado_em)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                   ON CONFLICT (municipio_ibge) DO UPDATE SET
                       municipio_nome=EXCLUDED.municipio_nome, uf=EXCLUDED.uf,
                       pop_inicial=EXCLUDED.pop_inicial, pop_final=EXCLUDED.pop_final,
                       ano_inicial=EXCLUDED.ano_inicial, ano_final=EXCLUDED.ano_final,
                       crescimento_pct=EXCLUDED.crescimento_pct, atualizado_em=NOW()""",
                (ibge, nome, uf, p_ini, p_fim, ano_ini, ano_fim, cresc),
            )
            n += 1
        print(f"  {n} municípios upsertados.")

        print("  agregando receita per capita (score_municipios)…")
        cur.execute("""
            UPDATE radar_loteamento r SET receita_per_capita = s.receita_per_capita
            FROM score_municipios s WHERE s.municipio_ibge = r.municipio_ibge
        """)

        print("  agregando investimento em infraestrutura (licitacoes)…")
        cur.execute(f"""
            UPDATE radar_loteamento r SET infra_valor_12m = i.v
            FROM (SELECT municipio_ibge, SUM(valor_estimado) AS v
                  FROM licitacoes
                  WHERE valor_estimado <= 500000000 AND objeto ~* %s
                  GROUP BY municipio_ibge) i
            WHERE i.municipio_ibge = r.municipio_ibge
        """, (INFRA_REGEX,))

        print("  calculando score (percentis)…")
        cur.execute("""
            UPDATE radar_loteamento r SET score = t.score
            FROM (SELECT municipio_ibge,
                         ROUND((100 * (
                             0.50 * PERCENT_RANK() OVER (ORDER BY crescimento_pct) +
                             0.25 * PERCENT_RANK() OVER (ORDER BY COALESCE(infra_valor_12m,0)/NULLIF(pop_final,0)) +
                             0.25 * PERCENT_RANK() OVER (ORDER BY COALESCE(receita_per_capita,0))
                         ))::numeric, 1) AS score
                  FROM radar_loteamento
                  WHERE crescimento_pct IS NOT NULL) t
            WHERE t.municipio_ibge = r.municipio_ibge
        """)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(score) FROM radar_loteamento")
        total, com_score = cur.fetchone()
    conn.close()
    print(f"🏁 Radar concluído: {total} municípios, {com_score} com score.")


def main():
    parser = argparse.ArgumentParser(description="Radar de Loteamentos — IBGE + SICONFI + licitações")
    parser.add_argument("--importar", action="store_true")
    parser.add_argument("--ano-ini", type=int, default=2021)
    parser.add_argument("--ano-fim", type=int, default=2024)
    args = parser.parse_args()
    if not args.importar:
        parser.error("Especifique --importar")
    importar(args.ano_ini, args.ano_fim)


if __name__ == "__main__":
    main()
