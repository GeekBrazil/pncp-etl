#!/usr/bin/env python3
"""
Score Territorial — cruza população/UF (IBGE) com saúde fiscal (SICONFI/RREO)
por município, gerando receita per capita como indicador de capacidade de
investimento local. Ideia do vault: "Radar de Valorização" (IBGE + SICONFI).

Uso:
  python3 score_municipios_etl.py --importar [--exercicio 2024] [--periodo 6]
"""
import argparse, os, sys, time
import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
SICONFI_RREO = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
TIMEOUT_SEC = 30


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def listar_municipios() -> list[dict]:
    r = requests.get(IBGE_MUNICIPIOS, params={"orderBy": "nome"}, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    out = []
    for m in r.json():
        uf = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        out.append({"ibge": str(m["id"]), "nome": m["nome"], "uf": uf})
    return out


def buscar_rreo(cod_ibge: str, exercicio: int, periodo: int) -> dict | None:
    delay = 1.5
    for tentativa in range(5):
        try:
            r = requests.get(
                SICONFI_RREO,
                params={
                    "an_exercicio": exercicio,
                    "nr_periodo": periodo,
                    "co_tipo_demonstrativo": "RREO",
                    "id_ente": cod_ibge,
                },
                timeout=TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            print(f"  ⚠ erro de rede ibge={cod_ibge} (tentativa {tentativa+1}/5): {e}", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue

        if r.status_code == 429:
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue
        if r.status_code != 200:
            return None

        items = r.json().get("items", [])
        if not items:
            return None

        receita = None
        populacao = None
        for it in items:
            if it.get("cod_conta") == "TotalReceitas" and it.get("coluna", "").startswith("Até o Bimestre"):
                receita = it.get("valor")
                populacao = it.get("populacao")
                break
        if receita is None:
            return None
        return {"receita_realizada": receita, "populacao": populacao, "raw": items[0]}

    print(f"  ✗ desistindo ibge={cod_ibge} após 5 tentativas", file=sys.stderr)
    return None


UPSERT_SQL = """
INSERT INTO score_municipios (
    municipio_ibge, municipio_nome, uf, exercicio, periodo,
    populacao, receita_realizada, receita_per_capita, raw_json
) VALUES (
    %(municipio_ibge)s, %(municipio_nome)s, %(uf)s, %(exercicio)s, %(periodo)s,
    %(populacao)s, %(receita_realizada)s, %(receita_per_capita)s, %(raw_json)s
)
ON CONFLICT (municipio_ibge, exercicio, periodo) DO UPDATE SET
    populacao = EXCLUDED.populacao,
    receita_realizada = EXCLUDED.receita_realizada,
    receita_per_capita = EXCLUDED.receita_per_capita,
    raw_json = EXCLUDED.raw_json,
    atualizado_em = NOW()
"""


def get_progress(conn) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT ultimo_ibge FROM score_progress WHERE id = 1")
        row = cur.fetchone()
        return row[0] if row else None


def salvar_progress(conn, ibge: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO score_progress (id, ultimo_ibge, atualizado_em) VALUES (1, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET ultimo_ibge = EXCLUDED.ultimo_ibge, atualizado_em = NOW()
        """, (ibge,))
    conn.commit()


def importar_tudo(exercicio: int, periodo: int):
    conn = get_conn()
    municipios = listar_municipios()
    municipios.sort(key=lambda m: m["ibge"])
    print(f"[SCORE TERRITORIAL] {len(municipios)} municípios (IBGE).")

    ultimo = get_progress(conn)
    if ultimo:
        idx = next((i for i, m in enumerate(municipios) if m["ibge"] == ultimo), -1)
        municipios = municipios[idx + 1:]
        print(f"  Retomando após {ultimo} — {len(municipios)} restantes.")

    ok = erro = 0
    for i, m in enumerate(municipios, 1):
        try:
            dados = buscar_rreo(m["ibge"], exercicio, periodo)
            if not dados:
                erro += 1
                salvar_progress(conn, m["ibge"])
                continue

            populacao = dados["populacao"] or 0
            receita = dados["receita_realizada"] or 0
            per_capita = (receita / populacao) if populacao else None

            with conn.cursor() as cur:
                cur.execute(UPSERT_SQL, {
                    "municipio_ibge": m["ibge"],
                    "municipio_nome": m["nome"],
                    "uf": m["uf"],
                    "exercicio": exercicio,
                    "periodo": periodo,
                    "populacao": populacao or None,
                    "receita_realizada": receita,
                    "receita_per_capita": per_capita,
                    "raw_json": psycopg2.extras.Json(dados["raw"]),
                })
            conn.commit()
            salvar_progress(conn, m["ibge"])
            ok += 1
            if i % 100 == 0:
                print(f"  [{i}/{len(municipios)}] ok={ok} erro={erro} — {m['nome']}/{m['uf']}")
            time.sleep(0.2)
        except Exception as e:
            conn.rollback()
            erro += 1
            print(f"  ⚠ erro inesperado em {m['nome']}/{m['uf']}: {e}", file=sys.stderr)

    conn.close()
    print(f"\n🏁 Concluído: {ok} município(s) importados, {erro} com erro/sem dado.")


def main():
    parser = argparse.ArgumentParser(description="Score Territorial — IBGE + SICONFI")
    parser.add_argument("--importar", action="store_true")
    parser.add_argument("--exercicio", type=int, default=2024)
    parser.add_argument("--periodo", type=int, default=6)
    args = parser.parse_args()
    if not args.importar:
        parser.error("Especifique --importar")
    importar_tudo(args.exercicio, args.periodo)


if __name__ == "__main__":
    main()
