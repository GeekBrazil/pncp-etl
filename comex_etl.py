#!/usr/bin/env python3
"""
Comex Stat (MDIC/Siscomex) — comércio exterior por município, oficial e grátis.
Dá origem (município BR) → fluxo de exportação/importação. Endpoint /cities.
Uso:
  python3 comex_etl.py --importar [--ano 2025]
"""
import argparse, os, sys, time
import requests
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
BASE = "https://api-comexstat.mdic.gov.br/cities"
TIMEOUT = 120


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def buscar(fluxo: str, ano: int) -> list[dict]:
    body = {
        "flow": fluxo, "monthDetail": False,
        "period": {"from": f"{ano}-01", "to": f"{ano}-12"},
        "filters": [], "details": ["state", "city"], "metrics": ["metricFOB"],
    }
    for tentativa in range(4):
        try:
            r = requests.post(BASE, json=body, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()["data"]["list"]
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"  ⚠ Comex {fluxo} {ano} (tentativa {tentativa+1}/4): {e}", file=sys.stderr)
            time.sleep(5 * (tentativa + 1))
    raise RuntimeError(f"Comex indisponível ({fluxo} {ano})")


UPSERT = """
INSERT INTO comex_municipios (municipio_nome, uf, fluxo, ano, fob_usd, atualizado_em)
VALUES (%s,%s,%s,%s,%s,NOW())
ON CONFLICT (municipio_nome, uf, fluxo, ano)
DO UPDATE SET fob_usd = EXCLUDED.fob_usd, atualizado_em = NOW()
"""

# "Rio de Janeiro - RJ" → ("Rio de Janeiro", "RJ")
def split_muni(s: str) -> tuple[str, str]:
    nome, _, uf = (s or "").rpartition(" - ")
    return nome.strip(), uf.strip()[:2]


def importar(ano: int):
    conn = get_conn()
    total = 0
    for fluxo in ("export", "import"):
        linhas = buscar(fluxo, ano)
        print(f"[COMEX] {fluxo} {ano}: {len(linhas)} municípios")
        with conn.cursor() as cur:
            for it in linhas:
                nome, uf = split_muni(it.get("noMunMinsgUf", ""))
                if not nome or len(uf) != 2:
                    continue
                try:
                    fob = float(it.get("metricFOB") or 0)
                except (ValueError, TypeError):
                    continue
                cur.execute(UPSERT, (nome, uf, fluxo, ano, fob))
                total += 1
        conn.commit()
    conn.close()
    print(f"🏁 Comex concluído: {total} registros município×fluxo em {ano}.")


def main():
    p = argparse.ArgumentParser(description="Comex Stat por município")
    p.add_argument("--importar", action="store_true")
    p.add_argument("--ano", type=int, default=2025)
    args = p.parse_args()
    if not args.importar:
        p.error("Especifique --importar")
    importar(args.ano)


if __name__ == "__main__":
    main()
