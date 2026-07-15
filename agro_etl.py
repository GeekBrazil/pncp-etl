#!/usr/bin/env python3
"""
Perfil rural por município — IBGE Censo Agropecuário 2017 (v3 agregados).
Nº de estabelecimentos agropecuários por município. Puxa por UF (o bulk
nacional estoura o servidor do IBGE nesta tabela). Estável, tabular.
Uso:
  python3 agro_etl.py --importar
"""
import argparse, os, sys, time
import requests
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
# 6778 = Nº de estabelecimentos agropecuários; v183; Censo Agro 2017
BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/6778/periodos/2017/variaveis/183"
UF_COD = ["11","12","13","14","15","16","17","21","22","23","24","25","26","27","28","29",
          "31","32","33","35","41","42","43","50","51","52","53"]
COD_UF = {"11":"RO","12":"AC","13":"AM","14":"RR","15":"PA","16":"AP","17":"TO","21":"MA",
          "22":"PI","23":"CE","24":"RN","25":"PB","26":"PE","27":"AL","28":"SE","29":"BA",
          "31":"MG","32":"ES","33":"RJ","35":"SP","41":"PR","42":"SC","43":"RS","50":"MS",
          "51":"MT","52":"GO","53":"DF"}
TIMEOUT = 90


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def puxar_uf(cod: str) -> list[tuple[str, str, str, int | None]]:
    """Retorna [(ibge, nome, uf, estabelecimentos)] para os municípios da UF."""
    url = f"{BASE}?localidades=N6[N3[{cod}]]"
    for tentativa in range(4):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            series = r.json()[0]["resultados"][0]["series"]
            break
        except (requests.RequestException, KeyError, IndexError, ValueError) as e:
            print(f"  ⚠ UF {cod} (tentativa {tentativa+1}/4): {e}", file=sys.stderr)
            time.sleep(4 * (tentativa + 1))
    else:
        return []

    uf = COD_UF.get(cod, "")
    out = []
    for s in series:
        ibge = s["localidade"]["id"]
        nome = s["localidade"]["nome"].rsplit(" - ", 1)[0]  # "Angra dos Reis - RJ" → nome
        val_raw = list(s["serie"].values())[0]
        try:
            val = int(val_raw)
        except (ValueError, TypeError):
            val = None  # "-", "X" (sigiloso), "..." → sem dado
        out.append((ibge, nome, uf, val))
    return out


UPSERT = """
INSERT INTO agro_municipios (municipio_ibge, municipio_nome, uf, estabelecimentos, atualizado_em)
VALUES (%s,%s,%s,%s,NOW())
ON CONFLICT (municipio_ibge) DO UPDATE SET
    municipio_nome=EXCLUDED.municipio_nome, uf=EXCLUDED.uf,
    estabelecimentos=EXCLUDED.estabelecimentos, atualizado_em=NOW()
"""


def importar():
    conn = get_conn()
    total = 0
    for cod in UF_COD:
        linhas = puxar_uf(cod)
        with conn.cursor() as cur:
            for ibge, nome, uf, est in linhas:
                cur.execute(UPSERT, (ibge, nome, uf, est))
        conn.commit()
        total += len(linhas)
        print(f"[AGRO] {COD_UF[cod]}: {len(linhas)} municípios")
        time.sleep(0.4)
    conn.close()
    print(f"🏁 Perfil rural concluído: {total} municípios.")


def main():
    p = argparse.ArgumentParser(description="Perfil rural — IBGE Censo Agro 2017")
    p.add_argument("--importar", action="store_true")
    args = p.parse_args()
    if not args.importar:
        p.error("Especifique --importar")
    importar()


if __name__ == "__main__":
    main()
