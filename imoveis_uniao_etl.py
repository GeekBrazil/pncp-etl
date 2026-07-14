#!/usr/bin/env python3
"""
Imóveis da União — agrega os inventários de "Bens Imóveis" que cada órgão
federal publica no catálogo dados.gov.br (não existe lista nacional única).
Uso:
  python3 imoveis_uniao_etl.py --importar
"""
import argparse, csv, io, os, re, sys, time
import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
DADOS_GOV_API_KEY = os.environ.get("DADOS_GOV_API_KEY", "")
CATALOGO_BASE = "https://dados.gov.br/dados/api/publico/conjuntos-dados"
TIMEOUT_SEC = 30

# Browser UA — sem isso alguns domínios (ex: dados.ufrn.br) retornam 403.
BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Nomes de coluna variam por órgão — mapeamento flexível (case-insensitive).
ALIASES = {
    "rip": ["rip"],
    "nome_imovel": ["nome_imovel", "nome", "denominacao", "denominação"],
    "tipo_imovel": ["tipo_imovel", "tipo"],
    "municipio": ["municipio", "município", "municipio_nome", "cidade"],
    "uf": ["uf", "estado"],
    "endereco": ["endereco", "endereço", "logradouro"],
    "area_terreno": ["area_terreno", "área_terreno", "area_do_terreno"],
    "valor_terreno": ["valor_terreno"],
    "area_construida": ["qtd_area_construida", "area_construida", "área_construída", "qtd_area_construida_util"],
    "valor_imovel": ["valor_imovel", "valor_total", "valor"],
    "forma_aquisicao": ["forma_aquisicao", "forma_de_aquisicao"],
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def _headers():
    h = dict(BROWSER_HEADERS)
    if DADOS_GOV_API_KEY:
        h["chave-api-dados-abertos"] = DADOS_GOV_API_KEY
    return h


def listar_datasets(termos=("bens imoveis", "imoveis")) -> list[dict]:
    vistos = {}
    for termo in termos:
        for pagina in range(1, 20):
            r = requests.get(
                CATALOGO_BASE,
                params={"nomeConjuntoDados": termo, "pagina": pagina},
                headers=_headers(),
                timeout=TIMEOUT_SEC,
            )
            if r.status_code != 200:
                print(f"  ⚠ HTTP {r.status_code} buscando '{termo}' pág {pagina}", file=sys.stderr)
                break
            itens = r.json()
            if not itens:
                break
            for item in itens:
                vistos[item["id"]] = item
            if len(itens) < 10:
                break
            time.sleep(0.3)
    return list(vistos.values())


def detalhe_dataset(dataset_id: str) -> dict | None:
    r = requests.get(f"{CATALOGO_BASE}/{dataset_id}", headers=_headers(), timeout=TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    return r.json()


def _parse_numero_br(valor: str):
    if not valor:
        return None
    v = valor.strip().replace("R$", "").strip()
    if not v or v == "-":
        return None
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return None


def _achar_coluna(row_lower_keys: dict, campo: str):
    for alias in ALIASES.get(campo, []):
        if alias in row_lower_keys:
            return row_lower_keys[alias]
    return None


def normalizar_linha(row: dict, dataset_id: str, organizacao: str) -> dict:
    # csv.DictReader coloca colunas extras (sem cabeçalho correspondente) sob a chave None.
    lower_keys = {k.strip().lower(): k for k in row.keys() if k is not None}

    def val(campo):
        col = _achar_coluna(lower_keys, campo)
        return row.get(col, "").strip() if col else None

    return {
        "fonte_dataset_id": dataset_id,
        "fonte_organizacao": organizacao,
        "rip": val("rip"),
        "nome_imovel": val("nome_imovel"),
        "tipo_imovel": val("tipo_imovel"),
        "municipio": val("municipio"),
        "uf": val("uf"),
        "endereco": val("endereco"),
        "area_terreno": _parse_numero_br(val("area_terreno")),
        "valor_terreno": _parse_numero_br(val("valor_terreno")),
        "area_construida": _parse_numero_br(val("area_construida")),
        "valor_imovel": _parse_numero_br(val("valor_imovel")),
        "forma_aquisicao": val("forma_aquisicao"),
        "raw_row": psycopg2.extras.Json(row),
    }


def baixar_csv(url: str) -> list[dict] | None:
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=TIMEOUT_SEC)
    except requests.RequestException as e:
        print(f"  ⚠ erro de rede baixando {url}: {e}", file=sys.stderr)
        return None
    if r.status_code != 200:
        print(f"  ⚠ HTTP {r.status_code} baixando {url}", file=sys.stderr)
        return None

    texto = r.content.decode("utf-8", errors="replace")
    amostra = texto[:200]
    delimitador = ";" if amostra.count(";") > amostra.count(",") else ","
    leitor = csv.DictReader(io.StringIO(texto), delimiter=delimitador)
    return list(leitor)


UPSERT_SQL = """
INSERT INTO imoveis_uniao (
    fonte_dataset_id, fonte_organizacao, rip, nome_imovel, tipo_imovel, municipio, uf,
    endereco, area_terreno, valor_terreno, area_construida, valor_imovel, forma_aquisicao, raw_row
) VALUES (
    %(fonte_dataset_id)s, %(fonte_organizacao)s, %(rip)s, %(nome_imovel)s, %(tipo_imovel)s, %(municipio)s, %(uf)s,
    %(endereco)s, %(area_terreno)s, %(valor_terreno)s, %(area_construida)s, %(valor_imovel)s, %(forma_aquisicao)s, %(raw_row)s
)
ON CONFLICT (fonte_dataset_id, rip, nome_imovel) DO NOTHING
"""


def importar_tudo():
    conn = get_conn()
    datasets = listar_datasets()
    print(f"[IMÓVEIS DA UNIÃO] {len(datasets)} dataset(s) encontrados.")

    total_linhas = ok_datasets = erro_datasets = 0
    for i, ds in enumerate(datasets, 1):
        try:
            nome = ds.get("title", ds.get("nome", ds["id"]))
            org = ds.get("nomeOrganizacao", "?")
            print(f"[{i}/{len(datasets)}] {nome} ({org})", end=" ", flush=True)

            detalhe = detalhe_dataset(ds["id"])
            if not detalhe:
                print("→ erro ao buscar detalhe")
                erro_datasets += 1
                continue

            csvs = [r for r in detalhe.get("recursos", []) if (r.get("formato") or "").upper() == "CSV"]
            if not csvs:
                print("→ sem recurso CSV")
                erro_datasets += 1
                continue

            linhas_dataset = 0
            with conn.cursor() as cur:
                # idempotência: apaga as linhas deste dataset antes de reinserir
                # (o UNIQUE não protege quando rip/nome_imovel são NULL — NULL≠NULL)
                cur.execute("DELETE FROM imoveis_uniao WHERE fonte_dataset_id = %s", (ds["id"],))
                for recurso in csvs:
                    rows = baixar_csv(recurso["link"])
                    if not rows:
                        continue
                    for row in rows:
                        try:
                            dados = normalizar_linha(row, ds["id"], org)
                            cur.execute(UPSERT_SQL, dados)
                            linhas_dataset += 1
                        except Exception as e:
                            conn.rollback()
                            print(f"\n  ⚠ erro ao processar linha: {e}", file=sys.stderr)
            conn.commit()
            total_linhas += linhas_dataset
            ok_datasets += 1
            print(f"→ {linhas_dataset} imóvel(is)")
            time.sleep(0.3)
        except Exception as e:
            conn.rollback()
            erro_datasets += 1
            print(f"→ erro inesperado no dataset: {e}", file=sys.stderr)

    conn.close()
    print(f"\n🏁 Concluído: {ok_datasets} dataset(s) importados, {erro_datasets} com erro, {total_linhas} imóveis no total.")


def main():
    parser = argparse.ArgumentParser(description="Imóveis da União — dados.gov.br")
    parser.add_argument("--importar", action="store_true")
    args = parser.parse_args()
    if not args.importar:
        parser.error("Especifique --importar")
    importar_tudo()


if __name__ == "__main__":
    main()
