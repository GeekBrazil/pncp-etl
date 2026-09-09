#!/usr/bin/env python3
"""
ETL nacional generalizado de CNPJ (Receita Federal, via espelho público) —
irmão mais amplo do cnpj_imob_finder.py (que fica intocado, filtrado só pra
corretagem imobiliária). Este aqui carrega TODO estabelecimento ativo, com
endereço estruturado (pra geocodificar depois) e os sócios de cada um.

Streaming igual ao cnpj_imob_finder.py: lê o CSV de dentro do zip sem
extrair, grava, apaga o zip antes do próximo — nunca segura mais que ~1
arquivo em disco por vez.

Uso:
    python3 cnpj_nacional.py --estabelecimentos          # Brasil inteiro, só ativas
    python3 cnpj_nacional.py --estabelecimentos --uf RJ,SP
    python3 cnpj_nacional.py --socios                    # carrega sócios de quem já está em `empresas`
    python3 cnpj_nacional.py --tudo                      # estabelecimentos + sócios em sequência
"""
import argparse
import csv
import io
import os
import re
import sys
import time
import zipfile
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
MIRROR_BASE = "https://dados-abertos-rf-cnpj.casadosdados.com.br"
TMP_DIR = os.environ.get("CNPJ_TMP_DIR", "/tmp/cnpj_nacional")
SITUACAO_ATIVA = "02"

_UFS_RAW = os.environ.get("UFS_ALVO", "").strip().upper()
UFS_ALVO = None if not _UFS_RAW or _UFS_RAW in ("ALL", "*", "BR") else {u.strip() for u in _UFS_RAW.split(",") if u.strip()}

MUNICIPIOS = {}
_MINUSCULAS = {"da", "de", "do", "das", "dos", "e"}


def _titulo_pt(nome):
    palavras = nome.strip().lower().split()
    return " ".join(p if p in _MINUSCULAS and i > 0 else p.capitalize() for i, p in enumerate(palavras))


def _baixar(url, destino, tentativas=4):
    delay = 3
    for t in range(tentativas):
        try:
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(destino, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            return
        except requests.RequestException as e:
            print(f"  ⚠ erro baixando {url} (tentativa {t+1}/{tentativas}): {e}", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 60)
    raise RuntimeError(f"não consegui baixar {url}")


def _pasta_mais_recente():
    r = requests.get(f"{MIRROR_BASE}/arquivos/", timeout=30)
    r.raise_for_status()
    pastas = sorted(set(re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', r.text)))
    if not pastas:
        raise RuntimeError("não achei nenhuma pasta de dados no espelho")
    return pastas[-1]


def _parse_data(s):
    s = (s or "").strip()
    if len(s) != 8 or s == "00000000":
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def _carregar_municipios(pasta):
    if MUNICIPIOS:
        return
    destino = os.path.join(TMP_DIR, "Municipios.zip")
    _baixar(f"{MIRROR_BASE}/arquivos/{pasta}/Municipios.zip", destino)
    with zipfile.ZipFile(destino) as z:
        with z.open(z.namelist()[0]) as raw:
            for cod, nome in csv.reader(io.TextIOWrapper(raw, encoding="latin-1"), delimiter=";"):
                MUNICIPIOS[cod] = _titulo_pt(nome)
    os.remove(destino)
    print(f"[cnpj_nacional] {len(MUNICIPIOS)} municípios carregados")


LOTE = 3000  # linhas por round-trip — round-trip pro Postgres remoto (VPS na Alemanha)
             # custa ~0.25s cada; sem lote, 1 milhão de linhas levaria ~70 HORAS.

_SQL_ESTABELECIMENTO = """
    INSERT INTO empresas (cnpj, nome_fantasia, situacao_cadastral, data_situacao,
            cnae_principal, cnae_secundarios, uf, municipio, telefone, email,
            data_abertura, logradouro, numero, complemento, bairro, cep, raw_json)
    VALUES %s
    ON CONFLICT (cnpj) DO UPDATE SET
        situacao_cadastral=EXCLUDED.situacao_cadastral, telefone=EXCLUDED.telefone,
        email=EXCLUDED.email, cnae_principal=EXCLUDED.cnae_principal,
        cnae_secundarios=EXCLUDED.cnae_secundarios, logradouro=EXCLUDED.logradouro,
        numero=EXCLUDED.numero, bairro=EXCLUDED.bairro, cep=EXCLUDED.cep,
        atualizado_em=NOW()
"""


def _processar_zip_estabelecimentos(caminho_zip, conn):
    gravados = 0
    lote = []
    cur = conn.cursor()

    def flush():
        nonlocal lote
        if lote:
            psycopg2.extras.execute_values(cur, _SQL_ESTABELECIMENTO, lote, template=None, page_size=LOTE)
            conn.commit()
            lote = []

    with zipfile.ZipFile(caminho_zip) as z:
        nome_membro = z.namelist()[0]
        with z.open(nome_membro) as raw:
            texto = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            leitor = csv.reader(texto, delimiter=";")
            for row in leitor:
                uf = row[19]
                if UFS_ALVO is not None and uf not in UFS_ALVO:
                    continue
                if row[5] != SITUACAO_ATIVA:
                    continue

                cnpj = f"{row[0]}{row[1]}{row[2]}"
                nome_municipio = MUNICIPIOS.get(row[20], row[20])
                telefone = f"({row[21]}) {row[22]}" if row[21] and row[22] else None
                email = (row[27] or "").strip().lower() or None
                cnae_principal = row[11]
                cnae_secundarios = [c for c in (row[12] or "").split(",") if c]
                raw_dict = {"cnpj_basico": row[0], "razao_social_disponivel_em": "Empresas*.zip"}

                lote.append((
                    cnpj, row[4] or None, row[5], _parse_data(row[6]), cnae_principal, cnae_secundarios,
                    uf, nome_municipio, telefone, email, _parse_data(row[10]),
                    row[14] or None, row[15] or None, row[16] or None, row[17] or None, row[18] or None,
                    psycopg2.extras.Json(raw_dict),
                ))
                gravados += 1
                if len(lote) >= LOTE:
                    flush()
            flush()
    return gravados


def rodar_estabelecimentos():
    os.makedirs(TMP_DIR, exist_ok=True)
    pasta = _pasta_mais_recente()
    alvo = "Brasil inteiro" if UFS_ALVO is None else "/".join(sorted(UFS_ALVO))
    print(f"[cnpj_nacional] pasta {pasta} · alvo: {alvo} (estabelecimentos ativos)")
    _carregar_municipios(pasta)
    conn = psycopg2.connect(DATABASE_URL, keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3)
    total = 0
    for i in range(10):
        url = f"{MIRROR_BASE}/arquivos/{pasta}/Estabelecimentos{i}.zip"
        destino = os.path.join(TMP_DIR, f"Estabelecimentos{i}.zip")
        print(f"  baixando {url} ...")
        _baixar(url, destino)
        gravados = _processar_zip_estabelecimentos(destino, conn)
        os.remove(destino)
        total += gravados
        print(f"  Estabelecimentos{i}.zip: {gravados} gravado(s) — total {total}")
    conn.close()
    print(f"\n🏁 total de estabelecimentos ativos ({alvo}): {total}")


_SQL_SOCIO = """
    INSERT INTO socios (cnpj, nome_socio, qualificacao, data_entrada_sociedade, faixa_etaria)
    VALUES %s
    ON CONFLICT (cnpj, nome_socio, qualificacao) DO NOTHING
"""


def _processar_zip_socios(caminho_zip, conn, cnpjs_conhecidos):
    gravados = 0
    lote = []
    cur = conn.cursor()

    def flush():
        nonlocal lote, gravados
        if lote:
            psycopg2.extras.execute_values(cur, _SQL_SOCIO, lote, page_size=LOTE)
            gravados += cur.rowcount
            conn.commit()
            lote = []

    with zipfile.ZipFile(caminho_zip) as z:
        nome_membro = z.namelist()[0]
        with z.open(nome_membro) as raw:
            texto = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            leitor = csv.reader(texto, delimiter=";")
            for row in leitor:
                cnpj_basico = row[0]
                if cnpj_basico not in cnpjs_conhecidos:
                    continue
                for cnpj_completo in cnpjs_conhecidos[cnpj_basico]:
                    lote.append((cnpj_completo, row[2] or None, row[3] or None, _parse_data(row[5]), row[10] or None))
                if len(lote) >= LOTE:
                    flush()
            flush()
    return gravados


def rodar_socios():
    """Só grava sócio de CNPJ que já está em `empresas` (economiza — sócio de
    quem a gente nem carregou não serve pra nada)."""
    os.makedirs(TMP_DIR, exist_ok=True)
    pasta = _pasta_mais_recente()
    conn = psycopg2.connect(DATABASE_URL, keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3)
    cur = conn.cursor()
    cur.execute("SELECT cnpj FROM empresas")
    cnpjs_conhecidos = {}
    for (cnpj,) in cur.fetchall():
        cnpjs_conhecidos.setdefault(cnpj[:8], []).append(cnpj)
    print(f"[cnpj_nacional] {len(cnpjs_conhecidos)} cnpj_básico(s) conhecido(s) em `empresas` — sócios só desses.")

    total = 0
    for i in range(10):
        url = f"{MIRROR_BASE}/arquivos/{pasta}/Socios{i}.zip"
        destino = os.path.join(TMP_DIR, f"Socios{i}.zip")
        print(f"  baixando {url} ...")
        _baixar(url, destino)
        gravados = _processar_zip_socios(destino, conn, cnpjs_conhecidos)
        os.remove(destino)
        total += gravados
        print(f"  Socios{i}.zip: {gravados} gravado(s) — total {total}")
    conn.close()
    print(f"\n🏁 total de sócios gravados: {total}")


def _parse_capital(s):
    s = (s or "").strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


_SQL_EMPRESA_UPDATE = """
    UPDATE empresas SET razao_social = v.razao_social, capital_social = v.capital_social
    FROM (VALUES %s) AS v(cnpj_basico, razao_social, capital_social)
    WHERE left(empresas.cnpj, 8) = v.cnpj_basico
"""


def _processar_zip_empresas(caminho_zip, conn):
    atualizados = 0
    lote = []
    cur = conn.cursor()

    def flush():
        nonlocal lote, atualizados
        if lote:
            psycopg2.extras.execute_values(cur, _SQL_EMPRESA_UPDATE, lote, page_size=LOTE)
            atualizados += cur.rowcount
            conn.commit()
            lote = []

    with zipfile.ZipFile(caminho_zip) as z:
        nome_membro = z.namelist()[0]
        with z.open(nome_membro) as raw:
            texto = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            leitor = csv.reader(texto, delimiter=";")
            for row in leitor:
                lote.append((row[0], row[1] or None, _parse_capital(row[4])))
                if len(lote) >= LOTE:
                    flush()
            flush()
    return atualizados


def rodar_empresas():
    """Completa razao_social e capital_social (só existem em Empresas*.zip,
    não em Estabelecimentos*.zip) pra quem já está em `empresas`."""
    os.makedirs(TMP_DIR, exist_ok=True)
    pasta = _pasta_mais_recente()
    conn = psycopg2.connect(DATABASE_URL, keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3)
    total = 0
    for i in range(10):
        url = f"{MIRROR_BASE}/arquivos/{pasta}/Empresas{i}.zip"
        destino = os.path.join(TMP_DIR, f"Empresas{i}.zip")
        print(f"  baixando {url} ...")
        _baixar(url, destino)
        atualizados = _processar_zip_empresas(destino, conn)
        os.remove(destino)
        total += atualizados
        print(f"  Empresas{i}.zip: {atualizados} atualizado(s) — total {total}")
    conn.close()
    print(f"\n🏁 total de razão social/capital social preenchidos: {total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--estabelecimentos", action="store_true")
    parser.add_argument("--socios", action="store_true")
    parser.add_argument("--empresas", action="store_true")
    parser.add_argument("--tudo", action="store_true")
    parser.add_argument("--uf", help="UFs alvo, ex: RJ,SP (default: Brasil inteiro, via env UFS_ALVO)")
    args = parser.parse_args()
    if args.uf:
        UFS_ALVO = {u.strip().upper() for u in args.uf.split(",")}

    if args.tudo or args.estabelecimentos:
        rodar_estabelecimentos()
    if args.tudo or args.empresas:
        rodar_empresas()
    if args.tudo or args.socios:
        rodar_socios()
    if not (args.tudo or args.estabelecimentos or args.empresas or args.socios):
        parser.error("Especifique --estabelecimentos, --empresas, --socios ou --tudo")
