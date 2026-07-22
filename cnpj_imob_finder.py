#!/usr/bin/env python3
"""
Finder de imobiliárias via dados abertos de CNPJ (Receita Federal).

Filtra o cadastro nacional de estabelecimentos pelo CNAE de corretagem
imobiliária (6821-8/01 e /02) por UF. Fonte: espelho público da Receita
Federal (atualiza uma vez por mês) — sem API key, sem billing.

O download já é nacional (os 10 arquivos cobrem o Brasil inteiro); o filtro
por UF decide o que grava. Prioridade atual: **Rio de Janeiro**. Pra varrer
o Brasil todo, é só `UFS_ALVO=ALL`.

Diferente do imob_coletor.py (que raspa o SITE da imobiliária pra pegar
anúncios), este script só descobre QUEM existe formalmente registrado —
nome, telefone, e-mail — e vira lead pro diretório (mesmo sem site).

Uso:
    python3 cnpj_imob_finder.py                  # UFS_ALVO (default RJ)
    UFS_ALVO=RJ,SP python3 cnpj_imob_finder.py    # estados específicos
    UFS_ALVO=ALL python3 cnpj_imob_finder.py      # Brasil inteiro
    python3 cnpj_imob_finder.py --avisar          # lista/marca leads novos
"""
import csv
import io
import os
import re
import sys
import time
import zipfile
from datetime import datetime

import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
MIRROR_BASE = "https://dados-abertos-rf-cnpj.casadosdados.com.br"
TMP_DIR = os.environ.get("CNPJ_TMP_DIR", "/tmp/cnpj_imob")

# UFs alvo: "RJ" (default, prioridade), "RJ,SP,ES", ou "ALL" pro Brasil inteiro.
_ufs_raw = os.environ.get("UFS_ALVO", "RJ").strip().upper()
UFS_ALVO = None if _ufs_raw in ("ALL", "*", "BR") else {u.strip() for u in _ufs_raw.split(",") if u.strip()}

# 6821-8/01 Corretagem na compra e venda e avaliação de imóveis; /02 corretagem no aluguel.
# (6822-6 é "gestão/administração de propriedade" — administradora de condomínio, não corretora.)
CNAES_ALVO = {"6821801", "6821802"}
SITUACAO_ATIVA = "02"

# código-do-município-da-Receita → nome. Preenchido por _carregar_municipios().
MUNICIPIOS = {}


_MINUSCULAS = {"da", "de", "do", "das", "dos", "e"}


def _titulo_pt(nome):
    """Title-case pt-BR: conectivos minúsculos (Angra Dos Reis → Angra dos Reis)."""
    palavras = nome.strip().lower().split()
    return " ".join(p if p in _MINUSCULAS and i > 0 else p.capitalize()
                    for i, p in enumerate(palavras))


def _carregar_municipios(pasta):
    """Baixa o Municipios.zip (42KB) do espelho e monta código→nome."""
    destino = os.path.join(TMP_DIR, "Municipios.zip")
    _baixar(f"{MIRROR_BASE}/arquivos/{pasta}/Municipios.zip", destino)
    with zipfile.ZipFile(destino) as z:
        with z.open(z.namelist()[0]) as raw:
            for cod, nome in csv.reader(io.TextIOWrapper(raw, encoding="latin-1"), delimiter=";"):
                MUNICIPIOS[cod] = _titulo_pt(nome)
    os.remove(destino)
    print(f"[cnpj_imob_finder] {len(MUNICIPIOS)} municípios carregados")


def _pasta_mais_recente():
    """Acha a pasta de data mais recente publicada no espelho (ex: 2026-07-12)."""
    r = requests.get(f"{MIRROR_BASE}/arquivos/", timeout=30)
    r.raise_for_status()
    pastas = sorted(set(re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', r.text)))
    if not pastas:
        raise RuntimeError("não achei nenhuma pasta de dados no espelho")
    return pastas[-1]


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


def _parse_data(s):
    s = (s or "").strip()
    if len(s) != 8 or s == "00000000":
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def _processar_zip(caminho_zip, conn):
    """Lê o CSV de dentro do zip sem extrair (economiza disco), filtra e grava."""
    achados = 0
    with zipfile.ZipFile(caminho_zip) as z:
        nome_membro = z.namelist()[0]
        with z.open(nome_membro) as raw:
            texto = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            leitor = csv.reader(texto, delimiter=";")
            cur = conn.cursor()
            for row in leitor:
                uf = row[19]
                if UFS_ALVO is not None and uf not in UFS_ALVO:
                    continue
                cnae_principal = row[11]
                cnae_secundarias = (row[12] or "").split(",")
                if cnae_principal not in CNAES_ALVO and not (CNAES_ALVO & set(cnae_secundarias)):
                    continue
                if row[5] != SITUACAO_ATIVA:
                    continue  # só empresa ativa vira lead

                cnpj = f"{row[0]}{row[1]}{row[2]}"
                nome_municipio = MUNICIPIOS.get(row[20], row[20])
                telefone = f"({row[21]}) {row[22]}" if row[21] and row[22] else None
                email = (row[27] or "").strip().lower() or None
                raw_dict = {
                    "cnpj_basico": row[0], "nome_fantasia": row[4], "situacao_cadastral": row[5],
                    "cnae_fiscal_principal": cnae_principal, "cnae_fiscal_secundaria": row[12],
                    "logradouro": f"{row[14]} {row[15]}".strip(), "bairro": row[17], "cep": row[18],
                }

                cur.execute(
                    """INSERT INTO empresas (cnpj, nome_fantasia, situacao_cadastral, data_situacao,
                            cnae_principal, uf, municipio, telefone, email, data_abertura, raw_json, atualizado_em)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                       ON CONFLICT (cnpj) DO UPDATE SET
                           situacao_cadastral=EXCLUDED.situacao_cadastral, telefone=EXCLUDED.telefone,
                           email=EXCLUDED.email, atualizado_em=NOW()""",
                    (cnpj, row[4] or None, row[5], _parse_data(row[6]), cnae_principal, uf,
                     nome_municipio, telefone, email, _parse_data(row[10]),
                     psycopg2.extras.Json(raw_dict)),
                )
                cur.execute(
                    """INSERT INTO leads_imobiliarias (cnpj, cidade_alvo, uf)
                       VALUES (%s,%s,%s) ON CONFLICT (cnpj) DO NOTHING""",
                    (cnpj, nome_municipio, uf),
                )
                achados += 1
            conn.commit()
    return achados


def rodar():
    os.makedirs(TMP_DIR, exist_ok=True)
    pasta = _pasta_mais_recente()
    alvo = "Brasil inteiro" if UFS_ALVO is None else "/".join(sorted(UFS_ALVO))
    print(f"[cnpj_imob_finder] pasta {pasta} · alvo: {alvo}")
    _carregar_municipios(pasta)
    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    for i in range(10):
        url = f"{MIRROR_BASE}/arquivos/{pasta}/Estabelecimentos{i}.zip"
        destino = os.path.join(TMP_DIR, f"Estabelecimentos{i}.zip")
        print(f"  baixando {url} ...")
        _baixar(url, destino)
        achados = _processar_zip(destino, conn)
        os.remove(destino)  # libera disco antes do próximo
        total += achados
        print(f"  Estabelecimentos{i}.zip: {achados} lead(s) — total {total}")
    conn.close()
    print(f"\n🏁 total de leads ({alvo}, CNAE corretagem imobiliária): {total}")


def avisar(conn):
    cur = conn.cursor()
    cur.execute(
        """SELECT l.cnpj, e.nome_fantasia, e.telefone, e.email, l.cidade_alvo, l.uf, e.data_abertura
           FROM leads_imobiliarias l JOIN empresas e ON e.cnpj = l.cnpj
           WHERE l.alertado = FALSE ORDER BY l.cidade_alvo, e.nome_fantasia"""
    )
    pendentes = cur.fetchall()
    if not pendentes:
        print("Nenhum lead novo de imobiliária desde o último aviso.")
        return
    print(f"📬 {len(pendentes)} imobiliária(s) nova(s) pra dar boas-vindas:\n")
    for cnpj, nome, tel, email, cidade, uf, abertura in pendentes:
        print(f"  {nome or '(sem nome fantasia)':<40} {cidade}/{uf}  tel={tel or '-'}  email={email or '-'}  "
              f"aberta={abertura}  cnpj={cnpj}")
    cur.execute(
        "UPDATE leads_imobiliarias SET alertado = TRUE WHERE cnpj = ANY(%s)",
        ([p[0] for p in pendentes],),
    )
    conn.commit()


if __name__ == "__main__":
    if "--avisar" in sys.argv:
        conn = psycopg2.connect(DATABASE_URL)
        avisar(conn)
        conn.close()
    else:
        rodar()
