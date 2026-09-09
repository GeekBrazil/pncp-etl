#!/usr/bin/env python3
"""
Carrega a tabela oficial de CNAEs (Receita Federal) e semeia categorias
amigáveis por palavra-chave na descrição oficial.

A ideia: "restaurante", "imobiliária", "pousada" etc não são código de CNAE —
são um rótulo humano que aponta pra uma LISTA de códigos. Esse script:
  1. Baixa Cnaes.zip do mesmo espelho que o cnpj_imob_finder.py já usa e
     carrega a tabela `cnaes` (código, descrição oficial).
  2. Casa palavra-chave contra a descrição oficial e grava sugestões em
     `categorias_cnae` (origem='auto') — o Allan cura os casos estranhos
     depois direto na tabela (mudar origem pra 'manual' quando ajustar).

Uso:
    python3 cnpj_categorias.py                 # carrega CNAEs + semeia categorias padrão
    python3 cnpj_categorias.py --listar         # mostra as categorias e quantos códigos cada uma tem
"""
import csv
import io
import os
import sys
import zipfile

import psycopg2
import psycopg2.extras
import requests

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
MIRROR_BASE = "https://dados-abertos-rf-cnpj.casadosdados.com.br"
TMP_DIR = os.environ.get("CNPJ_TMP_DIR", "/tmp/cnpj_nacional")

# categoria -> lista de palavras-chave (case-insensitive) que, se aparecerem
# na descrição oficial do CNAE, sugerem essa categoria. Curadoria manual
# depois é só editar a tabela categorias_cnae direto (origem='manual' pra
# marcar que não é mais sugestão automática).
PALAVRAS_CHAVE = {
    "restaurante": ["restaurante", "lanchonete", "casa de comida", "comida pronta"],
    "bar": ["bares e outros estabelecimentos especializados em servir bebidas"],
    "pousada": ["hotéis", "pousadas", "albergues", "campings", "colônia de férias"],
    "imobiliaria": ["corretagem na compra", "corretagem no aluguel", "compra e venda de imóveis próprios"],
    "oficina_mecanica": ["manutenção e reparação de veículos automotores", "reparação mecânica de veículos"],
    "posto_combustivel": ["comércio varejista de combustíveis"],
    "mercado_mercearia": ["comércio varejista de mercadorias em geral", "minimercados", "mercearias", "armazéns"],
    "padaria": ["padaria", "fabricação de produtos de padaria"],
    "salao_beleza": ["cabeleireiros", "outras atividades de tratamento de beleza"],
    "loja_roupas": ["comércio varejista de artigos do vestuário"],
    "transporte_frete": ["transporte rodoviário de carga", "transporte rodoviário de produtos"],
    "turismo_passeio": ["agências de viagens", "operadores turísticos", "atividades de guias de turismo"],
    "camping_estacionamento": ["campings", "estacionamento de veículos"],
    "artesanato": ["fabricação de artefatos", "fabricação de produtos de artesanato"],
    "agropecuaria": ["cultivo de", "criação de", "atividades de apoio à agricultura"],
}


def get_conn():
    # keepalive: sem isso, o túnel SSH pode morrer em silêncio numa instabilidade
    # de rede e a primeira query trava pra sempre esperando resposta que não vem.
    return psycopg2.connect(DATABASE_URL, keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3)


def _baixar(url, destino, tentativas=4):
    import time
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
    import re
    r = requests.get(f"{MIRROR_BASE}/arquivos/", timeout=30)
    r.raise_for_status()
    pastas = sorted(set(re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', r.text)))
    if not pastas:
        raise RuntimeError("não achei nenhuma pasta de dados no espelho")
    return pastas[-1]


def carregar_cnaes(conn):
    os.makedirs(TMP_DIR, exist_ok=True)
    pasta = _pasta_mais_recente()
    destino = os.path.join(TMP_DIR, "Cnaes.zip")
    print(f"[cnpj_categorias] baixando Cnaes.zip da pasta {pasta}...")
    _baixar(f"{MIRROR_BASE}/arquivos/{pasta}/Cnaes.zip", destino)

    total = 0
    with zipfile.ZipFile(destino) as z:
        with z.open(z.namelist()[0]) as raw:
            texto = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            leitor = csv.reader(texto, delimiter=";")
            linhas = [(row[0].strip(), row[1].strip()) for row in leitor if len(row) >= 2]
            cur = conn.cursor()
            # lote único (round-trip pro Postgres remoto é caro; 1359 linhas cabe folgado)
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO cnaes (codigo, descricao) VALUES %s
                   ON CONFLICT (codigo) DO UPDATE SET descricao = EXCLUDED.descricao""",
                linhas,
            )
            total = len(linhas)
            conn.commit()
    os.remove(destino)
    print(f"[cnpj_categorias] {total} CNAEs carregados na tabela `cnaes`.")
    return total


def semear_categorias(conn):
    cur = conn.cursor()
    cur.execute("SELECT codigo, descricao FROM cnaes")
    todos = cur.fetchall()

    linhas = []
    for categoria, palavras in PALAVRAS_CHAVE.items():
        codigos = [
            codigo for codigo, descricao in todos
            if any(p.lower() in descricao.lower() for p in palavras)
        ]
        linhas.extend((categoria, codigo, "auto") for codigo in codigos)
        print(f"  {categoria}: {len(codigos)} código(s) de CNAE")

    inseridos = 0
    if linhas:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO categorias_cnae (categoria, codigo_cnae, origem) VALUES %s
               ON CONFLICT (categoria, codigo_cnae) DO NOTHING""",
            linhas,
        )
        inseridos = cur.rowcount
    conn.commit()
    print(f"\n[cnpj_categorias] {inseridos} associação(ões) categoria→CNAE inserida(s).")


def listar(conn):
    cur = conn.cursor()
    cur.execute(
        """SELECT categoria, count(*), array_agg(codigo_cnae ORDER BY codigo_cnae)
           FROM categorias_cnae GROUP BY categoria ORDER BY categoria"""
    )
    for categoria, n, codigos in cur.fetchall():
        print(f"{categoria} ({n}): {', '.join(codigos[:5])}{' ...' if n > 5 else ''}")


if __name__ == "__main__":
    conn = get_conn()
    try:
        if "--listar" in sys.argv:
            listar(conn)
        else:
            carregar_cnaes(conn)
            semear_categorias(conn)
    finally:
        conn.close()
