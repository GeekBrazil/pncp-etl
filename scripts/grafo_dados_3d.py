"""Grafo dos dados do allancandido.com para o modelo 3D do creative-lab.

O brain (brain_dados.py) mostra a ligação técnica (tabela ↔ ETL ↔ rota ↔
página). Este grafo mostra a ligação que interessa ao visitante:
  fonte oficial → base de dados → chave que liga → cruzamento → pergunta que
  o cruzamento responde → página do site onde a resposta aparece.
Os números (linhas, municípios cobertos, exemplo de cidade) vêm do banco na
hora de gerar; nada é inventado. Os cruzamentos são curados à mão (CRUZAMENTOS)
e cada um diz quais bases usa e por qual chave.

Saída: ~/creative-lab/data/grafo-dados.json
Uso: DATABASE_URL=... python scripts/grafo_dados_3d.py [--cidade 3304524]
"""
import argparse
import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

SAIDA = os.path.expanduser("~/creative-lab/data/grafo-dados.json")

FONTES = {
    "ibge": "IBGE",
    "receita": "Receita Federal (CNPJ)",
    "mte": "Ministério do Trabalho (Novo CAGED)",
    "pncp": "Portal Nacional de Contratações Públicas",
    "tesouro": "Tesouro Nacional (SICONFI)",
    "cgu": "Portal da Transparência (CGU)",
    "mdic": "Comex Stat (MDIC)",
    "spu": "Secretaria do Patrimônio da União",
    "mercado": "Sites de imobiliárias, OLX e Zap",
    "campo": "Placas fotografadas em campo (bot Sofia)",
}

CHAVES = {
    "ibge": "Código IBGE do município",
    "cnpj": "CNPJ",
    "cnae": "Atividade econômica (CNAE)",
    "nome_uf": "Nome do município + UF",
    "geo": "Coordenada (lat/lon)",
}

# base: (tabela, fonte, chaves, rótulo para o visitante, coluna do município para cobertura, tipo da coluna)
BASES = {
    "licitacoes": ("licitacoes", "pncp", ["ibge", "cnpj"], "Licitações", "municipio_ibge", "ibge"),
    "contratos": ("contratos", "pncp", ["ibge", "cnpj"], "Contratos públicos", "municipio_ibge", "ibge"),
    "score": ("score_municipios", "tesouro", ["ibge"], "Receita do município", "municipio_ibge", "ibge"),
    "bolsa": ("bolsa_familia_municipio", "cgu", ["ibge"], "Bolsa Família", "codigo_ibge", "ibge"),
    "sancoes": ("sancoes", "cgu", ["cnpj"], "Empresas sancionadas", None, None),
    "caged": ("caged_agregado", "mte", ["ibge", "cnae"], "Emprego formal", "municipio_ibge", "ibge"),
    "empresas": ("empresas", "receita", ["cnpj", "cnae", "nome_uf", "geo"], "Empresas", None, None),
    "imobiliarias": ("leads_imobiliarias", "receita", ["cnpj", "nome_uf"], "Imobiliárias registradas", "cidade_alvo", "nome"),
    "radar_cnpj": ("radar_cnpj_heatmap", "receita", ["cnae", "nome_uf"], "Aberturas de empresas", "municipio", "nome"),
    "anuncios": ("imoveis_mercado", "mercado", ["nome_uf", "geo"], "Anúncios de imóveis", "cidade", "nome"),
    "placas": ("placas_campo", "campo", ["ibge", "geo", "cnpj"], "Placas de venda/aluguel na rua", "municipio_ibge", "ibge"),
    "radar_lot": ("radar_loteamento", "ibge", ["ibge"], "Crescimento da população", "municipio_ibge", "ibge"),
    "agro": ("agro_municipios", "ibge", ["ibge"], "Perfil rural", "municipio_ibge", "ibge"),
    "comex": ("comex_municipios", "mdic", ["nome_uf"], "Exportação e importação", "municipio_nome", "nome"),
    "uniao": ("imoveis_uniao", "spu", ["nome_uf"], "Imóveis da União", "municipio", "nome"),
    "litoral": ("regiao_litoral", "ibge", ["ibge"], "Recorte do litoral e portos", "municipio_ibge", "ibge"),
}

# Cada cruzamento: as bases que junta, a chave, a pergunta do visitante e onde aparece.
CRUZAMENTOS = [
    ("emprego_x_preco", ["caged", "anuncios"], "ibge",
     "A cidade está gerando emprego e o metro quadrado acompanha?", "Raio-X da Cidade"),
    ("imobiliarias_x_anuncios", ["imobiliarias", "anuncios", "placas"], "nome_uf",
     "Quantas imobiliárias disputam a cidade e quanto anunciam?", "Raio-X da Cidade"),
    ("dono_direto", ["anuncios", "placas"], "geo",
     "Quanto da oferta é direto com o dono, fora dos portais?", "Raio-X da Cidade"),
    ("receita_x_crescimento", ["score", "radar_lot", "licitacoes"], "ibge",
     "A prefeitura tem caixa e está investindo em obras onde a população cresce?", "Radar de Loteamentos"),
    ("renda_x_aluguel", ["bolsa", "anuncios", "caged"], "ibge",
     "O aluguel cabe no bolso de quem mora ali?", "Mapa de calor"),
    ("fornecedor_sancionado", ["contratos", "sancoes", "empresas"], "cnpj",
     "Algum órgão contratou empresa proibida de contratar com o governo?", "Consulta de CNPJ / Sanções"),
    ("empresa_x_governo", ["empresas", "licitacoes", "contratos", "sancoes"], "cnpj",
     "Essa empresa vende para o governo? Tem sanção?", "Consulta de CNPJ"),
    ("setor_em_alta", ["radar_cnpj", "caged"], "cnae",
     "Que tipo de negócio está abrindo e contratando na cidade?", "Radar CNPJ"),
    ("porto_x_economia", ["litoral", "comex", "caged", "anuncios"], "nome_uf",
     "O porto puxa exportação, emprego e preço de imóvel na região?", "Mapa do litoral"),
    ("patrimonio_publico", ["uniao", "licitacoes"], "nome_uf",
     "Que imóveis da União existem na cidade e o que o governo licita ali?", "Leilões e concessões"),
    ("campo_x_cidade", ["agro", "comex", "radar_lot"], "nome_uf",
     "A economia é rural ou urbana, e para onde está indo?", "Raio-X da Cidade"),
]


def contar(cur, tabela, col, tipo):
    cur.execute(f"SELECT count(*) AS n FROM {tabela}")
    n = cur.fetchone()["n"]
    municipios = None
    if col:
        expr = col if tipo == "ibge" else f"unaccent(lower({col}))"
        cur.execute(f"SELECT count(DISTINCT {expr}) AS m FROM {tabela} WHERE {col} IS NOT NULL")
        municipios = cur.fetchone()["m"]
    return n, municipios


def exemplo_cidade(cur, ibge):
    """Os números reais de uma cidade, um por base — o 'zoom' do modelo."""
    cur.execute("SELECT municipio_nome AS nome, uf FROM score_municipios WHERE municipio_ibge = %s LIMIT 1", (ibge,))
    c = cur.fetchone()
    if not c:
        return None
    nome, uf = c["nome"], c["uf"]
    v = {}

    def um(chave, sql, params):
        try:
            cur.execute(sql, params)
            r = cur.fetchone()
            v[chave] = r and list(r.values())[0]
        except Exception:
            cur.connection.rollback()

    um("score", "SELECT round(receita_per_capita) FROM score_municipios WHERE municipio_ibge=%s ORDER BY exercicio DESC LIMIT 1", (ibge,))
    um("licitacoes", "SELECT count(*) FROM licitacoes WHERE municipio_ibge=%s", (ibge,))
    um("bolsa", "SELECT round(valor) FROM bolsa_familia_municipio WHERE codigo_ibge=%s ORDER BY ano_mes DESC LIMIT 1", (ibge,))
    um("caged", """SELECT sum(saldo) FROM caged_municipios WHERE municipio_ibge=%s
                   AND competencia IN (SELECT competencia FROM caged_meses_completos ORDER BY 1 DESC LIMIT 12)""", (ibge,))
    um("imobiliarias", """SELECT count(*) FROM leads_imobiliarias l JOIN empresas e USING (cnpj)
                          WHERE e.situacao_cadastral='02' AND l.uf=%s AND unaccent(lower(l.cidade_alvo))=unaccent(lower(%s))""", (uf, nome))
    um("anuncios", "SELECT count(*) FROM imoveis_mercado WHERE uf=%s AND unaccent(lower(cidade))=unaccent(lower(%s))", (uf, nome))
    um("preco_m2", """SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY preco_m2)::numeric) FROM imoveis_mercado
                      WHERE uf=%s AND unaccent(lower(cidade))=unaccent(lower(%s)) AND finalidade='venda'
                        AND preco_m2 BETWEEN 300 AND 60000 AND tipo IS DISTINCT FROM 'terreno'""", (uf, nome))
    um("placas", "SELECT count(*) FROM placas_campo WHERE municipio_ibge=%s", (ibge,))
    um("radar_lot", "SELECT crescimento_pct FROM radar_loteamento WHERE municipio_ibge=%s", (ibge,))
    um("agro", "SELECT estabelecimentos FROM agro_municipios WHERE municipio_ibge=%s", (ibge,))
    um("litoral", "SELECT porto FROM regiao_litoral WHERE municipio_ibge=%s", (ibge,))
    return {"ibge": ibge, "nome": nome, "uf": uf,
            "valores": {k: (float(x) if hasattr(x, "as_integer_ratio") or str(type(x)).endswith("Decimal'>") else x) for k, x in v.items()}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cidade", default="3304524", help="código IBGE do exemplo (padrão: Rio das Ostras)")
    args = ap.parse_args()
    url = os.environ.get("DATABASE_URL") or open(os.path.expanduser("~/.config/pncp/database_url")).read().strip()
    conn = psycopg2.connect(url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    nos, arestas = [], []
    for k, rotulo in FONTES.items():
        nos.append({"id": f"fonte:{k}", "tipo": "fonte", "rotulo": rotulo})
    for k, rotulo in CHAVES.items():
        nos.append({"id": f"chave:{k}", "tipo": "chave", "rotulo": rotulo})
    for k, (tabela, fonte, chaves, rotulo, col, tipo) in BASES.items():
        try:
            n, m = contar(cur, tabela, col, tipo)
        except Exception:
            conn.rollback()
            continue
        nos.append({"id": f"base:{k}", "tipo": "base", "rotulo": rotulo, "tabela": tabela,
                    "linhas": n, "municipios": m})
        arestas.append({"de": f"fonte:{fonte}", "para": f"base:{k}", "tipo": "alimenta"})
        for c in chaves:
            arestas.append({"de": f"base:{k}", "para": f"chave:{c}", "tipo": "liga_por"})
    existentes = {n["id"] for n in nos}
    paginas = {}
    for k, bases, chave, pergunta, pagina in CRUZAMENTOS:
        bases_ok = [b for b in bases if f"base:{b}" in existentes]
        if len(bases_ok) < 2:
            continue
        nos.append({"id": f"cruz:{k}", "tipo": "cruzamento", "rotulo": pergunta, "chave": f"chave:{chave}"})
        for b in bases_ok:
            arestas.append({"de": f"base:{b}", "para": f"cruz:{k}", "tipo": "cruza_em", "via": f"chave:{chave}"})
        pid = "pagina:" + pagina.lower().replace(" ", "-").replace("/", "")
        if pid not in paginas:
            paginas[pid] = pagina
            nos.append({"id": pid, "tipo": "pagina", "rotulo": pagina})
        arestas.append({"de": f"cruz:{k}", "para": pid, "tipo": "aparece_em"})

    grafo = {
        "gerado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "descricao": "Fontes oficiais → bases → chaves → cruzamentos (a pergunta que cada um responde) → páginas do allancandido.com",
        "nos": nos,
        "arestas": arestas,
        "exemplo_cidade": exemplo_cidade(cur, args.cidade),
    }
    os.makedirs(os.path.dirname(SAIDA), exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8") as f:
        json.dump(grafo, f, ensure_ascii=False, indent=2, default=str)
    tipos = {}
    for n in nos:
        tipos[n["tipo"]] = tipos.get(n["tipo"], 0) + 1
    print(f"[grafo] {SAIDA}: {tipos}, {len(arestas)} arestas")


if __name__ == "__main__":
    main()
