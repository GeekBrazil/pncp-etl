#!/usr/bin/env python3
"""Importa a lista oficial de imóveis à venda da Caixa (Lista_imoveis_UF.csv).

A Caixa publica a lista por estado em venda-imoveis.caixa.gov.br, mas o site fica
atrás de um CAPTCHA anti-robô (Radware) — não se contorna. O Allan baixa o
arquivo no navegador (um clique por estado, na página "Baixar lista de
imóveis"); ele cai em ~/Downloads e a unidade systemd caixa-importar.path chama
este script na hora. O arquivo processado vai para ~/Downloads/caixa-importados/.

Formato (conferido com leitor aberto da mesma lista): cp1252, separador ";",
algumas linhas de título antes do cabeçalho; colunas achadas pelo NOME
(N° do imóvel, UF, Cidade, Bairro, Endereço, Preço, Valor de avaliação,
Desconto, Descrição, Modalidade de venda, Link de acesso).

Grava em leiloes_outros (fonte='caixa'). valor = avaliação (mesmo sentido das
outras fontes); preco = preço de venda; desconto_pct; area_m2 (privativa, senão
total, senão terreno). Imóvel de uma UF importada que não está no arquivo novo
vira ativo = FALSE — só se o arquivo tiver pelo menos metade do que já havia
(arquivo cortado não apaga o catálogo).

Uso:  python3 caixa_importar.py [arquivo.csv ...]      (sem arquivo: ~/Downloads)
"""
import csv
import glob
import io
import os
import re
import shutil
import sys
import unicodedata
from datetime import date

import psycopg2
import psycopg2.extras

PASTA = os.path.expanduser("~/Downloads")
DESTINO = os.path.join(PASTA, "caixa-importados")

DDL = """
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS preco NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS desconto_pct NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS area_m2 NUMERIC;
ALTER TABLE leiloes_outros ADD COLUMN IF NOT EXISTS bairro TEXT;
"""


def norm(s):
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn").replace("º", "").replace("°", "")
    return re.sub(r"[^a-zA-Z0-9]+", " ", s).strip().lower()


def num(s):
    t = re.sub(r"[^0-9,.-]", "", str(s or ""))
    if not t:
        return None
    if "," in t:
        t = t.replace(".", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def ler(caminho):
    raw = open(caminho, "rb").read()
    for enc in ("cp1252", "utf-8-sig", "latin1"):
        try:
            texto = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    linhas = texto.splitlines()
    inicio = None
    for i, l in enumerate(linhas[:40]):
        cel = [norm(c) for c in next(csv.reader([l], delimiter=";"), [])]
        if any("imovel" in c for c in cel) and "cidade" in cel:
            inicio = i
            break
    if inicio is None:
        raise RuntimeError("cabeçalho não reconhecido — a Caixa mudou o layout?")
    leitor = csv.reader(io.StringIO("\n".join(linhas[inicio:])), delimiter=";")
    cab = [norm(h) for h in next(leitor)]

    def col(pred):
        return next((i for i, h in enumerate(cab) if pred(h)), -1)

    ix = {
        "id": col(lambda h: "imovel" in h and ("n" == h.split()[0] or "numero" in h)),
        "uf": col(lambda h: h == "uf"), "cidade": col(lambda h: h == "cidade"),
        "bairro": col(lambda h: h == "bairro"), "endereco": col(lambda h: "endereco" in h),
        "preco": col(lambda h: h == "preco"), "aval": col(lambda h: "avaliacao" in h),
        "desc_pct": col(lambda h: "desconto" in h), "descricao": col(lambda h: "descricao" in h),
        "modalidade": col(lambda h: "modalidade" in h), "link": col(lambda h: "link" in h),
    }
    faltam = [k for k in ("id", "uf", "cidade", "preco", "modalidade") if ix[k] < 0]
    if faltam:
        raise RuntimeError(f"colunas não achadas: {faltam} — cabeçalho: {cab}")
    g = lambda r, k: r[ix[k]].strip() if 0 <= ix[k] < len(r) else ""
    out = []
    for r in leitor:
        if not r or not g(r, "id") or not re.search(r"\d", g(r, "id")):
            continue
        out.append({k: g(r, k) for k in ix})
    return out


def tipo_e_area(descricao):
    d = descricao or ""
    primeiro = norm(d.split(",", 1)[0])
    tipo = None
    for rot, chaves in (("apartamento", ("apartamento",)), ("casa", ("casa", "sobrado")),
                        ("terreno", ("terreno", "lote", "gleba")), ("comercial", ("loja", "sala", "predio", "galpao", "comercial")),
                        ("rural", ("rural", "fazenda", "sitio", "chacara")), ("garagem", ("garagem", "vaga"))):
        if any(c in primeiro for c in chaves):
            tipo = rot
            break
    area = None
    for rx in (r"([\d.,]+)\s*(?:m2|m²)?\s*de\s+[áa]rea\s+privativa", r"([\d.,]+)\s*(?:m2|m²)?\s*de\s+[áa]rea\s+total",
               r"([\d.,]+)\s*(?:m2|m²)?\s*de\s+[áa]rea\s+(?:do\s+)?terreno"):
        m = re.search(rx, d, re.I)
        if m and (v := num(m.group(1))) and v > 0:
            area = v
            break
    return tipo, area


def modalidade(m):
    n = norm(m)
    if "leilao" in n:
        return "leilao"
    if "licitacao" in n or "concorrencia" in n:
        return "concorrencia"
    return "venda_direta"


def municipios(conn):
    idx = {}
    with conn.cursor() as cur:
        cur.execute("SELECT municipio_ibge, municipio_nome, uf FROM radar_loteamento")
        for ibge, nome, uf in cur.fetchall():
            idx[(uf, norm(nome))] = (ibge, nome)
    return idx


def importar(conn, caminho, mun):
    linhas = ler(caminho)
    regs = []
    for l in linhas:
        uf = l["uf"].upper()[:2]
        tipo, area = tipo_e_area(l["descricao"])
        m = mun.get((uf, norm(l["cidade"])))
        so_num = re.sub(r"\D", "", l["id"])
        link = l["link"] or f"https://venda-imoveis.caixa.gov.br/sistema/detalhe-imovel.asp?hdnimovel={so_num}"
        regs.append({
            "fonte": "caixa", "id_externo": re.sub(r"\D", "", l["id"]), "uf": uf,
            "cidade": m[1] if m else l["cidade"].title(), "municipio_ibge": m[0] if m else None,
            "tipo": tipo, "modalidade": modalidade(l["modalidade"]), "valor": num(l["aval"]) or num(l["preco"]),
            "preco": num(l["preco"]), "desconto_pct": num(l["desc_pct"]), "area_m2": area,
            "bairro": (l["bairro"] or "").title() or None, "url": link, "origem": (l["modalidade"] or "")[:60],
            "publicado": date.today(),
        })
    regs = list({r["id_externo"]: r for r in regs}.values())
    if not regs:
        raise RuntimeError("arquivo sem imóveis")
    cols = ["fonte", "id_externo", "uf", "cidade", "municipio_ibge", "tipo", "modalidade", "valor", "preco",
            "desconto_pct", "area_m2", "bairro", "url", "origem", "publicado"]
    ufs = sorted({r["uf"] for r in regs})
    with conn.cursor() as cur:
        cur.execute(DDL)
        psycopg2.extras.execute_values(cur, f"""
            INSERT INTO leiloes_outros ({",".join(cols)}) VALUES %s
            ON CONFLICT (fonte, id_externo) DO UPDATE SET
              uf = EXCLUDED.uf, cidade = EXCLUDED.cidade, municipio_ibge = EXCLUDED.municipio_ibge, tipo = EXCLUDED.tipo,
              modalidade = EXCLUDED.modalidade, valor = EXCLUDED.valor, preco = EXCLUDED.preco,
              desconto_pct = EXCLUDED.desconto_pct, area_m2 = EXCLUDED.area_m2, bairro = EXCLUDED.bairro,
              url = EXCLUDED.url, origem = EXCLUDED.origem, ativo = TRUE, ultimo_visto = now()
        """, [tuple(r[c] for c in cols) for r in regs])
        saidos = 0
        for uf in ufs:
            ids = [r["id_externo"] for r in regs if r["uf"] == uf]
            cur.execute("SELECT count(*) FROM leiloes_outros WHERE fonte='caixa' AND ativo AND uf=%s", (uf,))
            antes = cur.fetchone()[0]
            if len(ids) >= antes * 0.5:
                cur.execute("UPDATE leiloes_outros SET ativo=FALSE WHERE fonte='caixa' AND ativo AND uf=%s AND id_externo <> ALL(%s)", (uf, ids))
                saidos += cur.rowcount
            else:
                print(f"  ! {uf}: arquivo com {len(ids)} de {antes} — não desativo nada (parece cortado)")
    conn.commit()
    por_mod = {}
    for r in regs:
        por_mod[r["modalidade"]] = por_mod.get(r["modalidade"], 0) + 1
    sem_mun = sum(1 for r in regs if not r["municipio_ibge"])
    print(f"  {os.path.basename(caminho)}: {len(regs)} imóveis {por_mod} · UF {','.join(ufs)} · "
          f"{sem_mun} sem município · {saidos} saíram da lista")
    return len(regs)


def main():
    arquivos = sys.argv[1:] or sorted(glob.glob(os.path.join(PASTA, "Lista_imoveis*.csv")))
    if not arquivos:
        print("nenhum Lista_imoveis*.csv para importar")
        return
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    mun = municipios(conn)
    os.makedirs(DESTINO, exist_ok=True)
    falhas = 0
    for a in arquivos:
        try:
            importar(conn, a, mun)
            if os.path.dirname(os.path.abspath(a)) == PASTA:
                shutil.move(a, os.path.join(DESTINO, f"{date.today().isoformat()}_{os.path.basename(a)}"))
        except Exception as e:
            conn.rollback()
            falhas += 1
            print(f"  ✗ {os.path.basename(a)}: {e}")
            if os.path.dirname(os.path.abspath(a)) == PASTA:  # não fica tentando de novo em loop
                shutil.move(a, os.path.join(DESTINO, f"ERRO_{date.today().isoformat()}_{os.path.basename(a)}"))
    print(f"🏁 Caixa: {len(arquivos) - falhas} arquivo(s) importado(s), {falhas} com erro.")
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
