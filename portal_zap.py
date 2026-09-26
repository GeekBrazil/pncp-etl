#!/usr/bin/env python3
"""
Coletor de portal — Zap Imóveis, via Playwright.

Zap e OLX são do mesmo grupo hoje (o card usa a classe `olx-core-card`, o
mesmo componente), mas o Zap NÃO expõe uma etiqueta de "direto com o
proprietário" na listagem como o OLX expõe. O que a listagem mostra no lugar
do selo de destaque é às vezes o NOME do anunciante — mas isso não separa
dono de corretor autônomo (o Zap deixa isso claro só na página do anúncio,
com a contagem "N imóvel(is)" do anunciante). Por isso `anunciante_tipo` fica
NULL aqui por enquanto: melhor não preencher do que adivinhar errado numa
lista pensada pra prospecção. Ver DEPLOY.md / conversa do dia sobre uma
segunda fase que abre a página do anúncio só pra quem tem poucos imóveis.

Uso:
    python3 portal_zap.py --cidade angra-dos-reis --uf rj --finalidade venda --paginas 3
"""
import argparse
import os
import re
import time

import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
PAUSA_PAGINA = float(os.environ.get("ZAP_PAUSA", "4.0"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"

TIPOS = ["apartamento", "cobertura", "casa", "lote/terreno", "terreno", "sítio", "chácara",
         "loja", "sala", "galpão", "fazenda", "kitnet", "flat", "sobrado", "duplex"]


def _tipo_do_titulo(titulo):
    t = titulo.lower()
    return next((tp for tp in TIPOS if tp in t), None)


def _extrair_cards(page):
    return page.eval_on_selector_all(
        "a.olx-core-card",
        """els => els.map(el => ({
            href: el.href,
            titulo: el.title || '',
            texto: el.innerText || '',
        }))""",
    )


def _parse_card(c, cidade_default, uf):
    texto = c["texto"]
    preco = None
    m = re.search(r"R\$\s*([\d.]+)", texto)
    if m:
        preco = float(m.group(1).replace(".", ""))
    area = quartos = banheiros = vagas = None
    m = re.search(r"Tamanho do imóvel\s*\n?\s*([\d.]+)\s*m", texto)
    if m:
        area = float(m.group(1).replace(".", ""))
    m = re.search(r"Quantidade de quartos\s*\n?\s*(\d+)", texto)
    if m:
        quartos = int(m.group(1))
    m = re.search(r"Quantidade de banheiros\s*\n?\s*(\d+)", texto)
    if m:
        banheiros = int(m.group(1))
    m = re.search(r"Quantidade de vagas de garagem\s*\n?\s*(\d+)", texto)
    if m:
        vagas = int(m.group(1))

    bairro = cidade = None
    # o título vem como "<descrição> em <complemento?>, <bairro>, <cidade>" — os dois
    # últimos pedaços separados por vírgula são sempre bairro e cidade, não importa
    # quantos vierem antes (condomínio, endereço etc. variam por anúncio).
    partes = [p.strip() for p in re.sub(r"^.*?\bem\s+", "", c["titulo"]).split(",")]
    if len(partes) >= 2:
        bairro, cidade = partes[-2], partes[-1]
    elif partes:
        cidade = partes[-1]

    preco_m2 = round(preco / area, 2) if preco and area else None
    return {
        "preco": preco, "area": area, "quartos": quartos, "banheiros": banheiros,
        "vagas": vagas, "preco_m2": preco_m2, "bairro": bairro,
        "cidade": cidade or cidade_default, "tipo": _tipo_do_titulo(c["titulo"]),
    }


def coletar_zap(conn, cidade_slug, uf, cidade=None, finalidade="venda", paginas=3, visivel=False):
    """cidade_slug é o formato da URL do Zap, ex: 'angra-dos-reis' (sem acento, com hífen)."""
    url_base = f"https://www.zapimoveis.com.br/{finalidade}/imoveis/{uf.lower()}+{cidade_slug}/"
    cur = conn.cursor()
    gravados = 0
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=not visivel)
        page = navegador.new_page(user_agent=UA)
        for pagina in range(1, paginas + 1):
            url = url_base if pagina == 1 else f"{url_base}?pagina={pagina}"
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_selector("a.olx-core-card", timeout=10000)
                page.wait_for_timeout(1000)
            except Exception as e:
                print(f"[zap] falha ao abrir {url}: {e}")
                break
            cards = _extrair_cards(page)
            if not cards:
                print(f"[zap] página {pagina}: sem cards, parando")
                break
            for c in cards:
                if not c["href"] or not c["titulo"]:
                    continue
                campos = _parse_card(c, cidade, uf)
                if not campos["preco"]:
                    continue
                cur.execute(
                    """INSERT INTO imoveis_mercado
                        (fonte, origem, finalidade, tipo, preco, area, preco_m2, quartos,
                         bairro, cidade, uf, url, titulo)
                       VALUES ('zapimoveis.com.br', 'portal', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO UPDATE SET
                           preco=EXCLUDED.preco, area=EXCLUDED.area, preco_m2=EXCLUDED.preco_m2,
                           quartos=EXCLUDED.quartos, coletado_em=NOW()""",
                    (finalidade, campos["tipo"], campos["preco"], campos["area"], campos["preco_m2"],
                     campos["quartos"], campos["bairro"], campos["cidade"], uf, c["href"], c["titulo"]),
                )
                gravados += 1
            conn.commit()
            print(f"[zap] {cidade_slug}/{finalidade} página {pagina}: {len(cards)} cards, {gravados} gravados até aqui")
            time.sleep(PAUSA_PAGINA)
        navegador.close()
    return gravados


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cidade-slug", dest="cidade_slug", required=True, help="ex: angra-dos-reis")
    ap.add_argument("--uf", required=True)
    ap.add_argument("--cidade", default=None, help="nome bonito, fallback se o card não trouxer")
    ap.add_argument("--finalidade", choices=["venda", "aluguel"], default="venda")
    ap.add_argument("--paginas", type=int, default=3)
    ap.add_argument("--visivel", action="store_true", help="abre o Chromium na tela em vez de rodar invisível")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    total = coletar_zap(conn, args.cidade_slug, args.uf, args.cidade, args.finalidade, args.paginas, visivel=args.visivel)
    print(f"TOTAL gravado: {total}")
    conn.close()
