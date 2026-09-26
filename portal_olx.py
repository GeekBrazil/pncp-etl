#!/usr/bin/env python3
"""
Coletor de portal — OLX Imóveis, via Playwright (a listagem é montada em JS,
diferente do site de imobiliária individual que `imob_coletor.py` já resolve
com requests puro).

Diferente do imob_coletor.py, aqui o alvo é o card da lista (não a página do
anúncio): dá pra pegar preço, local, quartos/banheiros/vagas/área e — o motivo
de existir este script — a etiqueta "Direto com o proprietário" que o próprio
OLX expõe no card, sem precisar abrir cada anúncio. É essa etiqueta que separa
prospecção (dono) de mercado (imobiliária).

Uso:
    python3 portal_olx.py --regiao rio-de-janeiro-e-regiao --uf RJ --cidade "Rio de Janeiro"
    python3 portal_olx.py --regiao serra-angra-dos-reis-e-regiao --uf RJ --cidade "Angra dos Reis" \
        --finalidade aluguel --paginas 3
"""
import argparse
import os
import re
import time

import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
PAUSA_PAGINA = float(os.environ.get("OLX_PAUSA", "3.0"))  # gentil entre páginas de listagem
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"

TIPOS = ["apartamento", "cobertura", "casa", "terreno", "área", "sítio", "chácara",
         "loja", "sala", "galpão", "fazenda", "kitnet", "flat"]


def _tipo_do_titulo(titulo):
    t = titulo.lower()
    return next((tp for tp in TIPOS if tp in t), None)


def _num(txt):
    """'5+ quartos' -> 5, '2 banheiros' -> 2, None se não achar dígito."""
    if not txt:
        return None
    m = re.search(r"\d+", txt)
    return int(m.group(0)) if m else None


def _extrair_cards(page):
    # textContent, não innerText: o OLX usa content-visibility e o card fora da
    # tela devolve innerText vazio (era por isso que só ~5 de 50 vinham com preço).
    # O local é o .olx-adcard__location exato — o [class*=location] pegava o
    # bloco "local + data", que às vezes começa pela data ("Hoje, 06:08").
    return page.eval_on_selector_all(
        "section.olx-adcard",
        """els => els.map(el => {
            const link = el.querySelector('[data-testid="adcard-link"]');
            const priceEl = el.querySelector('.olx-adcard__price');
            const locEl = el.querySelector('.olx-adcard__location');
            const details = [...el.querySelectorAll('.olx-adcard__detail')]
                .map(d => d.getAttribute('aria-label'));
            return {
                href: link ? link.href : null,
                titulo: link ? link.title : null,
                preco_txt: priceEl ? priceEl.textContent : null,
                loc_txt: locEl ? locEl.textContent : null,
                details,
                dono: el.textContent.includes('Direto com o proprietário'),
            };
        })""",
    )


def coletar_olx(conn, regiao, uf, cidade=None, finalidade="venda", paginas=3, sort_recente=True, visivel=False):
    """Percorre N páginas da listagem OLX de uma região e grava em imoveis_mercado.

    `visivel` abre uma janela de Chromium de verdade (headless=False) — pra
    ver rodando ao vivo, via painel de automações. Só funciona numa sessão
    com tela (DISPLAY setado); rodando via systemd/cron sem tela, ignora."""
    url_base = f"https://www.olx.com.br/imoveis/{finalidade}/estado-{uf.lower()}/{regiao}"
    cur = conn.cursor()
    gravados = 0
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=not visivel)
        page = navegador.new_page(user_agent=UA)
        for pagina in range(1, paginas + 1):
            qs = f"?o={pagina}" if pagina > 1 else ""
            if sort_recente:
                qs += "&sf=1" if qs else "?sf=1"
            url = url_base + qs
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                # o preço renderiza depois do card em si — sem isso, a coleta perde
                # anúncio por corrida (visto na prática: 5/50 virou 50/50 só com isso).
                page.wait_for_selector(".olx-adcard__price", timeout=10000)
                page.wait_for_timeout(800)
            except Exception as e:
                print(f"[olx] falha ao abrir {url}: {e}")
                break
            cards = _extrair_cards(page)
            if not cards:
                print(f"[olx] página {pagina}: sem cards, parando (região pode não existir ou fim da lista)")
                break
            for c in cards:
                if not c["href"] or not c["preco_txt"]:
                    continue
                preco = None
                digs = re.sub(r"[^\d]", "", c["preco_txt"])
                if digs:
                    preco = float(digs)
                # "Rio das Ostras, Costazul" ou "Rio das Ostras - RJ"
                loc = re.sub(r"\s+-\s+[A-Z]{2}\s*$", "", (c["loc_txt"] or "").strip())
                partes = [x.strip() for x in loc.split(",") if x.strip()]
                cidade_card = partes[0] if partes else cidade
                bairro = partes[1] if len(partes) > 1 else None
                area = quartos = banheiros = None
                for d in c["details"] or []:
                    if not d:
                        continue
                    dl = d.lower()
                    if "metro" in dl:
                        area = _num(d)
                    elif "quarto" in dl:
                        quartos = _num(d)
                    elif "banheiro" in dl:
                        banheiros = _num(d)
                if area is None:  # sem o detalhe, o título costuma trazer "210 m²"
                    mt = re.search(r"(\d[\d.]*)\s*m[²2]", c["titulo"] or "")
                    area = _num(mt.group(1).replace(".", "")) if mt else None
                preco_m2 = round(preco / area, 2) if preco and area else None
                anunciante_tipo = "proprietario" if c["dono"] else None
                tipo = _tipo_do_titulo(c["titulo"] or "")
                cur.execute(
                    """INSERT INTO imoveis_mercado
                        (fonte, origem, finalidade, tipo, preco, area, preco_m2, quartos,
                         bairro, cidade, uf, url, titulo, anunciante_tipo)
                       VALUES ('olx.com.br', 'portal', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (url) DO UPDATE SET
                           preco=EXCLUDED.preco, area=EXCLUDED.area, preco_m2=EXCLUDED.preco_m2,
                           quartos=EXCLUDED.quartos, anunciante_tipo=EXCLUDED.anunciante_tipo,
                           coletado_em=NOW()""",
                    (finalidade, tipo, preco, area, preco_m2, quartos, bairro,
                     cidade_card or cidade, uf, c["href"], c["titulo"], anunciante_tipo),
                )
                gravados += 1
            conn.commit()
            print(f"[olx] {regiao}/{finalidade} página {pagina}: {len(cards)} cards, {gravados} gravados até aqui")
            time.sleep(PAUSA_PAGINA)
        navegador.close()
    return gravados


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--regiao", required=True, help="slug da região OLX, ex: serra-angra-dos-reis-e-regiao")
    ap.add_argument("--uf", required=True)
    ap.add_argument("--cidade", default=None, help="fallback se o card não trouxer local")
    ap.add_argument("--finalidade", choices=["venda", "aluguel"], default="venda")
    ap.add_argument("--paginas", type=int, default=3)
    ap.add_argument("--visivel", action="store_true", help="abre o Chromium na tela em vez de rodar invisível")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    total = coletar_olx(conn, args.regiao, args.uf, args.cidade, args.finalidade, args.paginas, visivel=args.visivel)
    print(f"TOTAL gravado: {total}")
    conn.close()
