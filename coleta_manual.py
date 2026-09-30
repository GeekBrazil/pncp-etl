#!/usr/bin/env python3
"""
Coleta manual — VOCÊ navega, o coletor salva o que está na tela.

Abre um Google Chrome comum (perfil próprio em ~/.cache/coleta-manual-perfil,
sem a marca de automação do Playwright) já na busca escolhida do OLX ou do Zap.
Você rola, filtra e passa de página como qualquer pessoa; um painel flutuante
no canto da página mostra quantos anúncios estão na tela e tem o botão
"Capturar esta página" (e a opção de capturar sozinho a cada página nova).
O coletor só LÊ a página aberta — como salvar a página — e grava em
imoveis_mercado com as mesmas regras dos robôs (portal_olx / portal_zap).

Por que existe: o Zap (Cloudflare) bloqueia a 2ª página feita por robô. Isso
é proposital do site e não é contornado; aqui quem navega é uma pessoa.

Uso (normalmente pelo painel de Automações, localhost:3010):
    python3 coleta_manual.py --portal zap --cidade Paraty --finalidade venda
    python3 coleta_manual.py --portal olx --cidade "Angra dos Reis" --finalidade aluguel
Fecha quando você fecha a janela do Chrome (ou pelo botão Parar do painel).
"""
import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

import psycopg2
from playwright.sync_api import sync_playwright

import portal_olx
import portal_zap
from espelho import espelhar

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")
PERFIL = os.path.expanduser(os.environ.get("COLETA_PERFIL", "~/.cache/coleta-manual-perfil"))
CIDADES_TXT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cidades_portais.txt")

PAINEL_JS = r"""
(() => {
  if (window.top !== window || window.__coletaPronta) return;
  window.__coletaPronta = true;
  const monta = () => {
    if (document.getElementById('__coleta')) return;
    const box = document.createElement('div');
    box.id = '__coleta';
    box.innerHTML = `
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
        <b style="font:700 13px system-ui">Coleta manual</b>
        <span style="margin-left:auto;font:11px system-ui;opacity:.7">allancandido</span>
      </div>
      <div id="__coletaInfo" style="font:12px/1.45 system-ui;opacity:.9">Procurando anúncios…</div>
      <button id="__coletaBtn" type="button" style="margin-top:8px;width:100%;padding:8px 10px;border:0;border-radius:8px;
        background:#CCFF00;color:#050814;font:700 13px system-ui;cursor:pointer">Capturar esta página</button>
      <label style="display:flex;gap:6px;align-items:center;margin-top:7px;font:12px system-ui;cursor:pointer">
        <input id="__coletaAuto" type="checkbox" checked> capturar sozinho a cada página nova
      </label>
      <div id="__coletaMsg" style="margin-top:6px;font:12px system-ui;color:#CCFF00;min-height:1em"></div>`;
    Object.assign(box.style, {position:'fixed', right:'16px', bottom:'16px', zIndex:2147483647, width:'250px',
      background:'rgba(5,8,20,.94)', color:'#F4F6FB', border:'1px solid rgba(204,255,0,.5)', borderRadius:'12px',
      padding:'12px 14px', boxShadow:'0 10px 30px rgba(0,0,0,.45)'});
    document.documentElement.appendChild(box);
    const auto = document.getElementById('__coletaAuto');
    try { auto.checked = localStorage.getItem('__coletaAuto') !== '0'; } catch (e) {}
    auto.addEventListener('change', () => { try { localStorage.setItem('__coletaAuto', auto.checked ? '1' : '0'); } catch (e) {} });
    document.getElementById('__coletaBtn').addEventListener('click', () => { window.__pedidoCaptura = Date.now(); });
  };
  window.__coletaAtualiza = (info, msg) => {
    monta();
    if (info != null) document.getElementById('__coletaInfo').innerHTML = info;
    if (msg != null) document.getElementById('__coletaMsg').textContent = msg;
  };
  window.__coletaAuto = () => { const a = document.getElementById('__coletaAuto'); return !a || a.checked; };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', monta); else monta();
})();
"""


def ler_cidades():
    """cidade bonita -> (uf, regiao_olx, slug_zap), de cidades_portais.txt"""
    out = {}
    with open(CIDADES_TXT) as fh:
        for linha in fh:
            if not linha.strip() or linha.startswith("#"):
                continue
            uf, cidade, regiao, slug = [x.strip() for x in linha.split("|")]
            out[cidade] = (uf, regiao, slug)
    return out


def porta_livre():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def url_inicial(portal, cidade, finalidade, cidades):
    uf, regiao, slug = cidades[cidade]
    if portal == "olx":
        return f"https://www.olx.com.br/imoveis/{finalidade}/estado-{uf.lower()}/{regiao}"
    return f"https://www.zapimoveis.com.br/{finalidade}/imoveis/{uf.lower()}+{slug}/"


def contexto_da_url(url, cidades, cidade_padrao):
    """Da URL que você abriu: portal, finalidade, UF e cidade (para gravar certo
    mesmo se você trocar de cidade na própria busca do site)."""
    u = urlparse(url)
    host = u.hostname or ""
    fin = "aluguel" if "/aluguel" in u.path else "venda" if "/venda" in u.path else None
    if "olx.com.br" in host:
        m = re.search(r"/estado-([a-z]{2})", u.path)
        return "olx", fin, (m.group(1).upper() if m else None), cidade_padrao
    if "zapimoveis.com.br" in host:
        m = re.search(r"/imoveis/([a-z]{2})\+([a-z0-9-]+)", u.path)
        if m:
            uf, slug = m.group(1).upper(), m.group(2)
            nome = next((c for c, (_, _, s) in cidades.items() if s == slug), None)
            return "zap", fin, uf, nome or slug.replace("-", " ").title()
        return "zap", fin, None, cidade_padrao
    return None, fin, None, None


def main():
    ap = argparse.ArgumentParser(description="Coleta manual OLX/Zap: você navega, o coletor salva")
    ap.add_argument("--portal", choices=["olx", "zap"], default="zap")
    ap.add_argument("--cidade", default="Angra dos Reis")
    ap.add_argument("--finalidade", choices=["venda", "aluguel"], default="venda")
    ap.add_argument("--teste", action="store_true", help="só para teste: abre a janela, captura a 1ª página sozinho e fecha")
    ap.add_argument("--visivel", action="store_true", help="aceito por compatibilidade com o painel (a janela sempre abre)")
    a = ap.parse_args()

    cidades = ler_cidades()
    if a.cidade not in cidades:
        sys.exit(f"cidade fora de cidades_portais.txt: {a.cidade} (tem: {', '.join(cidades)})")
    inicio = url_inicial(a.portal, a.cidade, a.finalidade, cidades)

    chrome = shutil.which("google-chrome") or shutil.which("google-chrome-stable") or shutil.which("chromium")
    if not chrome:
        sys.exit("Google Chrome não encontrado")
    os.makedirs(PERFIL, exist_ok=True)
    porta = porta_livre()
    args = [chrome, f"--user-data-dir={PERFIL}", f"--remote-debugging-port={porta}", "--remote-debugging-address=127.0.0.1",
            "--no-first-run", "--no-default-browser-check", "--new-window"]
    # Sem --headless: Chrome sem janela é barrado pelo Cloudflare do OLX e do Zap.
    proc = subprocess.Popen(args + [inicio], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"[manual] Chrome aberto em {inicio}", flush=True)
    print("[manual] Navegue à vontade; o painel verde no canto da página captura os anúncios.", flush=True)

    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    vistos = set()  # url da página + qtd de cards já capturados (evita gravar 2x a mesma tela)
    try:
        with sync_playwright() as p:
            navegador = None
            for _ in range(40):
                try:
                    navegador = p.chromium.connect_over_cdp(f"http://127.0.0.1:{porta}")
                    break
                except Exception:
                    time.sleep(0.5)
            if navegador is None:
                sys.exit("não consegui falar com o Chrome")
            ctx = navegador.contexts[0]
            ctx.add_init_script(PAINEL_JS)
            for pg in ctx.pages:
                try:
                    pg.evaluate(PAINEL_JS)
                except Exception:
                    pass

            def capturar(pg, motivo):
                nonlocal total
                portal, fin, uf, cidade = contexto_da_url(pg.url, cidades, a.cidade)
                fin = fin or a.finalidade
                uf = uf or cidades[a.cidade][0]
                if portal == "olx":
                    try:
                        pg.wait_for_selector(".olx-adcard__price", timeout=4000)
                    except Exception:
                        pass
                    cards = portal_olx._extrair_cards(pg)
                    salvar = lambda cur: portal_olx.gravar_cards_olx(cur, cards, fin, uf, cidade)
                elif portal == "zap":
                    cards = portal_zap._extrair_cards(pg)
                    salvar = lambda cur: portal_zap.gravar_cards_zap(cur, cards, fin, uf, cidade)
                else:
                    pg.evaluate("window.__coletaAtualiza && window.__coletaAtualiza(null, 'Esta página não é do OLX nem do Zap')")
                    return
                if not cards:
                    pg.evaluate("window.__coletaAtualiza && window.__coletaAtualiza(null, 'Nenhum anúncio nesta tela ainda')")
                    return
                chave = (pg.url, len(cards))
                if motivo == "auto" and chave in vistos:
                    return
                with conn.cursor() as cur:
                    n = salvar(cur)
                conn.commit()
                vistos.add(chave)
                total += n
                espelhar(pg, f"coleta manual · {portal} · {cidade}")
                print(f"[manual] {portal} · {cidade} · {fin}: {n} anúncios salvos desta página ({motivo}); {total} na sessão", flush=True)
                pg.evaluate("([n,t]) => window.__coletaAtualiza && window.__coletaAtualiza(null, `✓ ${n} salvos · ${t} na sessão`)", [n, total])

            ultima = {}
            comeco = time.time()
            while proc.poll() is None and navegador.is_connected():
                for pg in list(ctx.pages):
                    try:
                        if pg.is_closed() or not pg.url.startswith("http"):
                            continue
                        estado = pg.evaluate("""() => {
                            const cards = document.querySelectorAll('section.olx-adcard, a.olx-core-card').length;
                            const pedido = window.__pedidoCaptura || 0; window.__pedidoCaptura = 0;
                            const auto = window.__coletaAuto ? window.__coletaAuto() : true;
                            if (window.__coletaAtualiza) window.__coletaAtualiza(cards ? `<b>${cards}</b> anúncios nesta tela` : 'Procurando anúncios…', null);
                            return {cards, pedido, auto};
                        }""")
                        if os.environ.get("COLETA_DEBUG"):
                            print(f"[manual:debug] {pg.title()[:40]!r} {estado}", flush=True)
                        if estado["pedido"]:
                            capturar(pg, "botão")
                        elif estado["auto"] and estado["cards"]:
                            # captura sozinho quando a página muda e os cards já carregaram
                            if ultima.get(pg) != (pg.url, estado["cards"]):
                                ultima[pg] = (pg.url, estado["cards"])
                                pg.wait_for_timeout(1200)
                                capturar(pg, "auto")
                    except Exception as e:
                        if os.environ.get("COLETA_DEBUG"):
                            print(f"[manual:debug] {type(e).__name__}: {str(e)[:200]}", flush=True)
                        continue  # página navegando/fechando: tenta na próxima volta
                if a.teste and (total or time.time() - comeco > 60):
                    break
                time.sleep(0.7)
    finally:
        conn.close()
        if proc.poll() is None:
            proc.terminate()
        print(f"TOTAL gravado: {total}", flush=True)


if __name__ == "__main__":
    main()
