"""Espelho do navegador do robô para o painel de Automações (localhost:3010).

Quando o painel dispara um coletor, ele passa ESPELHO=<arquivo.jpg>; o coletor
chama `espelhar(page)` nos momentos certos (página carregada, rolagem) e o
painel mostra a imagem se atualizando — dá para assistir sem abrir janela.
Sem a variável (cron, terminal), não faz nada.
"""
import os

ARQ = os.environ.get("ESPELHO")


def espelhar(page, legenda=None):
    if not ARQ:
        return
    try:
        tmp = f"{ARQ}.tmp.jpg"
        page.screenshot(path=tmp, type="jpeg", quality=55)
        os.replace(tmp, ARQ)  # troca atômica: o painel nunca lê imagem pela metade
        if legenda:
            with open(f"{ARQ}.txt", "w") as fh:
                fh.write(legenda)
    except Exception:
        pass  # espelho é conforto; nunca derruba a coleta


def esperar(page, ms, legenda=None):
    """page.wait_for_timeout com espelho a cada ~0,8 s."""
    if not ARQ:
        page.wait_for_timeout(ms)
        return
    resto = ms
    while resto > 0:
        passo = min(800, resto)
        page.wait_for_timeout(passo)
        espelhar(page, legenda)
        resto -= passo


def rolar(page, legenda=None, passos=6):
    """Rola a lista até o fim como gente (o espelho mostra a rolagem)."""
    for _ in range(passos):
        page.mouse.wheel(0, 900)
        esperar(page, 350, legenda)
