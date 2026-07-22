#!/usr/bin/env python3
"""
Ponte leads → coletor: descobre o SITE das imobiliárias (do domínio do e-mail
da Receita) e roda o coletor de anúncios em cima.

A base de CNPJ traz e-mail mas não site. Só que imobiliária costuma usar e-mail
no próprio domínio (contato@fulanoimoveis.com.br → fulanoimoveis.com.br). Este
script pega esses domínios dos leads e alimenta o coletor (imob_coletor.py),
que raspa JSON-LD/sitemap. Domínio sem site ou sem anúncio é pulado sozinho.

Uso:
    python3 imob_coletor_leads.py --uf RJ [--limite 300] [--so-imob]
    --so-imob : só domínios que parecem de imobiliária (imov/imob/corretor/…)
    --limite N: para após N sites (resumível — pula quem já tem anúncio hoje)
"""
import argparse
import os
import re
import sys

import psycopg2

from imob_coletor import coleta_imobiliaria

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")

# domínios de e-mail que NÃO são o site da imobiliária (provedores + terceiros comuns)
DOMINIOS_GENERICOS = {
    "gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "yahoo.com",
    "bol.com.br", "uol.com.br", "terra.com.br", "icloud.com", "hotmail.com.br",
    "live.com", "msn.com", "gmail.com.br", "oi.com.br", "globo.com", "ig.com.br",
    "me.com", "yahoo.com.mx", "gmail.co",
}
# pistas de que o domínio é de contador/advogado (não da imobiliária)
RUIDO = re.compile(r"contab|contador|advog|juridic|escritorio|assessor|cartorio", re.I)
IMOB = re.compile(r"imov|imob|corretor|corretagem|predial|lar|casa|lancamento|negocios", re.I)


def dominio_do_email(email):
    if not email or "@" not in email:
        return None
    dom = email.split("@", 1)[1].strip().lower().strip(".")
    if not re.match(r"^[a-z0-9.-]+\.[a-z]{2,}$", dom) or dom in DOMINIOS_GENERICOS:
        return None
    return dom


def candidatos(conn, uf, so_imob):
    cur = conn.cursor()
    cur.execute(
        """SELECT DISTINCT ON (lower(split_part(e.email,'@',2)))
                  e.email, e.nome_fantasia, l.cidade_alvo, l.uf
           FROM empresas e JOIN leads_imobiliarias l ON l.cnpj = e.cnpj
           WHERE l.uf = %s AND e.email ~ '@' AND e.situacao_cadastral = '02'
           ORDER BY lower(split_part(e.email,'@',2)), e.nome_fantasia""",
        (uf,),
    )
    vistos = set()
    for email, nome, cidade, uf_l in cur.fetchall():
        dom = dominio_do_email(email)
        if not dom or dom in vistos:
            continue
        if RUIDO.search(dom):
            continue
        if so_imob and not IMOB.search(dom):
            continue
        vistos.add(dom)
        yield dom, nome, cidade, uf_l


def ja_raspado(conn, dom):
    cur = conn.cursor()
    cur.execute(
        """SELECT 1 FROM imobiliarias i JOIN imoveis_mercado m ON m.imobiliaria_id = i.id
           WHERE i.dominio = %s AND m.coletado_em > NOW() - INTERVAL '20 days' LIMIT 1""",
        (dom,),
    )
    return cur.fetchone() is not None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uf", default="RJ")
    ap.add_argument("--limite", type=int, default=0)
    ap.add_argument("--so-imob", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    lista = list(candidatos(conn, args.uf, args.so_imob))
    print(f"[coletor-leads] {len(lista)} domínios candidatos (uf={args.uf}, so_imob={args.so_imob})", flush=True)

    feitos = anuncios = com_site = 0
    for dom, nome, cidade, uf in lista:
        if ja_raspado(conn, dom):
            continue
        try:
            n = coleta_imobiliaria(conn, dom, nome=nome, cidade=cidade, uf=uf, max_anuncios=40)
        except Exception as e:
            print(f"[{dom}] erro: {e}", flush=True)
            n = 0
        feitos += 1
        if n > 0:
            com_site += 1
            anuncios += n
        if args.limite and feitos >= args.limite:
            break
    conn.close()
    print(f"\n🏁 {feitos} sites tentados · {com_site} com anúncios · {anuncios} anúncios gravados", flush=True)


if __name__ == "__main__":
    main()
