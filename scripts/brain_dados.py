#!/usr/bin/env python3
"""Brain dos dados do allancandido.com — de onde vem cada número do site.

Gera uma pasta de notas Markdown ligadas por [[wikilinks]] e o grafo
interativo do brain-map (~/dev/brain-map). Só LÊ:
  - o Postgres do VPS pelo túnel (localhost:5433, sessão read-only):
    tabelas, linhas, última atualização;
  - os ETLs deste repositório: que tabela cada um grava ou lê;
  - a API do painel (pncp_dashboard.py): que tabela cada rota consulta;
  - o código do allancandido.com: que página chama cada rota;
  - o crontab do VPS: quando cada ETL roda.
As ligações entre dados saem das colunas-chave em comum: código do
município no IBGE, CNPJ, CNAE e município por nome + UF.

Uso:  DATABASE_URL=$(cat ~/.config/pncp/database_url) pncpvenv/bin/python scripts/brain_dados.py
Saída: ~/brain-dados/ (notas) e ~/brain-dados/.brain-map/ (grafo; servir com
       o serviço systemd brain-dados → http://localhost:4711)
"""
import datetime
import glob
import os
import re
import shutil
import subprocess
import sys

import psycopg2

H = os.path.expanduser("~")
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = f"{H}/allancandido.com/src"
SAIDA = f"{H}/brain-dados"
BRAIN_MAP = f"{H}/dev/brain-map/build.py"
VPS = "root@188.245.70.109"

# fonte oficial de cada ETL (do mapa "Motores do site" no vault + docstrings)
FONTES = {
    "etl.py": "PNCP", "contratos_etl.py": "PNCP",
    "transparencia_etl.py": "Portal da Transparência (CGU)", "sancoes_etl.py": "Portal da Transparência (CGU)",
    "score_municipios_etl.py": "IBGE + SICONFI", "radar_loteamento_etl.py": "IBGE + SICONFI",
    "agro_etl.py": "IBGE", "comex_etl.py": "MDIC Comex Stat", "imoveis_uniao_etl.py": "dados.gov.br (SPU)",
    "cnpj_enrich.py": "BrasilAPI (Receita)", "cnpj_imob_finder.py": "Receita Federal (CNPJ aberto)",
    "cnpj_nacional.py": "Receita Federal (CNPJ aberto)", "cnpj_categorias.py": "Receita Federal (CNAE)",
    "radar_cnpj_heatmap.py": "Receita Federal (CNPJ aberto)", "imob_finder.py": "Google Places",
    "imob_coletor.py": "Sites das imobiliárias", "imob_coletor_leads.py": "Sites das imobiliárias",
    "portal_olx.py": "OLX / Zap", "portal_zap.py": "OLX / Zap", "trends_etl.py": "licitações (interno)",
}
# chaves de ligação: nota → colunas que a representam
CHAVES = {
    "Código IBGE do município": ("municipio_ibge", "codigo_ibge", "cod_ibge", "ibge"),
    "CNPJ": ("cnpj", "cnpj_fornecedor", "orgao_cnpj", "cnpj_basico"),
    "CNAE (atividade)": ("cnae_principal", "codigo_cnae", "cnae"),
    "Município por nome + UF": ("municipio", "municipio_nome", "cidade", "cidade_alvo"),
}
TAB_RE = r"(?:FROM|JOIN|INTO|UPDATE|TABLE(?: IF NOT EXISTS)?|COPY)\s+\"?(?:public\.)?{t}\b"


def slug(s):
    return re.sub(r"[/\\:*?\"<>|#^\[\]]", "-", s).strip()


def nota(pasta, nome, corpo):
    os.makedirs(os.path.join(SAIDA, pasta), exist_ok=True)
    with open(os.path.join(SAIDA, pasta, slug(nome) + ".md"), "w", encoding="utf-8") as f:
        f.write(corpo.rstrip() + "\n")


def link(nome):
    return f"[[{slug(nome)}]]"


def banco():
    c = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=10)
    c.set_session(readonly=True)
    cur = c.cursor()
    cur.execute("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE' order by 1")
    tabelas = {}
    for (t,) in cur.fetchall():
        cur.execute("select column_name, data_type from information_schema.columns where table_schema='public' and table_name=%s order by ordinal_position", (t,))
        cols = cur.fetchall()
        cur.execute("select reltuples::bigint from pg_class where relname=%s", (t,))
        est = cur.fetchone()[0]
        if est < 300_000:
            cur.execute(f'select count(*) from "{t}"')
            est = cur.fetchone()[0]
        ult = None
        for n, d in cols:
            if n in ("importado_em", "atualizado_em", "coletado_em", "capturado_em", "executado_em", "buscado_em"):
                try:
                    cur.execute(f'select max("{n}") from "{t}"')
                    v = cur.fetchone()[0]
                    if v and (ult is None or v > ult[1]):
                        ult = (n, v)
                except psycopg2.Error:
                    c.rollback()
        tabelas[t] = {"cols": [n for n, _ in cols], "linhas": int(max(est, 0)), "ultima": ult}
    return tabelas


def usos_nos_scripts(tabelas):
    grava, le = {}, {}
    for f in sorted(glob.glob(f"{REPO}/*.py")):
        nome = os.path.basename(f)
        if nome == "pncp_dashboard.py":
            continue
        txt = open(f, encoding="utf-8", errors="ignore").read()
        for t in tabelas:
            if re.search(r"(?:INSERT\s+INTO|UPDATE|COPY|TABLE(?: IF NOT EXISTS)?)\s+\"?(?:public\.)?" + t + r"\b", txt, re.I):
                grava.setdefault(nome, set()).add(t)
            elif re.search(r"(?:FROM|JOIN)\s+\"?(?:public\.)?" + t + r"\b", txt, re.I):
                le.setdefault(nome, set()).add(t)
    return grava, le


def rotas_da_api(tabelas):
    txt = open(f"{REPO}/pncp_dashboard.py", encoding="utf-8", errors="ignore").read()
    partes = re.split(r"\n(?=@app\.(?:get|post)\()", txt)
    rotas = {}
    for p in partes[1:]:
        m = re.match(r'@app\.(get|post)\("([^"]+)"', p)
        if not m:
            continue
        caminho = m.group(2)
        ts = {t for t in tabelas if re.search(TAB_RE.format(t=t), p, re.I)}
        publica = "verify_admin" not in p.split("def ", 1)[0] or "verify_api_key" in p.split("def ", 1)[0]
        rotas[caminho] = {"tabelas": ts, "metodo": m.group(1).upper(), "admin": not publica}
    return rotas


def paginas_do_site(rotas):
    """rota → páginas. O site chama a API por funções (ex.: lib/insightsData.ts)
    que as páginas importam; segue até dois saltos: função → arquivo que a usa →
    página que usa esse componente."""
    arquivos = [f for f in glob.glob(f"{SITE}/**/*.ts*", recursive=True) if "/node_modules/" not in f]
    textos = {f: open(f, encoding="utf-8", errors="ignore").read() for f in arquivos}

    def eh_pagina(f):
        return not pagina_de(f).startswith("componente")

    def quem_usa(nome, excluir):
        pat = re.compile(r"\b" + re.escape(nome) + r"\b")
        return {f for f, t in textos.items() if f != excluir and pat.search(t)}

    def ate_paginas(arqs, profundidade=2):
        achadas = {f for f in arqs if eh_pagina(f)}
        if profundidade:
            for f in arqs - achadas:
                comp = os.path.splitext(os.path.basename(f))[0]
                achadas |= ate_paginas({g for g in quem_usa(comp, f) if f"/{comp}" in textos[g] or f"{comp}'" in textos[g] or f'{comp}"' in textos[g]}, profundidade - 1)
        return achadas

    usos = {}
    for caminho in rotas:
        base = re.sub(r"\{[^}]+\}.*$", "", caminho).rstrip("/")
        if len(base) < 4:
            continue
        pat = re.compile(r"[`'\"]" + re.escape(base) + r"(?:[/?`'\"$]|\$\{)")
        for f, t in textos.items():
            for m in pat.finditer(t):
                if eh_pagina(f):
                    usos.setdefault(caminho, set()).add(pagina_de(f))
                    continue
                antes = t[: m.start()]
                fns = re.findall(r"export\s+(?:async\s+)?function\s+(\w+)|export\s+const\s+(\w+)\s*=", antes)
                if not fns:
                    continue
                fn = [a or b for a, b in fns][-1]
                for g in ate_paginas(quem_usa(fn, f)):
                    usos.setdefault(caminho, set()).add(pagina_de(g))
    return usos


def pagina_de(arquivo):
    rel = arquivo.replace(SITE + "/", "")
    m = re.match(r"app/\[locale\]/(.*?)/?(page|layout|[A-Z][\w]*Client)\.tsx$", rel)
    if m:
        return "/" + m.group(1) if m.group(1) else "/ (home)"
    m = re.match(r"app/api/(.*)/route\.ts$", rel)
    if m:
        return "/api/" + m.group(1)
    return "componente " + os.path.basename(arquivo)


def agenda_vps():
    try:
        cron = subprocess.run(["ssh", "-o", "ConnectTimeout=8", VPS, "crontab -l; echo ===; cat /root/pncp-cron.sh"],
                              capture_output=True, text=True, timeout=20).stdout
    except Exception:
        return {}
    tab, script = cron.split("===", 1) if "===" in cron else (cron, "")
    alvo = {}
    for m in re.finditer(r"^\s*(\w+)\)\s+.*?python3(?: -m)? (\w+)", script, re.M):
        alvo[m.group(1)] = m.group(2) + ("" if m.group(2).endswith(".py") else ".py")
    agenda = {}
    for l in tab.splitlines():
        m = re.match(r"^([\d*/,\- ]+?)\s+/root/pncp-cron\.sh (\w+)\s*(#.*)?$", l.strip())
        if m and m.group(2) in alvo:
            campos = m.group(1).split()
            # tolerância antes de alertar: diário 3 dias, semanal 10, mensal 40
            limite = 40 if campos[2] != "*" else 10 if campos[4] != "*" else 3
            agenda[alvo[m.group(2)]] = (f"cron `{m.group(1).strip()}` no VPS" + (f" ({m.group(3)[1:].strip()})" if m.group(3) else ""), limite)
    return agenda


def main():
    tabelas = banco()
    grava, le = usos_nos_scripts(tabelas)
    rotas = rotas_da_api(tabelas)
    paginas = paginas_do_site(rotas)
    agenda = agenda_vps()
    agora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    if os.path.isdir(SAIDA):
        for d in os.listdir(SAIDA):
            if not d.startswith("."):
                shutil.rmtree(os.path.join(SAIDA, d), ignore_errors=True) if os.path.isdir(os.path.join(SAIDA, d)) else os.remove(os.path.join(SAIDA, d))

    # chaves → tabelas
    chave_tabs = {k: sorted(t for t, d in tabelas.items() if set(d["cols"]) & set(cols)) for k, cols in CHAVES.items()}
    for k, ts in chave_tabs.items():
        nota("Chaves", k, f"# {k}\n\nLiga os dados destas tabelas (coluna em comum):\n\n" + "\n".join(f"- {link(t + ' (tabela)')}" for t in ts))

    # fontes
    for fonte in sorted(set(FONTES.values())):
        etls = sorted(e for e, f in FONTES.items() if f == fonte and os.path.exists(f"{REPO}/{e}"))
        nota("Fontes", fonte + " (fonte)", f"# {fonte}\n\nAlimenta:\n\n" + "\n".join(f"- {link(e)}" for e in etls))

    # ETLs
    for e in sorted(set(grava) | set(le) | {x for x in FONTES if os.path.exists(f'{REPO}/{x}')}):
        linhas = [f"# {e}", ""]
        if e in FONTES:
            linhas.append(f"Fonte: {link(FONTES[e] + ' (fonte)')}")
        linhas.append(f"Quando roda: {agenda[e][0] if e in agenda else 'manual ou sob demanda'}")
        if grava.get(e):
            linhas += ["", "Grava:"] + [f"- {link(t + ' (tabela)')}" for t in sorted(grava[e])]
        if le.get(e):
            linhas += ["", "Lê:"] + [f"- {link(t + ' (tabela)')}" for t in sorted(le[e])]
        nota("ETLs", e, "\n".join(linhas))

    # tabelas
    for t, d in tabelas.items():
        quem = sorted(e for e, ts in grava.items() if t in ts)
        rotas_t = sorted(r for r, v in rotas.items() if t in v["tabelas"])
        chaves_t = [k for k, ts in chave_tabs.items() if t in ts]
        ult = f"{d['ultima'][1]:%Y-%m-%d %H:%M} (`{d['ultima'][0]}`)" if d["ultima"] and hasattr(d["ultima"][1], "strftime") else "sem coluna de data de carga"
        corpo = [f"# {t}", "", f"- Linhas: **{d['linhas']:,}**".replace(",", "."), f"- Última carga: {ult}",
                 f"- Colunas: {', '.join(d['cols'])}"]
        if quem:
            corpo += ["", "Alimentada por:"] + [f"- {link(e)}" for e in quem]
        if chaves_t:
            corpo += ["", "Liga com outras tabelas por:"] + [f"- {link(k)}" for k in chaves_t]
        if rotas_t:
            corpo += ["", "Servida pela API:"] + [f"- {link('API ' + r)}" for r in rotas_t]
        if not quem and not rotas_t:
            corpo += ["", "_Nenhum ETL deste repositório grava nela e nenhuma rota da API a lê._"]
        nota("Tabelas", t + " (tabela)", "\n".join(corpo))

    # rotas da API
    for r, v in rotas.items():
        if not v["tabelas"]:
            continue
        corpo = [f"# API {v['metodo']} {r}", "", "Uso: " + ("painel admin" if v["admin"] else "pública para os sites (com chave)"), "", "Consulta:"]
        corpo += [f"- {link(t + ' (tabela)')}" for t in sorted(v["tabelas"])]
        if paginas.get(r):
            corpo += ["", "Usada no site por:"] + [f"- {link('Site ' + p)}" for p in sorted(paginas[r])]
        nota("API", "API " + r, "\n".join(corpo))

    # páginas do site
    por_pagina = {}
    for r, ps in paginas.items():
        if rotas[r]["tabelas"]:
            for p in ps:
                por_pagina.setdefault(p, set()).add(r)
    for p, rs in por_pagina.items():
        nota("Site", "Site " + p, f"# allancandido.com {p}\n\nDados vêm de:\n\n" + "\n".join(f"- {link('API ' + r)}" for r in sorted(rs)))

    # índice + alertas de frescor
    hoje = datetime.datetime.now()
    alertas = []
    for t, d in sorted(tabelas.items()):
        if t.endswith("_progress"):
            continue  # controle interno de retomada, não é dado do site
        u = d["ultima"][1] if d["ultima"] else None
        agendados = [e for e, ts in grava.items() if t in ts and e in agenda]
        if d["linhas"] == 0:
            alertas.append(f"- {link(t + ' (tabela)')} está **vazia**")
        elif u and hasattr(u, "replace"):
            dias = (hoje - u.replace(tzinfo=None)).days
            if agendados and dias > min(agenda[e][1] for e in agendados):
                alertas.append(f"- ⚠️ {link(t + ' (tabela)')} é **agendada** ({', '.join(link(e) for e in agendados)}) mas está **sem carga há {dias} dias** — o cron roda e não carrega")
            elif not agendados and dias > 45:
                alertas.append(f"- {link(t + ' (tabela)')} sem carga há {dias} dias (ETL manual)")
    tab = ["| Tabela | Linhas | Última carga |", "|---|---:|---|"] + [
        f"| {link(t + ' (tabela)')} | {d['linhas']:,} | {d['ultima'][1]:%Y-%m-%d} |".replace(",", ".") if d["ultima"] and hasattr(d["ultima"][1], "strftime")
        else f"| {link(t + ' (tabela)')} | {d['linhas']:,} | — |".replace(",", ".") for t, d in sorted(tabelas.items())]
    nota(".", "Brain allancandido", "\n".join([
        "# Brain dos dados do allancandido.com", "",
        f"Gerado em {agora} por `~/pncp-etl/scripts/brain_dados.py` (só leitura).", "",
        f"**{len(tabelas)} tabelas · {len(set(grava) | set(FONTES))} ETLs · {sum(1 for v in rotas.values() if v['tabelas'])} rotas de API com dados · {len(por_pagina)} páginas do site**", "",
        "Chaves que ligam os dados: " + ", ".join(link(k) for k in CHAVES), "",
        "## Alertas", ""] + (alertas or ["- nenhum"]) + ["", "## Frescor", ""] + tab))

    r = subprocess.run([sys.executable, BRAIN_MAP, "--vault", SAIDA, "--out", os.path.join(SAIDA, ".brain-map")], capture_output=True, text=True)
    print(r.stdout[-300:] or r.stderr[-300:])
    print(f"brain: {len(tabelas)} tabelas, {len(grava)} ETLs gravando, {len(rotas)} rotas, {len(por_pagina)} páginas → {SAIDA}")


if __name__ == "__main__":
    main()
