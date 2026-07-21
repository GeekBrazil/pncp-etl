"""
Portal da Transparência (CGU) — gasto executado (5º pilar).

Dois recortes, ambos on-demand com cache no banco (sem backfill nacional):
- recursos_por_cnpj(cnpj)      → recursos federais recebidos por uma empresa
                                  (despesas/recursos-recebidos). Cruza com sanções:
                                  "empresa sancionada recebendo da União".
- bolsa_familia_municipio(ibge)→ Novo Bolsa Família por município (gasto social
                                  executado) — sinal territorial pro raio-X.

Auth: header `chave-api-dados` (env TRANSPARENCIA_API_KEY) — o mesmo do sancoes_etl.
Limites da API: 90 req/min (06h–23h59) · 300 req/min (00h–05h59).
"""
import os
import time
from datetime import date

import requests
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
API_BASE     = "https://api.portaldatransparencia.gov.br/api-de-dados"
API_KEY      = os.environ.get("TRANSPARENCIA_API_KEY", "")
CACHE_DIAS   = int(os.environ.get("TRANSPARENCIA_CACHE_DIAS", "30"))
PAUSA        = float(os.environ.get("TRANSPARENCIA_PAUSA", "0.8"))  # ~75/min, folga sobre o limite diurno
PAGE_SIZE    = 15  # tamanho de página observado nesses endpoints


def _conn():
    return psycopg2.connect(DATABASE_URL)


def _get(path, params, _retries=3):
    """GET na API com header de auth e backoff simples em 429."""
    r = requests.get(f"{API_BASE}/{path}", params=params,
                     headers={"chave-api-dados": API_KEY}, timeout=40)
    if r.status_code == 429 and _retries > 0:
        time.sleep(20)
        return _get(path, params, _retries - 1)
    r.raise_for_status()
    return r.json()


# ─── Recursos federais recebidos por CNPJ ────────────────────────────────────
def recursos_por_cnpj(cnpj, anos=None, force=False, buscar_api=True, max_paginas=15):
    """Cache-first. Retorna resumo dos recursos federais recebidos pelo CNPJ,
    ou None se CNPJ inválido / chave ausente. Nunca levanta exceção pra fora
    quando a API falha — devolve o que houver em cache (ou None).

    buscar_api=False → só lê o cache (não bate na API). Use na consulta inline
    do /cnpj pra não adicionar latência de rede à ferramenta; a busca ao vivo
    fica no endpoint dedicado /transparencia/recursos-cnpj."""
    digitos = "".join(ch for ch in str(cnpj) if ch.isdigit())
    if len(digitos) != 14:
        return None
    if anos is None:
        y = date.today().year
        anos = [y - 1, y]

    conn = _conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT buscado_em FROM recursos_cache WHERE cnpj = %s", (digitos,))
        row = cur.fetchone()
        fresco = bool(row) and (date.today() - row["buscado_em"].date()).days < CACHE_DIAS

        if fresco and not force:
            return _resumo_recursos(digitos, _ler_recursos(cur, digitos))

        # sem permissão de bater na API: devolve cache (mesmo velho) se existir
        if not buscar_api:
            return _resumo_recursos(digitos, _ler_recursos(cur, digitos)) if row else None

        # cache frio/inexistente → busca na API (se possível)
        if not API_KEY:
            # sem chave: devolve cache antigo se existir, senão None
            rows = _ler_recursos(cur, digitos)
            return _resumo_recursos(digitos, rows) if row else None

        try:
            registros = []
            for ano in anos:
                pagina = 1
                while pagina <= max_paginas:
                    data = _get("despesas/recursos-recebidos", {
                        "mesAnoInicio": f"01/{ano}", "mesAnoFim": f"12/{ano}",
                        "codigoFavorecido": digitos, "pagina": pagina,
                    })
                    if not data:
                        break
                    registros.extend(data)
                    if len(data) < PAGE_SIZE:
                        break
                    pagina += 1
                    time.sleep(PAUSA)
        except Exception:
            # API caiu: devolve cache antigo se existir
            rows = _ler_recursos(cur, digitos)
            return _resumo_recursos(digitos, rows) if row else None

        # persiste (substitui o conjunto do CNPJ)
        cur.execute("DELETE FROM recursos_federais WHERE cnpj = %s", (digitos,))
        vals = [(
            digitos, r.get("anoMes"),
            str(r.get("codigoOrgao") or ""), r.get("nomeOrgao"),
            str(r.get("codigoUG") or ""), r.get("nomeUG"),
            r.get("valor") or 0,
        ) for r in registros]
        if vals:
            psycopg2.extras.execute_values(cur,
                """INSERT INTO recursos_federais
                   (cnpj, ano_mes, cod_orgao, nome_orgao, cod_ug, nome_ug, valor) VALUES %s""",
                vals)
        total = sum(float(r.get("valor") or 0) for r in registros)
        cur.execute(
            """INSERT INTO recursos_cache (cnpj, total, registros, buscado_em)
               VALUES (%s, %s, %s, NOW())
               ON CONFLICT (cnpj) DO UPDATE
                 SET total = EXCLUDED.total, registros = EXCLUDED.registros, buscado_em = NOW()""",
            (digitos, total, len(registros)))
        conn.commit()
        return _resumo_recursos(digitos, _ler_recursos(cur, digitos))
    finally:
        conn.close()


def _ler_recursos(cur, cnpj):
    cur.execute(
        """SELECT ano_mes, cod_orgao, nome_orgao, cod_ug, nome_ug, valor
           FROM recursos_federais WHERE cnpj = %s ORDER BY ano_mes DESC, valor DESC""",
        (cnpj,))
    return [dict(r) for r in cur.fetchall()]


def _resumo_recursos(cnpj, rows):
    total = sum(float(r["valor"] or 0) for r in rows)
    por_orgao = {}
    for r in rows:
        k = r["nome_orgao"] or r["cod_orgao"] or "—"
        por_orgao[k] = por_orgao.get(k, 0.0) + float(r["valor"] or 0)
    top = sorted(({"orgao": k, "valor": v} for k, v in por_orgao.items()),
                 key=lambda x: -x["valor"])[:5]
    meses = sorted({str(r["ano_mes"]) for r in rows if r["ano_mes"]})
    periodo = None
    if meses:
        fmt = lambda m: f"{m[4:6]}/{m[:4]}"
        periodo = fmt(meses[0]) if len(meses) == 1 else f"{fmt(meses[0])}–{fmt(meses[-1])}"
    return {
        "cnpj": cnpj,
        "recebeu_recursos": total > 0,
        "total": round(total, 2),
        "registros": len(rows),
        "periodo": periodo,
        "top_orgaos": [{"orgao": t["orgao"], "valor": round(t["valor"], 2)} for t in top],
        "detalhe": [{
            "ano_mes": r["ano_mes"], "orgao": r["nome_orgao"],
            "unidade_gestora": r["nome_ug"], "valor": float(r["valor"] or 0),
        } for r in rows[:50]],
    }


# ─── Novo Bolsa Família por município (gasto social executado) ───────────────
def bolsa_familia_municipio(ibge, meses=6, force=False):
    """Cache-first. Últimos `meses` disponíveis de Novo Bolsa Família no município.
    A fonte tem defasagem ~2 meses; buscamos a partir de (hoje - 2 meses)."""
    ibge = "".join(ch for ch in str(ibge) if ch.isdigit())
    if len(ibge) < 6:
        return None

    # meses-alvo: AAAAMM de (hoje-2) voltando `meses`
    alvo = []
    y, m = date.today().year, date.today().month
    m -= 2
    while m <= 0:
        m += 12
        y -= 1
    for _ in range(meses):
        alvo.append(y * 100 + m)
        m -= 1
        if m == 0:
            m = 12
            y -= 1

    conn = _conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT ano_mes, valor, beneficiarios, importado_em FROM bolsa_familia_municipio "
            "WHERE codigo_ibge = %s AND ano_mes = ANY(%s) ORDER BY ano_mes DESC",
            (ibge, alvo))
        cache = {r["ano_mes"]: dict(r) for r in cur.fetchall()}

        faltando = [am for am in alvo if am not in cache]
        # revalida meses cacheados há mais de CACHE_DIAS
        for am, r in cache.items():
            if (date.today() - r["importado_em"].date()).days >= CACHE_DIAS:
                faltando.append(am)

        if faltando and API_KEY:
            for am in sorted(set(faltando)):
                try:
                    data = _get("novo-bolsa-familia-por-municipio",
                                {"mesAno": str(am), "codigoIbge": ibge, "pagina": 1})
                except Exception:
                    continue
                valor = sum(float(d.get("valor") or 0) for d in data)
                benef = sum(int(d.get("quantidadeBeneficiados") or 0) for d in data)
                cur.execute(
                    """INSERT INTO bolsa_familia_municipio (codigo_ibge, ano_mes, valor, beneficiarios, importado_em)
                       VALUES (%s, %s, %s, %s, NOW())
                       ON CONFLICT (codigo_ibge, ano_mes) DO UPDATE
                         SET valor = EXCLUDED.valor, beneficiarios = EXCLUDED.beneficiarios, importado_em = NOW()""",
                    (ibge, am, valor, benef))
                cache[am] = {"ano_mes": am, "valor": valor, "beneficiarios": benef}
                time.sleep(PAUSA)
            conn.commit()

        rows = [cache[am] for am in alvo if am in cache]
        total = sum(float(r["valor"] or 0) for r in rows)
        ult = rows[0] if rows else None
        return {
            "codigo_ibge": ibge,
            "meses": [{
                "ano_mes": r["ano_mes"], "valor": float(r["valor"] or 0),
                "beneficiarios": int(r["beneficiarios"] or 0),
            } for r in rows],
            "total_periodo": round(total, 2),
            "ultimo_mes": ult["ano_mes"] if ult else None,
            "ultimo_valor": float(ult["valor"]) if ult else 0.0,
            "ultimo_beneficiarios": int(ult["beneficiarios"]) if ult else 0,
        }
    finally:
        conn.close()


def _ibge_municipios():
    """Lista de todos os municípios do país (IBGE localidades) → [(codigo_ibge, uf)]."""
    r = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios",
                     timeout=60)
    r.raise_for_status()
    out = []
    for m in r.json():
        cod = str(m.get("id"))
        uf = (((m.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF") or {}).get("sigla")
        if cod:
            out.append((cod, uf))
    return out


def _mes_bf_disponivel():
    """Descobre o mês mais recente com dado de Novo Bolsa Família (testa de hoje-1 p/ trás)."""
    if not API_KEY:
        return None
    y, m = date.today().year, date.today().month
    for _ in range(6):
        m -= 1
        if m == 0:
            m, y = 12, y - 1
        am = y * 100 + m
        try:
            # município grande garante retorno se o mês existe (São Paulo capital = 3550308)
            data = _get("novo-bolsa-familia-por-municipio",
                        {"mesAno": str(am), "codigoIbge": "3550308", "pagina": 1})
            if data:
                return am
        except Exception:
            continue
    return None


def backfill_bolsa_familia(mes_ano=None, log=print):
    """Backfill de Novo Bolsa Família p/ TODOS os municípios (resumível: pula os já
    gravados no mês). Roda no VPS; ~5.570 municípios × PAUSA ≈ 75 min."""
    if not API_KEY:
        log("[ERRO] sem TRANSPARENCIA_API_KEY"); return
    am = mes_ano or _mes_bf_disponivel()
    if not am:
        log("[ERRO] não achei mês disponível de Bolsa Família"); return
    municipios = _ibge_municipios()
    log(f"[INFO] mês {am} · {len(municipios)} municípios")

    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT codigo_ibge FROM bolsa_familia_municipio WHERE ano_mes = %s", (am,))
    ja = {r[0] for r in cur.fetchall()}
    log(f"[INFO] já gravados neste mês: {len(ja)} — retomando")

    ok = 0
    for i, (cod, _uf) in enumerate(municipios, 1):
        if cod in ja:
            continue
        try:
            data = _get("novo-bolsa-familia-por-municipio",
                        {"mesAno": str(am), "codigoIbge": cod, "pagina": 1})
        except Exception as e:
            log(f"[{i}/{len(municipios)}] {cod} erro: {repr(e)[:80]}")
            continue
        valor = sum(float(d.get("valor") or 0) for d in data)
        benef = sum(int(d.get("quantidadeBeneficiados") or 0) for d in data)
        cur.execute(
            """INSERT INTO bolsa_familia_municipio (codigo_ibge, ano_mes, valor, beneficiarios, importado_em)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (codigo_ibge, ano_mes) DO UPDATE
                 SET valor = EXCLUDED.valor, beneficiarios = EXCLUDED.beneficiarios, importado_em = NOW()""",
            (cod, am, valor, benef))
        ok += 1
        if ok % 100 == 0:
            conn.commit()
            log(f"[{i}/{len(municipios)}] gravados {ok} (último {cod}: R$ {valor:,.0f})")
        time.sleep(PAUSA)
    conn.commit()
    conn.close()
    log(f"[DONE] backfill BF mês {am}: {ok} municípios gravados.")


if __name__ == "__main__":
    import sys, json
    if not API_KEY:
        sys.exit("Defina TRANSPARENCIA_API_KEY.")
    if len(sys.argv) >= 3 and sys.argv[1] == "cnpj":
        print(json.dumps(recursos_por_cnpj(sys.argv[2], force=True), ensure_ascii=False, indent=2))
    elif len(sys.argv) >= 3 and sys.argv[1] == "bf":
        print(json.dumps(bolsa_familia_municipio(sys.argv[2], force=True), ensure_ascii=False, indent=2))
    elif len(sys.argv) >= 2 and sys.argv[1] == "backfill-bf":
        mes = int(sys.argv[2]) if len(sys.argv) >= 3 else None
        backfill_bolsa_familia(mes)
    else:
        print("uso: transparencia_etl.py [cnpj <cnpj> | bf <ibge> | backfill-bf [AAAAMM]]")
