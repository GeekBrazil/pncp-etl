"""Regularidade rural do município — CAR (SICAR) somado por município + contorno do IBGE.

Fontes (públicas, sem login):
  - CAR: GeoServer do SICAR, uma camada por UF `sicar:sicar_imoveis_<uf>`
    https://geoserver.car.gov.br/geoserver/sicar/wfs
    Campos: cod_imovel, status_imovel (AT ativo, PE pendente, SU suspenso,
    CA cancelado), area (ha), condicao (texto da análise), tipo_imovel
    (IRU imóvel rural, AST assentamento, PCT povos e comunidades tradicionais),
    m_fiscal (quantos módulos fiscais o imóvel tem), cod_municipio_ibge.
  - Contorno: IBGE malhas v3 (qualidade intermediária, ~10 KB por município).
INCRA/Sigef ficou de fora: o download exige login gov.br desde 2026.

Cache-first na tabela `rural_cache` (chave `car:<ibge>` / `malha:<ibge>`),
revalidado a cada CACHE_DIAS. Sem geometria do CAR aqui: o mapa usa o WMS
do próprio SICAR direto no navegador.
"""
import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")
WFS = "https://geoserver.car.gov.br/geoserver/sicar/wfs"
MALHA = "https://servicodados.ibge.gov.br/api/v3/malhas/municipios/{ibge}"
CACHE_DIAS = 30
PAGINA = 5000
TIMEOUT = 90
HEADERS = {"User-Agent": "allancandido.com (dados abertos)"}

# 2 primeiros dígitos do código IBGE → UF
UF_POR_CODIGO = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}

DDL = """
CREATE TABLE IF NOT EXISTS rural_cache (
    chave        TEXT PRIMARY KEY,
    dados        JSONB NOT NULL,
    importado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _ibge(valor):
    d = "".join(ch for ch in str(valor) if ch.isdigit())
    return d if len(d) == 7 and d[:2] in UF_POR_CODIGO else None


def _cache(chave, buscar, force=False):
    """Devolve o JSON guardado se tiver menos de CACHE_DIAS; senão chama buscar().
    Se a fonte cair, devolve o cache velho em vez de nada."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(DDL)
        cur.execute("SELECT dados, importado_em FROM rural_cache WHERE chave = %s", (chave,))
        linha = cur.fetchone()
        idade = (datetime.now(timezone.utc) - linha["importado_em"]).days if linha else None
        if linha and not force and idade < CACHE_DIAS:
            return linha["dados"]
        try:
            dados = buscar()
        except (requests.RequestException, ValueError):
            return linha["dados"] if linha else None
        cur.execute("""
            INSERT INTO rural_cache (chave, dados, importado_em) VALUES (%s, %s, NOW())
            ON CONFLICT (chave) DO UPDATE SET dados = EXCLUDED.dados, importado_em = NOW()""",
            (chave, json.dumps(dados)))
        conn.commit()
        return dados
    finally:
        conn.close()


def _imoveis_car(ibge, uf):
    """Todos os imóveis do CAR do município, só atributos (sem geometria), paginado."""
    props = "cod_imovel,status_imovel,area,condicao,tipo_imovel,m_fiscal,municipio"
    out, inicio = [], 0
    while True:
        r = requests.get(WFS, headers=HEADERS, timeout=TIMEOUT, params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": f"sicar:sicar_imoveis_{uf.lower()}", "outputFormat": "application/json",
            "propertyName": props, "CQL_FILTER": f"cod_municipio_ibge={ibge}",
            "sortBy": "cod_imovel", "count": PAGINA, "startIndex": inicio,
        })
        r.raise_for_status()
        feats = r.json().get("features", [])
        out.extend(f["properties"] for f in feats)
        if len(feats) < PAGINA:
            return out
        inicio += PAGINA


def _analise(condicao):
    """Agrupa o texto livre da análise do CAR em 5 faixas legíveis."""
    c = (condicao or "").lower()
    if c.startswith("cancelado"):
        return "cancelado"
    if "conformidade" in c:
        return "conforme"
    if "regulariza" in c:
        return "regularizacao"
    if "notifica" in c:
        return "notificacao"
    return "aguardando"  # "Aguardando análise", "Em análise", sem texto


def _resumir(ibge, uf, imoveis):
    def soma(lista):
        return {"n": len(lista), "area_ha": round(sum(float(i.get("area") or 0) for i in lista), 1)}

    vigentes = [i for i in imoveis if i.get("status_imovel") != "CA"]
    # m_fiscal = quantos módulos fiscais o imóvel tem; área ÷ m_fiscal dá o módulo do município
    razoes = sorted(float(i["area"]) / float(i["m_fiscal"]) for i in imoveis
                    if i.get("area") and i.get("m_fiscal") and float(i["m_fiscal"]) > 0)
    mf = round(razoes[len(razoes) // 2], 1) if razoes else None

    def porte(i):
        if i.get("m_fiscal") is None:
            return None
        modulos = float(i["m_fiscal"])
        return "pequena" if modulos <= 4 else "media" if modulos <= 15 else "grande"

    return {
        "ibge": ibge, "uf": uf,
        "municipio": next((i["municipio"] for i in imoveis if i.get("municipio")), None),
        "total": len(imoveis),
        "area_ha": soma(imoveis)["area_ha"],
        "vigentes": soma(vigentes),
        "status": {s: soma([i for i in imoveis if i.get("status_imovel") == s]) for s in ("AT", "PE", "SU", "CA")},
        "tipos": {t: soma([i for i in imoveis if i.get("tipo_imovel") == t]) for t in ("IRU", "AST", "PCT")},
        "analise": {a: soma([i for i in vigentes if _analise(i.get("condicao")) == a])
                    for a in ("aguardando", "notificacao", "regularizacao", "conforme")},
        # porte pela Lei 8.629/93: pequena até 4 módulos fiscais, média até 15
        "porte": {p: soma([i for i in vigentes if porte(i) == p]) for p in ("pequena", "media", "grande")},
        "modulo_fiscal_ha": mf,
        "consultado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fonte": "SICAR — Serviço Florestal Brasileiro (geoserver.car.gov.br)",
    }


def regularidade_municipio(ibge, force=False):
    """Resumo do CAR do município (cache de 30 dias). None se o código for inválido
    ou a fonte nunca respondeu."""
    ibge = _ibge(ibge)
    if not ibge:
        return None
    uf = UF_POR_CODIGO[ibge[:2]]
    return _cache(f"car:{ibge}", lambda: _resumir(ibge, uf, _imoveis_car(ibge, uf)), force)


def malha_municipio(ibge, force=False):
    """Contorno do município em GeoJSON (IBGE), cache de 30 dias."""
    ibge = _ibge(ibge)
    if not ibge:
        return None

    def buscar():
        r = requests.get(MALHA.format(ibge=ibge), headers=HEADERS, timeout=TIMEOUT,
                         params={"formato": "application/vnd.geo+json", "qualidade": "intermediaria"})
        r.raise_for_status()
        return r.json()

    return _cache(f"malha:{ibge}", buscar, force)


if __name__ == "__main__":
    import sys
    for cod in sys.argv[1:] or ["3300100"]:
        print(json.dumps(regularidade_municipio(cod, force=True), ensure_ascii=False, indent=2))
