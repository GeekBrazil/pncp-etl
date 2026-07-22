"""
AVM — estimador de valor de referência por comparáveis.

Não é avaliação legal (NBR 14653); é estimativa de mercado a partir da base
imoveis_mercado. Estratégia: preço/m² mediano dos comparáveis, do recorte mais
específico pro mais amplo (bairro+tipo → bairro → cidade+tipo → cidade), com
faixa (p25–p75) e confiança pelo nº de comparáveis.
"""
import os
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://pncp:x@localhost:5433/pncp_db")


def _stats(cur, where, params):
    cur.execute(f"""
        SELECT count(*) n,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY preco_m2) AS med,
               percentile_cont(0.25) WITHIN GROUP (ORDER BY preco_m2) AS p25,
               percentile_cont(0.75) WITHIN GROUP (ORDER BY preco_m2) AS p75
        FROM imoveis_mercado
        WHERE preco_m2 IS NOT NULL AND preco_m2 > 0 AND {where}""", params)
    return cur.fetchone()


def avaliar(conn, cidade, tipo=None, area=None, bairro=None, uf=None):
    """Estima o valor de referência. Retorna recorte usado, preço/m² e faixa."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cidade_l = (cidade or "").strip()
    # níveis do mais específico ao mais amplo
    niveis = []
    if bairro and tipo:
        niveis.append(("bairro+tipo", "cidade ILIKE %s AND bairro ILIKE %s AND tipo = %s", [cidade_l, bairro, tipo]))
    if bairro:
        niveis.append(("bairro", "cidade ILIKE %s AND bairro ILIKE %s", [cidade_l, bairro]))
    if tipo:
        niveis.append(("cidade+tipo", "cidade ILIKE %s AND tipo = %s", [cidade_l, tipo]))
    niveis.append(("cidade", "cidade ILIKE %s", [cidade_l]))

    usado = None
    for nome, where, params in niveis:
        s = _stats(cur, where, params)
        if s and s["n"] and s["n"] >= 3:   # mínimo pra ter mediana com sentido
            usado = (nome, s)
            break
    if not usado:                          # sem 3 comparáveis: usa o mais amplo que houver
        s = _stats(cur, "cidade ILIKE %s", [cidade_l])
        usado = ("cidade", s)

    nome, s = usado
    n = s["n"] or 0
    med = float(s["med"]) if s["med"] else None
    p25 = float(s["p25"]) if s["p25"] else med
    p75 = float(s["p75"]) if s["p75"] else med
    conf = "alta" if n >= 8 else "média" if n >= 3 else "baixa"
    out = {
        "cidade": cidade_l, "bairro": bairro, "tipo": tipo, "area": area,
        "recorte": nome, "comparaveis": n, "confianca": conf,
        "preco_m2_mediano": round(med, 2) if med else None,
        "preco_m2_faixa": [round(p25, 2), round(p75, 2)] if med else None,
    }
    if area and med:
        out["valor_estimado"] = round(med * area, 2)
        out["valor_faixa"] = [round(p25 * area, 2), round(p75 * area, 2)]
    return out


if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    import json
    for args in [
        dict(cidade="Angra dos Reis", tipo="casa", area=150, bairro="Vila do Abraão"),
        dict(cidade="Angra dos Reis", tipo="casa", area=150),
        dict(cidade="Angra dos Reis", tipo="terreno", area=500),
        dict(cidade="Angra dos Reis", area=120),
    ]:
        print(json.dumps(avaliar(conn, **args), ensure_ascii=False))
    conn.close()
