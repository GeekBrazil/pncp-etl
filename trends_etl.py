#!/usr/bin/env python3
"""
Termos em alta nas compras públicas — radar de conteúdo (só nossos dados).
Compara frequência/valor de termos-setor nos últimos 30 dias vs os 30 anteriores
e grava em trends_termos. Termos curados (setores relevantes), sem extração
ruidosa de palavras. Rode diário via cron/job.
Uso:
  python3 trends_etl.py --atualizar
"""
import argparse, os, sys
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pncp_db")

# Setores/produtos com apelo de conteúdo e mercado (base: pesquisa do vault +
# categorias mais licitadas). Cada um vira um "termo em alta" medido.
TERMOS = [
    "energia solar", "placa solar", "usina fotovoltaica", "iluminação pública led",
    "merenda escolar", "uniforme escolar", "material escolar", "transporte escolar",
    "medicamento", "vacina", "equipamento hospitalar", "ambulância", "prótese",
    "pavimentação", "recapeamento", "drenagem", "saneamento", "esgoto",
    "software", "licença de software", "nuvem", "inteligência artificial", "câmera de segurança",
    "veículo", "caminhão", "trator", "ônibus", "combustível", "pneu",
    "obra", "reforma", "construção de escola", "construção de creche", "quadra poliesportiva",
    "coleta de lixo", "limpeza urbana", "vigilância", "internet", "fibra óptica",
    "cesta básica", "gêneros alimentícios", "café", "água mineral",
    "consultoria", "publicidade", "evento", "show", "locação de veículo",
    "poço artesiano", "asfalto", "ponte", "praça",
]


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def atualizar():
    conn = get_conn()
    n = 0
    with conn.cursor() as cur:
        for termo in TERMOS:
            padrao = f"%{termo}%"
            cur.execute("""
                SELECT
                  count(*) FILTER (WHERE data_publicacao >= CURRENT_DATE - 30) AS recente,
                  count(*) FILTER (WHERE data_publicacao >= CURRENT_DATE - 60 AND data_publicacao < CURRENT_DATE - 30) AS anterior,
                  COALESCE(SUM(valor_estimado) FILTER (WHERE data_publicacao >= CURRENT_DATE - 30 AND valor_estimado <= 500000000), 0) AS valor_rec,
                  count(*) FILTER (WHERE data_encerramento >= CURRENT_DATE) AS abertas
                FROM licitacoes WHERE objeto ILIKE %s
            """, (padrao,))
            rec, ant, valor, abertas = cur.fetchone()
            if (rec or 0) + (ant or 0) == 0:
                continue
            # variação %: se anterior=0 e recente>0 → 100% (novo); senão (rec-ant)/ant
            if ant and ant > 0:
                variacao = round((rec - ant) * 100.0 / ant, 2)
            else:
                variacao = 100.0 if rec else 0.0
            cur.execute("""
                INSERT INTO trends_termos (termo, freq_recente, freq_anterior, valor_recente, variacao_pct, abertas, atualizado_em)
                VALUES (%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (termo) DO UPDATE SET
                    freq_recente=EXCLUDED.freq_recente, freq_anterior=EXCLUDED.freq_anterior,
                    valor_recente=EXCLUDED.valor_recente, variacao_pct=EXCLUDED.variacao_pct,
                    abertas=EXCLUDED.abertas, atualizado_em=NOW()
            """, (termo, rec, ant, valor, variacao, abertas))
            n += 1
    conn.commit()
    conn.close()
    print(f"🏁 Trends atualizado: {n} termos medidos.")


def main():
    p = argparse.ArgumentParser(description="Termos em alta nas compras públicas")
    p.add_argument("--atualizar", action="store_true")
    args = p.parse_args()
    if not args.atualizar:
        p.error("Especifique --atualizar")
    atualizar()


if __name__ == "__main__":
    main()
