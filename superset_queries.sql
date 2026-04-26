-- ═══════════════════════════════════════════════════════════════
-- SUPERSET — Queries para dashboards de licitações
-- Banco: pncp_db (PostgreSQL porta 5433)
-- ═══════════════════════════════════════════════════════════════

-- ─── 1. Total licitações + valor por mês ────────────────────────────────────
-- Gráfico: linha dupla (qtd + valor)
SELECT
    DATE_TRUNC('month', data_publicacao)::DATE  AS mes,
    COUNT(*)                                     AS total_licitacoes,
    SUM(valor_estimado)                          AS valor_total,
    AVG(valor_estimado)                          AS valor_medio
FROM licitacoes
WHERE data_publicacao >= NOW() - INTERVAL '12 months'
GROUP BY 1
ORDER BY 1;

-- ─── 2. Ranking de municípios por valor licitado ─────────────────────────────
-- Gráfico: bar chart horizontal
SELECT
    municipio_nome                          AS municipio,
    uf,
    COUNT(*)                                AS total,
    SUM(valor_estimado)                     AS valor_total,
    ROUND(AVG(valor_estimado)::NUMERIC, 2)  AS valor_medio
FROM licitacoes
WHERE data_publicacao >= NOW() - INTERVAL '90 days'
  AND municipio_nome IS NOT NULL
GROUP BY municipio_nome, uf
ORDER BY valor_total DESC
LIMIT 30;

-- ─── 3. Top palavras no objeto da licitação ──────────────────────────────────
-- Gráfico: word cloud / bar chart
SELECT
    palavra,
    SUM(frequencia)   AS freq_total,
    SUM(valor_total)  AS valor_associado
FROM top_palavras
WHERE mes_ref = TO_CHAR(NOW(), 'YYYY-MM')
GROUP BY palavra
ORDER BY freq_total DESC
LIMIT 20;

-- ─── 4. Distribuição por modalidade ──────────────────────────────────────────
-- Gráfico: pie / donut
SELECT
    modalidade_nome,
    COUNT(*)           AS total,
    SUM(valor_estimado) AS valor_total,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM licitacoes
WHERE data_publicacao >= NOW() - INTERVAL '30 days'
GROUP BY modalidade_nome
ORDER BY total DESC;

-- ─── 5. Licitações abertas agora ─────────────────────────────────────────────
-- Tabela: monitor de oportunidades
SELECT
    orgao_nome,
    municipio_nome,
    uf,
    objeto,
    valor_estimado,
    modalidade_nome,
    data_publicacao,
    data_encerramento,
    EXTRACT(DAY FROM (data_encerramento::TIMESTAMPTZ - NOW())) AS dias_restantes,
    url_pncp
FROM licitacoes
WHERE data_encerramento >= NOW()::DATE
  AND valor_estimado > 0
ORDER BY data_encerramento ASC
LIMIT 100;

-- ─── 6. Evolução diária de importação (saúde do ETL) ─────────────────────────
-- Gráfico: área
SELECT
    executado_em::DATE  AS dia,
    uf,
    SUM(inseridos)      AS inseridos,
    SUM(erros)          AS erros,
    AVG(duracao_seg)    AS duracao_media
FROM etl_log
WHERE executado_em >= NOW() - INTERVAL '30 days'
GROUP BY 1, 2
ORDER BY 1, 2;

-- ─── 7. Heatmap UF × Mês ────────────────────────────────────────────────────
-- Gráfico: heatmap
SELECT
    uf,
    DATE_TRUNC('month', data_publicacao)::DATE  AS mes,
    COUNT(*)                                     AS total,
    SUM(valor_estimado)                          AS valor_total
FROM licitacoes
WHERE data_publicacao >= NOW() - INTERVAL '6 months'
GROUP BY uf, mes
ORDER BY mes, uf;

-- ─── 8. Órgãos que mais licitam (CNPJ) ───────────────────────────────────────
-- Tabela: fornecedores recorrentes
SELECT
    orgao_cnpj,
    orgao_nome,
    uf,
    COUNT(*)             AS total_licitacoes,
    SUM(valor_estimado)  AS valor_total,
    MIN(data_publicacao) AS primeira,
    MAX(data_publicacao) AS ultima
FROM licitacoes
WHERE orgao_cnpj != ''
GROUP BY orgao_cnpj, orgao_nome, uf
ORDER BY total_licitacoes DESC
LIMIT 50;

-- ─── 9. KPIs do dashboard principal ──────────────────────────────────────────
-- Big numbers
SELECT
    COUNT(*)                                                          AS total_geral,
    SUM(valor_estimado)                                               AS valor_total_geral,
    COUNT(*) FILTER (WHERE data_publicacao >= NOW() - INTERVAL '7 days')  AS ultimos_7_dias,
    COUNT(*) FILTER (WHERE data_encerramento >= NOW()::DATE)          AS abertas_agora,
    COUNT(DISTINCT municipio_ibge)                                    AS municipios_cobertos,
    COUNT(DISTINCT uf)                                                AS ufs_cobertas
FROM licitacoes;
