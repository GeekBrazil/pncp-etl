-- ETL PNCP — Schema PostgreSQL (porta 5433)
-- Executar: psql -h localhost -p 5433 -U postgres -f schema.sql

CREATE DATABASE pncp_db;
\c pncp_db;

-- Licitações brutas vindas do PNCP
CREATE TABLE IF NOT EXISTS licitacoes (
    pncp_id              TEXT PRIMARY KEY,
    municipio_ibge       VARCHAR(7),
    municipio_nome       TEXT,
    uf                   VARCHAR(2),
    orgao_cnpj           VARCHAR(18),
    orgao_nome           TEXT,
    objeto               TEXT,
    valor_estimado       NUMERIC(15,2),
    modalidade_id        SMALLINT,
    modalidade_nome      TEXT,
    data_publicacao      DATE,
    data_encerramento    DATE,
    situacao             TEXT,
    url_pncp             TEXT,
    importado_em         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lic_uf         ON licitacoes(uf);
CREATE INDEX IF NOT EXISTS idx_lic_municipio  ON licitacoes(municipio_ibge);
CREATE INDEX IF NOT EXISTS idx_lic_data       ON licitacoes(data_publicacao);
CREATE INDEX IF NOT EXISTS idx_lic_modalidade ON licitacoes(modalidade_id);
CREATE INDEX IF NOT EXISTS idx_lic_valor      ON licitacoes(valor_estimado);

-- Log de execuções do ETL
CREATE TABLE IF NOT EXISTS etl_log (
    id            SERIAL PRIMARY KEY,
    executado_em  TIMESTAMPTZ DEFAULT NOW(),
    uf            VARCHAR(2),
    data_inicial  DATE,
    data_final    DATE,
    paginas_lidas INT,
    inseridos     INT,
    duplicados    INT,
    erros         INT,
    duracao_seg   NUMERIC(8,2),
    status        TEXT  -- 'ok' | 'parcial' | 'erro'
);

-- View: resumo por município (para Superset/Metabase)
CREATE OR REPLACE VIEW v_resumo_municipio AS
SELECT
    municipio_ibge,
    municipio_nome,
    uf,
    COUNT(*)                                AS total_licitacoes,
    SUM(valor_estimado)                     AS valor_total,
    AVG(valor_estimado)                     AS valor_medio,
    MAX(data_publicacao)                    AS ultima_publicacao,
    COUNT(*) FILTER (WHERE data_encerramento >= NOW()::DATE) AS abertas
FROM licitacoes
GROUP BY municipio_ibge, municipio_nome, uf;

-- View: top objetos (palavras mais frequentes — pré-calculado)
CREATE TABLE IF NOT EXISTS top_palavras (
    id           SERIAL PRIMARY KEY,
    uf           VARCHAR(2),
    municipio_ibge VARCHAR(7),
    palavra      TEXT,
    frequencia   INT,
    valor_total  NUMERIC(15,2),
    mes_ref      VARCHAR(7),  -- YYYY-MM
    UNIQUE(uf, municipio_ibge, palavra, mes_ref)
);

-- View: licitações abertas (útil para monitoramento)
CREATE OR REPLACE VIEW v_abertas AS
SELECT *
FROM licitacoes
WHERE data_encerramento >= NOW()::DATE
   OR data_encerramento IS NULL
ORDER BY data_publicacao DESC;
