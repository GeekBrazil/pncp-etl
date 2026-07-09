-- ETL PNCP — Schema PostgreSQL
-- Executar contra o banco já criado (ex: pelo serviço Postgres do Coolify):
--   psql "$DATABASE_URL" -f schema.sql

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

-- Progresso do backfill (12 meses) por modalidade — pra ser resumível se cair no meio.
CREATE TABLE IF NOT EXISTS sync_progress (
    modalidade_id         SMALLINT PRIMARY KEY,
    ultima_data_concluida DATE,
    atualizado_em         TIMESTAMPTZ DEFAULT NOW()
);

-- ── Enriquecimento de CNPJ (Receita Federal via BrasilAPI) ──────────────────
CREATE TABLE IF NOT EXISTS empresas (
    cnpj               TEXT PRIMARY KEY,
    razao_social       TEXT,
    nome_fantasia      TEXT,
    situacao_cadastral TEXT,
    data_situacao      DATE,
    cnae_principal     TEXT,
    cnae_descricao     TEXT,
    uf                 VARCHAR(2),
    municipio          TEXT,
    capital_social     NUMERIC(15,2),
    data_abertura      DATE,
    raw_json           JSONB,
    atualizado_em      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS socios (
    id                     SERIAL PRIMARY KEY,
    cnpj                   TEXT REFERENCES empresas(cnpj) ON DELETE CASCADE,
    nome_socio             TEXT,
    qualificacao           TEXT,
    data_entrada_sociedade DATE,
    faixa_etaria           TEXT,
    UNIQUE(cnpj, nome_socio, qualificacao)
);
CREATE INDEX IF NOT EXISTS idx_socios_nome ON socios(nome_socio);

-- ── Imóveis da União (inventários patrimoniais via dados.gov.br) ───────────
CREATE TABLE IF NOT EXISTS imoveis_uniao (
    id                  SERIAL PRIMARY KEY,
    fonte_dataset_id    TEXT,       -- id do conjunto de dados no dados.gov.br
    fonte_organizacao   TEXT,       -- ex: universidade-federal-do-rio-grande-do-norte-ufrn
    rip                 TEXT,       -- Registro Imobiliário Patrimonial (SPU), quando presente
    nome_imovel         TEXT,
    tipo_imovel         TEXT,
    municipio           TEXT,
    uf                  VARCHAR(2),
    endereco            TEXT,
    area_terreno        NUMERIC(15,2),
    valor_terreno       NUMERIC(15,2),
    area_construida     NUMERIC(15,2),
    valor_imovel        NUMERIC(15,2),
    forma_aquisicao     TEXT,
    raw_row             JSONB,      -- linha original completa (schemas variam por órgão)
    importado_em        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(fonte_dataset_id, rip, nome_imovel)
);
CREATE INDEX IF NOT EXISTS idx_imoveis_uniao_municipio ON imoveis_uniao(municipio);
CREATE INDEX IF NOT EXISTS idx_imoveis_uniao_org ON imoveis_uniao(fonte_organizacao);

-- ── Score Territorial (IBGE + SICONFI: receita per capita por município) ───
CREATE TABLE IF NOT EXISTS score_municipios (
    municipio_ibge      TEXT,
    municipio_nome      TEXT,
    uf                  VARCHAR(2),
    exercicio           SMALLINT,
    periodo             SMALLINT,
    populacao           BIGINT,
    receita_realizada   NUMERIC(18,2),
    receita_per_capita  NUMERIC(15,4),
    raw_json            JSONB,
    atualizado_em       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (municipio_ibge, exercicio, periodo)
);
CREATE INDEX IF NOT EXISTS idx_score_uf ON score_municipios(uf);
CREATE INDEX IF NOT EXISTS idx_score_percapita ON score_municipios(receita_per_capita);

CREATE TABLE IF NOT EXISTS score_progress (
    id            SMALLINT PRIMARY KEY DEFAULT 1,
    ultimo_ibge   TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
);
