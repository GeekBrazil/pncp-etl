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
    telefone           TEXT,
    email              TEXT,
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

-- ── Radar de Loteamentos (crescimento populacional + fiscal + infra) ────────
-- Materializado pelo radar_loteamento_etl.py; leitura direta pelo dashboard.
CREATE TABLE IF NOT EXISTS radar_loteamento (
    municipio_ibge      TEXT PRIMARY KEY,
    municipio_nome      TEXT,
    uf                  VARCHAR(2),
    pop_inicial         BIGINT,      -- estimativa IBGE ano inicial
    pop_final           BIGINT,      -- estimativa IBGE ano final
    ano_inicial         SMALLINT,
    ano_final           SMALLINT,
    crescimento_pct     NUMERIC(8,3),   -- variação % no período
    receita_per_capita  NUMERIC(15,4),  -- de score_municipios
    infra_valor_12m     NUMERIC(18,2),  -- licitações de infraestrutura no período coletado
    score               NUMERIC(8,3),   -- 0-100, fórmula documentada no ETL
    atualizado_em       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_radar_uf ON radar_loteamento(uf);
CREATE INDEX IF NOT EXISTS idx_radar_score ON radar_loteamento(score);

-- ── Busca por palavra-chave no objeto (ferramenta Mercado Público) ─────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_lic_objeto_trgm ON licitacoes USING gin (objeto gin_trgm_ops);

-- ── Comércio exterior por município (Comex Stat / MDIC) ─────────────────────
CREATE TABLE IF NOT EXISTS comex_municipios (
    municipio_nome  TEXT,
    uf              VARCHAR(2),
    fluxo           VARCHAR(6),     -- 'export' | 'import'
    ano             SMALLINT,
    fob_usd         NUMERIC(18,2),
    atualizado_em   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (municipio_nome, uf, fluxo, ano)
);
CREATE INDEX IF NOT EXISTS idx_comex_uf ON comex_municipios(uf);

-- ── Perfil rural por município (IBGE Censo Agropecuário 2017) ───────────────
CREATE TABLE IF NOT EXISTS agro_municipios (
    municipio_ibge   TEXT PRIMARY KEY,
    municipio_nome   TEXT,
    uf               VARCHAR(2),
    estabelecimentos INT,           -- nº de estabelecimentos agropecuários
    atualizado_em    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_agro_uf ON agro_municipios(uf);

-- ── Termos em alta nas compras públicas (radar de conteúdo) ─────────────────
-- Materializado pelo trends_etl.py: freq/valor recente vs período anterior.
CREATE TABLE IF NOT EXISTS trends_termos (
    termo           TEXT PRIMARY KEY,
    freq_recente    INT,
    freq_anterior   INT,
    valor_recente   NUMERIC(18,2),
    variacao_pct    NUMERIC(8,2),   -- crescimento % recente vs anterior
    abertas         INT,            -- oportunidades abertas com o termo
    atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

-- ── Sanções (Portal da Transparência: CEIS, CNEP, CEPIM) ──────────────────────
-- Empresas/pessoas impedidas de licitar/contratar. Base do cruzamento
-- investigativo (fornecedor sancionado) e da consulta de risco no site.
CREATE TABLE IF NOT EXISTS sancoes (
    id                BIGINT PRIMARY KEY,     -- id do registro na Transparência
    cadastro          VARCHAR(10),            -- CEIS | CNEP | CEPIM
    cnpj              VARCHAR(14),            -- só dígitos (casa com empresas.cnpj); NULL se pessoa física
    cpf               VARCHAR(20),            -- mascarado pela fonte quando PF
    nome              TEXT,
    tipo_pessoa       TEXT,
    tipo_sancao       TEXT,
    orgao_sancionador TEXT,
    fonte             TEXT,                   -- nomeExibicao da fonte da sanção
    data_inicio       DATE,
    data_fim          DATE,
    data_publicacao   DATE,
    fundamentacao     TEXT,
    link_publicacao   TEXT,
    processo          TEXT,
    importado_em      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sancoes_cnpj     ON sancoes(cnpj);
CREATE INDEX IF NOT EXISTS idx_sancoes_cadastro ON sancoes(cadastro);
CREATE INDEX IF NOT EXISTS idx_sancoes_fim      ON sancoes(data_fim);

-- Progresso resumível da carga (uma linha por cadastro)
CREATE TABLE IF NOT EXISTS sancoes_progress (
    cadastro       VARCHAR(10) PRIMARY KEY,
    ultima_pagina  INT DEFAULT 0,
    concluido      BOOLEAN DEFAULT FALSE,
    total_gravado  INT DEFAULT 0,
    atualizado_em  TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Portal da Transparência (CGU) — gasto executado (5º pilar) ──────────────
-- Recursos federais recebidos por CNPJ (despesas/recursos-recebidos). Cache on-demand.
CREATE TABLE IF NOT EXISTS recursos_federais (
    cnpj         VARCHAR(14),
    ano_mes      INT,               -- AAAAMM
    cod_orgao    VARCHAR(20),
    nome_orgao   TEXT,
    cod_ug       VARCHAR(20),
    nome_ug      TEXT,
    valor        NUMERIC(16,2),
    importado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_recursos_cnpj ON recursos_federais(cnpj);

-- Marcador de cache por CNPJ (evita rebater a API a cada consulta).
CREATE TABLE IF NOT EXISTS recursos_cache (
    cnpj       VARCHAR(14) PRIMARY KEY,
    total      NUMERIC(18,2),
    registros  INT,
    buscado_em TIMESTAMPTZ DEFAULT NOW()
);

-- Novo Bolsa Família por município (gasto social executado). Cache on-demand.
CREATE TABLE IF NOT EXISTS bolsa_familia_municipio (
    codigo_ibge   VARCHAR(7),
    ano_mes       INT,              -- AAAAMM
    valor         NUMERIC(16,2),
    beneficiarios INT,
    importado_em  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (codigo_ibge, ano_mes)
);

-- ─── Diretório de imobiliárias + base de mercado (preços coletados) ──────────
CREATE TABLE IF NOT EXISTS imobiliarias (
    id            SERIAL PRIMARY KEY,
    creci         VARCHAR(30),
    nome          TEXT,
    site          TEXT,
    telefone      VARCHAR(40),
    email         TEXT,
    cidade        TEXT,
    uf            VARCHAR(2),
    endereco      TEXT,
    coletavel     BOOLEAN,            -- listagem pública acessível p/ coleta?
    dominio       TEXT,               -- host do site (dedup)
    obs           TEXT,
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (creci),
    UNIQUE (dominio)
);
CREATE INDEX IF NOT EXISTS idx_imob_cidade ON imobiliarias(cidade, uf);

CREATE TABLE IF NOT EXISTS imoveis_mercado (
    id             SERIAL PRIMARY KEY,
    imobiliaria_id INT REFERENCES imobiliarias(id) ON DELETE SET NULL,
    fonte          TEXT,              -- domínio/portal de origem
    origem         VARCHAR(10),       -- anuncio | itbi
    finalidade     VARCHAR(10),       -- venda | aluguel
    tipo           TEXT,              -- casa | apartamento | terreno | ...
    preco          NUMERIC(14,2),
    area           NUMERIC(10,2),
    preco_m2       NUMERIC(12,2),
    quartos        INT,
    bairro         TEXT,
    cidade         TEXT,
    uf             VARCHAR(2),
    url            TEXT UNIQUE,
    lat            NUMERIC(9,6),
    lng            NUMERIC(9,6),
    coletado_em    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_merc_local ON imoveis_mercado(uf, cidade, bairro);
CREATE INDEX IF NOT EXISTS idx_merc_tipo  ON imoveis_mercado(tipo);
CREATE INDEX IF NOT EXISTS idx_merc_preco ON imoveis_mercado(preco);

-- Leads de imobiliárias achadas via CNAE de corretagem (cnpj_imob_finder.py) —
-- entram aqui mesmo sem site (o coletor só raspa depois de alguém achar o domínio).
CREATE TABLE IF NOT EXISTS leads_imobiliarias (
    cnpj         TEXT PRIMARY KEY REFERENCES empresas(cnpj) ON DELETE CASCADE,
    cidade_alvo  TEXT,
    uf           VARCHAR(2),
    alertado     BOOLEAN DEFAULT FALSE,
    capturado_em TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_leads_imob_alertado ON leads_imobiliarias(alertado);

-- ── Contratos do PNCP (API PNCP /api/consulta/v1/contratos) ──────────────────
CREATE TABLE IF NOT EXISTS contratos (
    numero_controle_pncp TEXT PRIMARY KEY,
    cnpj_fornecedor      VARCHAR(14),            -- CNPJ do contratado/vencedor (niFornecedor)
    nome_fornecedor      TEXT,                   -- razão social do fornecedor
    orgao_cnpj           VARCHAR(18),            -- CNPJ do órgão contratante
    orgao_nome           TEXT,                   -- Nome do órgão contratante
    objeto               TEXT,                   -- Objeto do contrato
    valor_inicial        NUMERIC(15,2),
    valor_global         NUMERIC(15,2),
    data_assinatura      DATE,
    data_vigencia_inicio DATE,
    data_vigencia_fim    DATE,
    uf                   VARCHAR(2),
    municipio_ibge       VARCHAR(7),
    municipio_nome       TEXT,
    url_pncp             TEXT,
    importado_em         TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_contratos_cnpj       ON contratos(cnpj_fornecedor);
CREATE INDEX IF NOT EXISTS idx_contratos_uf         ON contratos(uf);
CREATE INDEX IF NOT EXISTS idx_contratos_assinatura ON contratos(data_assinatura);

-- Progresso incremental da carga de contratos
CREATE TABLE IF NOT EXISTS contratos_progress (
    data_processada DATE PRIMARY KEY,
    concluido       BOOLEAN DEFAULT FALSE,
    total_gravado   INT DEFAULT 0,
    atualizado_em   TIMESTAMPTZ DEFAULT NOW()
);

