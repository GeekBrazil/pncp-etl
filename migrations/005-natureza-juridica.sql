-- Natureza jurídica (Receita, Empresas*.zip + Naturezas.zip): separa MEI/EI de
-- LTDA/SA etc. Carregada por cnpj_nacional.py (rodar_empresas → carregar_naturezas).
CREATE TABLE IF NOT EXISTS naturezas (
    codigo    VARCHAR(4) PRIMARY KEY,
    descricao TEXT NOT NULL
);
ALTER TABLE empresas ADD COLUMN IF NOT EXISTS natureza_juridica VARCHAR(4);
