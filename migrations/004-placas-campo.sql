-- Placas de venda/aluguel fotografadas em campo pelo Allan (bot Sofia, /placa).
-- Amostra de oferta de rua: inclui dono direto, que não aparece em portal.
-- Telefone e foto ficam só aqui; o site mostra apenas números agregados (LGPD).
CREATE TABLE IF NOT EXISTS placas_campo (
    id               SERIAL PRIMARY KEY,
    lat              DOUBLE PRECISION NOT NULL,
    lon              DOUBLE PRECISION NOT NULL,
    municipio_ibge   INTEGER,
    municipio_nome   TEXT,
    uf               CHAR(2),
    bairro           TEXT,
    rua              TEXT,
    finalidade       TEXT,          -- venda | aluguel | venda_e_aluguel | desconhecida
    anunciante       TEXT,          -- imobiliaria | proprietario | desconhecido
    nome_anunciante  TEXT,
    telefone         TEXT,          -- só dígitos, como lido na placa
    telefone_chave   TEXT,          -- DDD + últimos 8 dígitos (casa com a Receita)
    creci            TEXT,
    tipo_imovel      TEXT,
    texto_lido       TEXT,
    cnpj             TEXT,          -- imobiliária achada pelo telefone (empresas)
    foto_arquivo     TEXT,          -- caminho local no PC do Allan, nunca público
    nota             TEXT,
    visto_em         TIMESTAMPTZ NOT NULL,   -- quando a foto foi tirada (EXIF) ou recebida
    revisto_em       TIMESTAMPTZ,            -- última vez que a mesma placa foi vista no lugar
    vezes_vista      INTEGER NOT NULL DEFAULT 1,
    criado_em        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS placas_campo_mun ON placas_campo (municipio_ibge, visto_em);
CREATE INDEX IF NOT EXISTS placas_campo_tel ON placas_campo (telefone_chave);
