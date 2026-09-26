-- Campos que o coletor de portais precisa e o coletor de site de imobiliária não:
-- contato direto (prospecção) e se quem anuncia é dono ou imobiliária.
ALTER TABLE imoveis_mercado ADD COLUMN IF NOT EXISTS anunciante_tipo VARCHAR(15); -- proprietario | imobiliaria | null (não deu pra saber)
ALTER TABLE imoveis_mercado ADD COLUMN IF NOT EXISTS contato TEXT;               -- telefone/whatsapp do anúncio, quando visível sem login
ALTER TABLE imoveis_mercado ADD COLUMN IF NOT EXISTS titulo TEXT;                -- título/descrição curta do anúncio (portais não têm "tipo" tão limpo quanto imob_coletor.py)

CREATE INDEX IF NOT EXISTS idx_merc_anunciante ON imoveis_mercado(anunciante_tipo) WHERE anunciante_tipo = 'proprietario';
