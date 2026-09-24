-- "Tempo no mercado" sem depender de ITBI (que não cobre município fora de
-- capital): coletado_em já é sobrescrito a cada rodada (ON CONFLICT DO UPDATE
-- coletado_em=NOW() nos 3 coletores), então funciona como "última vez visto".
-- Faltava "primeira vez visto" pra virar "dias no mercado" / "sumiu há X dias".
ALTER TABLE imoveis_mercado ADD COLUMN IF NOT EXISTS primeiro_visto_em TIMESTAMPTZ DEFAULT NOW();

COMMENT ON COLUMN imoveis_mercado.primeiro_visto_em IS 'Setado só no INSERT (fora do ON CONFLICT DO UPDATE dos 3 coletores) — não muda depois. coletado_em - primeiro_visto_em = dias no mercado. Registros antigos (antes desta migration) recebem a data da migration, não o primeiro visto real.';
COMMENT ON COLUMN imoveis_mercado.coletado_em IS 'Atualizado a cada rodada em que o anúncio ainda existe — funciona como "última vez visto". Se parar de atualizar por bastante tempo (o coletor rodando), é sinal de que saiu do ar.';
