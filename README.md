# pncp-etl — dados públicos do allancandido.com

ETLs + API (FastAPI `pncp_dashboard.py`, painel.allancandido.com) que alimentam o
allancandido.com. Mapa técnico completo (cada motor, fonte, tabela, frequência):
vault → `002 - Produção Servidor/Claude Code/Motores do site — como cada um funciona.md`.

## Onde cada coisa roda
- **VPS, fila noturna** (`scripts/vps/etl-noturno.sh` → `/root/etl-noturno.sh`, 02:00 BRT, só com folga de memória):
  contratos, leiloes_bancos (BB + União), leiloes_judiciais (DJEN), comex, radar, uniao, agro, agro_producao.
- **PC do Allan** (systemd do usuário, `scripts/systemd/`, túnel `pncp-tunnel` na porta 5433):
  caged-etl, cnpj-imob, **caixa-importar** (vigia `~/Downloads/Lista_imoveis*.csv`),
  **leiloes-privados.timer** (04:20, diário — a API do Bradesco recusa IP de datacenter, por isso não roda no VPS), coletores de mercado.

## Leilões (`leiloes_outros`)
| fonte | script | origem |
|---|---|---|
| judicial | `leiloes_judiciais_etl.py` | DJEN/CNJ — editais de leilão, só imóveis, sem dados pessoais |
| caixa | `caixa_importar.py` | lista oficial baixada à mão (site com CAPTCHA) |
| bb | `leiloes_bancos_etl.py` | catálogo Seu Imóvel BB |
| spu | `leiloes_bancos_etl.py` | VendasGov / Imóveis da União |
| bradesco | `leiloes_privados_etl.py` | Vitrine Bradesco (API pública da vitrine oficial) |
| santander, itau | `leiloes_privados_etl.py` | Portal Zuk, leiloeiro dos dois bancos (só páginas públicas; o site do Itaú bloqueia robôs) |

## Deploy
`bash scripts/deploy-pc.sh` — build no PC, `docker save | ssh docker load`, Coolify sem rebuild.
Nunca interromper nem envolver em `timeout`. Segurança: ver `SEGURANCA.md`.
