# 🔒 Documentação de Segurança — painel.allancandido.com (VPS)

Este repositório (`pncp-etl`) implementa controles de acesso e autenticação para proteger o painel e os endpoints consumidos pelo site `allancandido.com`.

---

## 1. Variáveis de Ambiente Necessárias

A aplicação lê as seguintes chaves de ambiente:

| Variável | Descrição | Onde Configurar | Exemplo de Valor |
|---|---|---|---|
| `ADMIN_SECRET` | Senha para o painel administrativo (`/`) e disparos de ETL / enriquecimento. | **Coolify (VPS)** | `suasenhaforte` |
| `PNCP_API_KEY` | Chave de segurança para as chamadas de API pública consumidas pelo Next.js. | **Coolify (VPS)** e **Vercel** | `hash_secreta_de_integracao` |

---

## 2. Fluxo de Autenticação

### A. Painel Administrativo e Escrita (`/`, `/etl/*`, `/empresas/enriquecer*`, `/pdf`)
* **Mecanismo**: HTTP Basic Authentication.
* **Usuário**: `allan`
* **Senha**: Valor configurado na variável de ambiente `ADMIN_SECRET`.
* **Uso**: Quando o usuário acessar `https://painel.allancandido.com` ou o subdomínio direto `sslip.io`, o navegador exibirá uma caixa de prompt nativa solicitando o usuário e senha. O navegador salva esta sessão em cache, permitindo que todas as requisições AJAX e conexões SSE (`EventSource`) subsequentes funcionem automaticamente sem novas solicitações.

### B. Leituras Públicas (`/kpis`, `/dados`, `/dashboard-live`, `/stats/gerais`, etc.)
* **Mecanismo**: Chave de API por cabeçalho (`X-API-Key`) **OU** Basic Auth ativo.
* **Validação**:
  * A requisição deve conter o cabeçalho `X-API-Key` correspondente à variável `PNCP_API_KEY`.
  * **Alternativa**: Se a chamada for feita de dentro do próprio painel administrativo pelo navegador (onde o usuário já se autenticou via Basic Auth), as credenciais Basic Auth serão aceitas como válidas e a chamada será permitida. Isso evita a necessidade de expor ou injetar a chave de API no código JavaScript executado no navegador do usuário.

---

## 3. Rodando o Ambiente Localmente

Para rodar localmente com os parâmetros de segurança ativos:

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/pncp_db"
export ADMIN_SECRET="suasenha"
export PNCP_API_KEY="suachave"

uvicorn pncp_dashboard:app --host 0.0.0.0 --port 8790
```
