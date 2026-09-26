#!/usr/bin/env bash
set -euo pipefail

# Deploy do pncp-control (painel.allancandido.com) com BUILD NO PC DO ALLAN — regra de 2026-09-25 para
# tudo que roda no VPS (Coolify). Mesmo esquema do
# allancandido.com (scripts/deploy-pc.sh de lá): a imagem é construída aqui a
# partir do COMMIT (git archive), com o nome que o Coolify usa
# (<uuid>:<sha>), vai pro VPS por docker save/load e o deploy sai com
# force_rebuild=false — o Coolify acha a imagem e pula o build.
#
# Código = este repositório (GeekBrazil/pncp-etl, branch master). No Coolify o
# app se chama pncp-control e PRECISA apontar para GeekBrazil/pncp-etl@master
# (antes apontava para saas-dados-publicos, que não existe mais) — o Coolify
# consulta o repositório (git ls-remote) mesmo quando pula o build.
#
# NUNCA interromper no meio nem envolver em `timeout`: deploy cortado deixa
# coolify-helper órfão no VPS.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$DIR")"
APP_UUID="l7o87txsftz6r3u97g1ra1j8"
BRANCH="master"
VPS="root@188.245.70.109"

echo "==> [1/6] Enviando commits para origin $BRANCH..."
git -C "$ROOT_DIR" push origin "$BRANCH"
SHA="$(git -C "$ROOT_DIR" rev-parse HEAD)"
REMOTE_SHA="$(git -C "$ROOT_DIR" ls-remote origin "refs/heads/$BRANCH" | cut -f1)"
if [ "$SHA" != "$REMOTE_SHA" ]; then
  echo "❌ HEAD local ($SHA) difere de origin/$BRANCH ($REMOTE_SHA)."; exit 1
fi
IMAGE="$APP_UUID:$SHA"

echo "==> [2/6] Build local da imagem $IMAGE (a partir do commit)..."
# --network=host: a rede padrão do Docker neste PC não sai para a internet
git -C "$ROOT_DIR" archive --format=tar HEAD | docker build --network=host -t "$IMAGE" -

echo "==> [3/6] Enviando imagem para o VPS..."
docker save "$IMAGE" | gzip -1 | ssh -o ConnectTimeout=10 "$VPS" "gunzip | docker load"
# helpers órfãos no VPS (o nome é uuid; 'coolify-helper' só aparece na imagem)
ssh "$VPS" "docker ps -a --format '{{.ID}} {{.Image}}' | awk '/coolify-helper/ {print \$1}' | xargs -r docker rm -f > /dev/null 2>&1 || true"

echo "==> [4/6] Deploy no Coolify SEM rebuild..."
DISPATCH_OUTPUT=$(ssh -o ConnectTimeout=10 "$VPS" "docker exec coolify php artisan tinker --execute='
\$app = App\Models\Application::where(\"uuid\", \"$APP_UUID\")->first();
\$dep = App\Models\ApplicationDeploymentQueue::create([
    \"application_id\" => \$app->id,
    \"deployment_uuid\" => (string) Illuminate\Support\Str::uuid(),
    \"pull_request_id\" => 0,
    \"force_rebuild\" => false,
    \"commit\" => \"$SHA\",
    \"status\" => \"queued\",
    \"is_webhook\" => false,
    \"is_api\" => true,
    \"server_id\" => \$app->destination->server->id,
    \"destination_id\" => \$app->destination->id,
    \"only_this_server\" => false,
    \"rollback\" => false,
    \"application_name\" => \$app->name,
    \"server_name\" => \$app->destination->server->name,
    \"deployment_url\" => \"\",
]);
App\Jobs\ApplicationDeploymentJob::dispatch(\$dep->id);
echo \"DEP_ID:\" . \$dep->id;
'")
DEP_ID=$(echo "$DISPATCH_OUTPUT" | grep -o 'DEP_ID:[0-9]*' | cut -d: -f2)
echo "    Deploy ID: $DEP_ID"

FINISHED=false
for i in {1..150}; do
  STATUS=$(timeout 25 ssh -o ConnectTimeout=10 "$VPS" "docker exec coolify php artisan tinker --execute='
  \$d = App\Models\ApplicationDeploymentQueue::find($DEP_ID);
  echo \"STATUS:\" . (\$d ? \$d->status : \"unknown\");
  '" 2>/dev/null | (grep -o 'STATUS:[a-z_-]*' || true) | cut -d: -f2 || true)
  STATUS="${STATUS:-waiting}"
  echo "    Progresso ($i/150): $STATUS"
  if [ "$STATUS" = "finished" ]; then FINISHED=true; break; fi
  if [ "$STATUS" = "failed" ] || [ "$STATUS" = "cancelled" ]; then echo "❌ Deploy falhou no Coolify!"; exit 1; fi
  sleep 5
done
[ "$FINISHED" = "true" ] || { echo "❌ Timeout aguardando o Coolify (o deploy segue lá — NÃO matar)."; exit 1; }

SKIPPED=$(ssh "$VPS" "docker exec coolify php artisan tinker --execute='
\$d = App\Models\ApplicationDeploymentQueue::find($DEP_ID);
echo (stripos(\$d->logs ?? \"\", \"Build step skipped\") !== false) ? \"SKIP_OK\" : \"SKIP_NAO\";
'" 2>/dev/null | grep -o 'SKIP_[A-Z]*' || true)
if [ "$SKIPPED" = "SKIP_OK" ]; then echo "    ✅ Coolify usou a imagem do PC (Build step skipped)."
else echo "    ⚠️ Não achei 'Build step skipped' no log do deploy $DEP_ID — conferir se o VPS compilou."; fi

echo "    Atualizando a rota da API que o site usa (sslip.io → container novo)..."
# O site (allancandido.com) chama a API pelo endereço sslip.io, sem o login do
# Cloudflare Access que protege painel.allancandido.com. O Coolify só gera a
# rota do painel, então esta rota fica num arquivo dinâmico do Traefik e
# precisa seguir o container novo a cada deploy (incidente de 2026-09-25: o
# site ficou sem dados depois de um redeploy).
ssh "$VPS" 'NOVO=$(docker ps --filter name='"$APP_UUID"' --format "{{.CreatedAt}}|{{.Names}}" | sort -r | head -1 | cut -d"|" -f2)
cat > /data/coolify/proxy/dynamic/pncp-api.yaml <<EOF2
http:
  routers:
    pncp-api-sslip:
      rule: "Host(\`'"$APP_UUID"'.188.245.70.109.sslip.io\`)"
      entryPoints:
        - http
      service: pncp-api-service
  services:
    pncp-api-service:
      loadBalancer:
        servers:
          - url: "http://${NOVO}:8790"
EOF2
echo "    rota → $NOVO"'

echo "==> [5/6] Verificando produção..."
sleep 4
CODIGO=$(curl -s -o /dev/null -m 20 -w "%{http_code}" "https://painel.allancandido.com/")
API=$(curl -s -o /dev/null -m 20 -w "%{http_code}" "http://$APP_UUID.188.245.70.109.sslip.io/kpis" || true)
echo "    API para o site (sslip.io, sem chave) → $API (esperado 401/403; 404 = rota quebrada)"
[ "$API" = "404" ] && { echo "❌ O site ficou sem a API (rota sslip.io)"; exit 1; }
echo "    https://painel.allancandido.com/ → $CODIGO"
case "$CODIGO" in 2*|3*) ;; *) echo "❌ Produção respondeu $CODIGO"; exit 1;; esac

echo "==> [6/6] Limpando imagens antigas do app (VPS: mantém a atual; PC: atual + anterior)..."
ssh "$VPS" "docker images '$APP_UUID' --format '{{.Repository}}:{{.Tag}}' | grep -v ':$SHA\\$' | xargs -r docker rmi > /dev/null 2>&1 || true"
# no PC: mantém a imagem atual e a anterior (para voltar atrás)
docker images "$APP_UUID" --format '{{.Repository}}:{{.Tag}}' | tail -n +3 | xargs -r docker rmi > /dev/null 2>&1 || true
docker builder prune -f --filter until=72h > /dev/null 2>&1 || true
echo "✅ pncp-control (painel.allancandido.com) publicado (build no PC)."
