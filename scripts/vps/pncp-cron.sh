#!/bin/bash
# Refresh automático dos dados do SaaS (roda no host; sobrevive a redeploys do Coolify).
# Acha o container do pncp-control dinamicamente pelo prefixo do uuid.
C=$(docker ps --format "{{.Names}}" | grep -E "^l7o87" | head -1)
LOG=/var/log/pncp-cron.log
if [ -z "$C" ]; then echo "$(date "+%F %T") [ERRO] container pncp-control não encontrado" >> "$LOG"; exit 1; fi
echo "$(date "+%F %T") [$1] início (container $C)" >> "$LOG"
case "$1" in
  bf)        docker exec "$C" python3 -m transparencia_etl backfill-bf ;;
  score)     docker exec "$C" python3 score_municipios_etl.py --importar ;;
  licit)     docker exec "$C" python3 etl.py --todas-ufs --dias 7 ;;
  cnpjimob)  docker exec -e UFS_ALVO=${UFS_ALVO:-RJ,BA,CE} "$C" python3 cnpj_imob_finder.py && docker exec "$C" python3 cnpj_imob_finder.py --avisar ;;
  sitesimob) for U in $(echo ${UFS_ALVO:-RJ,BA,CE} | tr , " "); do docker exec "$C" python3 imob_coletor_leads.py --uf $U; done ;;
  sancoes)   docker exec "$C" python3 sancoes_etl.py ;;
  trends)    docker exec "$C" python3 trends_etl.py --atualizar ;;
  *)         echo "uso: pncp-cron.sh {bf|score|licit|cnpjimob|sitesimob|sancoes|trends}" ;;
esac >> "$LOG" 2>&1
echo "$(date "+%F %T") [$1] fim" >> "$LOG"
