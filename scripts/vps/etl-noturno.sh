#!/bin/bash
# Fila noturna dos ETLs leves do allancandido.com (cron 05:00 UTC = 02:00 em Brasília).
# - Um ETL por vez, dentro do container do pncp-control, com nice.
# - Antes de cada um: memória livre >= MIN_LIVRE_MB e carga < MAX_CARGA; senão
#   espera até 10 min e, se continuar apertado, deixa o resto para a noite seguinte.
# - Cada ETL tem um período em dias; /root/etl-noturno.estado guarda o último
#   sucesso (nome=epoch). Só roda o que venceu. Falha não grava estado: tenta de novo amanhã.
# - Não começa ETL novo depois de LIMITE_HHMM (horário UTC do VPS).
# Medido em 2026-09-26 (pico do container): radar 85 MiB/31 s, comex 73 MiB/12 s, agro 78 MiB/206 s,
# imóveis da União ~7 min, contratos ~1 min; agro_producao (PAM+PPM) ~6 min com --anos 2.
LOG=/var/log/pncp-cron.log
ESTADO=/root/etl-noturno.estado
MIN_LIVRE_MB=${MIN_LIVRE_MB:-600}
MAX_CARGA=${MAX_CARGA:-3}
LIMITE_HHMM=${LIMITE_HHMM:-0800}   # UTC = 05:00 em Brasília
ANO=$(date +%Y)

# nome|período em dias|comando (dentro do container)
FILA="contratos|7|python3 contratos_etl.py --dias 8
comex_ano|7|python3 comex_etl.py --importar --ano $ANO
radar|30|python3 radar_loteamento_etl.py --importar --ano-fim $((ANO-1))
comex|30|python3 comex_etl.py --importar --ano $((ANO-1))
uniao|30|python3 imoveis_uniao_etl.py --importar
agro|365|python3 agro_etl.py --importar
agro_producao|30|python3 agro_producao_etl.py --importar --anos 2"

log() { echo "$(date "+%F %T") [noturno] $*" >> "$LOG"; }
livre_mb() { awk '/MemAvailable/ {print int($2/1024)}' /proc/meminfo; }
carga() { cut -d" " -f1 /proc/loadavg; }
folga() { [ "$(livre_mb)" -ge "$MIN_LIVRE_MB" ] && awk -v c="$(carga)" -v m="$MAX_CARGA" "BEGIN{exit !(c < m)}"; }

C=$(docker ps --format "{{.Names}}" | grep -E "^l7o87" | head -1)
[ -z "$C" ] && { log "ERRO container pncp-control não encontrado"; exit 1; }
touch "$ESTADO"
agora=$(date +%s)
log "início · livre $(livre_mb) MiB · carga $(carga)"

while IFS="|" read -r nome periodo cmd; do
  [ -z "$nome" ] && continue
  ultimo=$(grep "^$nome=" "$ESTADO" | cut -d= -f2); ultimo=${ultimo:-0}
  if [ $(( (agora - ultimo) / 86400 )) -lt "$periodo" ]; then continue; fi
  if [ "$(date +%H%M)" -ge "$LIMITE_HHMM" ]; then log "passou de $LIMITE_HHMM — $nome fica para amanhã"; break; fi
  espera=0
  until folga; do
    if [ $espera -ge 10 ]; then log "sem folga (livre $(livre_mb) MiB, carga $(carga)) — resto fica para amanhã"; exit 0; fi
    sleep 60; espera=$((espera + 1))
  done
  ini=$(date +%s)
  log "$nome: começando (livre $(livre_mb) MiB)"
  timeout 2h docker exec "$C" nice -n 10 $cmd >> "$LOG" 2>&1
  rc=$?
  if [ $rc -eq 0 ]; then
    sed -i "/^$nome=/d" "$ESTADO"; echo "$nome=$(date +%s)" >> "$ESTADO"
    log "$nome: ok em $(( $(date +%s) - ini ))s"
  else
    log "$nome: FALHOU (código $rc) — tenta de novo amanhã"
  fi
done <<< "$FILA"
log "fim · livre $(livre_mb) MiB"
