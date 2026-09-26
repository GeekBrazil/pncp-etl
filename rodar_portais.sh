#!/usr/bin/env bash
# Roda os coletores de portal (OLX + Zap) pra cada cidade em cidades_portais.txt,
# venda e aluguel, com pausa generosa entre rodadas — devagar de propósito, pra
# não repetir o que aconteceu no OLX em teste (hits repetidos demais na mesma
# URL fizeram o preço parar de vir preenchido, provável mitigação anti-bot).
#
# Uso: ./rodar_portais.sh
# Cron sugerido (rodar 1x/dia, de madrugada): 0 4 * * * cd /home/allan/pncp-etl && ./rodar_portais.sh >> /var/log/portais-imob.log 2>&1
set -euo pipefail
cd "$(dirname "$0")"
source pncpvenv/bin/activate
export DATABASE_URL="$(cat ~/.config/pncp/database_url)"

PAUSA_ENTRE_RODADAS="${PAUSA_ENTRE_RODADAS:-60}"
PAGINAS="${PAGINAS:-3}"

while IFS='|' read -r uf cidade regiao_olx slug_zap; do
    [[ "$uf" =~ ^#.*$ || -z "$uf" ]] && continue
    for finalidade in venda aluguel; do
        echo "=== OLX $cidade ($finalidade) ==="
        python3 portal_olx.py --regiao "$regiao_olx" --uf "$uf" --cidade "$cidade" --finalidade "$finalidade" --paginas "$PAGINAS" || echo "[aviso] OLX $cidade/$finalidade falhou, seguindo"
        sleep "$PAUSA_ENTRE_RODADAS"
        echo "=== Zap $cidade ($finalidade) ==="
        python3 portal_zap.py --cidade-slug "$slug_zap" --uf "$uf" --cidade "$cidade" --finalidade "$finalidade" --paginas "$PAGINAS" || echo "[aviso] Zap $cidade/$finalidade falhou, seguindo"
        sleep "$PAUSA_ENTRE_RODADAS"
    done
done < "${CIDADES:-cidades_portais.txt}"

echo "=== fim da rodada $(date) ==="
