#!/bin/bash
# Auto-update do dashboard IBC
# Roda via cron todo dia a meia-noite
# Verifica se tem atualizacao no GitHub e aplica automaticamente

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$DEPLOY_DIR/update.log"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Verificando atualizacoes..." >> "$LOG_FILE"

cd "$DEPLOY_DIR" || exit 1

# Buscar atualizacoes do GitHub
git fetch origin master 2>> "$LOG_FILE"

# Comparar com o local
LOCAL=$(git rev-parse HEAD 2>/dev/null)
REMOTE=$(git rev-parse origin/master 2>/dev/null)

if [ "$LOCAL" = "$REMOTE" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Nenhuma atualizacao disponivel." >> "$LOG_FILE"
    exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') — Atualizacao encontrada! Aplicando..." >> "$LOG_FILE"

# Aplicar atualizacao (preserva .env e users.json)
git stash 2>> "$LOG_FILE"
git pull origin master 2>> "$LOG_FILE"

# Reinstalar dependencias caso tenham mudado
pip install -r requirements.txt 2>> "$LOG_FILE"

# Reiniciar o servico
sudo systemctl restart ibc-dash 2>> "$LOG_FILE"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Atualizado com sucesso! De $(echo $LOCAL | cut -c1-7) para $(echo $REMOTE | cut -c1-7)" >> "$LOG_FILE"
