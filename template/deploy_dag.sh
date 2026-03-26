#!/bin/bash

set -e
set -u

DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    -n|--dry-run) DRY_RUN=true ;;
    -h|--help)
      echo "Usage: $(basename "$0") [-n|--dry-run]"
      exit 0
      ;;
  esac
done

BASE_DIR="/local/AIRFLOW/template"
PYTHON_DIR="$BASE_DIR/python"
DAGS_DIRS=("collect")
MERGED_DIR="$BASE_DIR/merged_dags"
SERVERS=("gw-app-001" "gw-app-002" "gw-app-005" "gw-app-006" "gw-app-007" "gw-ha-001-01" "gw-ha-001-02" "gw-ha-002-01" "gw-ha-002-02")

echo "Starting DAG generation (DRY_RUN=$DRY_RUN)"

for section in "${DAGS_DIRS[@]}"; do
    echo "Generating DAGs for section: $section"
    python3 "$PYTHON_DIR/gen_dag.py" "$PYTHON_DIR/gen_dag.ini" "$section"
done

if $DRY_RUN; then
    echo "[DRY-RUN] Skip removing and recreating $MERGED_DIR"
else
    echo "Clearing and recreating merged directory: $MERGED_DIR"
    rm -rf "$MERGED_DIR"  
    mkdir -p "$MERGED_DIR"
fi

for section in "${DAGS_DIRS[@]}"; do
    echo "$([ $DRY_RUN = true ] && echo '[DRY-RUN] ' )Copying DAGs from section: $section to $MERGED_DIR"
    if ! $DRY_RUN; then
        mkdir -p "$MERGED_DIR"
        cp "$BASE_DIR/dags/$section/product/"*.py "$MERGED_DIR/"
    fi
done

RSYNC_ARGS=(
    -acP     
    --omit-dir-times 
    --no-times 
    --include='C-*.py' 
    --include='P-*.py' 
    --exclude='*'     
    --delete
)

if $DRY_RUN; then
    RSYNC_ARGS+=(-n)
    echo "[DRY-RUN] rsync would run with: ${RSYNC_ARGS[@]}"
fi


for server in "${SERVERS[@]}"; do
    echo "$([ $DRY_RUN = true ] && echo '[DRY-RUN] ')Synchronizing DAGs to server: $server"
    rsync ${RSYNC_ARGS[@]} $MERGED_DIR/ "$server:/local/AIRFLOW/dags/" 
done

echo "DAG synchronization $( $DRY_RUN && echo 'simulated' || echo 'completed') completed successfully."
