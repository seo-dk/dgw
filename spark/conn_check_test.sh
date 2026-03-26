#!/usr/bin/env bash
set -euo pipefail

# ===== 사용자 값 =====
SPARK_HOST="90.90.43.101"      # spark_server_info.ip
SPARK_USER="hadoop"                  # spark_server_info.username
SPARK_PORT="22"                      # spark_server_info.port
HDFS_URI='hdfs://90.90.43.21:8020/tap_d/staging/db=o_datalake/tb=aim_nw_tp_5g/dt=20260320/hh=18/'
# ====================

echo "==== [1] worker local check ===="
date; hostname; whoami
echo "[worker] hdfs -test -d"
bash -lc "hdfs dfs -test -d \"${HDFS_URI}\"; echo RC:\$?"

echo "[worker] hdfs -ls"
bash -lc "hdfs dfs -ls \"${HDFS_URI}\"; echo RC:\$?"

echo
echo "==== [2] ssh to spark_server_info check ===="
# DistcpEncryptManager의 ssh + 원격 hdfs -test 재현
ssh -p "${SPARK_PORT}" -o BatchMode=yes -o ConnectTimeout=10 "${SPARK_USER}@${SPARK_HOST}" \
"bash -lc 'date; hostname; whoami; \
hdfs dfs -test -d \"${HDFS_URI}\"; echo RC:\$?; \
hdfs dfs -ls \"${HDFS_URI}\" >/dev/null; echo LS_RC:\$?'"

echo
echo "==== [3] network port check (optional) ===="
nc -vz 90.90.43.21 8020 || true
nc -vz 90.90.43.23 8020 || true

echo "==== done ===="