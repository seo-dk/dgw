#!/bin/bash

current_time=$(date '+%Y-%m-%d %H:%M:%S')

input_path=$1
output_path=$2
tb_name=$3
memory_usage=$4
queue=$5
exec_file_path=$6
pyspark_file=$7
python_argument=$8

DECODED_PYTHON_ARGS=$(echo "$python_argument" | base64 --decode)
echo "DECODED_PYTHON_ARGS : $DECODED_PYTHON_ARGS"
echo "output_path : $output_path"
echo " SCHEMA_LOCAL_PATH : $SCHEMA_LOCAL_PATH"
echo " SCHEMA_HADOOP_PATH : $SCHEMA_HADOOP_PATH"

env_path="$exec_file_path/env.sh"

. "$env_path"

schema_full_path=""
if [[ "$SCHEMA_LOCAL_PATH" != "None" && -f "$SCHEMA_LOCAL_PATH" ]]; then
  ./schema_hadoop_uploader.sh "$SCHEMA_LOCAL_PATH" "$SCHEMA_HADOOP_PATH"
  status=$?
  schema_full_path=$SCHEMA_HADOOP_PATH/$(basename "$SCHEMA_LOCAL_PATH")
  echo "status : $status"
  if [[ $status -eq 0 ]]; then
    echo "schema upload successfully : $SCHEMA_HADOOP_PATH"
  elif [[ $status -eq 2 ]]; then
    echo "schema already exist : $(basename $SCHEMA_LOCAL_PATH)"
  else
    echo "failed upload schema: $SCHEMA_HADOOP_PATH"
    exit 1
  fi
else
  echo "There is no schema file..."
fi

tb=$(echo "$output_path" | grep -oP "(?<=tb=)[^/]+")
dt=$(echo "$output_path" | grep -oP "(?<=dt=)[^/]+")
hh=$(echo "$output_path" | grep -oP "(?<=hh=)[^/]+")

if [ -z "$hh" ]; then
    path_identifier="${tb}_${dt}"
else
    path_identifier="${tb}_${dt}_${hh}"
fi

echo "$current_time - YARN application (table: $tb_name, path_identifier: $path_identifier) started." >> enc_test.log

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "encrypt: ${path_identifier}" \
  --conf spark.yarn.tags=$tb \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.executorEnv.PATH=/local/HADOOP/bin:$PATH \
  --conf spark.yarn.queue="parquet_conversion" \
  --conf spark.executor.memory=$memory_usage \
  --conf spark.executor.instances=13 \
  --conf spark.executor.cores=4 \
  --conf spark.task.cpus=1 \
  --conf spark.app.config="${DECODED_PYTHON_ARGS}" \
  --conf spark.schema.path="${schema_full_path}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://dataGWCluster/spark/eventLogs \
  --conf spark.eventLog.compress=true \
  --py-files StandaloneCipherWrapper.py \
  --jars /local/HADOOP/share/hadoop/tools/lib/zstd-jni-1.4.9-1.jar \
  --files libstandalone_cipher.so,schema/o_tpani.user_hdvoice_info.json \
  "$pyspark_file" "$input_path" "$output_path" "$tb_name"

echo "submit finished"
echo "$path_identifier"
yarn application -list -appStates ALL | grep "$path_identifier" | sort -k1 | tail -n1 >> enc_test.log
read app_id app_status log_url <<< $(yarn application -list -appStates ALL | grep "$path_identifier" | awk '{print $1, $8, $10}' | sort -k1 | tail -n1)

echo "app_id : $app_id"
echo "log_url: $log_url"

if [ -z "$app_id" ]; then
    echo "$current_time - Failed to retrieve application ID." >> run_test.log
    exit 1
fi

if [ "$app_status" == "SUCCEEDED" ]; then
    echo "$current_time - YARN application $app_id completed successfully." >> enc_test.log
    exit 0
elif [ "$app_status" == "FAILED" ]; then
    echo "$current_time - YARN application $app_id failed." >> enc_test.log
    exit 1
else
    echo "$app_status - YARN application $app_id killed." >> enc_test.log
    exit 2
fi