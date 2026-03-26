#!/bin/bash

current_time=$(date '+%Y-%m-%d %H:%M:%S')

input_path=$1
output_path=$2
tb_name=$3
ciphers_keys=$4
enc_rules=$5

DECODED_JSON=$(echo "$ENCRYPT_CONFIG" | base64 --decode)

echo "output_path : $output_path"

crypto_type=$(echo "$DECODED_JSON" | grep -o '"crypto_type": "[^"]*' | awk -F'"' '{print $4}')
echo "crypto_type : $crypto_type"

memory_usage=$(echo "$DECODED_JSON" | grep -o '"enc_memory_usage": "[^"]*' | awk -F'"' '{print $4}')
echo "memory : $memory_usage"

file_format=$(echo "$DECODED_JSON" | grep -o '"format": "[^"]*' | awk -F'"' '{print $4}')
echo "file_format : $file_format"

target_compression=$(echo "$DECODED_JSON" | grep -o '"target_compression": "[^"]*' | awk -F'"' '{print $4}')
echo "target_compression : $target_compression"

queue=$(echo "$DECODED_JSON" | grep -o '"queue": "[^"]*' | awk -F'"' '{print $4}')
echo "queue : $queue"

exec_file_path=$(echo "$DECODED_JSON" | grep -o '"exec_file_path": "[^"]*' | awk -F'"' '{print $4}')
echo "exec_file_path : $exec_file_path"

target_format=$(echo "$DECODED_JSON" | grep -o '"target_format": "[^"]*' | awk -F'"' '{print $4}')
echo "target_format : $target_format"

delimiter=$(echo "$DECODED_JSON" | grep -o '"delimiter": "[^"]*' | awk -F'"' '{print $4}')
echo "delimiter : $delimiter"

encoding=$(echo "$DECODED_JSON" | grep -o '"encoding": "[^"]*' | awk -F'"' '{print $4}')
echo "encoding : $encoding"

merge=$(echo "$DECODED_JSON" | grep -o '"merge":[^,}]*' | awk -F: '{print $2}' | tr -d ' ')
echo "merge : $merge"

echo " encoded_schema : $ENCODED_SCHEMA"

env_path="$exec_file_path/env.sh"

. "$env_path"

tb=$(echo "$output_path" | grep -oP "(?<=tb=)[^/]+")
dt=$(echo "$output_path" | grep -oP "(?<=dt=)[^/]+")
hh=$(echo "$output_path" | grep -oP "(?<=hh=)[^/]+")
hm=$(echo "$output_path" | grep -oP "(?<=hm=)[^/]+")

if [ -n "$hm" ]; then
    path_identifier="${tb}_${dt}_${hm}"
elif [ -n "$hh" ]; then
    path_identifier="${tb}_${dt}_${hh}"
else
    path_identifier="${tb}_${dt}"
fi

echo "$current_time - YARN application (table: $tb_name, path_identifier: $path_identifier) started." >> enc.log

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "encrypt: ${path_identifier}" \
  --conf spark.yarn.tags=$tb \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.executorEnv.PATH=/local/HADOOP/bin:$PATH \
  --conf spark.yarn.queue=$queue \
  --conf spark.executor.memory=$memory_usage \
  --conf spark.executor.instances=13 \
  --conf spark.executor.cores=4 \
  --conf spark.task.cpus=1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=hdfs://dataGWCluster/spark/eventLogs \
  --conf spark.eventLog.compress=true \
  --files libstandalone_cipher.so \
  --py-files StandaloneCipherWrapper.py \
  --jars /local/HADOOP/share/hadoop/tools/lib/zstd-jni-1.4.9-1.jar \
  enc_test.py "$input_path" "$output_path" "$ciphers_keys" "$enc_rules" "$file_format" "$target_compression" "$tb_name" "$target_format" "$delimiter" "$encoding" "$ENCODED_SCHEMA" "$crypto_type" "$merge"

echo "submit finished"
echo "$path_identifier"
yarn application -list -appStates ALL | grep "$path_identifier" | sort -k1 | tail -n1 >> enc_test.log
read app_id app_status log_url <<< $(yarn application -list -appStates ALL | grep "$path_identifier" | awk '{print $1, $8, $10}' | sort -k1 | tail -n1)

echo "app_id : $app_id"
echo "log_url: $log_url"

if [ -z "$app_id" ]; then
    echo "$current_time - Failed to retrieve application ID." >> enc_test.log
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
