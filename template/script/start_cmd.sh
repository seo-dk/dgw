source ~/.bash_profile

#/local/platform/redis-7.0.10/src/redis-server /local/platform/redis-7.0.10/redis.conf &
#sleep 1
airflow webserver >> /home/hadoop/airflow/logs/daemon/webserver.log  2>&1 &
sleep 1
airflow scheduler >> /home/hadoop/airflow/logs/daemon/scheduler.log  2>&1 &
sleep 1
airflow celery worker -q default,worker_q1 >> /home/hadoop/airflow/logs/daemon/worker.log  2>&1 &
sleep 1
airflow celery flower >> /home/hadoop/airflow/logs/daemon/flower.log  2>&1 &
