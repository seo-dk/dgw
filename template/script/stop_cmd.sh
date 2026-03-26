PID_LIST=`ps -ef | grep airflow | grep -v "grep" | awk '{print $2}'`
for PID in $PID_LIST
do
        echo "kill -9  $PID"
        kill -9  $PID
done


PID_LIST=`ps -ef | grep celery | grep -v "grep" | awk '{print $2}'`
for PID in $PID_LIST
do
        echo "kill -9  $PID"
        kill -9  $PID
done


PID_LIST=`ps -ef | grep "gunicorn" | grep -v "grep" | awk '{print $2}'`
for PID in $PID_LIST
do
        echo "kill -9  $PID"
        kill -9  $PID
done
