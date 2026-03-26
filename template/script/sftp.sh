
FILE=$@

host=("90.90.47.121 90.90.47.122 90.90.47.22 90.90.47.32") # App, HA
#host=("90.90.47.61 90.90.47.62 90.90.47.63 90.90.47.64") # hdfs

remote_dir=/local/AIRFLOW/dags
#remote_dir=/home/hadoop/bin

function SFTP_SAMSON(){
HOST=$1
FILE=$2
expect << EOF
set timeout 600
spawn sftp samson@$HOST
expect "sftp>" { send "cd $remote_dir\r"}
expect "sftp>" { send "put ${FILE}\r"}
expect "sftp>" { send "bye\r"}
expect eof
EOF
}


function SFTP_HADOOP(){
HOST=$1
FILE=$2
expect << EOF
set timeout 600
spawn sftp hadoop@$HOST
expect "password:" { send "gkdid2023^_\r"}
expect "sftp>" { send "cd $remote_dir\r"}
expect "sftp>" { send "put ${FILE}\r"}
expect "sftp>" { send "bye\r"}
expect eof
EOF
}

for h in $host
do
	for f in ${FILE[@]}
	do
		SFTP_SAMSON $h $f
		#SFTP_HADOOP $h $f
	done
done



