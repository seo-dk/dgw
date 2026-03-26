#set -e

src="tpaniCluster"
dest="dataGWCluster"

src_path=$1
dest_path=$2

if [ $# -ne 2 ]
then
        resp_msg="-1"
        echo $resp_msg
        exit
fi

############# Destiny Path Check
array_path=($(echo $dest_path | tr "/" "\n"))
if [ ${#array_path[@]} -lt 3 ]
then
        resp_msg="1" # risk path delete
        echo $resp_msg
        exit
fi
hadoop fs -rmr hdfs://${dest}${dest_path}

############# Souece Path check
hadoop fs -stat hdfs://${src}${src_path}
return_code=$?
if [ $return_code -ne 0 ]
then
        resp_msg="2" # no such dir or connect refused
        echo $resp_msg
        exit
fi

############# Distcp Job Run
hadoop distcp -skipcrccheck hdfs://${src}${src_path} hdfs://${dest}${dest_path}
return_code=$?
if [ $return_code -ne 0 ]
then
        resp_msg="3" # distcp job fail
        echo $resp_msg
        exit
fi

############# Data Equality Check
src_cnt=`hadoop fs -count hdfs://${src}${src_path} | awk '{print $2}'`
desc_cnt=`hadoop fs -count hdfs://${dest}${dest_path} | awk '{print $2}'`
if [ $src_cnt -ne $desc_cnt ]
then
        resp_msg="4" # data inquality
        echo $resp_msg
        exit
fi

#############
resp_msg="0"
echo $resp_msg
