
DAG_ID="reRun_dbconn"
URL="http://80.80.60.120:9090/api/v1/dags/${DAG_ID}/dagRuns"
USER="samson"
PASSWORD="shxmqnr2021^_"
INTERFACE_ID="IF-T-TEST-0002"
DATA_LIST="0001,0002,0003"
BASE_TIME="202306211500"
PROTOCOL_CD="SFTP"


declare -A map

map[key1]=value1
echo $map

create_post_data()
{
	cat << EOF
{
	"conf": {
			"interface":"${INTERFACE_ID}",
			"files":"${DATA_LIST}",
			"sdate":"${BASE_TIME}"
	}
}
EOF
}

curl -X POST $URL -H "Content-Type: application/json" --user "${USER}:${PASSWORD}" -d "$(create_post_data)"
