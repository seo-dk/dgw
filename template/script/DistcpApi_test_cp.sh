
DAG_ID="DISTCP_TPANI_DAG_TEST"
URL="http://90.90.47.121:9090/api/v1/dags/${DAG_ID}/dagRuns"
USER="samson"
PASSWORD="shfkd2023^_"

create_post_data()
{
	cat << EOF
{
        "conf":{
		"interface_id":"C-TPANI-DISTCP-OD-0001",
                "source_path":"/user/samson/cem/period=1h/data=summary/type=BRAN",
                "destination_path":"/idcube_out/db=o_tpani/tb=cem/period=1h/data=summary/type=BRAN",
                "partitions":{
                        "dt":"20230822",
                        "hh":""
                }
        }
}
EOF
}

curl -X POST $URL -H "Content-Type: application/json" --user "${USER}:${PASSWORD}" -d "$(create_post_data)"
