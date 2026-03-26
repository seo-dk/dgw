from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

#  {
#    "interface_id": "C-TMS-DBCONN-MI-0002",
#         "files": [
#               "C-TMS-DBCONN-MI-0002,01H68SSJZDDZ86R5J47TEQZRNK,42442,202307261846,DBCONN,2023-07-31 14:49:54,319",
#               "C-TMS-DBCONN-MI-0002,01H68SZ69YPFSYDJGZZBQGP3JH,42448,202307261850,DBCONN,2023-07-31 14:50:58,322"
#              ]
#  }


def re_Run(**context):
    from MetaInfoHook import MetaInfoHook
    from OracleHook import OracleHook
    from WriteOracleResult import WriteOracleResult
    from HdfsCopy import HdfsCopy
    from Parse import Parse
    from CollectHistory import CollectHistory
    from KafkaSend import KafkaSend
    from SetPath import SetPath
    from DbConnThread import DbConnThread
    import concurrent.futures
    import socket
    from Infodat_IO import InfoDat_IO


    INTERFACE_ID = context['dag_run'].conf.get('interface_id')
    collect_files = context['dag_run'].conf.get('files')
    execution_date = context['execution_date']

    try:
        setPath = SetPath()
        parse = Parse()

        # setting source_seq_list for rerun fileList
        collect_file_info_list = {}
        source_seq_list = []
        
        for collect_file in collect_files:
            collect_file_list = collect_file.split(",")
            collect_hist_id = collect_file_list[1]
            collect_source_seq = collect_file_list[2]
            target_dt = collect_file_list[3]
            protocol_cd = collect_file_list[4]
            started_at = collect_file_list[5]
            retry_id_seq = collect_file_list[6]
            
            collect_file_info_list[collect_source_seq] = {"collect_hist_id": collect_hist_id, "target_dt": target_dt, "protocol_cd": protocol_cd, "started_at": started_at, "retry_id_seq": retry_id_seq}
            source_seq_list.append(collect_source_seq)

        # get connect db info
        metaInfoHook = MetaInfoHook()
        metaInfo = metaInfoHook.GetMetaInfo(INTERFACE_ID)
        dataClassInfo = metaInfoHook.DbJsonerverInfo(metaInfo.PROTOCOL_DETAILS_JSON)
        fileListInfo = metaInfoHook.GetSourceFolderInfo(INTERFACE_ID, source_seq_list)
        statusCodeInfo = metaInfoHook.GetStatusCode()   
        metaInfoHook.dbSessionClose()

        currentHostName = socket.gethostname()
        collectHistory = CollectHistory(INTERFACE_ID,DAG_ID,TASK_ID,currentHostName,"1",execution_date, statusCodeInfo)

        collectHistory.setServerInfo(metaInfo)
        collectHistory.setReTryTrue()

        kafka_send = KafkaSend(statusCodeInfo)

        oracleHook = OracleHook(kafka_send, collectHistory, dataClassInfo)
        oracleHook.setDns()
        oracleHook.connectOracle()

        infodat_IO = InfoDat_IO()
        writeOracleResult = WriteOracleResult(kafka_send, collectHistory, setPath, infodat_IO)

        local_info_file_path = setPath.setOracleLocalInfoFilePath(dataClassInfo, INTERFACE_ID)
        dbconnThread = DbConnThread(oracleHook, writeOracleResult, local_info_file_path)
        
        for file_info in fileListInfo:
            target_file_info_dic = collect_file_info_list[str(file_info.COLLECT_SOURCE_SEQ)]
            current_time = target_file_info_dic["target_dt"]
            started_at = target_file_info_dic["started_at"]
            collect_hist_id = target_file_info_dic["collect_hist_id"]

            # get count sql result
            result_dic = parse.getSqlParams(file_info.DB_QUERY_INFO.count_params, current_time)
            count_params = parse.replaceSqlParamsResult(file_info.DB_QUERY_INFO.count_query, result_dic['param_result'])
            record_count = oracleHook.executeCountSql(file_info.DB_QUERY_INFO.count_query, count_params)

            # get data sql's params
            result_dic = parse.getSqlParams(file_info.DB_QUERY_INFO.data_params, current_time)
            sql_params = parse.replaceSqlParamsResult(file_info.DB_QUERY_INFO.data_query, result_dic['param_result'])
            num_rows = int(file_info.DB_QUERY_INFO.num_rows)

            row_num_list = parse.getRowNumList(record_count, num_rows)      
            query_list = parse.getDataQueryList(row_num_list, num_rows, file_info.DB_QUERY_INFO.data_query, file_info.DB_QUERY_INFO.count_query)

            target_time = parse.getOracleTragetTime(result_dic['param_result'])

            # /data/airflow/C-TMS-DBCONN-MI-0002/db=o_tms/TMS_SUBWAY_AREA_QUALITY/TMS_SUBWAY_AREA_QUALITY_202307261420_
            local_file_path = setPath.setOracleLocalFilePath(dataClassInfo, INTERFACE_ID, file_info, target_time)

            # multi thread
            worker_min = len(query_list)
            worker = min(10, worker_min)
            future_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers = worker) as executor:
                # execute query and write to local
                for query in query_list:
                    future = executor.submit(dbconnThread.run, query, sql_params, local_file_path, file_info, current_time, target_time, target_file_info_dic) 
                    future_list.append(future)
               
                # concurrent.futures.wait(future_list, timeout=90, return_when=concurrent.futures.ALL_COMPLETED)
                completed_futures, failed_futures = concurrent.futures.wait(future_list, timeout=90, return_when=concurrent.futures.ALL_COMPLETED)
                
                if failed_futures:
                    kafka_send.sendErrorKafka(collectHistory, 10, False, "Some Thread failed")
                    raise AirflowException("Thread failed")
                else:
                    print('Completed Thread')

        if local_info_file_path is not None:
            hdfs_copy = HdfsCopy(kafka_send, collectHistory, "")
            hdfs_copy.checkDir(local_info_file_path)
            hdfs_copy.main(local_info_file_path, stdout=True, retry=True)
        else:
            raise AirflowException("No info.dat path")
            
        oracleHook.closeConnection()       
        kafka_send.closeKafka()
        
    except Exception as e:
        kafka_send.closeKafka()
        raise AirflowException(str(e))    

                       
###################################### dag define #############################################
TASK_ID="reRun"
DAG_ID = "reRun_dbconn"

args = {'owner':'seo'}                
with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2023, 1, 1, tzinfo=local_tz),
        schedule_interval='@once',
        #schedule_interval='00 * * * *',
        catchup=False,
        tags=['reRun','restAPI','DBCONN'],
        default_args=args
) as dag:
    
    StartSchedule = DummyOperator(task_id="Start_Schedule")    
    reRun = PythonOperator(task_id=TASK_ID, provide_context=True, python_callable=re_Run) 
    EndSchedule = DummyOperator(task_id="End_Schedule",trigger_rule='one_success')  

######################################### task sequence ##########################################
    
    StartSchedule >> reRun >> EndSchedule
