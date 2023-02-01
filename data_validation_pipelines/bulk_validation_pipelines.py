import json
import subprocess
import sys
import time
from configparser import ConfigParser

import pkg_resources

required = {'requests'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

import requests
import csv

MAX_RETRIES = 20
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
session.mount('https://', adapter)
session.mount('http://', adapter)

# Read global variables
config = ConfigParser()
config.read('config.ini')
proxy_host = "{}://{}".format(config.get('infoworks_details', 'protocol'), config.get('infoworks_details', 'host'))
proxy_port = config.get('infoworks_details', 'port')
delegation_token = ""
refresh_token = ""

import queue
from threading import Thread

num_fetch_threads = 10
job_queue = queue.Queue(maxsize=100)


def refresh_delegation_token():
    global delegation_token
    global refresh_token
    url = "{ip}:{port}/v3/security/token/access/".format(ip=proxy_host, port=proxy_port)
    headers = {
        'Authorization': 'Basic ' + refresh_token,
        'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, verify=False)
    print(response.text)
    delegation_token = response.json().get("result").get("authentication_token")


def poll_job(job_id):
    failed_count = 0
    retries = 3
    polling_frequency = 5
    response = {}
    timeout = time.time() + 300
    while True:
        if time.time() > timeout:
            break
        try:
            job_monitor_url = f"{proxy_host}:{proxy_port}/v3/admin/jobs/{job_id}"
            response = requests.get(job_monitor_url,
                                    headers={'Authorization': 'Bearer ' + delegation_token,
                                             'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                refresh_delegation_token()
                response = requests.get(job_monitor_url,
                                        headers={'Authorization': 'Bearer ' + delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
            parsed_response = json.loads(response.content)
            result = parsed_response.get('result', None)
            if len(result) != 0:
                job_status = result["status"]
                if job_status in ["completed", "failed", "aborted"]:
                    if job_status == "completed":
                        return 0
                    else:
                        return 1
            else:
                if failed_count >= retries - 1:
                    return 1
                failed_count = failed_count + 1
        except Exception as e:
            if failed_count >= retries - 1:
                print(response)
                return 1
            failed_count = failed_count + 1
        time.sleep(polling_frequency)


def trigger_pipeline_metadata_build(domain_id, pipeline_id):
    url_for_pipeline_metadata_build = '{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/jobs'.format(
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        domain_id=domain_id,
        pipeline_id=pipeline_id)

    request_body = {
        "job_type": "pipeline_metadata"
    }

    response = requests.post(url_for_pipeline_metadata_build, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                       'Content-Type': 'application/json'},
                             data=json.dumps(request_body), verify=False)
    if response.status_code == 406:
        refresh_delegation_token()
        response = requests.post(url_for_pipeline_metadata_build,
                                 headers={'Authorization': 'Bearer ' + delegation_token,
                                          'Content-Type': 'application/json'},
                                 data=json.dumps(request_body), verify=False)
    parsed_response = json.loads(response.content)
    result = parsed_response.get('result', None)
    job_id = result["id"]
    if poll_job(job_id) == 0:
        print("Pipeline metadata build job finished successfully")
    else:
        print("Pipeline metadata build job failed")


def import_sql(sql, domain_id, pipeline_id):
    sql_import_body = {
        "dry_run": False,
        "sql": sql,
        "sql_import_configuration": {
            "quoted_identifier": "DOUBLE_QUOTE",
            "sql_dialect": "LENIENT"
        }
    }
    sql_import_body_json = json.dumps(sql_import_body)
    print(f"SQL import body json : {sql_import_body_json} \n\n")
    url_for_sql_import = f"{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/sql-import"
    print("URL for sql Import: ", url_for_sql_import)
    print("\n")
    try:
        response = session.post(url_for_sql_import, headers={'Authorization': 'Bearer ' + delegation_token,
                                                             'Content-Type': 'application/json'},
                                data=sql_import_body_json,
                                timeout=60, verify=False)
        if response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_for_sql_import, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                  'Content-Type': 'application/json'},
                                     data=sql_import_body_json, verify=False, timeout=60)
        parsed_response = json.loads(response.content)
        result = parsed_response.get('result', None)
        if result:
            print("pipeline imported successfully! \n")
            return True
        else:
            print("Failed to import sql \n")
            print(parsed_response)
            return False
    except Exception as e:
        print("Failed to import sql \n")
        print(str(e))
        return False


def get_all_columns(source_name, datasetNameAtSource, origTableName):
    all_columns_with_datatype_dict = {}
    filter_condition = json.dumps({"name": source_name})
    url_to_list_source = f'{proxy_host}:{proxy_port}/v3/sources' + f"?filter={{filter_condition}}".format(
        filter_condition=filter_condition)
    response = requests.request("GET", url_to_list_source, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                    'Content-Type': 'application/json'},
                                verify=False)
    if response.status_code == 200 and len(response.json().get("result", [])) > 0:
        result = response.json().get("result", [])
        source_id = result[0]["id"]
        if source_id is not None:
            filter_condition_dict = {"datasetNameAtSource": datasetNameAtSource,
                                     "origTableName": origTableName}
            filter_condition = json.dumps(filter_condition_dict)
            url_to_list_tables = f'{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables' + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            response = requests.request("GET", url_to_list_tables,
                                        headers={'Authorization': 'Bearer ' + delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                result = response.json().get("result", [])
                columns = result[0]["columns"]
                all_columns_with_datatype_dict = {}
                for i in columns:
                    if not i["name"].lower().startswith("ziw"):
                        all_columns_with_datatype_dict[i["name"]] = i['target_sql_type']

    return all_columns_with_datatype_dict


def prepare_sql_import_query_columnchksum(domain_id, pipeline_id, distinct_count_cols, chksum_cols, group_by_cols,
                                          source_table_name,
                                          target_table_name,
                                          pipeline_tgt_schema_name,
                                          pipeline_tgt_table_name, regex_replace=True, regex_exp=None,
                                          remove_ascii_from_cols=None, src_columns_with_datatype={}, tgt_columns_with_datatype={},
                                          data_profiling=True):
    if regex_replace:
        if regex_exp is None:
            regex_exp = '[\\W&&[^\\s+]]'
    src_schema, src_table = source_table_name.split('.')
    tgt_schema, tgt_table = target_table_name.split('.')
    src_columns = src_columns_with_datatype.keys()
    src_columns = [i for i in src_columns if (not i.lower().startswith("ziw"))]

    tgt_columns_with_datatype = {k.lower(): v for k, v in tgt_columns_with_datatype.items()}
    # Handle the group by columns.
    grp_by_statement = []
    grp_by_cols_to_select = []
    for item in group_by_cols:
        if item in src_columns:
            grp_by_statement.append(f"{item}")
            grp_by_cols_to_select.append(f"{item}")
        else:
            item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
            grp_by_statement.append(f"{item} as {item_modified}")
            grp_by_cols_to_select.append(f"{item_modified}")

    # For row validation
    if len(distinct_count_cols) > 0:
        columns_to_select_row_validation = distinct_count_cols
    else:
        if len(src_columns_with_datatype) > 0:
            columns_to_select_row_validation = [i for i in src_columns if (not i.lower().startswith("ziw"))]
        else:
            columns_to_select_row_validation = []
    count_expression = "1"
    distinct_count_expression = f"DISTINCT({','.join(columns_to_select_row_validation)}) "

    if len(chksum_cols) == 0:
        columns_to_select = []
        dict_columns = {}
        for i in src_columns_with_datatype:
            if regex_replace and remove_ascii_from_cols is not None:
                if i.lower() in remove_ascii_from_cols:
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
                else:
                    if (not i.lower().startswith("ziw")) and (
                            not i.lower().startswith("ifw")):
                        dict_columns[i] = (i, src_columns_with_datatype[i])
            elif regex_replace and remove_ascii_from_cols is None:
                if src_columns_with_datatype[i] == 12 and (not i.lower().startswith("ziw")) and (
                        not i.lower().startswith("ifw")):
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
                elif (not i.lower().startswith("ziw")) and (
                        not i.lower().startswith("ifw")):
                    dict_columns[i] = (i, src_columns_with_datatype[i])
            else:
                dict_columns[i] = (i, src_columns_with_datatype[i])

            if (not i.lower().startswith("ziw")) and (not i.lower().startswith("ifw")):
                columns_to_select.append(i)
        if len(dict_columns.keys()) == 0:
            for i in columns_to_select:
                dict_columns[i] = i
    else:
        dict_columns = {}
        for i in chksum_cols:
            if regex_replace and remove_ascii_from_cols is not None:
                if i.lower() in remove_ascii_from_cols:
                    dict_columns[i] = (f"regexp_replace({i}, '{regex_exp}', '')", src_columns_with_datatype[i])
            else:
                dict_columns[i] = (i, src_columns_with_datatype[i])

    select_cols_landing = []
    select_cols_staging = []
    select_cols = []
    join_cols = grp_by_cols_to_select
    select_clause = ""

    for j in dict_columns.keys():
        item_modified = j.lower().replace("(", "").replace(")", "")
        select_cols.append(f"chksum_{item_modified}")
        value, datatype = dict_columns[j]
        select_cols_staging.append(
            f"SUM(CAST(COALESCE(FARM_FINGERPRINT(nullif(trim(CAST({value} AS STRING)),'')),0) AS BIGNUMERIC)) AS source_chksum_{item_modified}")
        select_cols_landing.append(
            f"SUM(CAST(COALESCE(FARM_FINGERPRINT(nullif(trim(CAST({value} AS STRING)),'')),0) AS BIGNUMERIC)) AS target_chksum_{item_modified}")

        if data_profiling:
            tgt_datatype = tgt_columns_with_datatype[value.lower()]
            if datatype != tgt_datatype:
                col_name = f"max(CHAR_LENGTH(CAST({value} AS STRING)))"
                col_name_modified = f"max_charlength_cast_{value}_string"
                select_cols_staging.append(f"{col_name} as src_{col_name_modified}")
                select_cols_landing.append(f"{col_name} as tgt_{col_name_modified}")
                select_clause = select_clause + f"src_{col_name_modified}, " + f"tgt_{col_name_modified}, "
            # Numeric Columns
            elif datatype in [3, 7, 8, 4, -5]:
                for func in ["min", "max", "avg", "sum"]:
                    if func == "sum":
                        col_name = f"{func}(CAST({value} AS BIGNUMERIC))"
                        col_name_modified = f"sum{value}"
                    else:
                        col_name = f"{func}({value})"
                        col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                    select_cols_staging.append(f"{col_name} as src_{col_name_modified}")
                    select_cols_landing.append(f"{col_name} as tgt_{col_name_modified}")
                    select_clause = select_clause + f"src_{col_name_modified}, " + f"tgt_{col_name_modified}, "
            elif datatype == 12:
                col_name = f"max(CHAR_LENGTH({value}))"
                col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                select_cols_staging.append(f"{col_name} as src_{col_name_modified}")
                select_cols_landing.append(f"{col_name} as tgt_{col_name_modified}")
                select_clause = select_clause + f"src_{col_name_modified}, " + f"tgt_{col_name_modified}, "

    select_cols_staging.append(
        f"COUNT({distinct_count_expression}) AS SOURCE_DISTINCT_COUNT, COUNT({count_expression}) as SOURCE_COUNT")
    select_cols_landing.append(
        f"COUNT({distinct_count_expression}) AS TARGET_DISTINCT_COUNT, COUNT({count_expression}) as TARGET_COUNT")

    join_clause = ""
    for i, item in enumerate(join_cols):
        join_clause = join_clause + f"SRC_BRANCH.{item}=TGT_BRANCH.{item}"
        if i != len(join_cols) - 1:
            join_clause = join_clause + " AND "

    select_clause = select_clause.strip().strip(
        ",") + ",TARGET_DISTINCT_COUNT,TARGET_COUNT,SOURCE_DISTINCT_COUNT,SOURCE_COUNT, "
    COL_CKSUM_VALIDATION = "CASE "
    for item in select_cols:
        COL_CKSUM_VALIDATION = COL_CKSUM_VALIDATION + f" WHEN target_{item} <> source_{item} then " \
                                                      f"'FAIL' "
    COL_CKSUM_VALIDATION = COL_CKSUM_VALIDATION + " ELSE 'PASS' END"

    DUPLICATE_CHECK = """case when source_count <> source_distinct_count then 'Duplicates in source' 
    when target_count <> target_distinct_count then 'Duplicates in target' 
    else ' No Duplicates Identified' 
    END"""
    if len(join_cols) > 0:
        for i in join_cols:
            select_clause = select_clause + f'{"".join(e for e in i if e.isalnum() or e in ["_"]).upper()}' + " , "
    for item in select_cols:
        select_clause = select_clause + f"""
        case when source_{item} - target_{item} = 0 then 'PASS'
        else 'FAIL'
        END as {item}_result
        """ + ","
    select_clause = select_clause.strip(",")
    sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {select_clause} , {COL_CKSUM_VALIDATION} AS COL_CKSUM_VALIDATION, {DUPLICATE_CHECK} as DUPLICATE_CHECK, case when source_count - target_count = 0 then 'PASS' else 'FAIL' END AS COUNT_VALIDATION, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {",".join(select_cols_staging)}, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {" , ".join(grp_by_statement)}  FROM "{src_schema}"."{src_table}" ) GROUP BY {",".join(grp_by_cols_to_select)} ) AS SRC_BRANCH FULL OUTER JOIN ( SELECT {",".join(select_cols_landing)}, {" , ".join(grp_by_cols_to_select)} FROM ( SELECT {",".join(grp_by_statement)}  FROM "{tgt_schema}"."{tgt_table}" ) GROUP BY {",".join(grp_by_cols_to_select)} ) AS TGT_BRANCH ON {join_clause} """
    print("prepared sql for import : ", sql)
    print("\n\n")
    return import_sql(sql, domain_id, pipeline_id), select_cols, grp_by_cols_to_select


def prepare_sql_import_query_rowhashvalidation(domain_id, pipeline_id, source_table_name,
                                               target_table_name, colchksum_table,
                                               pipeline_tgt_schema_name,
                                               pipeline_tgt_table_name, inner_join_cols, join_type,
                                               src_columns_with_datatype={}):
    tgt_schema, tgt_table = target_table_name.split('.')
    src_schema, src_table = source_table_name.split('.')
    colchksum_table_schema, colchksum_table_name = colchksum_table.split('.')

    FULL_OUTER_JOIN_CLAUSE = f"SOURCE_BRANCH.row_hash_source = TARGET_BRANCH.row_hash_target"

    columns_to_select_src = []
    columns_to_select_tgt = []
    col_chksum_table_columns = ["COL_CKSUM_VALIDATION"]

    src_columns = [i.upper() for i in src_columns_with_datatype.keys()]

    src_inner_join_clause = ""
    tgt_inner_join_clause = ""
    columns_to_select_inner = []
    for i, item in enumerate(inner_join_cols):
        item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
        if item_modified not in src_columns:
            columns_to_select_inner.append(f"{item} as {item_modified}")
        col_chksum_table_columns.append(f"{item_modified}")
        columns_to_select_src.append(f"{item_modified}")
        columns_to_select_tgt.append(f"{item_modified}")

        src_inner_join_clause = src_inner_join_clause + f"SRC_TABLE.{item_modified}=COLCHSUM_TABLE.{item_modified}"
        tgt_inner_join_clause = tgt_inner_join_clause + f"TGT_TABLE.{item_modified}=COLCHSUM_TABLE.{item_modified}"
        if i != len(inner_join_cols) - 1:
            src_inner_join_clause = src_inner_join_clause + " AND "
            tgt_inner_join_clause = tgt_inner_join_clause + " AND "

    columns_to_select = []
    ROW_HASH = "SHA256(CONCAT("
    for col_name, datatype in src_columns_with_datatype.items():
        if not col_name.lower().startswith("ziw"):
            if col_name.upper() not in columns_to_select_src and col_name.upper() not in columns_to_select_tgt:
                columns_to_select.append(col_name)
                columns_to_select_src.append(f"{col_name} as {col_name}_src")
                columns_to_select_tgt.append(f"{col_name} as {col_name}_tgt")
            # col_chksum_table_columns.append(f"chksum_{col_name}_result")

        ROW_HASH = ROW_HASH + f"COALESCE(CAST({col_name}   AS  STRING),'')" + ","

    ROW_HASH = ROW_HASH.strip(",") + "))"

    temp_rownum_partitionby = ""
    for i, item in enumerate(inner_join_cols):
        item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
        temp_rownum_partitionby = temp_rownum_partitionby + item_modified + ","
    row_rank = f"ROW_NUMBER() OVER(PARTITION BY {temp_rownum_partitionby.strip(',')})"
    number_rows_mismatch = f"SUM (CASE WHEN row_hash_validation='FAIL' THEN 1 ELSE 0 END) OVER (PARTITION BY {temp_rownum_partitionby.strip(',')})"
    num_rows = f"SUM(1) OVER (PARTITION BY {temp_rownum_partitionby.strip(',')})"
    rowhash_validation_result = "CASE WHEN number_rows_mismatch > 0 THEN 'FAIL' ELSE 'PASS' END"
    if len(columns_to_select_inner) == 0:
        sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {row_rank} as row_rank,{number_rows_mismatch} as number_rows_mismatch, {num_rows} as num_rows, {rowhash_validation_result} as rowhash_validation_result, {",".join(col_chksum_table_columns)}, {",".join([i.strip() + "_SRC" for i in columns_to_select])}, {",".join([i.strip() + "_TGT" for i in columns_to_select])} , CASE WHEN COALESCE(TO_BASE64(ROW_HASH_SOURCE),'') != COALESCE(TO_BASE64(ROW_HASH_TARGET),'') THEN 'FAIL' ELSE 'PASS' END AS ROW_HASH_VALIDATION FROM ( SELECT * FROM ( SELECT {ROW_HASH} AS ROW_HASH_SOURCE , * FROM ( SELECT {",".join(columns_to_select_src)} FROM "{src_schema}"."{src_table}" AS SRC_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {src_inner_join_clause} ) AS SOURCE_BRANCH ) {join_type.upper()} OUTER JOIN ( SELECT {ROW_HASH} AS ROW_HASH_TARGET , * FROM ( SELECT {",".join(columns_to_select_tgt)} FROM "{tgt_schema}"."{tgt_table}" AS TGT_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {tgt_inner_join_clause} ) ) AS TARGET_BRANCH ON {FULL_OUTER_JOIN_CLAUSE} ) WHERE ROW_RANK < 5 """
    else:
        sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT '{tgt_table}' as ENTITY_NAME, {row_rank} as row_rank,{number_rows_mismatch} as number_rows_mismatch, {num_rows} as num_rows, {rowhash_validation_result} as rowhash_validation_result, {",".join(col_chksum_table_columns)}, {",".join([i.strip() + "_SRC" for i in columns_to_select])}, {",".join([i.strip() + "_TGT" for i in columns_to_select])} , CASE WHEN COALESCE(TO_BASE64(ROW_HASH_SOURCE),'') != COALESCE(TO_BASE64(ROW_HASH_TARGET),'') THEN 'FAIL' ELSE 'PASS' END AS ROW_HASH_VALIDATION FROM ( SELECT * FROM ( SELECT {ROW_HASH} AS ROW_HASH_SOURCE , * FROM ( SELECT {",".join(columns_to_select_src)} FROM ( select * , {",".join(columns_to_select_inner)} from "{src_schema}"."{src_table}" ) AS SRC_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {src_inner_join_clause} ) AS SOURCE_BRANCH ) {join_type.upper()} OUTER JOIN ( SELECT {ROW_HASH} AS ROW_HASH_TARGET , * FROM ( SELECT {",".join(columns_to_select_tgt)} FROM ( select * , {",".join(columns_to_select_inner)} from "{tgt_schema}"."{tgt_table}" ) AS TGT_TABLE INNER JOIN ( SELECT {",".join(col_chksum_table_columns)} FROM "{colchksum_table_schema}"."{colchksum_table_name}" WHERE COL_CKSUM_VALIDATION = 'FAIL' ) AS COLCHSUM_TABLE ON {tgt_inner_join_clause} ) ) AS TARGET_BRANCH ON {FULL_OUTER_JOIN_CLAUSE} ) WHERE ROW_RANK < 5 """

    print("prepared sql for import : ", sql)
    print("\n\n")

    return import_sql(sql, domain_id, pipeline_id)


def prepare_sql_import_query_datavalidation_summary(domain_id, pipeline_id, colchksum_table, rowchksum_table,
                                                    pipeline_tgt_schema_name,
                                                    pipeline_tgt_table_name,
                                                    colchksum_columns,
                                                    grp_by_cols_to_select,
                                                    entity_name):
    colchksum_table_schema, colchksum_table_name = colchksum_table.split('.')
    rowchksum_table_schema, rowchksum_table_name = rowchksum_table.split('.')
    src_columns_for_chksum = ["source_count", "target_count", "count_validation",
                              "source_distinct_count", "target_distinct_count", "duplicate_check"]
    matched_columns = ""
    mismatch_columns = ""
    for item in colchksum_columns:
        matched_columns = matched_columns + f" case when {item}_result = 'PASS' then 1 else 0 end" + "+"
        mismatch_columns = mismatch_columns + f" case when {item}_result = 'FAIL' then 1 else 0 end" + "+"

    matched_columns = matched_columns.strip("+") + " AS matched_columns"
    mismatch_columns = mismatch_columns.strip("+") + " AS mismatched_columns"
    src_columns_for_chksum.extend([matched_columns, mismatch_columns])

    grp_by_clause = ""
    for i, item in enumerate(grp_by_cols_to_select):
        grp_by_clause = grp_by_clause + f"TABLE_A.{item} = TABLE_B.{item}"
        if i < len(grp_by_cols_to_select) - 1:
            grp_by_clause = grp_by_clause + " AND "

    number_of_mismatched_cols = "case when mismatched_columns is null then 0 else mismatched_columns END AS number_of_mismatched_columns"
    # number_of_mismatched_rows = "case when Number_of_mismatched_rows_temp is null then 0 else Number_of_mismatched_rows_temp END AS number_of_mismatched_rows"
    number_of_matched_cols = "case when matched_columns is null then 0 else matched_columns END AS number_of_matched_columns"
    final_columns_to_select = f"source_count as Row_Count, count_validation,duplicate_check,'{entity_name}' as Entity_Name, current_timestamp() as Execution_date, {number_of_matched_cols}, {number_of_mismatched_cols}"

    # sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT {final_columns_to_select} FROM ( SELECT * FROM ( SELECT {','.join(src_columns_for_chksum)}, {','.join(grp_by_cols_to_select)} FROM "{colchksum_table_schema}"."{colchksum_table_name}") TABLE_A FULL OUTER JOIN ( SELECT {','.join(grp_by_cols_to_select)} , COUNT(1) AS NUMBER_OF_MISMATCHED_ROWS_TEMP FROM "{rowchksum_table_schema}"."{rowchksum_table_name}" WHERE "row_hash_validation" = 'FAIL' GROUP BY {','.join(grp_by_cols_to_select)} ) TABLE_B ON {grp_by_clause} )"""
    sql = f"""INSERT INTO "{pipeline_tgt_schema_name}"."{pipeline_tgt_table_name}" SELECT {final_columns_to_select} FROM ( SELECT * FROM ( SELECT { ','.join(src_columns_for_chksum)},{ ','.join(grp_by_cols_to_select+['entity_name','lv_row_hash_validation_result'])} FROM "{colchksum_table_schema}"."{colchksum_table_name}" ) TABLE_A FULL OUTER JOIN ( SELECT { ','.join(grp_by_cols_to_select)}, min(num_rows) as min_num_rows, MIN(number_rows_mismatch) as min_number_rows_mismatch FROM ( SELECT * , LAST_VALUE(rowhash_validation_result) OVER (PARTITION BY {','.join(grp_by_cols_to_select)} ORDER BY num_rows) AS lv_row_hash_validation_result FROM "{rowchksum_table_schema}"."{rowchksum_table_name}" ) GROUP BY { ','.join(grp_by_cols_to_select+['entity_name','lv_row_hash_validation_result'])} ) TABLE_B ON {grp_by_clause} )"""
    print("prepared sql for import : ", sql)
    print("\n\n")
    return import_sql(sql, domain_id, pipeline_id)


def create_pipeline(domain_id, pipeline_name, env_id, storage_id, compute_id):
    pipeline_obj = {}
    pipeline_obj["name"] = str(pipeline_name)
    pipeline_obj["batch_engine"] = str("BIGQUERY")
    pipeline_obj["domain_id"] = str(domain_id)
    pipeline_obj["environment_id"] = str(env_id)
    pipeline_obj["storage_id"] = str(storage_id)
    pipeline_obj["compute_template_id"] = str(compute_id)
    pipeline_obj["run_job_on_data_plane"] = False
    url_for_creating_pipeline = f"{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines"
    print(url_for_creating_pipeline)
    pipeline_body = json.dumps(pipeline_obj)

    try:
        response = requests.post(url_for_creating_pipeline, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                     'Content-Type': 'application/json'},
                                 data=pipeline_body, verify=False)
        if response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_for_creating_pipeline, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                         'Content-Type': 'application/json'},
                                     data=pipeline_body, verify=False)
        parsed_response = json.loads(response.content)
        result = parsed_response.get('result', None)
        if result:
            print("pipeline created successfully! ", result['id'])
            return str(result['id'])
        else:
            print("Failed to create pipeline")
            pipeline_base_url = url_for_creating_pipeline
            filter_condition = json.dumps({"name": pipeline_name})
            pipeline_get_url = pipeline_base_url + f"?filter={{filter_condition}}".format(
                filter_condition=filter_condition)
            response = requests.request("GET", pipeline_get_url, headers={'Authorization': 'Bearer ' + delegation_token,
                                                                          'Content-Type': 'application/json'},
                                        verify=False)
            if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                existing_pipeline_id = response.json().get("result", [])[0]["id"]
                return existing_pipeline_id
            elif response.status_code == 406:
                refresh_delegation_token()
                response = requests.request("GET", pipeline_get_url,
                                            headers={'Authorization': 'Bearer ' + delegation_token,
                                                     'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                    existing_pipeline_id = response.json().get("result", [])[0]["id"]
                    return existing_pipeline_id
    except Exception as e:
        print("Failed to create pipeline")
        print(str(e))
        return False


def modify_active_version_pipeline(domain_id, pipeline_id, tgt_properties_to_update):
    url_to_get_pl_configuration = f'{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/config-migration'
    response = requests.get(url_to_get_pl_configuration,
                            headers={'Authorization': 'Bearer ' + delegation_token, 'Content-Type': 'application/json'},
                            verify=False)
    if response.status_code == 200:
        parsed_response = json.loads(response.content)
    elif response.status_code == 406:
        refresh_delegation_token()
        response = requests.get(url_to_get_pl_configuration,
                                headers={'Authorization': 'Bearer ' + delegation_token,
                                         'Content-Type': 'application/json'},
                                verify=False)
        parsed_response = json.loads(response.content)
    else:
        parsed_response = {}

    result_configuration = parsed_response.get('result', None)
    if result_configuration is not None:
        node_keys = result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"].keys()
        for item in node_keys:
            if item.startswith("TARGET"):
                tgt_properties = result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"][item][
                    "properties"]
                tgt_properties.pop("reference_table_id", None)
                for i in tgt_properties_to_update:
                    tgt_properties[i] = tgt_properties_to_update[i]
                tgt_properties["is_existing_dataset"] = False
                result_configuration["configuration"]["pipeline_configs"]["model"]["nodes"][item][
                    "properties"] = tgt_properties
                break
        # Post call to update the details
        import_configs = {
            "run_pipeline_metadata_build": False,
            "is_pipeline_version_active": True,
            "import_data_connection": True,
            "include_optional_properties": True
        }
        response = requests.post(url_to_get_pl_configuration,
                                 headers={'Authorization': 'Bearer ' + delegation_token,
                                          'Content-Type': 'application/json'},
                                 data=json.dumps({"configuration": result_configuration["configuration"],
                                                  "import_configs": import_configs}),
                                 verify=False)
        if response.status_code == 200:
            print(f"Pipeline: {pipeline_id} re-configured")
        elif response.status_code == 406:
            refresh_delegation_token()
            response = requests.post(url_to_get_pl_configuration,
                                     headers={'Authorization': 'Bearer ' + delegation_token,
                                              'Content-Type': 'application/json'},
                                     data=json.dumps(result_configuration),
                                     verify=False)
            if response.status_code == 200:
                print(f"Pipeline: {pipeline_id} re-configured")
        else:
            print(f"Pipeline reconfiguration failed for {pipeline_id}: " + str(response.json()))


def create_import_validation_pipelines(i, q):
    while True:
        item = q.get()
        row, domain_id, environment_id, storage_id, compute_id = item
        try:
            source_name_to_fetchcols = row['source_name_to_fetchcols']
            chksum_agg_cols = row['chksum_agg_cols'].split(",") if row['chksum_agg_cols'] != "" else []
            chksum_group_cols = row['chksum_group_cols'].split(",") if row['chksum_group_cols'] != "" else []
            distinct_count_cols = row['distinct_count_cols'].split(",") if row['distinct_count_cols'] != "" else []
            join_type = row['join_type'].strip() if row['join_type'] != "" else "left"
            source_table_name = row["source_table_name"]
            target_table_name = row["target_table_name"]
            pipeline_tgt_schema_name = row['pipeline_tgt_dataset_name']
            natural_keys = row['natural_keys'].split(",") if row['natural_keys'] != "" else []
            regex_exp = row["regex_exp"] if row["regex_exp"] != "" else None
            regex_replace = row["regex_replace"] if row["regex_replace"] != "" else "False"
            if regex_replace.lower() not in ["true", "false"]:
                regex_replace = "False"
            regex_replace = eval(regex_replace.title())
            data_profiling = row["data_profiling"] if row["data_profiling"] != "" else "True"
            if data_profiling.lower() not in ["true", "false"]:
                data_profiling = "False"
            data_profiling = eval(data_profiling.title())
            remove_ascii_from_cols = row["remove_ascii_from_cols"].split(",") if row[
                                                                                     "remove_ascii_from_cols"] != "" else None

            tgt_dataset, tgt_table_name = target_table_name.split('.')
            tgt_columns_with_datatype = get_all_columns(source_name_to_fetchcols, tgt_dataset, tgt_table_name)
            src_dataset, src_table_name = source_table_name.split('.')
            src_columns_with_datatype = get_all_columns(source_name_to_fetchcols, src_dataset, src_table_name)
            if len(src_columns_with_datatype) == 0 or len(tgt_columns_with_datatype) == 0:
                print("Table column list is empty!!")
                break
            if [i.upper() for i in src_columns_with_datatype.keys()] != [i.upper() for i in
                                                                         tgt_columns_with_datatype.keys()]:
                print("Column names between Source and Target Dataset does not match. Skipping pipeline creation")
                continue
            for col_name in src_columns_with_datatype:
                src_datatype = src_columns_with_datatype[col_name]
                tgt_datatype = tgt_columns_with_datatype.get(col_name, None)
                if tgt_datatype is None:
                    if col_name.islower():
                        tgt_datatype = tgt_columns_with_datatype.get(col_name.upper(), None)
                    else:
                        tgt_datatype = tgt_columns_with_datatype.get(col_name.lower(), None)
                if src_datatype != tgt_datatype:
                    print(
                        f"Datatypes doesn't match for column {col_name} in Source ({src_datatype}) and Target ({tgt_datatype}) Datasets.")

            pipeline_id_colchksum = create_pipeline(domain_id,
                                                    f"automation_pipeline_{row['pl_suffix']}_colchksumvalidation",
                                                    environment_id, storage_id, compute_id)
            pipeline_id_rowhash = create_pipeline(domain_id,
                                                  f"automation_pipeline_{row['pl_suffix']}_rowhashvalidation",
                                                  environment_id, storage_id, compute_id)
            pipeline_id_datavalidation = create_pipeline(domain_id,
                                                         f"automation_pipeline_{row['pl_suffix']}_datavalidationsumary",
                                                         environment_id, storage_id, compute_id)

            if pipeline_id_colchksum is not None:
                pipeline_tgt_table_name = f"{row['pl_suffix'].upper()}_COLCHKSUM_VALIDATION_RESULTS"
                result, col_chksum_select_cols, grp_by_cols = prepare_sql_import_query_columnchksum(domain_id,
                                                                                                    pipeline_id_colchksum,
                                                                                                    distinct_count_cols,
                                                                                                    chksum_agg_cols,
                                                                                                    chksum_group_cols,
                                                                                                    source_table_name,
                                                                                                    target_table_name,
                                                                                                    pipeline_tgt_schema_name,
                                                                                                    pipeline_tgt_table_name,
                                                                                                    regex_replace=regex_replace,
                                                                                                    regex_exp=regex_exp,
                                                                                                    remove_ascii_from_cols=remove_ascii_from_cols,
                                                                                                    src_columns_with_datatype=src_columns_with_datatype,
                                                                                                    tgt_columns_with_datatype=tgt_columns_with_datatype,
                                                                                                    data_profiling=data_profiling)
                if result:
                    trigger_pipeline_metadata_build(domain_id, pipeline_id_colchksum)

            if pipeline_id_rowhash is not None:
                pipeline_tgt_table_name = f"{row['pl_suffix'].upper()}_ROWHASH_VALIDATION_RESULTS"
                colchksum_table = f"{pipeline_tgt_schema_name}.{row['pl_suffix'].upper()}_COLCHKSUM_VALIDATION_RESULTS"
                inner_join_cols = chksum_group_cols
                result = prepare_sql_import_query_rowhashvalidation(domain_id, pipeline_id_rowhash,
                                                                    source_table_name,
                                                                    target_table_name,
                                                                    colchksum_table,
                                                                    pipeline_tgt_schema_name,
                                                                    pipeline_tgt_table_name, inner_join_cols, join_type,
                                                                    src_columns_with_datatype=tgt_columns_with_datatype)
                if result:
                    trigger_pipeline_metadata_build(domain_id, pipeline_id_rowhash)

            if pipeline_id_datavalidation is not None:
                prepare_sql_import_query_datavalidation_summary(domain_id, pipeline_id_datavalidation,
                                                                f"{pipeline_tgt_schema_name}.{row['pl_suffix'].upper()}_COLCHKSUM_VALIDATION_RESULTS",
                                                                f"{pipeline_tgt_schema_name}.{row['pl_suffix'].upper()}_ROWHASH_VALIDATION_RESULTS",
                                                                pipeline_tgt_schema_name,
                                                                f"DATAVALIDATION_SUMMARY_RESULTS",
                                                                col_chksum_select_cols,
                                                                grp_by_cols,
                                                                target_table_name.split(".")[-1])

            # Update tgt_base_paths based on schema
            # tgt_base_paths = [
            #     f"/{pipeline_tgt_schema_name}/{row['pl_suffix']}_colhash_validation",
            #     f"/{pipeline_tgt_schema_name}/{row['pl_suffix']}_rowhash_validation",
            #     f"/{pipeline_tgt_schema_name}/{row['pl_suffix']}_datavalidation_summary"]
            sync_type_list = ["OVERWRITE", "OVERWRITE", "APPEND"]
            if pipeline_id_colchksum is not None and pipeline_id_rowhash is not None and pipeline_id_datavalidation is not None:
                for pipeline_id, sync_type in zip(
                        [pipeline_id_colchksum, pipeline_id_rowhash, pipeline_id_datavalidation], sync_type_list):
                    tgt_properties_to_update = {"build_mode": sync_type,
                                                "natural_keys": natural_keys}
                    modify_active_version_pipeline(domain_id, pipeline_id, tgt_properties_to_update)
        except Exception as e:
            print("Script failed for " + str(row) + str(e))
        finally:
            q.task_done()


def main():
    domain_id = config.get('environment_details', 'domain_id')
    environment_id = config.get('environment_details', 'environment_id')
    storage_id = config.get('environment_details', 'storage_id')
    compute_id = config.get('environment_details', 'compute_id')
    global refresh_token
    refresh_token = config.get('environment_details', 'refresh_token')
    refresh_delegation_token()

    for i in range(num_fetch_threads):
        worker = Thread(target=create_import_validation_pipelines, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()

    input_file = csv.DictReader(open("input_data.csv"))
    for row in input_file:
        job_queue.put((row, domain_id, environment_id, storage_id, compute_id))

    print('*** Main thread waiting ***')
    job_queue.join()
    print('*** Done ***')


if __name__ == '__main__':
    main()