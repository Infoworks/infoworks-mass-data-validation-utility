import argparse
import csv
import json
import queue
import subprocess
import sys
import time
from configparser import ConfigParser
from threading import Thread

import pkg_resources
import requests

from local_configurations import *

required = {'requests'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)


class Profiling:
    def __init__(self, pipeline_number):
        self.delegation_token = ""
        self.refresh_token = ""
        self.proxy_host = ""
        self.proxy_port = ""
        self.session: requests.sessions.Session
        self.initialize_adapter()
        self.num_fetch_threads = NUMBER_OF_THREADS
        self.job_queue_1 = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self.job_queue_2 = queue.Queue(maxsize=QUEUE_MAX_SIZE)
        self.bigquery_tables_to_crawl = []
        self.sql_pipelines_final = []
        self.config_variables = {}
        self.pipeline_number = pipeline_number

    def initialize_adapter(self):
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=SESSION_MAX_RETRIES)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def read_variables_from_config_file(self):
        config = ConfigParser()
        config.read('config.ini')
        self.proxy_host = "{}://{}".format(config.get('infoworks_details', 'protocol'),
                                           config.get('infoworks_details', 'host'))
        self.proxy_port = config.get('infoworks_details', 'port')

        config_dict = {}
        for section in config.sections():
            config_dict[section] = {}
            for key, value in config.items(section):
                config_dict[section][key] = value

        self.config_variables = config_dict
        self.refresh_token = config.get('infoworks_details', 'refresh_token')

    def refresh_delegation_token(self):
        url = "{ip}:{port}/v3/security/token/access/".format(ip=self.proxy_host, port=self.proxy_port)
        headers = {
            'Authorization': 'Basic ' + self.refresh_token,
            'Content-Type': 'application/json'
        }
        response = requests.request("GET", url, headers=headers, verify=False)
        self.delegation_token = response.json().get("result").get("authentication_token")

    def poll_job(self, job_id):
        failed_count = 0
        response = {}
        timeout = time.time() + 300
        while True:
            if time.time() > timeout:
                break
            try:
                job_monitor_url = f"{self.proxy_host}:{self.proxy_port}/v3/admin/jobs/{job_id}"
                response = requests.get(job_monitor_url,
                                        headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 406:
                    self.refresh_delegation_token()
                    response = requests.get(job_monitor_url,
                                            headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                     'Content-Type': 'application/json'}, verify=False)
                parsed_response = json.loads(response.content)
                result = parsed_response.get('result', None)
                if len(result) != 0:
                    job_status = result["status"]
                    print(f"Polling job {job_id}. Status: {job_status} ")
                    if job_status in ["completed", "failed", "aborted"]:
                        if job_status == "completed":
                            return 0
                        else:
                            return 1
                else:
                    if failed_count >= JOB_POLLING_RETRIES - 1:
                        return 1
                    failed_count = failed_count + 1
            except Exception as e:
                if failed_count >= JOB_POLLING_RETRIES - 1:
                    print(response)
                    return 1
                failed_count = failed_count + 1
            time.sleep(POLLING_FREQUENCY)

    def import_sql(self, sql, domain_id, pipeline_id):
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
        url_for_sql_import = f"{self.proxy_host}:{self.proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/sql-import"
        print("URL for sql Import: ", url_for_sql_import)
        print("\n")
        try:
            response = self.session.post(url_for_sql_import,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'},
                                         data=sql_import_body_json,
                                         timeout=API_REQUEST_TIMEOUT, verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(url_for_sql_import,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'},
                                         data=sql_import_body_json, verify=False, timeout=API_REQUEST_TIMEOUT)
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

    def get_all_columns(self, source_name, schemaNameAtSource, origTableName):
        all_columns_with_datatype_dict = {}
        filter_condition = json.dumps({"name": source_name})
        url_to_list_source = f'{self.proxy_host}:{self.proxy_port}/v3/sources' + f"?filter={{filter_condition}}".format(
            filter_condition=filter_condition)
        response = requests.request("GET", url_to_list_source,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                                             'Content-Type': 'application/json'},
                                    verify=False)
        if response.status_code == 200 and len(response.json().get("result", [])) > 0:
            result = response.json().get("result", [])
            source_id = result[0]["id"]
            src_type = result[0]["type"]
            src_sub_type = result[0]["sub_type"]
            if source_id is not None:
                if src_type == "rdbms":
                    filter_condition_dict = {"schemaNameAtSource": schemaNameAtSource,
                                             "origTableName": origTableName}
                elif src_type == "analytics" and src_sub_type == "apachehive":
                    filter_condition_dict = {"schemaNameAtSource": schemaNameAtSource,
                                             "origTableName": origTableName}
                elif src_type == "metadatacrawl" and src_sub_type == "bigquerymetacrawl":
                    filter_condition_dict = {"datasetNameAtSource": schemaNameAtSource,
                                             "origTableName": origTableName}
                else:
                    filter_condition_dict = {"configuration.target_schema_name": schemaNameAtSource,
                                             "configuration.target_table_name": origTableName}

                filter_condition = json.dumps(filter_condition_dict)
                url_to_list_tables = f'{self.proxy_host}:{self.proxy_port}/v3/sources/{source_id}/tables' + f"?filter={{filter_condition}}".format(
                    filter_condition=filter_condition)
                response = requests.request("GET", url_to_list_tables,
                                            headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                     'Content-Type': 'application/json'}, verify=False)
                if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                    result = response.json().get("result", [])
                    columns = result[0]["columns"]
                    all_columns_with_datatype_dict = {}
                    for i in columns:
                        if not i["name"].lower().startswith("ziw"):
                            all_columns_with_datatype_dict[i["name"]] = i['target_sql_type']

        return all_columns_with_datatype_dict

    @staticmethod
    def prepare_sql_for_query_as_table(src_type, group_by_cols,
                                       source_table_name,
                                       src_columns_with_datatype={}
                                       ):
        src_schema, src_table = source_table_name.split('.')
        src_columns = src_columns_with_datatype.keys()
        src_columns = [i for i in src_columns if (not i.lower().startswith("ziw"))]

        # Handle the group by columns.
        grp_by_statement = []
        grp_by_cols_to_select = []
        join_cols = []
        for item in group_by_cols:
            if item.casefold() in map(str.casefold, src_columns):
                grp_by_statement.append(f"{item}")
                grp_by_cols_to_select.append(f"{item}")
                join_cols.append(f"{item}")
            else:
                item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
                grp_by_statement.append(f"{item} as {item_modified}")
                grp_by_cols_to_select.append(f"{item}")
                join_cols.append(f"{item_modified}")

        dict_columns = {}
        for i in src_columns_with_datatype:
            dict_columns[i] = (i, src_columns_with_datatype[i])

        select_cols_src = []
        select_cols_tgt = []
        select_clause = ",".join(grp_by_statement)
        for j in dict_columns.keys():
            value, datatype = dict_columns[j]
            # Numeric Columns
            if datatype in [3, 7, 8, 4, -5]:
                for func in ["min", "max", "avg", "sum"]:
                    if func == "sum":
                        col_name = f"{func}(CAST({value} AS NUMERIC))"
                        col_name_modified = f"SUM{value}"
                    else:
                        col_name = f"{func}({value})"
                        col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                    select_cols_src.append(f"{col_name} as {col_name_modified}_source")
                    select_cols_tgt.append(f"{col_name} as {col_name_modified}_target")
            elif datatype == 12:
                if src_type == "rdbms":
                    col_name = f"max(LEN({value}))"
                elif src_type.lower() == "hive":
                    col_name = f"max(LENGTH({value}))"
                else:
                    col_name = f"max(CHAR_LENGTH({value}))"
                col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                select_cols_src.append(f"{col_name} as {col_name_modified}_source")
                select_cols_tgt.append(f"{col_name} as {col_name_modified}_target")

        select_clause = select_clause + "," + ",".join(select_cols_src)
        select_clause = select_clause.strip(",")
        sql_pipeline_1 = f"""SELECT {select_clause}  FROM "{src_schema}"."{src_table}" GROUP BY {",".join(grp_by_cols_to_select)} """
        print("prepared sql for import : ", sql_pipeline_1)
        print("\n\n")
        return sql_pipeline_1

    @staticmethod
    def prepare_sql_for_bigquery_pipeline(src_type, src_columns_with_datatype, group_by_cols, target_table_name,
                                          pipeline_tgt_dataset_name,
                                          pipeline_tgt_table_name_1, pipeline_tgt_table_name_2):
        tgt_schema, tgt_table = target_table_name.split('.')
        src_columns = src_columns_with_datatype.keys()
        src_columns = [i for i in src_columns if (not i.lower().startswith("ziw"))]
        # Handle the group by columns.
        grp_by_statement = []
        grp_by_cols_to_select = []
        join_cols = []
        for item in group_by_cols:
            if item.casefold() in map(str.casefold, src_columns):
                grp_by_statement.append(f"{item}")
                grp_by_cols_to_select.append(f"{item}")
                join_cols.append(f"{item}")
            else:
                item_modified = ''.join(e for e in item if e.isalnum() or e in ["_"]).upper()
                grp_by_statement.append(f"{item} as {item_modified}")
                grp_by_cols_to_select.append(f"{item}")
                join_cols.append(f"{item_modified}")
        src_select_cols = join_cols[::]
        dict_columns = {}
        for i in src_columns_with_datatype:
            dict_columns[i] = (i, src_columns_with_datatype[i])
        select_cols_tgt = []
        select_cols_to_return = []
        for j in dict_columns.keys():
            value, datatype = dict_columns[j]
            # Numeric Columns
            if datatype in [3, 7, 8, 4, -5]:
                for func in ["min", "max", "avg", "sum"]:
                    if func == "sum":
                        col_name = f"{func}(CAST({value} AS NUMERIC))"
                        col_name_modified = f"sum{value}"
                    else:
                        col_name = f"{func}({value})"
                        col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                    select_cols_tgt.append(f"{col_name} as {col_name_modified}_target")
                    select_cols_to_return.append(col_name_modified)
            elif datatype == 12:
                if src_type.lower() == "rdbms":
                    col_name = f"max(CHAR_LENGTH({value}))"
                    col_name_modified = ''.join(e for e in f"max(LEN({value}))" if e.isalnum() or e in ["_"]).upper()
                elif src_type.lower() == "hive":
                    col_name = f"max(CHAR_LENGTH({value}))"
                    col_name_modified = ''.join(e for e in f"max(LENGTH({value}))" if e.isalnum() or e in ["_"]).upper()
                else:
                    col_name = f"max(CHAR_LENGTH({value}))"
                    col_name_modified = ''.join(e for e in col_name if e.isalnum() or e in ["_"]).upper()
                select_cols_tgt.append(f"{col_name} as {col_name_modified}_target")
                select_cols_to_return.append(col_name_modified)
        case_when = ""
        select_clause_tgt = ",".join(grp_by_statement)
        for item in select_cols_to_return:
            case_when = case_when + f"CASE WHEN {item}_SOURCE = {item}_TARGET THEN 'PASS' ELSE 'FAIL' END AS {item}_VALIDATION" + ","
            src_select_cols.append(f"{item}_SOURCE")
        select_clause_tgt = select_clause_tgt + "," + ",".join(select_cols_tgt)
        select_clause_tgt = select_clause_tgt.strip(",")
        case_when = case_when.strip(",")
        join_clause = ""
        for i, j in enumerate(join_cols):
            if i != len(join_cols) - 1:
                join_clause = join_clause + f"TABLE_SRC.{j} = TABLE_TGT.{j}" + " AND "
            else:
                join_clause = join_clause + f"TABLE_SRC.{j} = TABLE_TGT.{j}"
        sql_pipeline_2 = f"""
            INSERT INTO "{pipeline_tgt_dataset_name}"."{pipeline_tgt_table_name_2}" 
            SELECT {case_when} FROM 
            (
            SELECT {",".join(src_select_cols)}
            FROM "{pipeline_tgt_dataset_name}"."{pipeline_tgt_table_name_1}" 
            ) AS TABLE_SRC
            INNER JOIN
            (
            SELECT {select_clause_tgt}  FROM "{tgt_schema}"."{tgt_table}" GROUP BY {",".join(grp_by_cols_to_select)} 
            ) AS TABLE_TGT
            ON {join_clause}
            """
        print("prepared sql for import : ", sql_pipeline_2)
        print("\n\n")
        return sql_pipeline_2

    def create_query_as_table(self, source_id, query=None, target_schema_name=None, target_table_name=None,
                              tables_to_add=None):
        try:
            query_as_table_url = f"{self.proxy_host}:{self.proxy_port}/v3/sources/{source_id}/tables/query_tables"
            if tables_to_add is not None:
                table_payload = {"tables_to_add": tables_to_add}
            else:
                table_payload = {"tables_to_add": [{"table_type": "TABLE", "is_query_table": True,
                                                    "query": query,
                                                    "target_table_name": target_table_name,
                                                    "target_schema_name": target_schema_name
                                                    }
                                                   ]}
            response = requests.post(
                query_as_table_url,
                headers={'Authorization': 'Bearer ' + self.delegation_token,
                         'Content-Type': 'application/json'},
                json=table_payload, verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(
                    query_as_table_url,
                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                             'Content-Type': 'application/json'},
                    json=table_payload, verify=False)
            parsed_response = json.loads(response.content)
            if response is not None:
                result = parsed_response.get("result", None)
                if result is None:
                    print("Could not create query as table")
                    return False
                return result
        except Exception as e:
            print("Failed to create query as table")
            print(str(e))
            return False

    def create_pipeline(self, domain_id, pipeline_obj):
        pipeline_name = pipeline_obj["name"]
        url_for_creating_pipeline = f"{self.proxy_host}:{self.proxy_port}/v3/domains/{domain_id}/pipelines"
        print(url_for_creating_pipeline)
        pipeline_body = json.dumps(pipeline_obj)
        try:
            response = requests.post(url_for_creating_pipeline,
                                     headers={'Authorization': 'Bearer ' + self.delegation_token,
                                              'Content-Type': 'application/json'},
                                     data=pipeline_body, verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(url_for_creating_pipeline,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
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
                response = requests.request("GET", pipeline_get_url,
                                            headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                     'Content-Type': 'application/json'},
                                            verify=False)
                if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                    existing_pipeline_id = response.json().get("result", [])[0]["id"]
                    return existing_pipeline_id
                elif response.status_code == 406:
                    self.refresh_delegation_token()
                    response = requests.request("GET", pipeline_get_url,
                                                headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                         'Content-Type': 'application/json'}, verify=False)
                    if response.status_code == 200 and len(response.json().get("result", [])) > 0:
                        existing_pipeline_id = response.json().get("result", [])[0]["id"]
                        return existing_pipeline_id
        except Exception as e:
            print("Failed to create pipeline")
            print(str(e))
            return False

    def modify_active_version_pipeline(self, domain_id, pipeline_id, tgt_properties_to_update):
        url_to_get_pl_configuration = f'{self.proxy_host}:{self.proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/config-migration'
        response = requests.get(url_to_get_pl_configuration,
                                headers={'Authorization': 'Bearer ' + self.delegation_token,
                                         'Content-Type': 'application/json'},
                                verify=False)
        if response.status_code == 200:
            parsed_response = json.loads(response.content)
        elif response.status_code == 406:
            self.refresh_delegation_token()
            response = requests.get(url_to_get_pl_configuration,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
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
                                     headers={'Authorization': 'Bearer ' + self.delegation_token,
                                              'Content-Type': 'application/json'},
                                     data=json.dumps({"configuration": result_configuration["configuration"],
                                                      "import_configs": import_configs}),
                                     verify=False)
            if response.status_code == 200:
                print(f"Pipeline: {pipeline_id} re-configured")
            elif response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(url_to_get_pl_configuration,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'},
                                         data=json.dumps(result_configuration),
                                         verify=False)
                if response.status_code == 200:
                    print(f"Pipeline: {pipeline_id} re-configured")
            else:
                print(f"Pipeline reconfiguration failed for {pipeline_id}: " + str(response.json()))

    def browse_crawl_bq_source(self, source_id, project_name, schemas_filter, tables_filter):
        """
        filter_tables_properties = {
            "schemas_filter" : "%dbo",
            "catalogs_filter" : "%",
            "tables_filter" : "%csv_incremental_test",
            "is_data_sync_with_filter" : true,
            "is_filter_enabled" : true
        }
        """
        status_output = False
        tables_to_add_config = []
        for item in list(set(self.bigquery_tables_to_crawl)):
            ds, table = item.split(".")
            schemas_filter.append(ds)
            tables_filter.append(table)
            tables_to_add_config.append({
                "table_name": table,
                "schema_name": ds,
                "catalog_name": project_name,
                "table_type": "TABLE",
                "target_dataset_name": ds,
                "target_table_name": table
            })
        filter_tables_properties = {
            "schemas_filter": ",".join(schemas_filter),
            "tables_filter": ",".join(tables_filter),
            "is_data_sync_with_filter": True,
            "is_filter_enabled": True
        }
        poll_timeout = 300
        polling_frequency = 15
        retries = 3
        browse_job_status = False
        try:
            url_for_browse_source = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/source_tables'.format(
                proxy_host=self.proxy_host,
                proxy_port=self.proxy_port,
                source_id=source_id)
            if filter_tables_properties is not None:
                filter_condition = f"?is_filter_enabled=true&tables_filter={filter_tables_properties['tables_filter']}&schemas_filter={filter_tables_properties['schemas_filter']}"
                url_for_browse_source = url_for_browse_source + filter_condition

            response = requests.get(url_for_browse_source,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                                             'Content-Type': 'application/json'}, verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.get(url_for_browse_source,
                                        headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                 'Content-Type': 'application/json'}, verify=False)
            result = json.loads(response.content).get("result", {})
        except Exception as e:
            raise Exception(f"Failed to create browse table job for {source_id} " + str(e))
        if len(result) == 0 and "id" not in result:
            print(f"Failed to create browse table job")
        else:
            job_id = result.get("id")
            job_status = "running"
            failed_count = 0
            timeout = time.time() + poll_timeout
            while True:
                if time.time() > timeout:
                    break
                try:
                    url_for_interactive_job_poll = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/interactive-jobs/{interactive_job_id}'.format(
                        proxy_host=self.proxy_host,
                        proxy_port=self.proxy_port,
                        source_id=source_id,
                        interactive_job_id=job_id)
                    print('url to poll interactive job - ' + url_for_interactive_job_poll)
                    response = requests.get(url_for_interactive_job_poll,
                                            headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                     'Content-Type': 'application/json'}, verify=False)

                    if response.status_code == 406:
                        self.refresh_delegation_token()
                        response = requests.get(url_for_interactive_job_poll,
                                                headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                         'Content-Type': 'application/json'}, verify=False)
                    result = json.loads(response.content).get("result", {})
                    if len(result) == 0:
                        print(f"Failed to poll interactive job {job_id}")
                        job_status = None
                    else:
                        job_status = result["status"]
                    print("Browse source job poll status : " + job_status)
                    if job_status in ["completed", "failed", "aborted"]:
                        break
                    if job_status is None:
                        print(f"Error occurred during job {job_id} status poll")
                        if failed_count >= retries - 1:
                            break
                        failed_count = failed_count + 1
                except Exception as e:
                    print("Error occurred during job status poll")
                    if failed_count >= retries - 1:
                        raise Exception(f"Error occurred during job status poll {source_id} " + str(e))
                    failed_count = failed_count + 1
                time.sleep(polling_frequency)
            if job_status == "completed":
                print(f"Browse table job for source {source_id} was successful")
                browse_job_status = True
            else:
                print(f"Browse table job for source {source_id} failed with {job_status}")
                browse_job_status = False

        if browse_job_status:
            # Continue with crawl
            try:
                url_for_add_tables_to_source = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables/source_tables'.format(
                    proxy_host=self.proxy_host, proxy_port=self.proxy_port, source_id=source_id)
                add_tables_dict = {"tables_to_add": tables_to_add_config}

                response = requests.post(url_for_add_tables_to_source,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'}, data=json.dumps(add_tables_dict),
                                         verify=False)
                if response.status_code == 406:
                    self.refresh_delegation_token()
                    response = requests.post(url_for_add_tables_to_source,
                                             headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                      'Content-Type': 'application/json'},
                                             data=json.dumps(add_tables_dict),
                                             verify=False)

                result = json.loads(response.content).get("result", {})
                if len(result) != 0:
                    print(f"Added the below table Ids to the source {source_id}")
                    print(result["added_tables"])
                    print(f"Triggered metacrawl job for tables. Infoworks JobID {result['job_created']}")
                    job_id = result['job_created']
                    status = self.poll_job(job_id)
                    if status == 0:
                        print("Metacrawl job completed")
                        status_output = True
                else:
                    print(f"Failed to add the tables to the source {source_id}")
            except Exception as e:
                print(f"Failed to add the tables to the source {source_id}")
        return status_output

    def trigger_pipeline_job(self, domain_id, pipeline_id, pipeline_version_id=None):
        url_for_pipeline_build = '{proxy_host}:{proxy_port}/v3/domains/{domain_id}/pipelines/{pipeline_id}/jobs'.format(
            proxy_host=self.proxy_host,
            proxy_port=self.proxy_port,
            domain_id=domain_id,
            pipeline_id=pipeline_id)
        if pipeline_version_id is not None:
            request_body = {
                "job_type": "pipeline_build",
                "version_id": str(pipeline_version_id)
            }
        else:
            request_body = {
                "job_type": "pipeline_build"
            }
        try:
            response = requests.post(url_for_pipeline_build,
                                     headers={'Authorization': 'Bearer ' + self.delegation_token,
                                              'Content-Type': 'application/json'}, data=json.dumps(request_body),
                                     verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(url_for_pipeline_build,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'}, data=json.dumps(request_body),
                                         verify=False)

            result = json.loads(response.content).get('result', {})
        except Exception as e:
            raise Exception(f"Failed to submit pipeline build job for {pipeline_id} " + str(e))
        if len(result) != 0 and "id" in result:
            job_id = result["id"]
            status = self.poll_job(job_id=job_id)
            if status == 0:
                print("Pipeline build is completed")
            else:
                print("Pipeline build failed")
        else:
            raise Exception(f"Failed to submit pipeline build job for {pipeline_id} ")

    def submit_source_job(self, source_id, body, poll=False):
        """
        Function to trigger the jobs related to source
        :param source_id: source entity id
        :type source_id: String
        :param body: JSON body containing type of job to trigger
        :type body: JSON dict

        metadata_job_body_example = {
                "job_type": "source_fetch_metadata",
                "overwrite": true
            }

        ingest_table_body_example = {
            "job_type": "cdc_merge" or "truncate_reload",
            "job_name": "testing",
            "interactive_cluster_id": "536592c8ceb69bbbe730d452",
            "table_ids": [
                     "614618debf711275204a1b1f"
                ]
        }

        ingest_table_group_body_example = {
            "job_type": "truncate_reload",
            "table_group_id": "efb9d3e810c643b9930e1a00"
        }
        :param poll: Poll job until its completion
        :type poll: Boolean
        :return:  response dict
        """
        try:
            submit_source_job_url = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/jobs'.format(
                proxy_host=self.proxy_host,
                proxy_port=self.proxy_port,
                source_id=source_id)
            response = requests.post(submit_source_job_url,
                                     headers={'Authorization': 'Bearer ' + self.delegation_token,
                                              'Content-Type': 'application/json'}, data=json.dumps(body),
                                     verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.post(submit_source_job_url,
                                         headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                  'Content-Type': 'application/json'}, data=json.dumps(body),
                                         verify=False)

            result = json.loads(response.content).get('result', {})
            if len(result) != 0:
                job_id = result["id"]
                if not poll:
                    return True
                else:
                    return self.poll_job(job_id=job_id)
            else:
                print("Failed to submit the source job")
                return False
        except Exception as e:
            raise print("Failed to submit the source job")

    def get_table_details(self, source_id, table_id=None, filter_condition=None):
        try:
            if table_id is not None:
                url_to_get_source_details = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables/{table_id}'.format(
                    proxy_host=self.proxy_host,
                    proxy_port=self.proxy_port,
                    source_id=source_id, table_id=table_id)
            if filter_condition is not None:
                url_to_get_source_details = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables'.format(
                    proxy_host=self.proxy_host,
                    proxy_port=self.proxy_port,
                    source_id=source_id)
                url_to_get_source_details = url_to_get_source_details + f"?{json.dumps(filter_condition)}"
            response = requests.get(url_to_get_source_details,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                                             'Content-Type': 'application/json'},
                                    verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.get(url_to_get_source_details,
                                        headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                 'Content-Type': 'application/json'},
                                        verify=False)

            result = json.loads(response.content).get('result', {})
            if len(result) > 0:
                if type(result) == list:
                    return result[0]
                else:
                    return result

        except Exception as e:
            raise print("Failed to get the source details")

    def update_table_details(self, source_id, table_id, config):
        try:
            url_to_get_source_details = '{proxy_host}:{proxy_port}/v3/sources/{source_id}/tables/{table_id}'.format(
                proxy_host=self.proxy_host,
                proxy_port=self.proxy_port,
                source_id=source_id, table_id=table_id)
            response = requests.put(url_to_get_source_details,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                                             'Content-Type': 'application/json'},
                                    verify=False, data=json.dumps(config))
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.put(url_to_get_source_details,
                                        headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                 'Content-Type': 'application/json'},
                                        verify=False, data=json.dumps(config))
            if response.status_code == 200:
                print("Successfully updated the table configurations")

        except Exception as e:
            raise print("Failed to get the source details")

    def create_import_data_profiling_source_pipelines(self, rows, data_prof_source_id, src_type):
        tables_to_add = []
        for row in rows:
            try:
                source_name_to_fetchcols = row['source_name_to_fetchcols']
                group_by_cols = row['group_by_cols'].split(",") if row['group_by_cols'] != "" else []
                source_table_name = row["source_table_name"]
                target_table_name = row["target_table_name"]
                pipeline_tgt_schema_name = row['pipeline_tgt_dataset_name']
                tgt_schema_name, tgt_table_name = target_table_name.split('.')
                tgt_columns_with_datatype = self.get_all_columns(source_name_to_fetchcols, tgt_schema_name,
                                                                 tgt_table_name)

                sql_pipeline_1 = self.prepare_sql_for_query_as_table(src_type, group_by_cols,
                                                                     source_table_name,
                                                                     src_columns_with_datatype=tgt_columns_with_datatype,
                                                                     )
                tables_to_add.append({"table_type": "TABLE", "is_query_table": True,
                                      "query": sql_pipeline_1,
                                      "target_table_name": f"{row['pl_suffix'].upper()}_DATA_PROFILING_SOURCE",
                                      "target_schema_name": pipeline_tgt_schema_name
                                      }
                                     )
            except Exception as e:
                print("Script failed for " + str(row) + str(e))

        response = self.create_query_as_table(data_prof_source_id,
                                              tables_to_add=tables_to_add)
        if response is not False:
            tables_added = response["added_tables"]  # List
            metadata_crawl_job = response["job_created"]  # String
            print(response)
            return tables_added, metadata_crawl_job
        else:
            print(response)
            return [], None

    def create_import_data_profiling_pipelines(self, i, q):
        while True:
            item = q.get()
            pl_suffix, domain_id, environment_id, sql_pipeline, pipeline_tgt_schema_name = item
            try:
                pipeline_obj_data_profiling = {"name": str(f"automation_pipeline_{pl_suffix}_data_profiling"),
                                               "batch_engine": str("BIGQUERY"), "domain_id": str(domain_id),
                                               "environment_id": str(environment_id), "run_job_on_data_plane": False}
                pipeline_id_data_profiling = self.create_pipeline(domain_id, pipeline_obj_data_profiling)
                if pipeline_id_data_profiling is not None:
                    result = self.import_sql(sql_pipeline, domain_id, pipeline_id_data_profiling)
                    if not result:
                        print("Script failed to create/import pipeline " + str(
                            f"automation_pipeline_{pl_suffix}_data_profiling"))
                    else:
                        print(
                            f'Pipeline ' + str(
                                f"automation_pipeline_{pl_suffix}_data_profiling") + ' created successfully')
                        tgt_properties_to_update = {"build_mode": "OVERWRITE", "natural_keys": [],
                                                    "dataset_name": pipeline_tgt_schema_name}
                        self.modify_active_version_pipeline(domain_id, pipeline_id_data_profiling,
                                                            tgt_properties_to_update)

            except Exception as e:
                print("Script failed to create/import pipeline " + str(
                    f"automation_pipeline_{pl_suffix}_data_profiling" + str(e)))
            finally:
                q.task_done()

    def get_compute_template_details(self, environment_id):
        url_for_getting_compute_template = f"{self.proxy_host}:{self.proxy_port}/v3/admin/environment/{environment_id}/environment-interactive-clusters"
        compute_details = []
        try:
            response = requests.get(url_for_getting_compute_template,
                                    headers={'Authorization': 'Bearer ' + self.delegation_token,
                                             'Content-Type': 'application/json'},
                                    verify=False)
            if response.status_code == 406:
                self.refresh_delegation_token()
                response = requests.get(url_for_getting_compute_template,
                                        headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                 'Content-Type': 'application/json'},
                                        verify=False)
            parsed_response = json.loads(response.content)
            if response is not None and "result" in parsed_response:
                result = parsed_response.get("result", None)
                if result is None:
                    print("Failed to get environment compute details for given id")
                while len(result) > 0:
                    compute_details.extend(result)
                    nextUrl = '{ip}:{port}{next}'.format(next=parsed_response.get('links')['next'],
                                                         ip=self.proxy_host,
                                                         port=self.proxy_port
                                                         )
                    response = requests.get(nextUrl,
                                            headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                     'Content-Type': 'application/json'},
                                            verify=False)
                    if response.status_code == 406:
                        self.refresh_delegation_token()
                        response = requests.get(nextUrl,
                                                headers={'Authorization': 'Bearer ' + self.delegation_token,
                                                         'Content-Type': 'application/json'},
                                                verify=False)
                    parsed_response = json.loads(response.content)
                    result = parsed_response.get("result", [])
            else:
                print("Failed to get compute template details")
            return compute_details
        except Exception as e:
            print("Error in getting compute template details" + str(e))
            return None


def main():
    parser = argparse.ArgumentParser(description='Profiling pipelines creation')
    parser.add_argument('--pipeline_number', required=False, default="prof_pipeline1",
                        help='Pass either prof_pipeline1, prof_pipeline2 or both. prof_pipeline1 is for creating pipelines that read data from source and write to BQ. '
                             'prof_pipeline2 is for building pipelines that validate the profiling data in BQ'
                             'both -> 1. Will create query as table'
                             '        2. Ingests all the query as table created as part of the above step in a single interactive cluster '
                             '        3. Crawls the BigQuery tables that were created as part of Ingestion'
                             '        4. Then creates pipelines in Infoworks.'
                             'Note: ')
    parser.add_argument('--input_file_path', required=False, default="input_data.csv",
                        help="The the fully qualified path of the parameter file")
    parser.add_argument('--dataprofiling_source_id', required=True,
                        help='Provide the source id in which the query as a table profiling tables has to be created')
    parser.add_argument('--dataprofiling_source_type', required=False, default="hive",
                        help='Provide the source type, rdbms/hive')
    args = parser.parse_args()
    metacrawl_job_submitted = None
    pipeline_number = str(args.pipeline_number).lower()
    dataprofiling_source_id = str(args.dataprofiling_source_id)
    dataprofiling_source_type = str(args.dataprofiling_source_type)
    input_file_path = args.input_file_path
    profiling_obj = Profiling(pipeline_number)
    profiling_obj.read_variables_from_config_file()
    profiling_obj.refresh_delegation_token()

    input_file = csv.DictReader(open(input_file_path))
    domain_id = profiling_obj.config_variables.get("infoworks_details", {}).get("domain_id", "")
    bq_environment_id = profiling_obj.config_variables.get("bq_environment_details", {}).get("environment_id", "")

    list_of_all_computes = profiling_obj.get_compute_template_details(bq_environment_id)
    for item in list_of_all_computes:
        if item["is_default_cluster"]:
            interactive_compute_id = item["id"]
            break
    else:
        interactive_compute_id = list_of_all_computes[0]["id"]

    tables_added = []
    table_ds_mapping = {}
    rows = []
    ds_table_mapping = {}
    for row in input_file:
        table_ds_mapping[f"{row['pl_suffix'].upper()}_DATA_PROFILING_SOURCE"] = row['pipeline_tgt_dataset_name']
        rows.append(row)
        if row['pipeline_tgt_dataset_name'] in ds_table_mapping:
            ds_table_mapping[row['pipeline_tgt_dataset_name']].append(
                f"{row['pl_suffix'].upper()}_DATA_PROFILING_SOURCE")
        else:
            ds_table_mapping[row['pipeline_tgt_dataset_name']] = [f"{row['pl_suffix'].upper()}_DATA_PROFILING_SOURCE"]
    if pipeline_number in ["prof_pipeline1", "both"]:
        tables_added, metacrawl_job_submitted = profiling_obj.create_import_data_profiling_source_pipelines(rows,
                                                                                                            dataprofiling_source_id,
                                                                                                            dataprofiling_source_type)
        # For all the tables_added modify the target datasetname until IPD-21233 is resolved
        if metacrawl_job_submitted is not None:
            profiling_obj.poll_job(metacrawl_job_submitted)
        for table_id in tables_added:
            table_config_to_update = {}
            table_config = profiling_obj.get_table_details(source_id=dataprofiling_source_id, table_id=table_id)
            table_name = table_config["name"]
            table_config_to_update["name"] = table_config["name"]
            table_config_to_update["source"] = table_config["source"]
            table_config_to_update["configuration"] = table_config["configuration"]
            table_config_to_update["configuration"]["target_dataset_name"] = table_ds_mapping[table_name]
            profiling_obj.update_table_details(source_id=dataprofiling_source_id, table_id=table_id,
                                               config=table_config)

        print('*** Created the Query as Tables. Any errors are in the response printed above ***')

    if pipeline_number in ["prof_pipeline2", "both"]:
        if pipeline_number == "both":
            ingestion_job_status = False
            if metacrawl_job_submitted is not None and profiling_obj.config_variables.get('others').get(
                    'trigger_ingestion_src_tables').lower() == "true":
                profiling_obj.poll_job(metacrawl_job_submitted)
                # Trigger Ingestion of all the query as tables
                if len(tables_added) > 0:
                    body = {
                        "job_type": "truncate_reload",
                        "job_name": "full-load",
                        "interactive_cluster_id": interactive_compute_id,
                        "table_ids": tables_added
                    }
                    ingestion_job_status = profiling_obj.submit_source_job(source_id=dataprofiling_source_id, body=body, poll=True)
            if ingestion_job_status:
                print("Ingestion Job is successful. Hence crawling the BigQuery source")
                if profiling_obj.config_variables.get('others').get('browse_crawl_bq_source').lower() == "true":
                    bq_source_id = profiling_obj.config_variables.get("bq_source_details").get("bq_source_id")
                    project_name = profiling_obj.config_variables.get("bq_source_details").get("project_name")
                    print(f"Crawling Big Query Source")
                    for dataset in ds_table_mapping:
                        tables_to_crawl = ds_table_mapping[dataset]
                        status = profiling_obj.browse_crawl_bq_source(bq_source_id, project_name, schemas_filter=[dataset],
                                                                      tables_filter=tables_to_crawl)

        for i in range(NUMBER_OF_THREADS):
            worker = Thread(target=profiling_obj.create_import_data_profiling_pipelines,
                            args=(i, profiling_obj.job_queue_2,))
            worker.setDaemon(True)
            worker.start()

        for row in rows:
            source_name_to_fetchcols = row['source_name_to_fetchcols']
            group_by_cols = row['group_by_cols'].split(",") if row['group_by_cols'] != "" else []
            target_table_name = row["target_table_name"]
            tgt_schema_name, tgt_table_name = target_table_name.split('.')
            src_columns_with_datatype = profiling_obj.get_all_columns(source_name_to_fetchcols, tgt_schema_name,
                                                                      tgt_table_name)

            pipeline_tgt_schema_name = row['pipeline_tgt_dataset_name']
            sql_pipeline_2 = profiling_obj.prepare_sql_for_bigquery_pipeline(dataprofiling_source_type,
                                                                             src_columns_with_datatype,
                                                                             group_by_cols,
                                                                             target_table_name,
                                                                             pipeline_tgt_schema_name,
                                                                             f"{row['pl_suffix'].upper()}_DATA_PROFILING_SOURCE",
                                                                             f"{row['pl_suffix'].upper()}_DATA_PROFILING")
            profiling_obj.job_queue_2.put(
                (row['pl_suffix'], domain_id, bq_environment_id, sql_pipeline_2, pipeline_tgt_schema_name))

        print('*** Main thread waiting ***')
        profiling_obj.job_queue_2.join()
        print('*** Done ***')


if __name__ == '__main__':
    main()
