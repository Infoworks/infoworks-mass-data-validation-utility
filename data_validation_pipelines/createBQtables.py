import argparse
import csv
import sys
import traceback

from google.cloud import bigquery
from google.oauth2 import service_account

source_formats = {"parquet": bigquery.SourceFormat.PARQUET, "orc": bigquery.SourceFormat.ORC,
                  "json": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, "csv": bigquery.SourceFormat.CSV}


def create_load_bq_table(client, table_id, data_file_path, src_file_format):
    job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=source_formats.get(src_file_format.lower())
    )
    load_job = client.load_table_from_uri(
        data_file_path, table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def create_bq_external_table(client, table_id, data_file_path, src_file_format):
    query = f"""CREATE OR REPLACE EXTERNAL TABLE {table_id}
                OPTIONS (
                format = f"{src_file_format.title()}",
                uris = ['{data_file_path}']);"""
    query_job = client.query(query)
    print(query_job)


def main():
    parser = argparse.ArgumentParser(description='Bulk BQ Table Creation')
    parser.add_argument('--service_account_json_path', required=True, help='Pass the full path of the service account '
                                                                           'file')
    parser.add_argument('--parameter_file_path', required=True, help='Pass the full path of the parameter file '
                                                                     'containing the details to create table')
    parser.add_argument('--load_data_to_bq', required=False, default="no",
                        help='Pass yes if you need to load data to BQ. '
                             'Pass no if just want an external '
                             'table to created ')
    args = parser.parse_args()
    service_account_json_path = args.service_account_json_path
    parameter_file_path = args.parameter_file_path
    load_data_to_bq = args.load_data_to_bq
    try:
        input_file = csv.DictReader(open(parameter_file_path))
        credentials = service_account.Credentials.from_service_account_file(
            service_account_json_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
    except Exception as e:
        print("Failed to read the parameter file or Failed to create the big query client")
        print(str(e))
        print(traceback.print_exc())
        sys.exit()
    for row in input_file:
        try:
            table_id = row['dataset_name'] + "." + row["table_name"]
            datafile_path = row["datafile_path"]
            file_format = row["file_format"]
            if load_data_to_bq.lower() == "yes":
                create_load_bq_table(client, table_id, datafile_path, file_format)
            else:
                create_bq_external_table(client, table_id, datafile_path, file_format)
        except Exception as e:
            print(f"Failed to create/load table {row}")
            print((str(e)))
            print(traceback.print_exc())


if __name__ == '__main__':
    main()
