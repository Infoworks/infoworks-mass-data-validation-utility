import json
import traceback
from configparser import ConfigParser

import requests

config = ConfigParser()
config.read('config.ini')
protocol = config.get('infoworks_details', 'protocol')
proxy_host = config.get('infoworks_details', 'host')
proxy_port = config.get('infoworks_details', 'port')


def get_bearer_token(host, port, protocol, refresh_token):
    url = "{protocol}://{ip}:{port}/v3/security/token/access/".format(ip=host, port=port, protocol=protocol)
    headers = {
        'Authorization': 'Basic ' + refresh_token,
        'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers)
    bearer_token = response.json().get("result").get("authentication_token")
    headers_bearer = {
        'Authorization': 'Bearer ' + bearer_token,
        'Content-Type': 'application/json'
    }
    return headers_bearer


def find_pipelines_under_domain(domain_id):
    filter_condition_dict = {"name": {"$regex": "^automation_pipeline"}}
    filter_condition = json.dumps(filter_condition_dict)
    pipelines_url = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines'.format(protocol=protocol, ip=proxy_host,
                                                                                       port=proxy_port,
                                                                                       domain_id=domain_id) + f"?filter={{filter_condition}}".format(
        filter_condition=filter_condition)
    try:
        response = requests.request('GET', pipelines_url, headers=headers, verify=False)
        if response.status_code != 200:
            print(f"Failed to find pipelines for {domain_id}")
            print(response.text)
        else:
            result = response.json().get('result', {})
            if len(result) != 0:
                print(f"Found pipelines for under Domain {domain_id}")
                return result
            else:
                print(f"Failed to find pipelines under Domain {domain_id}")
    except Exception as e:
        traceback.print_exc()
        print(str(e))


def delete_pipeline(domain_id, pipeline_id):
    print(f"Deleting {pipeline_id}")
    url_to_delete_pipelines_url = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/pipelines/{pipeline_id}'.format(
        protocol=protocol, ip=proxy_host,
        port=proxy_port,
        domain_id=domain_id, pipeline_id=pipeline_id)
    response = requests.request('DELETE', url_to_delete_pipelines_url, headers=headers, verify=False)
    if response.status_code != 200:
        print(f"Failed to delete pipeline for {pipeline_id}")
        print(response.text)


def delete_workflow(domain_id, workflow_id):
    print(f"Deleting {workflow_id}")
    url_to_delete_pipelines_url = '{protocol}://{ip}:{port}/v3/domains/{domain_id}/workflows/{workflow_id}'.format(
        protocol=protocol, ip=proxy_host,
        port=proxy_port,
        domain_id=domain_id, workflow_id=workflow_id)
    response = requests.request('DELETE', url_to_delete_pipelines_url, headers=headers, verify=False)
    if response.status_code != 200:
        print(f"Failed to delete workflow for {workflow_id}")
        print(response.text)


if __name__ == '__main__':
    domain_id = config.get('environment_details', 'domain_id')
    refresh_token = config.get('environment_details', 'refresh_token')
    headers = get_bearer_token(proxy_host, proxy_port, protocol, refresh_token)
    result_obj = find_pipelines_under_domain(domain_id)
    if result_obj is not None:
        result = list(result_obj)
        for item in result:
            pipeline_id = str(item["id"])
            delete_pipeline(domain_id, pipeline_id)
    else:
        print(f"No Pipelines to Delete under Domain {domain_id}")
