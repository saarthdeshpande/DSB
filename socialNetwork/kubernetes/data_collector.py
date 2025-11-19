import argparse
import glob
import logging
import os
import shlex
import subprocess
import threading
import time
import yaml

from utils import interval_string_to_seconds

hpa_config_file = "hpa_config.yaml"
wrk2file_path = "../wrk2/scripts/social-network/compose-post-gpt.lua"
frontend_ip = "128.110.96.91:32000"  # <b>node's</b> internal IP

microservices = []

class LiteralDumper(yaml.SafeDumper):
    pass

def str_presenter(dumper, data):
    # Use block style for multi-line strings
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

LiteralDumper.add_representer(str, str_presenter)

# Optional: keep key order and avoid long line wrapping
LiteralDumper.ignore_aliases = lambda *args: True

yaml_width = 4096

def create_hpa_yaml(args):
    global microservices
    DEF_all = {"req":{"cpu":"500m","memory":"128Mi"},"lim":{"cpu":"1000m","memory":"256Mi"}}
    # DEF_redis = {"req":{"cpu":"1000m","memory":"128Mi"},"lim":{"cpu":"2000m","memory":"256Mi"}}
    metrics = []
    if getattr(args, "cpu", False):
        metrics.append({'type':'Resource','resource':{'name':'cpu','target':{'type':'Utilization','averageUtilization':50}}})
    if getattr(args, "memory", False):
        metrics.append({'type':'Resource','resource':{'name':'memory','target':{'type':'Utilization','averageUtilization':50}}})
    all_configs = []
    for folder in os.listdir('.'):
        if folder == 'scripts': continue
        for fn in glob.glob(f"{folder}/*.yaml"):
            with open(fn) as f:
                docs = list(yaml.safe_load_all(f))
            if fn.endswith('deployment.yaml') and 'mongodb' not in fn:
                # if '-redis' in fn:
                #    DEF = DEF_redis
                # else:
                DEF = DEF_all
                # if 'home-timeline' in fn or 'nginx' in fn:
                #     pMin, pMax = 1, 10
                # else:
                pMin, pMax = 1, 20
                for d in docs:
                    if isinstance(d, dict) and d.get('kind') == 'Deployment':
                        spec = (((d.get('spec') or {}).get('template') or {}).get('spec') or {})
                        for k in ('containers','initContainers'):
                            for c in spec.get(k, []) or []:
                                r = c.setdefault('resources', {})
                                rq, lm = r.setdefault('requests', {}), r.setdefault('limits', {})
                                rq.setdefault('cpu', DEF['req']['cpu']);   rq.setdefault('memory', DEF['req']['memory'])
                                lm.setdefault('cpu', DEF['lim']['cpu']);   lm.setdefault('memory', DEF['lim']['memory'])
                        name = d.get('metadata', {}).get('labels', {}).get('service') or d.get('metadata', {}).get('name')
                        if metrics and name:
                            microservices.append(name)
                            docs.append({
                                'apiVersion':'autoscaling/v2','kind':'HorizontalPodAutoscaler',
                                'metadata':{'name':name},
                                'spec':{'scaleTargetRef':{'apiVersion':'apps/v1','kind':'Deployment','name':name},
                                        'minReplicas':pMin,'maxReplicas':pMax,'metrics':metrics}
                            })
            all_configs.extend(docs)
    with open(hpa_config_file, 'w') as f:
        yaml.dump_all(all_configs, f, default_flow_style=False, Dumper=LiteralDumper, width=yaml_width)

# def create_hpa_yaml(args):
#     global microservices
#     metrics = []
#     if args.cpu is True:
#         metrics += [
#             {
#                 'type': 'Resource',
#                 'resource': {
#                     'name': 'cpu',
#                     'target': {
#                         'type': 'Utilization',
#                         'averageUtilization': 50
#                     }
#                 }
#             }
#         ]
#     if args.memory is True:
#         metrics += [
#             {
#                 'type': 'Resource',
#                 'resource': {
#                     'name': 'memory',
#                     'target': {
#                         'type': 'Utilization',
#                         'averageUtilization': 50
#                     }
#                 }
#             }
#         ]
#
#     # Read YAML file
#     all_configs = []
#     for folderName in os.listdir('.'):
#         if folderName != 'scripts':
#             for fileName in glob.glob(folderName + "/*.yaml"):
#                 with open(fileName, 'r') as file:
#                     current_config = list(yaml.safe_load_all(file))
#                     all_configs += current_config
#                     if len(metrics) > 0 and fileName.endswith('deployment.yaml') and 'mongodb' not in fileName:
#                         # microservice = fileName.split('/')[1].split('-deployment.yaml')[0]
#                         microservice = current_config[0]['metadata']['labels']['service']
#                         microservices.append(microservice)
#                         pMin, pMax = 1, 10
#                         additional_config = {
#                             'apiVersion': 'autoscaling/v2',
#                             'kind': 'HorizontalPodAutoscaler',
#                             'metadata': {
#                                 'name': microservice
#                             },
#                             'spec': {
#                                 'scaleTargetRef': {
#                                     'apiVersion': 'apps/v1',
#                                     'kind': 'Deployment',
#                                     'name': microservice
#                                 },
#                                 'minReplicas': pMin,
#                                 'maxReplicas': pMax,
#                                 'metrics': metrics
#                             }
#                         }
#                         all_configs += [additional_config]
#
#     with open(hpa_config_file, 'w') as file:
#         yaml.dump_all(
#             all_configs,
#             file,
#             default_flow_style=False,
#             Dumper=LiteralDumper,
#             width=yaml_width
#         )


def record_hpa_numbers(microservice, metric, duration):
    cmd = f"kubectl get hpa {microservice}"
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    start_time = time.time()
    duration_in_seconds = interval_string_to_seconds(duration)

    try:
        with open(f"{metric}/{microservice}.txt", "w") as hpa_output_file:
            while time.time() - start_time < duration_in_seconds:
                output = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if output.stdout:
                    hpa_output_file.write(output.stdout.strip().split("\n")[1])
                    hpa_output_file.write("\n")
                    hpa_output_file.flush()  # Flush after each write
                time.sleep(15)
                # if process.poll() is not None:
                #     break
        logging.info(f"Completed HPA monitoring for {microservice}")
    except Exception as e:
        logging.error(f"Error monitoring HPA for {microservice}: {str(e)}")
    finally:
        process.terminate()

def main():
    global microservices

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cpu", default=False, action='store_true')
    parser.add_argument("-m", "--memory", default=False, action='store_true')
    parser.add_argument("-t", "--time", default="10m")

    args = parser.parse_args()
    create_hpa_yaml(args)

    metric = ""
    if args.cpu is True:
        metric += "cpu_"
    if args.memory is True:
        metric += "memory_"

    if not metric:
        logging.warning(f"No metrics specified in args.")
        exit(-1)

    hpaApplyCmd = f"kubectl apply -f {hpa_config_file}"
    hpaApplyProcess = subprocess.Popen(hpaApplyCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True)

    print("Applied app config. Sleeping for 60s while resources provisioned.")
    time.sleep(60)
    # portFwdCmd = "kubectl port-forward svc/nginx-thrift 32000:8080"
    # print("Enabling port forwarding.")
    # portFwdProcess = subprocess.Popen(portFwdCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    #                                   close_fds=True)
    # time.sleep(5)
    # print("Enabled port forwarding.")
    print("Collecting HPA data.")

    threads = []
    for hpa in microservices:
        thread = threading.Thread(target=record_hpa_numbers, args=(hpa, metric, args.time))
        # thread.start()
        threads.append(thread)
    # print("Starting HPA threads.")
    # for thread in threads:
    #     thread.start()
    #
    # for i, thread in enumerate(threads):
    #     logging.info(f"Waiting for thread {i + 1}/{len(threads)} to complete")
    #     thread.join(timeout=interval_string_to_seconds(args.time) + 90)
    #     logging.info(f"Thread {i + 1}/{len(threads)} completed")
    wrk2Process = None

    try:
        # wrk2Cmd = f"/usr/local/bin/wrk -t4 -c100 -d{args.time} -R500 -s {wrk2file_path} http://{frontend_ip} -- {metric}"
        print("Applying wrk. Sleeping for 15s.")
        # TODO: configure number of threads and T* number of rps entries
        command = shlex.split(f'wrk -t1 -c200 -d{args.time} -R500 -s {wrk2file_path} http://{frontend_ip}')
        wrk2Process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(15)
        print("Wrk process completed.")

        for thread in threads:
            thread.start()
        stdout, stderr = wrk2Process.communicate(timeout=interval_string_to_seconds(args.time) + 90)
        print("STDOUT: ", stdout)
        print("STDERR: ", stderr)
    except subprocess.TimeoutExpired:
        logging.error("Wrk process timed out")
    finally:
        # Wait for all threads to complete
        for i, thread in enumerate(threads):
            logging.info(f"Waiting for thread {i + 1}/{len(threads)} to complete")
            thread.join()
            logging.info(f"Thread {i + 1}/{len(threads)} completed")

        # logging.info("Stopping port forwarding.")
        # portFwdProcess.kill()
        # logging.info("Stopped port forwarding.")

        logging.info("Deleting app config.")
        hpaDeleteCmd = f"kubectl delete -f {hpa_config_file}"
        hpaDeleteProcess = subprocess.Popen(hpaDeleteCmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = hpaDeleteProcess.communicate()

        if hpaDeleteProcess.returncode == 0:
            logging.info("HPA config deleted successfully")
        else:
            logging.error(f"Error deleting HPA config: {stderr}")

if __name__ == "__main__":
    main()
