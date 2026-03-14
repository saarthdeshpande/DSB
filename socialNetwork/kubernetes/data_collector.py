import argparse
import glob
import logging
import os
import shlex
import subprocess
import threading
import time
import yaml
import re
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

from utils import interval_string_to_seconds

hpa_config_file = "hpa_config.yaml"
# wrk2file_path = "../wrk2/scripts/social-network/compose-post-gpt.lua"
locustfile_path = "./locustfile.py"
locust_venv = "./venv/bin"
frontend_ip = "128.110.96.70:32000"  # <b>node's</b> internal IP

microservices = []

class LiteralDumper(yaml.SafeDumper):
    pass

def str_presenter(dumper, data):
    # Use block style for multi-line strings
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

LiteralDumper.add_representer(str, str_presenter)
LiteralDumper.ignore_aliases = lambda *args: True

yaml_width = 4096


def create_hpa_yaml(args):
    global microservices

    # Default resources for all (stateless-ish) services
    # This is a safe baseline that matches typical SocialNetwork setups.
    DEF_all = {
        "req": {"cpu": "500m", "memory": "64Mi"},
        "lim": {"cpu": "1000m", "memory": "128Mi"},
    }

    # Service-specific HPA tuning (p99-oriented)
    # Lower targets for latency-critical / cache tiers, slightly higher for auxiliary services.
    service_cpu_target = {
        # Frontend / entrypoint
        "nginx-thrift": 50,

        # Core critical path (compose & timelines + user/social graph/post storage)
        "compose-post-service": 55,
        "home-timeline-service": 55,
        "user-timeline-service": 55,
        "user-service": 55,
        "social-graph-service": 55,
        "post-storage-service": 55,

        # Redis / Memcached caches – very latency-sensitive, keep them cool
        "home-timeline-redis": 45,
        "user-timeline-redis": 45,
        "social-graph-redis": 45,
        "user-memcached": 45,
        "post-storage-memcached": 45,
        "url-shorten-memcached": 45,
        # If media-memcached exists in this tree, we want the same policy
        "media-memcached": 45,

        # Conditional services on compose/read paths
        "unique-id-service": 60,
        "text-service": 60,
        "user-mention-service": 60,
        "url-shorten-service": 60,
        "media-service": 60,
        "media-frontend": 60,
    }

    # Per-service min/max replicas.
    # Max values assume a ~3-node cluster; adjust upward if you have more capacity.
    service_min_max = {
        # Entry point – want fan-out headroom
        "nginx-thrift": (3, 24),

        # Core critical path business logic
        "compose-post-service": (2, 16),
        "home-timeline-service": (2, 16),
        "user-timeline-service": (2, 16),
        "user-service": (2, 12),
        "social-graph-service": (2, 16),
        "post-storage-service": (2, 16),

        # Cache tiers (Redis + Memcached).
        # Min=1 because vanilla Redis/Memcached in DSB aren’t clustered; >1 is allowed here
        # so you can experiment, but if you see cache thrash you can pin maxReplicas back to 1.
        "home-timeline-redis": (1, 8),
        "user-timeline-redis": (1, 8),
        "social-graph-redis": (1, 8),
        "user-memcached": (1, 8),
        "post-storage-memcached": (1, 8),
        "url-shorten-memcached": (1, 8),
        "media-memcached": (1, 8),

        # “Leaf” / auxiliary services
        "unique-id-service": (2, 8),
        "text-service": (2, 10),
        "user-mention-service": (2, 8),
        "url-shorten-service": (2, 8),
        "media-service": (2, 10),
        "media-frontend": (2, 8),
    }

    # Stateful / infra components we DON'T want HPAs on.
    # Note: *deliberately* do NOT include redis/memcached here so they get HPAs.
    HPA_SKIP_KEYWORDS = (
        "mongodb",     # all the user/url/post/social-graph/user-timeline mongodb backends
        "mongos",      # sharded routers, if present
        "configsvr",   # mongo config servers
        "jaeger",      # tracing infra
        "prometheus",  # monitoring stack
    )

    # Shared HPA behavior: aggressive scale-up, conservative scale-down.
    # This helps keep p99 low under bursts while avoiding thrashy downsizing.
    behavior_spec = {
        "scaleUp": {
            "stabilizationWindowSeconds": 0,
            "policies": [
                # At most double the replicas every 30s
                {"type": "Percent", "value": 100, "periodSeconds": 30},
            ],
            "selectPolicy": "Max",
        },
        "scaleDown": {
            # Keep replicas around for a while after load drops – protects the tail.
            "stabilizationWindowSeconds": 300,
            "policies": [
                # At most shrink by 20% per minute
                {"type": "Percent", "value": 20, "periodSeconds": 60},
            ],
            "selectPolicy": "Min",
        },
    }

    all_configs = []

    for folder in os.listdir("."):
        if folder == "scripts":
            continue

        for fn in glob.glob(f"{folder}/*.yaml"):
            with open(fn) as f:
                docs = list(yaml.safe_load_all(f))

            # We still skip mongodb deployments entirely here (no HPA + no mass-editing resources)
            if fn.endswith("deployment.yaml") and "mongodb" not in fn:
                for d in docs:
                    if not (isinstance(d, dict) and d.get("kind") == "Deployment"):
                        continue

                    spec = (
                        ((d.get("spec") or {}).get("template") or {}).get("spec")
                        or {}
                    )

                    name = (
                        d.get("metadata", {}).get("labels", {}).get("service")
                        or d.get("metadata", {}).get("name")
                    )
                    if not name:
                        continue

                    # Decide default resources: caches can use the same baseline for now.
                    DEF = DEF_all

                    # Ensure every container has resource requests/limits
                    for k in ("containers", "initContainers"):
                        for c in spec.get(k, []) or []:
                            r = c.setdefault("resources", {})
                            rq, lm = r.setdefault("requests", {}), r.setdefault("limits", {})
                            rq["cpu"] = DEF["req"]["cpu"]
                            rq["memory"] = DEF["req"]["memory"]
                            lm["cpu"] = DEF["lim"]["cpu"]
                            lm["memory"] = DEF["lim"]["memory"]

                    # Do not attach HPA to stateful/infra components
                    if any(kw in name for kw in HPA_SKIP_KEYWORDS):
                        continue

                    # Build metrics per-service so we can vary CPU target
                    metrics = []

                    if getattr(args, "cpu", False):
                        cpu_target = service_cpu_target.get(name, 60)
                        metrics.append(
                            {
                                "type": "Resource",
                                "resource": {
                                    "name": "cpu",
                                    "target": {
                                        "type": "Utilization",
                                        "averageUtilization": cpu_target,
                                    },
                                },
                            }
                        )

                    # if getattr(args, "memory", False):
                    #     # Memory-based scaling is noisy; keep target relaxed.
                    #     metrics.append(
                    #         {
                    #             "type": "Resource",
                    #             "resource": {
                    #                 "name": "memory",
                    #                 "target": {
                    #                     "type": "Utilization",
                    #                     "averageUtilization": 50,
                    #                 },
                    #             },
                    #         }
                    #     )

                    if metrics:
                        microservices.append(name)
                        pMin, pMax = service_min_max.get(name, (1, 20))

                        docs.append(
                            {
                                "apiVersion": "autoscaling/v2",
                                "kind": "HorizontalPodAutoscaler",
                                "metadata": {"name": name},
                                "spec": {
                                    "scaleTargetRef": {
                                        "apiVersion": "apps/v1",
                                        "kind": "Deployment",
                                        "name": name,
                                    },
                                    "minReplicas": pMin,
                                    "maxReplicas": pMax,
                                    "metrics": metrics,
                                    "behavior": behavior_spec,
                                },
                            }
                        )

            all_configs.extend(docs)

    with open(hpa_config_file, "w") as f:
        yaml.dump_all(
            all_configs,
            f,
            default_flow_style=False,
            Dumper=LiteralDumper,
            width=yaml_width,
        )

    return DEF_all

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

def parse_quantity(quantity):
    """
    Parse kubernetes resource quantity to raw value.
    m -> milli (values / 1000)
    Ki -> 1024
    Mi -> 1024^2
    Gi -> 1024^3
    """
    if not quantity:
        return 0
    
    # Handle millicores
    if quantity.endswith('m'):
        return int(quantity[:-1])
    
    # Handle limits (bytes)
    multipliers = {
        'Ki': 1024,
        'Mi': 1024 ** 2,
        'Gi': 1024 ** 3,
        'Ti': 1024 ** 4
    }
    
    for suffix, multiplier in multipliers.items():
        if quantity.endswith(suffix):
            return int(quantity[:-len(suffix)]) * multiplier
            
    # Plain integer
    return int(quantity)

def get_k8s_metrics(microservice, DEF_all):
    # 1. Get HPA metadata from text output (single call for age, ref, replicas, thresholds)
    hpa_cmd = f"kubectl get hpa {microservice} --no-headers"
    hpa_res = subprocess.run(hpa_cmd, shell=True, capture_output=True, text=True)

    ref, min_pods, max_pods, replicas, age = f"Deployment/{microservice}", -1, -1, 0, "<none>"
    cpu_target, mem_target = -1, -1

    if hpa_res.returncode == 0 and hpa_res.stdout.strip():
        parts = hpa_res.stdout.strip().split()
        if len(parts) >= 8:
            ref, min_pods, max_pods, replicas, age = parts[1], parts[-4], parts[-3], parts[-2], parts[-1]
            targets_str = " ".join(parts[2:-4])
            thresholds = re.findall(r'/(\d+)%', targets_str)
            cpu_target = thresholds[0] if len(thresholds) >= 1 else -1
            mem_target = thresholds[1] if len(thresholds) >= 2 else -1

    # 2. Get actual usage via kubectl top
    top_cmd = f"kubectl top pods -l service={microservice} --no-headers"
    top_res = subprocess.run(top_cmd, shell=True, capture_output=True, text=True)

    cpu_sum, mem_sum, pod_count = 0, 0, 0
    if top_res.returncode == 0 and top_res.stdout.strip():
        for pod_line in top_res.stdout.strip().split("\n"):
            p_parts = pod_line.split()
            if len(p_parts) >= 3:
                cpu_sum += parse_quantity(p_parts[1])
                mem_sum += parse_quantity(p_parts[2])
                pod_count += 1

    cpu_avg = (cpu_sum / pod_count) if pod_count > 0 else 0
    mem_avg = (mem_sum / pod_count) if pod_count > 0 else 0
    cpu_util = int((cpu_avg / parse_quantity(DEF_all["req"]["cpu"])) * 100)
    mem_util = int((mem_avg / parse_quantity(DEF_all["req"]["memory"])) * 100)

    return f"{microservice} {ref} cpu: {cpu_util}%/{cpu_target}% memory: {mem_util}%/{mem_target}% {min_pods} {max_pods} {replicas} {age}"

def record_hpa_numbers(microservice, metric, duration, DEF_all):
    start_time = time.time()
    duration_in_seconds = interval_string_to_seconds(duration)

    try:
        output_filename = f"{metric}/{microservice}.txt"
        with open(output_filename, "w") as hpa_output_file:
            hpa_output_file.write("NAME REFERENCE TARGETS MINPODS MAXPODS REPLICAS AGE\n")
            while time.time() - start_time < duration_in_seconds:
                line = get_k8s_metrics(microservice, DEF_all)
                if line:
                    hpa_output_file.write(line)
                    hpa_output_file.write("\n")
                    hpa_output_file.flush()  # Flush after each write
                time.sleep(15)

        if os.path.exists(output_filename) and os.stat(output_filename).st_size == 0:
            os.remove(output_filename)
            logging.info(f"Removed empty file for {microservice}")

        logging.info(f"Completed HPA monitoring for {microservice}")
    except Exception as e:
        logging.error(f"Error monitoring HPA for {microservice}: {str(e)}")

def main():
    global microservices

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--cpu", default=False, action='store_true')
    parser.add_argument("-m", "--memory", default=False, action='store_true')
    parser.add_argument("-r", "--realtime", default=False, action='store_true')
    parser.add_argument("-t", "--time", default="10m")

    args = parser.parse_args()
    DEF_all = create_hpa_yaml(args)

    # Always force metric to cpu_memory_ for uniform collection
    metric = "cpu_memory_"

    if not getattr(args, "cpu", False) and not getattr(args, "memory", False):
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

    # Ensure directory exists
    if not os.path.exists(metric):
        os.makedirs(metric)

    threads = []
    for hpa in microservices:
        thread = threading.Thread(target=record_hpa_numbers, args=(hpa, metric, args.time, DEF_all))
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
    # wrk2Process = None
    locustProcess = None
    flags = f"--headless -u 1000 -r 100 -t {args.time} -d {metric}"
    if args.realtime is True:
        flags += " --realtime"
    try:
        # wrk2Cmd = f"/usr/local/bin/wrk -t4 -c100 -d{args.time} -R500 -s {wrk2file_path} http://{frontend_ip} -- {metric}"
        # print("Applying wrk. Sleeping for 15s.")
        # # TODO: configure number of threads and T* number of rps entries
        # command = shlex.split(f'wrk -t1 -c200 -d{args.time} -R500 -s {wrk2file_path} http://{frontend_ip}')
        # wrk2Process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        locustCmd = f"-f {locustfile_path} --host=http://{frontend_ip} {flags}"
        logging.info("Applying locust.")
        command = [locust_venv + "/python", locust_venv + "/locust"] + shlex.split(locustCmd)
        locustProcess = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info("Locust process completed. Sleeping until load builds up.")
        for thread in threads:
            thread.start()
        stdout, stderr = locustProcess.communicate(timeout=interval_string_to_seconds(args.time) + 90)
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
