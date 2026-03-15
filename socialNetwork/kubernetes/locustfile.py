import os
import random
import time
import json
import logging
import math
import glob
import threading
import socket
import csv

from locust import FastHttpUser, task, between, events
import urllib3
import gevent

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)

# ========================================================================
#                           GLOBAL CONFIG
# ========================================================================

mean_iat = 1  # seconds (only relevant in fallback mode)

locust_environment = None
real_time = False
service_files_dir = None

# Alibaba Poisson replay
rps_schedules = {}         # endpoint_name -> [rps values]
replay_interval = 15.0
ENDPOINT_SCALE_FACTORS = {
    "compose_post": 0.03,          # baseline: avg 15.6 * 0.03 = 0.47/user
    "read_home_timeline": 0.05,   # avg 2.9  * 0.161 = 0.47/user
    "read_user_timeline": 0.1,   # avg 1.4  * 0.334 = 0.47/user
}
USE_ALIBABA_REPLAY = False

# --- Random helpers (mirroring your Lua script) ---
charset = ['q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's',
           'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm',
           'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F',
           'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M',
           '1', '2', '3', '4', '5', '6', '7', '8', '9', '0']

decset = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']

max_user_index = int(os.getenv("max_user_index", "962"))  # same as Lua env var


def random_string(length: int) -> str:
    return "".join(random.choice(charset) for _ in range(length))


def random_decimal(length: int) -> str:
    return "".join(random.choice(decset) for _ in range(length))


# ========================================================================
#                   CLI OPTIONS: DIR + REALTIME SOCKET
# ========================================================================

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "-d", "--dir",
        type=str,
        default=".",
        help="Directory to write rps.txt / rt_rps.txt",
    )
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="Enable real-time RPS/p95/p99 socket export",
    )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global locust_environment, service_files_dir, real_time
    locust_environment = environment
    service_files_dir = environment.parsed_options.dir
    real_time = environment.parsed_options.realtime

    logging.info(f"RPS logs → {service_files_dir}/rps.txt")
    logging.info(f"Realtime mode → {real_time}")

    t = threading.Thread(target=periodic_rps_writer, daemon=True)
    t.start()


# ========================================================================
#                         RPS LOGGING + SOCKET
# ========================================================================

def send_dict_via_socket(dictionary, host="localhost", port=8001):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((host, port))
        client.send(json.dumps(dictionary).encode("utf-8"))
    except Exception as e:
        logging.info(f"Socket error: {e}")
    finally:
        client.close()


def record_rps():
    """
    Writes RPS + p95 + p99 per endpoint to:
      - <dir>/rps.txt
      - <dir>/rt_rps.txt (if realtime enabled)
    """
    if not locust_environment:
        return

    stats = locust_environment.runner.stats
    os.makedirs(service_files_dir, exist_ok=True)

    # --- realtime mode: send + log rt_rps.txt ---
    if real_time:
        realtime_data = {}
        for endpoint, data in stats.entries.items():
            realtime_data[f"{endpoint}_rps"] = data.current_rps
            realtime_data[f"{endpoint}_p95"] = data.get_response_time_percentile(0.95)
            realtime_data[f"{endpoint}_p99"] = data.get_response_time_percentile(0.99)
        if realtime_data:
            send_dict_via_socket(realtime_data)

        rt_path = os.path.join(service_files_dir, "rt_rps.txt")
        with open(rt_path, "a") as f:
            for k, v in realtime_data.items():
                f.write(f"{k}: {v};")
            f.write("\n")

    # --- normal rps logging ---
    rps_path = os.path.join(service_files_dir, "rps.txt")
    with open(rps_path, "a") as f:
        for endpoint, data in stats.entries.items():
            rps = data.current_rps
            p95 = data.get_response_time_percentile(0.95)
            p99 = data.get_response_time_percentile(0.99)
            f.write(f"{endpoint}_rps: {rps};")
            f.write(f"{endpoint}_p95: {p95};")
            f.write(f"{endpoint}_p99: {p99};")
        f.write("\n")


def periodic_rps_writer():
    while True:
        try:
            record_rps()
            time.sleep(15)
        except Exception as e:
            logging.info(f"RPS writer error: {e}")


# ========================================================================
#                ALIBABA RPS TRACE → POISSON REPLAY
# ========================================================================

def load_rps_files(dir_path="alibaba_workload"):
    """
    Load CSVs named alibaba_*.csv and map:
      0 -> compose_post
      1 -> read_home_timeline
      2 -> read_user_timeline

    Each CSV needs a 'rps' column.
    """
    global rps_schedules

    files = sorted(glob.glob(os.path.join(dir_path, "alibaba_*.csv")))
    if len(files) < 3:
        raise FileNotFoundError("Alibaba trace CSVs not found (need at least 3).")

    endpoints = [
        "compose_post",
        "read_home_timeline",
        "read_user_timeline",
    ]

    for idx, endpoint in enumerate(endpoints):
        path = files[idx]
        series = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            #for _ in range(1440):
             #   next(reader, None)
            for row in reader:
                scale = ENDPOINT_SCALE_FACTORS[endpoint]
                series.append(float(row["rps"]) * scale)
        rps_schedules[endpoint] = series

    logging.info(f"Alibaba traces loaded for endpoints: {list(rps_schedules.keys())}")


def _call_endpoint(user, endpoint_name):
    if endpoint_name == "compose_post":
        user._compose_post_impl()
    elif endpoint_name == "read_home_timeline":
        user._read_home_timeline_impl()
    elif endpoint_name == "read_user_timeline":
        user._read_user_timeline_impl()
    else:
        logging.info(f"Ignoring unknown endpoint {endpoint_name}")


def _drive_endpoint(user, endpoint_name, series, t0):
    """
    For each RPS bucket, generate arrivals as a Poisson process.
    """
    for i, rps in enumerate(series):
        if rps <= 0:
            continue

        bucket_end = (i + 1) * replay_interval
        while time.time() - t0 < bucket_end:
            u = random.random() or 1e-9
            inter_arrival_time = -math.log(u) / rps
            gevent.sleep(inter_arrival_time)

            if time.time() - t0 >= bucket_end:
                break
            _call_endpoint(user, endpoint_name)


def replay_poisson(user):
    if not rps_schedules:
        return
    t0 = time.time()
    greens = []
    for endpoint, series in rps_schedules.items():
        if not series or all(r <= 0 for r in series):
            continue
        g = gevent.spawn(_drive_endpoint, user, endpoint, series, t0)
        greens.append(g)
    gevent.joinall(greens)


# ========================================================================
#                         SOCIAL NETWORK USER
# ========================================================================

class SocialMediaUser(FastHttpUser):
    """
    Locust user:
      - on_start: try to load Alibaba traces and start Poisson replay
      - fallback: weighted @tasks if traces are missing
      - compose_post/read_* implemented like your wrk Lua script
    """

    # Poisson replay or tasks will control effective rate; no extra waits needed
    wait_time = between(0, 0)

    def on_start(self):
        global USE_ALIBABA_REPLAY
        try:
            load_rps_files("alibaba_workload")
            USE_ALIBABA_REPLAY = True
            logging.info("Using Alibaba Poisson replay.")
            gevent.spawn(replay_poisson, self)
        except FileNotFoundError:
            logging.warning("No Alibaba traces found. Using task-based workload.")
            USE_ALIBABA_REPLAY = False

    # -------------------- IMPLEMENTATION METHODS -------------------- #

    def _compose_post_impl(self):
        """
        Mirror wrk Lua's compose_post():
          - random user_index
          - random text + mentions + URLs
          - random media_ids / media_types (png)
        """
        user_index = random.randint(0, max_user_index - 1)
        username = f"username_{user_index}"
        user_id = str(user_index)

        text = random_string(256)

        # user mentions
        num_user_mentions = random.randint(0, 5)
        for _ in range(num_user_mentions):
            while True:
                mention_id = random.randint(0, max_user_index - 1)
                if mention_id != user_index:
                    break
            text += f" @username_{mention_id}"

        # URLs
        num_urls = random.randint(0, 5)
        for _ in range(num_urls):
            text += " http://" + random_string(64)

        # media ids / types
        num_media = random.randint(0, 4)
        media_ids = []
        media_types = []

        if num_media > 0:
            for _ in range(num_media):
                media_id = random_decimal(18)
                media_ids.append(media_id)
                media_types.append("png")

        body = {
            "username": username,
            "user_id": user_id,
            "text": text,
            "post_type": "0",
        }

        if num_media > 0:
            body["media_ids"] = json.dumps(media_ids)
            body["media_types"] = json.dumps(media_types)
        else:
            body["media_ids"] = ""

        r = self.client.post(
            "/wrk2-api/post/compose",
            data=body,
            name="compose_post",
            context={
                "type": "compose_post",
                "user_id": user_id,
                "num_media": num_media,
            },
        )

        if r.status_code > 202:
            logging.warning(
                "compose_post resp.status = %d, text=%s",
                r.status_code,
                r.text,
            )

    def _read_home_timeline_impl(self):
        user_id = str(random.randint(0, max_user_index - 1))
        start = random.randint(0, 100)
        stop = start + 10

        r = self.client.get(
            "/wrk2-api/home-timeline/read",
            params={"user_id": user_id, "start": start, "stop": stop},
            name="read_home_timeline",
            context={
                "type": "read_home_timeline",
                "user_id": user_id,
                "start": start,
                "stop": stop,
            },
        )

        if r.status_code > 202:
            logging.warning(
                "read_home_timeline resp.status = %d, text=%s",
                r.status_code,
                r.text,
            )

    def _read_user_timeline_impl(self):
        user_id = str(random.randint(0, max_user_index - 1))
        start = random.randint(0, 100)
        stop = start + 10

        r = self.client.get(
            "/wrk2-api/user-timeline/read",
            params={"user_id": user_id, "start": start, "stop": stop},
            name="read_user_timeline",
            context={
                "type": "read_user_timeline",
                "user_id": user_id,
                "start": start,
                "stop": stop,
            },
        )

        if r.status_code > 202:
            logging.warning(
                "read_user_timeline resp.status = %d, text=%s",
                r.status_code,
                r.text,
            )

    # ------------------------- FALLBACK TASKS ------------------------ #

    @task(10)  # approx 0.10 ratio
    def compose_post(self):
        if USE_ALIBABA_REPLAY:
            return
        self._compose_post_impl()

    @task(60)  # approx 0.60 ratio
    def read_home_timeline(self):
        if USE_ALIBABA_REPLAY:
            return
        self._read_home_timeline_impl()

    @task(30)  # approx 0.30 ratio
    def read_user_timeline(self):
        if USE_ALIBABA_REPLAY:
            return
        self._read_user_timeline_impl()
