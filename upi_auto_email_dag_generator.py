#!/usr/bin/env python3

import argparse
import json
import os
import boto3
# import tempfile
import subprocess
import sys
from datetime import timedelta, datetime


# ============================================================
# PATH & API CONFIG
# ============================================================

EFS_DAG_PATH = "/app/efs/code/dag_codes/"
S3_BUCKET = "idfc-dl-mwaa"
S3_PREFIX = "audit-airflow/airflow-data/dags/idp-first-lens/"


# ============================================================
# KAFKA CONFIG
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "b-1.your-msk-cluster.kafka.ap-south-1.amazonaws.com:9094,"
    "b-2.your-msk-cluster.kafka.ap-south-1.amazonaws.com:9094"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "upi-transactions")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")


# ============================================================
# METADATA
# ============================================================

REPORT_NAME = "UPI Auto Email - Schedule"
DAG_TAGS = ["upi_auto_email", "scheduled_dag", "first_lens"]


# ============================================================
# TIME CONVERSION
# ============================================================

def convert_ist_hhmm_to_utc_cron(hhmm: str) -> str:
    if ":" not in hhmm:
        raise ValueError(f"Invalid time format, expected HH:MM, got: {hhmm}")

    hh, mm = hhmm.split(":", 1)

    if not hh.isdigit() or not mm.isdigit():
        raise ValueError(f"Invalid time value: {hhmm}")

    ist_time = datetime(2000, 1, 1, int(hh), int(mm))
    utc_time = ist_time - timedelta(hours=5, minutes=30)

    return f"{utc_time.minute} {utc_time.hour} * * *"


# ============================================================
# ARGUMENT PARSING
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="UPI AUTO EMAIL DAG Generator",
        allow_abbrev=False
    )

    parser.add_argument("-v", "--PAYEE_MERCHANTIDENTIFIER", required=True)
    parser.add_argument("-f", "--values_needed", nargs="?", default=None)
    parser.add_argument("-tm", "--cron_time", required=False)
    parser.add_argument("-sr", "--request_number", required=False)
    parser.add_argument("-m", "--emails", required=True)
    parser.add_argument("-c", "--user_consent", required=False)
    parser.add_argument("-o", "--run_type", required=True)
    parser.add_argument("-r", "--run_id", required=False)

    #execution switch
    parser.add_argument(
        "-x", "--execute",
        action="store_true",
        help="Execute Upi_Auto_Email_t_1 instead of generating DAG"
    )

    args, unknown = parser.parse_known_args()

    # Auto-heal badly spaced email input
    if unknown:
        email_extras = [u for u in unknown if "@" in u]
        if email_extras:
            args.emails = ",".join([args.emails] + email_extras)

    if args.run_type not in {"CSV", "EXCEL"}:
        raise ValueError(
            f"Invalid run_type '{args.run_type}'. Expected CSV, EXCEL, or PDF"
        )

    if args.cron_time:
        args.cron = convert_ist_hhmm_to_utc_cron(args.cron_time)

    return args


# ============================================================
# EXECUTION MODE
# ============================================================

def trigger_upi_smt_t1(args):
    cmd = [
        sys.executable,
        "upi_auto_email.py",
        "-v", args.PAYEE_MERCHANTIDENTIFIER,
        "-r", args.run_id,
        "-o", args.run_type,
        "-m", args.emails
    ]

    if args.values_needed:
        cmd.extend(["-f", args.values_needed])

    subprocess.run(cmd, check=True)


# ============================================================
# PAYLOAD
# ============================================================

def build_payload(args):
    emails = [m.strip() for m in args.emails.split(",") if m.strip()]
    values = [v.strip() for v in args.values_needed.split(",")] if args.values_needed else []

    return {
        "report_name": REPORT_NAME,
        "filters": {
            "PAYEE_MERCHANTIDENTIFIER": {
                "arg": "v",
                "type": "PAYEE_MERCHANTIDENTIFIER",
                "value": args.PAYEE_MERCHANTIDENTIFIER
            },
            "values_needed": {
                "arg": "f",
                "type": "choice",
                "value": values
            },
            "selected_run_action": {
                "arg": "o",
                "type": "Download",
                "option": args.run_type
            },
            "request_number": {
                "arg": "rn",
                "type": "text",
                "value": args.request_number
            },
            "user_consent": {
                "arg": "c",
                "type": "choice",
                "value": args.user_consent
            },
            "report_status": {
                "arg": "rs",
                "type": "text",
                "value": "SUBMITTED"
            },
            "execute": {
                "arg": "x",
                "type": "text",
                "value": "airflow_scheduler"
            },
            "email_list": {
                "arg": "m",
                "type": "text",
                "value": emails
            }
        }
    }


# ============================================================
# DAG CODE GENERATION
# ============================================================

def generate_dag_code(dag_id, schedule, payload, kafka_config):
    payload_json = json.dumps(payload, indent=4)

    return f'''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import yaml
import os
import json

# -----------------------------
# Environment configuration
# -----------------------------
ENV = "PROD"

# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "{kafka_config['bootstrap_servers']}"
KAFKA_TOPIC = "{kafka_config['topic']}"
KAFKA_SASL_USERNAME = "{kafka_config['username']}"
KAFKA_SASL_PASSWORD = "{kafka_config['password']}"

# -----------------------------
# YAML Loader
# -----------------------------
def yaml_loader(yaml_file):
    print(f"Attempting to load YAML file: {{yaml_file}}")

    if not os.path.exists(yaml_file):
        print(f"ERROR: YAML file does not exist: {{yaml_file}}")
        return None

    try:
        with open(yaml_file, "r") as f:
            data = yaml.safe_load(f)
        print("YAML file loaded successfully")
        return data
    except yaml.YAMLError as e:
        print(f"YAML parsing error: {{e}}")
    except Exception as e:
        print(f"Unexpected error loading YAML: {{e}}")

    return None


# -----------------------------
# YAML Path (SAFE for Airflow)
# -----------------------------
BASE_DIR = os.path.dirname(__file__)
paths_file = os.path.join(BASE_DIR, "fl_config.yaml")

file_paths = yaml_loader(paths_file)
if not file_paths:
    raise Exception(f"Failed to load YAML config: {{paths_file}}")

env_paths = file_paths.get(ENV)
if not env_paths:
    raise Exception(f"Environment '{{ENV}}' not found in YAML file")

print(f"Environment '{{ENV}}' configuration loaded")

# -----------------------------
# Load config variables
# -----------------------------
TOKEN_URL = env_paths.get("TOKEN_URL")
API_URL = env_paths.get("API_URL")
USERNAME = env_paths.get("USERNAME")
PASSWORD = env_paths.get("PASSWORD")
pool = env_paths.get("pool")

print("TOKEN_URL:", TOKEN_URL)
print("API_URL:", API_URL)
print("USERNAME:", USERNAME)
print("pool:", pool)

# -----------------------------
# API Payload
# -----------------------------
payload_query = {payload_json}

def get_token():
    r = requests.post(
        TOKEN_URL,
        json={{"username": USERNAME, "password": PASSWORD}},
        verify=False
    )
    r.raise_for_status()
    return r.json()["token"]

def submit_query(**context):
    token = get_token()
    headers = {{
        "Authorization": f"Token {{token}}",
        "Content-Type": "application/json"
    }}
    r = requests.post(API_URL, json=payload_query, headers=headers, verify=False)
    r.raise_for_status()

    # Push the API response to XCom so the Kafka task can read it
    response_data = r.json() if r.headers.get("Content-Type", "").startswith("application/json") else {{"status": r.status_code, "body": r.text}}
    context["ti"].xcom_push(key="api_response", value=response_data)
    print(f"API response pushed to XCom: {{response_data}}")


def push_to_kafka(**context):
    """Send the payload and API response to Kafka after successful submission."""
    from kafka_producer import MSKProducer

    # Pull API response from the previous task
    ti = context["ti"]
    api_response = ti.xcom_pull(task_ids="submit_query", key="api_response")

    kafka_message = {{
        "dag_id": "{dag_id}",
        "task": "submit_query",
        "status": "SUCCESS",
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload_query,
        "api_response": api_response,
    }}

    producer = MSKProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        username=KAFKA_SASL_USERNAME,
        password=KAFKA_SASL_PASSWORD,
    )

    try:
        success = producer.send(kafka_message, key="{dag_id}")
        if not success:
            raise Exception("Kafka delivery failed after retries")
        print(f"Kafka message delivered to {{KAFKA_TOPIC}}")
    finally:
        producer.close()


default_args = {{
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}}

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2025, 1, 1),
    schedule_interval="{schedule}",
    catchup=False,
    default_args=default_args,
    tags={DAG_TAGS}
) as dag:

    submit_query_task = PythonOperator(
        task_id="submit_query",
        pool=pool,
        python_callable=submit_query,
        provide_context=True,
    )

    push_to_kafka_task = PythonOperator(
        task_id="push_to_kafka",
        pool=pool,
        python_callable=push_to_kafka,
        provide_context=True,
    )

    submit_query_task >> push_to_kafka_task

'''


# ============================================================
# STORAGE
# ============================================================

def upload_to_s3(code: str, filename: str):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{S3_PREFIX}{filename}",
        Body=code.encode("utf-8"),
        ContentType="text/x-python"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()

    # EXECUTION MODE
    if args.execute:
        trigger_upi_smt_t1(args)
        return

    # DAG GENERATION MODE
    dag_id = f"Upi_Auto_Email_{args.request_number}"
    dag_file = f"{dag_id}.py"

    payload = build_payload(args)

    kafka_config = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "username": KAFKA_SASL_USERNAME,
        "password": KAFKA_SASL_PASSWORD,
    }

    dag_code = generate_dag_code(dag_id, args.cron, payload, kafka_config)

    os.makedirs(EFS_DAG_PATH, exist_ok=True)
    with open(os.path.join(EFS_DAG_PATH, dag_file), "w") as f:
        f.write(dag_code)

    upload_to_s3(dag_code, dag_file)


if __name__ == "__main__":
    main()
