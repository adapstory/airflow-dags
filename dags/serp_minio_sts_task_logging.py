"""Airflow logging configuration using projected MinIO web identity."""

from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

from dags.serp_minio_sts_task_log_io import MinioStsTaskLogIO

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
REMOTE_TASK_LOG = MinioStsTaskLogIO()
