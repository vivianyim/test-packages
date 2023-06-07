import json
from copy import deepcopy
from datetime import timedelta

import yaml
from airflow.kubernetes.secret import Secret
from airflow.models import Variable
from airflow.stats import Stats
from kubernetes.client import models


class Abstract:
    def __new__(cls):
        raise TypeError(f"{cls.__name__} cannot be instantiated.")


class Environment(Abstract):
    local = "local"
    dev = "dev"
    stage = "stage"
    prod = "prod"


def get_database(name, ENV):
    if ENV in {Environment.local, Environment.dev}:
        name = "dev_" + name
    elif ENV == Environment.stage:
        name = "stage_" + name
    return name.upper()


def get_hyak_role_name(ENV):
    if ENV in {Environment.local, Environment.dev}:
        ROLE_NAME = "hyak-marmalade-import-dev"
    elif ENV == Environment.stage:
        ROLE_NAME = "hyak-marmalade-import-staging"
    elif ENV == Environment.prod:
        ROLE_NAME = "hyak-marmalade-import-prod"
    return ROLE_NAME


def get_hyak_campaigns_role_name(ENV):
    if ENV in {Environment.local, Environment.dev}:
        ROLE_NAME = "hyak-campaigns-dev"
    elif ENV == Environment.stage:
        ROLE_NAME = "hyak-campaigns-staging"
    elif ENV == Environment.prod:
        ROLE_NAME = "hyak-campaigns-prod"
    return ROLE_NAME


def get_predictive_analysis(ENV):
    if ENV in {Environment.local, Environment.dev}:
        ROLE_NAME = "hyak-predictive-analysis-dev"
    elif ENV == Environment.stage:
        ROLE_NAME = "hyak-predictive-analysis-dev"
    elif ENV == Environment.prod:
        ROLE_NAME = "hyak-predictive-analysis-prod"
    return ROLE_NAME


def get_dynamodb_hyak_role_name(env):
    if env in {Environment.local, Environment.dev}:
        ROLE_NAME = "dps-dynamo-read-access-development"
    elif env == Environment.stage:
        ROLE_NAME = "dps-dynamo-read-access-stage"
    elif env == Environment.prod:
        ROLE_NAME = "dps-dynamo-read-access-production"
    return ROLE_NAME


def kubernetes_pod_operator_init(kwargs, task_id):
    kwargs.setdefault("name", task_id)

    if isinstance(kwargs.get("execution_timeout"), int):
        kwargs["execution_timeout"] = timedelta(minutes=kwargs["execution_timeout"])

    kwargs["secrets"] = []
    for deploy_target, _kwargs in kwargs.pop("env_secrets", {}).items():
        kwargs["secrets"].append(Secret("env", deploy_target, **_kwargs))
    for deploy_target, _kwargs in kwargs.pop("vol_secrets", {}).items():
        kwargs["secrets"].append(Secret("volume", deploy_target, **_kwargs))

    kwargs["volumes"] = [
        models.V1Volume(
            name=_kwargs["name"],
            host_path=models.V1HostPathVolumeSource(path=_kwargs["path"]),
        )
        for _kwargs in kwargs.get("volumes", [])
    ]

    kwargs["volume_mounts"] = [models.V1VolumeMount(**volume_mount) for volume_mount in kwargs.get("volume_mounts", [])]

    if "container_resources" in kwargs:
        kwargs["container_resources"] = models.V1ResourceRequirements(**kwargs["container_resources"])

    return kwargs


def extractor_arguments_init(task, start, end, options):
    extractor_arguments = [
        task,
        *("-s", start),
        *("-e", end),
        *(
            "-d",
            json.dumps(
                {
                    "dag": "{{ dag }}",
                    "dag_run": "{{ dag_run.dag_id }}-{{ dag_run.run_id }}",
                    "task_instance": "{{ task_instance_key_str }}",
                    "mapIndex": "{{ ti.map_index }}",
                }
            ),
        ),
        *("-j", json.dumps(options or {})),
        *("-l", "debug"),
    ]
    return extractor_arguments


def export_arguments_init(task, start, end, options):
    export_arguments = [
        task,
        *("-s", start),
        *("-e", end),
        *(
            "-d",
            json.dumps(
                {
                    "dag": "{{ dag }}",
                    "dag_run": "{{ dag_run.dag_id }}-{{ dag_run.run_id }}",
                    "task_instance": "{{ task_instance_key_str }}",
                }
            ),
        ),
        *("-j", json.dumps(options or {})),
        *("-l", "debug"),
    ]
    return export_arguments


def snowflake_hook_params_init(warehouse, database, role, schema, authenticator, session_parameters, hook_params):
    # refer to
    # https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_modules/airflow/providers/snowflake/operators/snowflake.html#SnowflakeOperator
    if any([warehouse, database, role, schema, authenticator, session_parameters]):
        hook_params = {
            "warehouse": warehouse,
            "database": database,
            "role": role,
            "schema": schema,
            "authenticator": authenticator,
            "session_parameters": session_parameters,
            **hook_params,
        }

    return hook_params


def update_recursive(a: dict, b: dict | None):
    """
    Given two dictionaries a and b, this function is similar
    to a.update(b) except that if a[k] and b[k] are dictionaries,
    then a[k].update(b[k]) is done first recursively.
    Args:
        a (dict): the dictionary that b mutates
        b (dict): the dictionary that mutates a
    """
    b = b or {}

    if not isinstance(a, dict) or not isinstance(b, dict):
        raise TypeError(f"Either {a} or {b} is not a dict.")

    for k in b:
        if isinstance(a.get(k), dict) and isinstance(b[k], dict):
            update_recursive(a[k], b[k])
        else:
            a[k] = b[k]


class load_kwargs:
    def __init__(self, file_path, ENV):
        with open(file_path) as file:
            config = yaml.safe_load(file)

        self.kwargs = config["default"]

        update_recursive(self.kwargs, config[ENV])

    def __call__(self, cls):
        new_kwargs = self.kwargs

        class wrapper(cls):
            def __init__(self, **kwargs):
                new_kwargs_copy = deepcopy(new_kwargs)

                update_recursive(new_kwargs_copy, kwargs)

                super().__init__(**new_kwargs_copy)

        return wrapper


def create_sla_miss_statsd_metric(*args):
    """
    The positional arguments are: dag, task_list, blocking_task_list, slas, blocking_tis
    """
    slas = args[3]
    sla = slas[0]
    dag_id = sla.dag_id
    Stats.incr(f"dagrun.{dag_id}.sla_miss")


ENV = Variable.get("ENV", default_var=Environment.local)
