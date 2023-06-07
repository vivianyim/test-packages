from setuptools import setup

setup(
    name="airflow_utils",
    version="0.0.2",
    author="DPS Platform",
    packages=["airflow_utils"],
    install_requires=[
        "apache-airflow",
        "apache-airflow-providers-airbyte==3.2.0",
        "apache-airflow-providers-celery==3.1.0",
        "apache-airflow-providers-cncf-kubernetes==5.0.0",
        "apache-airflow-providers-datadog==3.1.0",
        "apache-airflow-providers-github==2.2.0",
        "apache-airflow-providers-postgres==5.3.1",
        "apache-airflow-providers-slack==7.1.0",
        "apache-airflow-providers-snowflake==4.0.1",
        "apache-airflow-providers-ssh==3.3.0",
        "apache-beam==2.43.0",
    ],
)
