<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
    <img alt="Debussy" src="https://github.com/DotzInc/debussy_concert/raw/master/docs/images/debussy_logo.png" width="60" />
  </a>
</p>
<h1 align="center">
  Debussy Airflow
</h1>
  <h2 align="center">
  A set of Airflow Hooks, Operators and Sensors for Debussy Concert,
  <p>the core component of Debussy Framework
</h3>
<br/>

## ⚠️Not exactly a provider

This package provides operators, sensors, and hooks that enables running [Debussy Concert](https://github.com/DotzInc/debussy_concert) on Apache Airflow. However, it's not a traditional provider package in the sense Debussy Concert is not a tool, but rather a framework for data pipeline development and code generation.

```yaml
Current Release: 0.1.0
```

## Installation
### Prerequisites

An environment running `apache-airflow` with the following requirements:

```yaml
- Python Version >= 3.7
- Airflow Version >= 2.0.0
- apache-airflow-providers-google == 8.0.0
- facebook-business == 13.0.0
- mysql-connector-python == 8.0.24
- paramiko == 2.8.1
```

## Modules

### Hooks
- [db_api](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/db_api.py)
- [facebook_ads](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/facebook_ads.py)
- [http_api](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/http_api.py)
- [http](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/http.py)
- [storage_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/storage_hook.py)

### Operators
- [basic](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/basic.py)
- [db_to_storage](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/db_to_storage.py)
- [facebook_ads_to_storage](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/facebook_ads_to_storage.py)
- [http](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/http.py)
- [rest_api_to_storage](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/rest_api_to_storage.py)
- [storage_to_rdbms](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/storage_to_rdbms.py)
- [storage_to_storage](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/storage_to_storage.py)

### Sensors
- [http](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/sensors/http.py)

## Examples

See the [**examples**](https://github.com/DotzInc/debussy_airflow/tree/main/tests/example_dags) directory for an example DAG.

## Issues

Please submit [issues](https://github.com/DotzInc/debussy_airflow/issues) and [pull requests](https://github.com/DotzInc/debussy_airflow/pulls) in our official repo:
[https://github.com/DotzInc/debussy_airflow](https://github.com/DotzInc/debussy_airflow)

We are happy to hear from you. Please email any feedback to the authors at [dataengineer@dotz.com](mailto:dataengineer@dotz.com).

## Project Contributors and Maintainers

This project is built with active contributions from:

- [Lawrence Fernandes](https://github.com/lawrencestfs)
- [Nilton Duarte](https://github.com/NiltonDuarte)
- [Maycon Francis](https://github.com/mayconcad)

This project is formatted via `black`:

```bash
pip install black
black .
```
