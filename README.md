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

This package provides operators, sensors, and hooks that enables running [Debussy Concert](https://github.com/DotzInc/debussy_concert) on Apache Airflow. However, it's not a traditional Airflow provider package in the sense Debussy Concert is not a tool, but rather a framework for data pipeline development and code generation. Therefore, our hooks, operators and sensors acts as interfaces, extending other Airflow providers, and abstracting their usage.

## Installation

You can install this package via

```
pip install debussy-airflow
```

### Prerequisites

An environment running `apache-airflow` 2.2.0+ with the following requirements:

```yaml
- apache-airflow-providers-google == 8.0.0
- facebook-business == 13.0.0
- mysql-connector-python == 8.0.24
- paramiko == 2.8.1
```

## Modules

### Hooks
- [db_api_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/db_api_hook.py)
- [facebook_ads_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/facebook_ads_hook.py)
- [http_api_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/http_api_hook.py)
- [http_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/http_hook.py)
- [storage_hook](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/hooks/storage_hook.py)

### Operators
- [basic_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/basic_operator.py)
- [db_to_storage_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/db_to_storage_operator.py)
- [facebook_ads_to_storage_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/facebook_ads_to_storage_operator.py)
- [http_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/http_operator.py)
- [rest_api_to_storage_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/rest_api_to_storage_operator.py)
- [storage_to_rdbms_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/storage_to_rdbms_operator.py)
- [storage_to_storage_operator](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/operators/storage_to_storage_operator.py)

### Sensors
- [http_sensor](https://github.com/DotzInc/debussy_airflow/blob/main/debussy_airflow/sensors/http_sensor.py)

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
